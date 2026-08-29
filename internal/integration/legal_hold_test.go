//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/legalhold"
)

const (
	compliancePrincipalID       = "00000000-0000-0000-0000-000000003301"
	complianceTLSURI            = "spiffe://compliance.internal/legal-holds/primary"
	secondCompliancePrincipalID = "00000000-0000-0000-0000-000000003302"
)

func TestLegalHoldPlaceReleaseAndExactReplay(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "legal-hold-place-release", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"must remain outside non-content Legal Hold authority"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit Legal Hold target = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var job struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Legal Hold target Job: %v", err)
	}
	jobID := uuid.MustParse(job.JobID)
	seedCompliancePrincipal(t, database)
	pool := newRolePool(
		t,
		database.DSN,
		"vela_compliance_login",
		"vela-compliance-password",
	)
	service, err := legalhold.NewService(context.Background(), pool)
	if err != nil {
		t.Fatalf("create Legal Hold service: %v", err)
	}
	projectID := uuid.MustParse(testProjectID)
	effectiveAt := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	request := legalhold.Request{
		IdempotencyKey: "legal-hold-place-3301",
		SourceSequence: 1,
		HoldID:         uuid.MustParse("00000000-0000-0000-0000-000000003310"),
		Kind:           legalhold.KindHoldPlaced,
		Scope:          legalhold.ScopeJob,
		OrganizationID: uuid.MustParse(testOrganizationID),
		ProjectID:      &projectID,
		JobID:          &jobID,
		RecordClasses: []legalhold.RecordClass{
			legalhold.RecordClassFinancial,
			legalhold.RecordClassMetadata,
		},
		ReasonCode:        "LITIGATION",
		ExternalReference: "matter-3301/place",
		EffectiveAt:       effectiveAt,
	}
	placed, err := service.Apply(context.Background(), request)
	if err != nil {
		t.Fatalf("place Legal Hold: %v", err)
	}
	if placed.Replayed || placed.EventID == uuid.Nil || placed.HoldID != request.HoldID ||
		placed.State != legalhold.StateActive || placed.Scope != legalhold.ScopeJob ||
		len(placed.RecordClasses) != 2 ||
		placed.RecordClasses[0] != legalhold.RecordClassMetadata ||
		placed.RecordClasses[1] != legalhold.RecordClassFinancial || placed.RecordedAt.IsZero() {
		t.Fatalf("placed Legal Hold = %#v", placed)
	}
	replayed, err := service.Apply(context.Background(), request)
	if err != nil {
		t.Fatalf("replay Legal Hold placement: %v", err)
	}
	if !replayed.Replayed || replayed.EventID != placed.EventID ||
		!replayed.RecordedAt.Equal(placed.RecordedAt) {
		t.Fatalf("replayed Legal Hold = %#v, want %#v", replayed, placed)
	}
	assertActiveLegalHoldCount(t, database, jobID, "METADATA", 1)
	assertActiveLegalHoldCount(t, database, jobID, "FINANCIAL", 1)

	release := legalhold.Request{
		IdempotencyKey:    "legal-hold-release-3301",
		SourceSequence:    2,
		HoldID:            request.HoldID,
		Kind:              legalhold.KindHoldReleased,
		ReasonCode:        "ORDER_LIFTED",
		ExternalReference: "matter-3301/release",
		EffectiveAt:       effectiveAt.Add(time.Hour),
	}
	released, err := service.Apply(context.Background(), release)
	if err != nil {
		t.Fatalf("release Legal Hold: %v", err)
	}
	if released.Replayed || released.State != legalhold.StateReleased ||
		released.Scope != legalhold.ScopeJob || released.ReleasedAt == nil ||
		len(released.RecordClasses) != 2 {
		t.Fatalf("released Legal Hold = %#v", released)
	}
	replayedRelease, err := service.Apply(context.Background(), release)
	if err != nil {
		t.Fatalf("replay Legal Hold release: %v", err)
	}
	if !replayedRelease.Replayed || replayedRelease.EventID != released.EventID ||
		replayedRelease.ReleasedAt == nil ||
		!replayedRelease.ReleasedAt.Equal(*released.ReleasedAt) {
		t.Fatalf("replayed Legal Hold release = %#v, want %#v", replayedRelease, released)
	}
	assertActiveLegalHoldCount(t, database, jobID, "METADATA", 0)
	assertActiveLegalHoldCount(t, database, jobID, "FINANCIAL", 0)

	var prompt string
	if err := database.Admin.QueryRow(`SELECT request_content->>'prompt' FROM jobs WHERE id = $1`, jobID).Scan(&prompt); err != nil {
		t.Fatalf("read Legal Hold target Customer Content: %v", err)
	}
	if prompt != "must remain outside non-content Legal Hold authority" {
		t.Fatalf("Legal Hold changed Customer Content: %q", prompt)
	}
}

func TestLegalHoldRejectsInvalidShapesBeforeDurableChange(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedCompliancePrincipal(t, database)
	pool := newRolePool(t, database.DSN, "vela_compliance_login", "vela-compliance-password")
	service, err := legalhold.NewService(context.Background(), pool)
	if err != nil {
		t.Fatalf("create Legal Hold service: %v", err)
	}
	projectID := uuid.MustParse(testProjectID)
	jobID := uuid.New()
	base := legalhold.Request{
		IdempotencyKey:    "legal-hold-invalid",
		SourceSequence:    1,
		HoldID:            uuid.New(),
		Kind:              legalhold.KindHoldPlaced,
		Scope:             legalhold.ScopeProject,
		OrganizationID:    uuid.MustParse(testOrganizationID),
		ProjectID:         &projectID,
		RecordClasses:     []legalhold.RecordClass{legalhold.RecordClassMetadata},
		ReasonCode:        "LITIGATION",
		ExternalReference: "matter-invalid/place",
		EffectiveAt:       time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
	}
	tests := []struct {
		name string
		edit func(*legalhold.Request)
	}{
		{name: "unknown kind", edit: func(request *legalhold.Request) { request.Kind = "UNKNOWN" }},
		{name: "missing class", edit: func(request *legalhold.Request) { request.RecordClasses = nil }},
		{name: "duplicate class", edit: func(request *legalhold.Request) {
			request.RecordClasses = append(request.RecordClasses, legalhold.RecordClassMetadata)
		}},
		{name: "Customer Content class", edit: func(request *legalhold.Request) { request.RecordClasses = []legalhold.RecordClass{"CONTENT"} }},
		{name: "Project without Project", edit: func(request *legalhold.Request) { request.ProjectID = nil }},
		{name: "Project with Job", edit: func(request *legalhold.Request) { request.JobID = &jobID }},
		{name: "release with target", edit: func(request *legalhold.Request) {
			request.Kind = legalhold.KindHoldReleased
			request.RecordClasses = nil
		}},
		{name: "invalid reason", edit: func(request *legalhold.Request) { request.ReasonCode = "free form" }},
		{name: "sub-microsecond timestamp", edit: func(request *legalhold.Request) { request.EffectiveAt = request.EffectiveAt.Add(time.Nanosecond) }},
		{name: "zero sequence", edit: func(request *legalhold.Request) { request.SourceSequence = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := base
			request.RecordClasses = append([]legalhold.RecordClass(nil), base.RecordClasses...)
			test.edit(&request)
			_, err := service.Apply(context.Background(), request)
			assertLegalHoldFailure(t, err, legalhold.FailureInvalid)
		})
	}
	var holds, events, cursors int64
	if err := database.Admin.QueryRow(`
		SELECT (SELECT count(*) FROM legal_holds),
			(SELECT count(*) FROM legal_hold_events),
			(SELECT count(*) FROM compliance_event_cursors)
	`).Scan(&holds, &events, &cursors); err != nil {
		t.Fatalf("read invalid Legal Hold effects: %v", err)
	}
	if holds != 0 || events != 0 || cursors != 0 {
		t.Fatalf("invalid Legal Hold effects = holds/events/cursors %d/%d/%d", holds, events, cursors)
	}
}

func TestLegalHoldConflictsDoNotAdvanceEventAuthority(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "legal-hold-conflicts", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"Legal Hold conflict fixture"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit Legal Hold conflict target = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var job struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Legal Hold conflict Job: %v", err)
	}
	jobID := uuid.MustParse(job.JobID)
	projectID := uuid.MustParse(testProjectID)
	seedCompliancePrincipal(t, database)
	pool := newRolePool(t, database.DSN, "vela_compliance_login", "vela-compliance-password")
	service, err := legalhold.NewService(context.Background(), pool)
	if err != nil {
		t.Fatalf("create Legal Hold service: %v", err)
	}
	effectiveAt := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	placedRequest := legalhold.Request{
		IdempotencyKey: "legal-hold-conflict-place", SourceSequence: 1,
		HoldID: uuid.MustParse("00000000-0000-0000-0000-000000003320"),
		Kind:   legalhold.KindHoldPlaced, Scope: legalhold.ScopeJob,
		OrganizationID: uuid.MustParse(testOrganizationID), ProjectID: &projectID, JobID: &jobID,
		RecordClasses: []legalhold.RecordClass{legalhold.RecordClassMetadata},
		ReasonCode:    "LITIGATION", ExternalReference: "matter-conflict/place",
		EffectiveAt: effectiveAt,
	}
	if _, err := service.Apply(context.Background(), placedRequest); err != nil {
		t.Fatalf("place conflict fixture Legal Hold: %v", err)
	}

	idempotencyConflict := placedRequest
	idempotencyConflict.ReasonCode = "REGULATORY"
	_, err = service.Apply(context.Background(), idempotencyConflict)
	assertLegalHoldFailure(t, err, legalhold.FailureConflict)

	sequenceGap := placedRequest
	sequenceGap.IdempotencyKey = "legal-hold-conflict-gap"
	sequenceGap.SourceSequence = 3
	sequenceGap.HoldID = uuid.New()
	sequenceGap.ExternalReference = "matter-conflict/gap"
	_, err = service.Apply(context.Background(), sequenceGap)
	assertLegalHoldFailure(t, err, legalhold.FailureConflict)

	referenceConflict := placedRequest
	referenceConflict.IdempotencyKey = "legal-hold-conflict-reference"
	referenceConflict.SourceSequence = 2
	referenceConflict.HoldID = uuid.New()
	_, err = service.Apply(context.Background(), referenceConflict)
	assertLegalHoldFailure(t, err, legalhold.FailureConflict)

	holdIDConflict := placedRequest
	holdIDConflict.IdempotencyKey = "legal-hold-conflict-hold-id"
	holdIDConflict.SourceSequence = 2
	holdIDConflict.ExternalReference = "matter-conflict/other-place"
	_, err = service.Apply(context.Background(), holdIDConflict)
	assertLegalHoldFailure(t, err, legalhold.FailureConflict)

	release := legalhold.Request{
		IdempotencyKey: "legal-hold-conflict-release", SourceSequence: 2,
		HoldID: placedRequest.HoldID, Kind: legalhold.KindHoldReleased,
		ReasonCode: "ORDER_LIFTED", ExternalReference: "matter-conflict/release",
		EffectiveAt: effectiveAt.Add(-time.Second),
	}
	_, err = service.Apply(context.Background(), release)
	assertLegalHoldFailure(t, err, legalhold.FailureConflict)
	release.EffectiveAt = effectiveAt.Add(time.Second)
	if _, err := service.Apply(context.Background(), release); err != nil {
		t.Fatalf("release Legal Hold after rejected conflicts: %v", err)
	}

	secondRelease := release
	secondRelease.IdempotencyKey = "legal-hold-conflict-second-release"
	secondRelease.SourceSequence = 3
	secondRelease.ExternalReference = "matter-conflict/second-release"
	_, err = service.Apply(context.Background(), secondRelease)
	assertLegalHoldFailure(t, err, legalhold.FailureConflict)

	var state string
	var holds, events, lastSequence int64
	if err := database.Admin.QueryRow(`
		SELECT hold.state::text,
			(SELECT count(*) FROM legal_holds),
			(SELECT count(*) FROM legal_hold_events),
			(SELECT last_sequence FROM compliance_event_cursors WHERE principal_id = $2)
		FROM legal_holds AS hold WHERE hold.id = $1
	`, placedRequest.HoldID, compliancePrincipalID).Scan(&state, &holds, &events, &lastSequence); err != nil {
		t.Fatalf("read Legal Hold conflict state: %v", err)
	}
	if state != "RELEASED" || holds != 1 || events != 2 || lastSequence != 2 {
		t.Fatalf("Legal Hold conflict state = %s holds/events/sequence %d/%d/%d", state, holds, events, lastSequence)
	}
}

func TestLegalHoldScopeAndClassMatchExactDescendantJobs(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	submit := func(key string) uuid.UUID {
		t.Helper()
		accepted := submitJob(t, server.URL, key, []byte(`{
			"model":"minimax-h3",
			"generation_preset":"balanced",
			"service_class":"standard",
			"output_spec":"video-1080p-5s-24fps",
			"generation_count":1,
			"prompt":"Legal Hold scope fixture"
		}`))
		if accepted.StatusCode != 202 {
			t.Fatalf("submit %s = %d body=%s", key, accepted.StatusCode, accepted.Body)
		}
		var job struct {
			JobID string `json:"job_id"`
		}
		if err := json.Unmarshal(accepted.Body, &job); err != nil {
			t.Fatalf("decode %s Job: %v", key, err)
		}
		return uuid.MustParse(job.JobID)
	}
	firstJobID := submit("legal-hold-scope-first")
	secondJobID := submit("legal-hold-scope-second")
	projectID := uuid.MustParse(testProjectID)
	seedCompliancePrincipal(t, database)
	pool := newRolePool(t, database.DSN, "vela_compliance_login", "vela-compliance-password")
	service, err := legalhold.NewService(context.Background(), pool)
	if err != nil {
		t.Fatalf("create Legal Hold service: %v", err)
	}
	effectiveAt := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	placements := []legalhold.Request{
		{
			IdempotencyKey: "legal-hold-scope-organization", SourceSequence: 1,
			HoldID: uuid.New(), Kind: legalhold.KindHoldPlaced,
			Scope: legalhold.ScopeOrganization, OrganizationID: uuid.MustParse(testOrganizationID),
			RecordClasses: []legalhold.RecordClass{legalhold.RecordClassMetadata},
			ReasonCode:    "REGULATORY", ExternalReference: "matter-scope/organization",
			EffectiveAt: effectiveAt,
		},
		{
			IdempotencyKey: "legal-hold-scope-project", SourceSequence: 2,
			HoldID: uuid.New(), Kind: legalhold.KindHoldPlaced,
			Scope: legalhold.ScopeProject, OrganizationID: uuid.MustParse(testOrganizationID),
			ProjectID:     &projectID,
			RecordClasses: []legalhold.RecordClass{legalhold.RecordClassFinancial},
			ReasonCode:    "TAX", ExternalReference: "matter-scope/project",
			EffectiveAt: effectiveAt,
		},
		{
			IdempotencyKey: "legal-hold-scope-job", SourceSequence: 3,
			HoldID: uuid.New(), Kind: legalhold.KindHoldPlaced,
			Scope: legalhold.ScopeJob, OrganizationID: uuid.MustParse(testOrganizationID),
			ProjectID: &projectID, JobID: &firstJobID,
			RecordClasses: []legalhold.RecordClass{
				legalhold.RecordClassMetadata, legalhold.RecordClassFinancial,
			},
			ReasonCode: "LITIGATION", ExternalReference: "matter-scope/job",
			EffectiveAt: effectiveAt,
		},
	}
	for _, placement := range placements {
		if _, err := service.Apply(context.Background(), placement); err != nil {
			t.Fatalf("place %s Legal Hold: %v", placement.Scope, err)
		}
	}
	assertActiveLegalHoldCount(t, database, firstJobID, "METADATA", 2)
	assertActiveLegalHoldCount(t, database, firstJobID, "FINANCIAL", 2)
	assertActiveLegalHoldCount(t, database, secondJobID, "METADATA", 1)
	assertActiveLegalHoldCount(t, database, secondJobID, "FINANCIAL", 1)

	var count int64
	err = database.Admin.QueryRow(`
		SELECT count(*) FROM vela_private.lock_active_non_content_legal_holds($1, $2, $3, 'METADATA')
	`, testOrganizationID, testProjectID, uuid.New()).Scan(&count)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "P0002" {
		t.Fatalf("unknown Legal Hold descendant error = %v, want P0002", err)
	}
}

func TestLegalHoldActiveLockSerializesConcurrentRelease(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "legal-hold-active-lock", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"Legal Hold active lock fixture"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit Legal Hold lock target = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var job struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Legal Hold lock Job: %v", err)
	}
	jobID := uuid.MustParse(job.JobID)
	projectID := uuid.MustParse(testProjectID)
	seedCompliancePrincipal(t, database)
	pool := newRolePool(t, database.DSN, "vela_compliance_login", "vela-compliance-password")
	service, err := legalhold.NewService(context.Background(), pool)
	if err != nil {
		t.Fatalf("create Legal Hold service: %v", err)
	}
	effectiveAt := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	holdID := uuid.MustParse("00000000-0000-0000-0000-000000003330")
	if _, err := service.Apply(context.Background(), legalhold.Request{
		IdempotencyKey: "legal-hold-lock-place", SourceSequence: 1,
		HoldID: holdID, Kind: legalhold.KindHoldPlaced, Scope: legalhold.ScopeJob,
		OrganizationID: uuid.MustParse(testOrganizationID), ProjectID: &projectID, JobID: &jobID,
		RecordClasses: []legalhold.RecordClass{legalhold.RecordClassMetadata},
		ReasonCode:    "LITIGATION", ExternalReference: "matter-lock/place",
		EffectiveAt: effectiveAt,
	}); err != nil {
		t.Fatalf("place lock fixture Legal Hold: %v", err)
	}
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Legal Hold active-lock transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	var lockedID uuid.UUID
	if err := tx.QueryRow(`
		SELECT hold_id FROM vela_private.lock_active_non_content_legal_holds($1, $2, $3, 'METADATA')
	`, testOrganizationID, testProjectID, jobID).Scan(&lockedID); err != nil {
		t.Fatalf("lock active Legal Hold: %v", err)
	}
	if lockedID != holdID {
		t.Fatalf("locked Legal Hold = %s, want %s", lockedID, holdID)
	}

	releaseDone := make(chan error, 1)
	var started sync.WaitGroup
	started.Add(1)
	go func() {
		started.Done()
		_, releaseErr := service.Apply(context.Background(), legalhold.Request{
			IdempotencyKey: "legal-hold-lock-release", SourceSequence: 2,
			HoldID: holdID, Kind: legalhold.KindHoldReleased,
			ReasonCode: "ORDER_LIFTED", ExternalReference: "matter-lock/release",
			EffectiveAt: effectiveAt.Add(time.Hour),
		})
		releaseDone <- releaseErr
	}()
	started.Wait()
	select {
	case releaseErr := <-releaseDone:
		t.Fatalf("Legal Hold release bypassed active-lock transaction: %v", releaseErr)
	case <-time.After(200 * time.Millisecond):
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit Legal Hold active-lock transaction: %v", err)
	}
	select {
	case releaseErr := <-releaseDone:
		if releaseErr != nil {
			t.Fatalf("release Legal Hold after active-lock commit: %v", releaseErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Legal Hold release remained blocked after active-lock commit")
	}
}

func TestLegalHoldIdentityAndLeastPrivilegeFailClosed(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	pool := newRolePool(t, database.DSN, "vela_compliance_login", "vela-compliance-password")
	if _, err := legalhold.NewService(context.Background(), pool); err == nil {
		t.Fatal("unbound Compliance login resolved a Principal")
	}
	seedCompliancePrincipal(t, database)
	service, err := legalhold.NewService(context.Background(), pool)
	if err != nil {
		t.Fatalf("create bound Legal Hold service: %v", err)
	}
	var directCount int64
	err = pool.QueryRow(context.Background(), `SELECT count(*) FROM legal_holds`).Scan(&directCount)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
		t.Fatalf("Compliance direct Legal Hold table read error = %v, want 42501", err)
	}

	const applySignature = "vela_apply_legal_hold_event(uuid,text,bigint,uuid,legal_hold_event_kind,legal_hold_scope,uuid,uuid,uuid,legal_hold_record_class[],text,text,timestamp with time zone)"
	for _, test := range []struct {
		role string
		want bool
	}{
		{role: "vela_compliance_login", want: true},
		{role: "vela_finance_reconciliation_login"},
		{role: "vela_retention_login"},
		{role: "vela_retention_request_login"},
		{role: "vela_request_login"},
	} {
		var allowed bool
		if err := database.Admin.QueryRow(
			`SELECT has_function_privilege($1, $2, 'EXECUTE')`, test.role, applySignature,
		).Scan(&allowed); err != nil {
			t.Fatalf("inspect %s Legal Hold authority: %v", test.role, err)
		}
		if allowed != test.want {
			t.Fatalf("%s Legal Hold authority = %t, want %t", test.role, allowed, test.want)
		}
	}

	if _, err := database.Admin.Exec(`
		UPDATE compliance_principals
		SET status = 'DISABLED', disabled_at = clock_timestamp()
		WHERE id = $1
	`, compliancePrincipalID); err != nil {
		t.Fatalf("disable Compliance Principal: %v", err)
	}
	_, err = service.Apply(context.Background(), legalhold.Request{
		IdempotencyKey: "legal-hold-disabled", SourceSequence: 1,
		HoldID: uuid.New(), Kind: legalhold.KindHoldPlaced,
		Scope: legalhold.ScopeOrganization, OrganizationID: uuid.MustParse(testOrganizationID),
		RecordClasses: []legalhold.RecordClass{legalhold.RecordClassMetadata},
		ReasonCode:    "REGULATORY", ExternalReference: "matter-disabled/place",
		EffectiveAt: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
	})
	assertLegalHoldFailure(t, err, legalhold.FailureUnauthorized)
	_, err = database.Admin.Exec(`
		UPDATE compliance_principals
		SET status = 'ACTIVE', disabled_at = NULL
		WHERE id = $1
	`, compliancePrincipalID)
	assertPostgresConstraint(t, err, "compliance_principal_disablement_is_permanent")
}

func TestLegalHoldEvidenceRejectsDirectMutation(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedCompliancePrincipal(t, database)
	pool := newRolePool(t, database.DSN, "vela_compliance_login", "vela-compliance-password")
	service, err := legalhold.NewService(context.Background(), pool)
	if err != nil {
		t.Fatalf("create Legal Hold service: %v", err)
	}
	holdID := uuid.MustParse("00000000-0000-0000-0000-000000003340")
	placed, err := service.Apply(context.Background(), legalhold.Request{
		IdempotencyKey: "legal-hold-immutable-place", SourceSequence: 1,
		HoldID: holdID, Kind: legalhold.KindHoldPlaced,
		Scope: legalhold.ScopeOrganization, OrganizationID: uuid.MustParse(testOrganizationID),
		RecordClasses: []legalhold.RecordClass{legalhold.RecordClassMetadata},
		ReasonCode:    "REGULATORY", ExternalReference: "matter-immutable/place",
		EffectiveAt: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("place immutable Legal Hold fixture: %v", err)
	}
	for _, test := range []struct {
		name       string
		statement  string
		identifier uuid.UUID
		constraint string
	}{
		{
			name: "rewrite hold", statement: `UPDATE legal_holds
				SET placement_reason_code = 'OTHER' WHERE id = $1`, identifier: holdID,
			constraint: "legal_hold_transition_is_immutable",
		},
		{
			name: "delete hold", statement: `DELETE FROM legal_holds WHERE id = $1`,
			identifier: holdID, constraint: "legal_hold_evidence_is_immutable",
		},
		{
			name: "rewrite event", statement: `UPDATE legal_hold_events
				SET idempotency_key = idempotency_key WHERE id = $1`, identifier: placed.EventID,
			constraint: "legal_hold_event_is_immutable",
		},
		{
			name: "delete event", statement: `DELETE FROM legal_hold_events WHERE id = $1`,
			identifier: placed.EventID, constraint: "legal_hold_event_is_immutable",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := database.Admin.Exec(test.statement, test.identifier)
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
				postgresError.ConstraintName != test.constraint {
				t.Fatalf("direct Legal Hold mutation error = %v, want 55000/%s", err, test.constraint)
			}
		})
	}
}

func TestLegalHoldConcurrentCompliancePrincipalsKeepIndependentSequences(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedCompliancePrincipal(t, database)
	if _, err := database.Admin.Exec(`
		CREATE ROLE vela_compliance_login_two LOGIN PASSWORD 'vela-compliance-password-two'
			IN ROLE vela_compliance
	`); err != nil {
		t.Fatalf("create second Compliance login: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO compliance_principals (id, stable_id, tls_uri_identity)
		VALUES ($1, 'secondary-compliance', 'spiffe://compliance.internal/legal-holds/secondary')
	`, secondCompliancePrincipalID); err != nil {
		t.Fatalf("provision second Compliance Principal: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO compliance_database_bindings (database_role, principal_id)
		VALUES ('vela_compliance_login_two', $1)
	`, secondCompliancePrincipalID); err != nil {
		t.Fatalf("provision second Compliance Principal: %v", err)
	}
	firstPool := newRolePool(t, database.DSN, "vela_compliance_login", "vela-compliance-password")
	secondPool := newRolePool(t, database.DSN, "vela_compliance_login_two", "vela-compliance-password-two")
	first, err := legalhold.NewService(context.Background(), firstPool)
	if err != nil {
		t.Fatalf("create first Compliance service: %v", err)
	}
	second, err := legalhold.NewService(context.Background(), secondPool)
	if err != nil {
		t.Fatalf("create second Compliance service: %v", err)
	}
	services := []*legalhold.Service{first, second}
	requests := []legalhold.Request{
		{
			IdempotencyKey: "legal-hold-concurrent-primary", SourceSequence: 1,
			HoldID: uuid.MustParse("00000000-0000-0000-0000-000000003350"),
			Kind:   legalhold.KindHoldPlaced, Scope: legalhold.ScopeOrganization,
			OrganizationID: uuid.MustParse(testOrganizationID),
			RecordClasses:  []legalhold.RecordClass{legalhold.RecordClassMetadata},
			ReasonCode:     "REGULATORY", ExternalReference: "matter-concurrent/primary",
			EffectiveAt: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
		},
		{
			IdempotencyKey: "legal-hold-concurrent-secondary", SourceSequence: 1,
			HoldID: uuid.MustParse("00000000-0000-0000-0000-000000003351"),
			Kind:   legalhold.KindHoldPlaced, Scope: legalhold.ScopeOrganization,
			OrganizationID: uuid.MustParse(testOrganizationID),
			RecordClasses:  []legalhold.RecordClass{legalhold.RecordClassFinancial},
			ReasonCode:     "TAX", ExternalReference: "matter-concurrent/secondary",
			EffectiveAt: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
		},
	}
	start := make(chan struct{})
	errorsByPrincipal := make(chan error, len(services))
	var done sync.WaitGroup
	for index := range services {
		done.Add(1)
		go func() {
			defer done.Done()
			<-start
			_, applyErr := services[index].Apply(context.Background(), requests[index])
			errorsByPrincipal <- applyErr
		}()
	}
	close(start)
	done.Wait()
	close(errorsByPrincipal)
	for applyErr := range errorsByPrincipal {
		if applyErr != nil {
			t.Fatalf("concurrent Compliance Apply: %v", applyErr)
		}
	}
	var cursors, cursorsAtOne, events, eventPrincipals int64
	if err := database.Admin.QueryRow(`
		SELECT count(*), count(*) FILTER (WHERE last_sequence = 1)
		FROM compliance_event_cursors
	`).Scan(&cursors, &cursorsAtOne); err != nil {
		t.Fatalf("read concurrent Compliance cursors: %v", err)
	}
	if err := database.Admin.QueryRow(`
		SELECT count(*), count(DISTINCT principal_id) FROM legal_hold_events
	`).Scan(&events, &eventPrincipals); err != nil {
		t.Fatalf("read concurrent Compliance events: %v", err)
	}
	if cursors != 2 || cursorsAtOne != 2 || events != 2 || eventPrincipals != 2 {
		t.Fatalf(
			"concurrent Compliance evidence = cursors %d at-one %d events %d principals %d",
			cursors,
			cursorsAtOne,
			events,
			eventPrincipals,
		)
	}
}

func TestLegalHoldConcurrentPlaceReleasePreservesOrderedPrincipalStream(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedCompliancePrincipal(t, database)
	pool := newRolePool(t, database.DSN, "vela_compliance_login", "vela-compliance-password")
	service, err := legalhold.NewService(context.Background(), pool)
	if err != nil {
		t.Fatalf("create concurrent Legal Hold service: %v", err)
	}
	effectiveAt := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	holdID := uuid.MustParse("00000000-0000-0000-0000-000000003352")
	place := legalhold.Request{
		IdempotencyKey: "legal-hold-concurrent-place", SourceSequence: 1,
		HoldID: holdID, Kind: legalhold.KindHoldPlaced,
		Scope: legalhold.ScopeOrganization, OrganizationID: uuid.MustParse(testOrganizationID),
		RecordClasses: []legalhold.RecordClass{legalhold.RecordClassMetadata},
		ReasonCode:    "REGULATORY", ExternalReference: "matter-concurrent/place",
		EffectiveAt: effectiveAt,
	}
	release := legalhold.Request{
		IdempotencyKey: "legal-hold-concurrent-release", SourceSequence: 2,
		HoldID: holdID, Kind: legalhold.KindHoldReleased,
		ReasonCode: "ORDER_LIFTED", ExternalReference: "matter-concurrent/release",
		EffectiveAt: effectiveAt.Add(time.Hour),
	}
	type outcome struct {
		kind legalhold.Kind
		err  error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, 2)
	for _, request := range []legalhold.Request{place, release} {
		go func() {
			<-start
			_, applyErr := service.Apply(context.Background(), request)
			outcomes <- outcome{kind: request.Kind, err: applyErr}
		}()
	}
	close(start)
	var releaseNeedsRetry bool
	for range 2 {
		result := <-outcomes
		switch result.kind {
		case legalhold.KindHoldPlaced:
			if result.err != nil {
				t.Fatalf("concurrent Legal Hold placement: %v", result.err)
			}
		case legalhold.KindHoldReleased:
			if result.err != nil {
				assertLegalHoldFailure(t, result.err, legalhold.FailureConflict)
				releaseNeedsRetry = true
			}
		}
	}
	if releaseNeedsRetry {
		if _, err := service.Apply(context.Background(), release); err != nil {
			t.Fatalf("retry serialized Legal Hold release: %v", err)
		}
	}

	rows, err := database.Admin.Query(`
		SELECT source_sequence, kind::text
		FROM legal_hold_events
		WHERE principal_id = $1
		ORDER BY source_sequence
	`, compliancePrincipalID)
	if err != nil {
		t.Fatalf("read concurrent Legal Hold event stream: %v", err)
	}
	defer func() { _ = rows.Close() }()
	wantKinds := []string{"HOLD_PLACED", "HOLD_RELEASED"}
	index := 0
	for rows.Next() {
		var sequence int64
		var kind string
		if err := rows.Scan(&sequence, &kind); err != nil {
			t.Fatalf("scan concurrent Legal Hold event stream: %v", err)
		}
		if index >= len(wantKinds) || sequence != int64(index+1) || kind != wantKinds[index] {
			t.Fatalf("concurrent Legal Hold event %d = sequence %d kind %s", index, sequence, kind)
		}
		index++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate concurrent Legal Hold event stream: %v", err)
	}
	var state string
	var lastSequence int64
	if err := database.Admin.QueryRow(`
		SELECT hold.state::text, cursor.last_sequence
		FROM legal_holds AS hold
		JOIN compliance_event_cursors AS cursor ON cursor.principal_id = $2
		WHERE hold.id = $1
	`, holdID, compliancePrincipalID).Scan(&state, &lastSequence); err != nil {
		t.Fatalf("read concurrent Legal Hold final state: %v", err)
	}
	if index != 2 || state != "RELEASED" || lastSequence != 2 {
		t.Fatalf("concurrent Legal Hold stream = events %d state %s sequence %d", index, state, lastSequence)
	}
}

func TestLegalHoldMigrationEmptyDownUpAndDurableEvidenceRefusal(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty Down Up", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		if err := goose.DownTo(database.Admin, migrations, 29); err != nil {
			t.Fatalf("migrate empty Legal Hold schema down: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 29 {
			t.Fatalf("Legal Hold version after Down = %d error=%v", version, err)
		}
		for _, table := range []string{
			"compliance_principals",
			"compliance_database_bindings",
			"compliance_event_cursors",
			"legal_holds",
			"legal_hold_events",
		} {
			assertTableDoesNotExist(t, database.Admin, table)
		}
		if err := goose.UpTo(database.Admin, migrations, 30); err != nil {
			t.Fatalf("migrate Legal Hold schema up: %v", err)
		}
		version, err = goose.GetDBVersion(database.Admin)
		if err != nil || version != 30 {
			t.Fatalf("Legal Hold version after Down Up = %d error=%v", version, err)
		}
		seedAdmissionFixture(t, database.Admin)
		const restoredTLSURI = "https://compliance.example.test/legal-holds/primary"
		seedCompliancePrincipalWithTLSURI(t, database, restoredTLSURI)
		pool := newRolePool(t, database.DSN, "vela_compliance_login", "vela-compliance-password")
		if err := veladb.VerifyRole(context.Background(), pool, veladb.RoleCompliance); err != nil {
			t.Fatalf("verify Compliance role after Down Up: %v", err)
		}
		service, err := legalhold.NewService(context.Background(), pool)
		if err != nil {
			t.Fatalf("restore Legal Hold service after Down Up: %v", err)
		}
		if service.Identity().TLSURIIdentity != restoredTLSURI {
			t.Fatalf("restored Compliance TLS identity = %q", service.Identity().TLSURIIdentity)
		}
		if _, err := service.Apply(context.Background(), legalhold.Request{
			IdempotencyKey: "legal-hold-down-up-place", SourceSequence: 1,
			HoldID: uuid.MustParse("00000000-0000-0000-0000-000000003359"),
			Kind:   legalhold.KindHoldPlaced, Scope: legalhold.ScopeOrganization,
			OrganizationID: uuid.MustParse(testOrganizationID),
			RecordClasses:  []legalhold.RecordClass{legalhold.RecordClassFinancial},
			ReasonCode:     "TAX", ExternalReference: "matter-down-up/place",
			EffectiveAt: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
		}); err != nil {
			t.Fatalf("apply Legal Hold after Down Up: %v", err)
		}
	})

	t.Run("provisioning refuses Down", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		seedCompliancePrincipal(t, database)
		err := goose.DownTo(database.Admin, migrations, 29)
		assertPostgresConstraint(t, err, "legal_hold_contract_has_durable_evidence")
		version, versionErr := goose.GetDBVersion(database.Admin)
		if versionErr != nil || version != 30 {
			t.Fatalf("Legal Hold version after provisioning refusal = %d error=%v", version, versionErr)
		}
	})

	t.Run("events refuse Down", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		seedAdmissionFixture(t, database.Admin)
		seedCompliancePrincipal(t, database)
		pool := newRolePool(t, database.DSN, "vela_compliance_login", "vela-compliance-password")
		service, err := legalhold.NewService(context.Background(), pool)
		if err != nil {
			t.Fatalf("create Legal Hold service: %v", err)
		}
		if _, err := service.Apply(context.Background(), legalhold.Request{
			IdempotencyKey: "legal-hold-down-refusal", SourceSequence: 1,
			HoldID: uuid.MustParse("00000000-0000-0000-0000-000000003360"),
			Kind:   legalhold.KindHoldPlaced, Scope: legalhold.ScopeOrganization,
			OrganizationID: uuid.MustParse(testOrganizationID),
			RecordClasses:  []legalhold.RecordClass{legalhold.RecordClassMetadata},
			ReasonCode:     "REGULATORY", ExternalReference: "matter-down-refusal/place",
			EffectiveAt: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
		}); err != nil {
			t.Fatalf("place durable Legal Hold fixture: %v", err)
		}
		err = goose.DownTo(database.Admin, migrations, 29)
		assertPostgresConstraint(t, err, "legal_hold_contract_has_durable_evidence")
		version, versionErr := goose.GetDBVersion(database.Admin)
		if versionErr != nil || version != 30 {
			t.Fatalf("Legal Hold version after event refusal = %d error=%v", version, versionErr)
		}
	})
}

func TestLegalHoldCurrentAndExactNMinusOneSchemaCompatibility(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, legalHoldNMinusOneCommit)
	nMinusOneOutput := runCurrentControlStartupProbe(t, nMinusOne.Control, database.DSN)
	if strings.Contains(nMinusOneOutput, "database pool") {
		t.Fatalf("exact Slice 32 control failed schema 30 database preflight:\n%s", nMinusOneOutput)
	}
	if !strings.Contains(nMinusOneOutput, "configure Finance Reconciliation service") {
		t.Fatalf("exact Slice 32 control did not reach its post-preflight sentinel:\n%s", nMinusOneOutput)
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 29); err != nil {
		t.Fatalf("contract Legal Hold schema before current-control probe: %v", err)
	}
	currentBinary := filepath.Join(t.TempDir(), "vela-control-current-legal-hold")
	build := exec.Command("go", "build", "-o", currentBinary, "./cmd/vela-control")
	build.Dir = repositoryRoot(t)
	build.Env = environmentWith(map[string]string{"GOWORK": "off"})
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build current Legal Hold control: %v\n%s", err, output)
	}
	currentOutput := runCurrentControlStartupProbe(t, currentBinary, database.DSN)
	if !strings.Contains(currentOutput, "open Compliance database pool") ||
		!strings.Contains(currentOutput, "Legal Hold transaction privilege boundary") {
		t.Fatalf("current Legal Hold control did not fail closed against schema 29:\n%s", currentOutput)
	}
}

func seedCompliancePrincipal(t *testing.T, database testDatabase) {
	t.Helper()
	seedCompliancePrincipalWithTLSURI(t, database, complianceTLSURI)
}

func seedCompliancePrincipalWithTLSURI(t *testing.T, database testDatabase, tlsURI string) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		INSERT INTO compliance_principals (id, stable_id, tls_uri_identity)
		VALUES ($1, 'primary-compliance', $2)
	`, compliancePrincipalID, tlsURI); err != nil {
		t.Fatalf("seed Compliance Principal: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO compliance_database_bindings (database_role, principal_id)
		VALUES ('vela_compliance_login', $1)
	`, compliancePrincipalID); err != nil {
		t.Fatalf("bind Compliance Principal: %v", err)
	}
}

func assertActiveLegalHoldCount(
	t *testing.T,
	database testDatabase,
	jobID uuid.UUID,
	recordClass string,
	want int64,
) {
	t.Helper()
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin active Legal Hold inspection: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	var count int64
	if err := tx.QueryRow(`
		SELECT count(*)
		FROM vela_private.lock_active_non_content_legal_holds($1, $2, $3, $4)
	`, testOrganizationID, testProjectID, jobID, recordClass).Scan(&count); err != nil {
		t.Fatalf("inspect active %s Legal Holds: %v", recordClass, err)
	}
	if count != want {
		t.Fatalf("active %s Legal Holds = %d, want %d", recordClass, count, want)
	}
}

func assertLegalHoldFailure(t *testing.T, err error, code legalhold.FailureCode) {
	t.Helper()
	var failure *legalhold.Failure
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("Legal Hold error = %v, want %s", err, code)
	}
}
