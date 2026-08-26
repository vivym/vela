//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/artifactaccess"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/breakglass"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/httpapi"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/organizationreporting"
	"github.com/vivym/vela/internal/remediation"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/workercontrol"
)

const (
	platformOperatorRequesterID = "00000000-0000-0000-0000-000000000171"
	platformOperatorApproverID  = "00000000-0000-0000-0000-000000000172"
	platformOperatorThirdID     = "00000000-0000-0000-0000-000000000173"
)

func TestPlatformOperatorAuthenticationIsIndependentFromCustomerPrincipals(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	if _, err := database.Admin.Exec(`
		INSERT INTO platform_operator_oidc_bindings (
			id, issuer, subject, display_name
		) VALUES ($1, 'https://platform-identity.example.com', 'operator-requester', 'Requester')
	`, platformOperatorRequesterID); err != nil {
		t.Fatalf("seed Platform Operator binding: %v", err)
	}
	verifier := staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
		Issuer:    "https://platform-identity.example.com",
		Subject:   "operator-requester",
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	}}
	authenticator := breakglass.NewAuthenticator(
		newRolePool(
			t,
			database.DSN,
			"vela_platform_operator_auth_login",
			"vela-platform-operator-auth-password",
		),
		verifier,
	)
	operator, err := authenticator.Authenticate(context.Background(), "platform-oidc-token")
	if err != nil {
		t.Fatalf("authenticate Platform Operator: %v", err)
	}
	if operator.ID != uuid.MustParse(platformOperatorRequesterID) ||
		operator.SessionID == uuid.Nil || len(operator.SessionProof()) != sha256.Size {
		t.Fatalf("authenticated Platform Operator = %#v", operator)
	}
	replayedOperator, err := authenticator.Authenticate(
		context.Background(), "platform-oidc-token",
	)
	if err != nil || replayedOperator.SessionID != operator.SessionID {
		t.Fatalf("replayed Platform Operator session = %#v error=%v", replayedOperator, err)
	}
	rotatedOperator, err := authenticator.Authenticate(
		context.Background(), "rotated-platform-oidc-token",
	)
	if err != nil || rotatedOperator.SessionID == uuid.Nil ||
		rotatedOperator.SessionID == operator.SessionID {
		t.Fatalf("rotated Platform Operator session = %#v error=%v", rotatedOperator, err)
	}
	var createdAt, expiresAt time.Time
	if err := database.Admin.QueryRow(`
		SELECT created_at, expires_at
		FROM platform_operator_auth_sessions
		WHERE id = $1
	`, operator.SessionID).Scan(&createdAt, &expiresAt); err != nil {
		t.Fatalf("read proof-bound Platform Operator session: %v", err)
	}
	if !expiresAt.After(createdAt) || expiresAt.After(createdAt.Add(15*time.Minute)) {
		t.Fatalf("Platform Operator session lifetime = %s to %s", createdAt, expiresAt)
	}
	renewableToken := "renewable-platform-oidc-token"
	renewableProof := sha256.Sum256([]byte(renewableToken))
	expiredSessionID := uuid.New()
	if _, err := database.Admin.Exec(`
		WITH transaction_time AS (
			SELECT clock_timestamp() AS value
		)
			INSERT INTO platform_operator_auth_sessions (
				id, operator_id, token_proof, created_at, expires_at
			)
			SELECT
				$1, $2, $3,
				transaction_time.value - interval '20 minutes',
				transaction_time.value - interval '5 minutes'
			FROM transaction_time
		`, expiredSessionID, platformOperatorRequesterID, renewableProof[:]); err != nil {
		t.Fatalf("seed expired proof-bound Platform Operator session: %v", err)
	}
	renewedOperator, err := authenticator.Authenticate(context.Background(), renewableToken)
	if err != nil || renewedOperator.SessionID == uuid.Nil ||
		renewedOperator.SessionID == expiredSessionID ||
		!renewedOperator.ExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("renewed proof-bound Platform Operator session = %#v error=%v", renewedOperator, err)
	}
	var renewableSessions int
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM platform_operator_auth_sessions
		WHERE operator_id = $1 AND token_proof = $2
	`, platformOperatorRequesterID, renewableProof[:]).Scan(&renewableSessions); err != nil {
		t.Fatalf("count renewed proof-bound Platform Operator sessions: %v", err)
	}
	if renewableSessions != 2 {
		t.Fatalf("renewed proof-bound Platform Operator sessions = %d, want 2", renewableSessions)
	}
	const concurrentAuthLockKey int64 = 170217
	if _, err := database.Admin.Exec(`
		CREATE FUNCTION vela_test_pause_platform_operator_session_insert() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_catalog.pg_advisory_xact_lock(170217);
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER vela_test_pause_platform_operator_session_insert
		BEFORE INSERT ON platform_operator_auth_sessions
		FOR EACH ROW EXECUTE FUNCTION vela_test_pause_platform_operator_session_insert();
	`); err != nil {
		t.Fatalf("install concurrent Platform Operator authentication pause trigger: %v", err)
	}
	authBlocker, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin concurrent Platform Operator authentication blocker: %v", err)
	}
	defer func() { _ = authBlocker.Rollback() }()
	if _, err := authBlocker.Exec("SELECT pg_advisory_lock($1)", concurrentAuthLockKey); err != nil {
		t.Fatalf("acquire concurrent Platform Operator authentication blocker: %v", err)
	}
	type authenticationResult struct {
		operator breakglass.Operator
		err      error
	}
	authenticationResults := make(chan authenticationResult, 2)
	const concurrentToken = "concurrent-platform-oidc-token"
	for range 2 {
		go func() {
			concurrentOperator, authenticationErr := authenticator.Authenticate(
				context.Background(), concurrentToken,
			)
			authenticationResults <- authenticationResult{
				operator: concurrentOperator,
				err:      authenticationErr,
			}
		}()
	}
	concurrentAuthDeadline := time.Now().Add(6 * time.Second)
	for {
		var waiting int
		if err := database.Admin.QueryRow(`
			SELECT count(*)
			FROM pg_catalog.pg_stat_activity
			WHERE usename = 'vela_platform_operator_auth_login'
			  AND wait_event_type = 'Lock'
		`).Scan(&waiting); err != nil {
			t.Fatalf("inspect concurrent Platform Operator authentication locks: %v", err)
		}
		if waiting >= 2 {
			break
		}
		if !time.Now().Before(concurrentAuthDeadline) {
			t.Fatalf("concurrent Platform Operator authentication lock waits = %d, want at least 2", waiting)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var authUnlocked bool
	if err := authBlocker.QueryRow(
		"SELECT pg_advisory_unlock($1)", concurrentAuthLockKey,
	).Scan(&authUnlocked); err != nil || !authUnlocked {
		t.Fatalf("release concurrent Platform Operator authentication blocker = %t error=%v", authUnlocked, err)
	}
	if err := authBlocker.Commit(); err != nil {
		t.Fatalf("commit concurrent Platform Operator authentication blocker: %v", err)
	}
	var concurrentSessionID uuid.UUID
	for range 2 {
		select {
		case result := <-authenticationResults:
			if result.err != nil || result.operator.SessionID == uuid.Nil {
				t.Fatalf("concurrent Platform Operator authentication = %#v", result)
			}
			if concurrentSessionID == uuid.Nil {
				concurrentSessionID = result.operator.SessionID
			} else if result.operator.SessionID != concurrentSessionID {
				t.Fatalf(
					"concurrent Platform Operator sessions = %s and %s",
					concurrentSessionID,
					result.operator.SessionID,
				)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("concurrent Platform Operator authentication did not finish")
		}
	}
	concurrentProof := sha256.Sum256([]byte(concurrentToken))
	var concurrentSessions int
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM platform_operator_auth_sessions
		WHERE operator_id = $1 AND token_proof = $2
	`, platformOperatorRequesterID, concurrentProof[:]).Scan(&concurrentSessions); err != nil {
		t.Fatalf("count concurrent proof-bound Platform Operator sessions: %v", err)
	}
	if concurrentSessions != 1 {
		t.Fatalf("concurrent proof-bound Platform Operator sessions = %d, want 1", concurrentSessions)
	}
	wrongProof := sha256.Sum256([]byte("wrong-platform-session-proof"))
	wrongContext, err := database.Admin.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin wrong Platform Operator proof transaction: %v", err)
	}
	var wrongProofOperatorID uuid.UUID
	err = wrongContext.QueryRow(`
		SELECT operator_id
		FROM vela_set_break_glass_request_context($1, $2)
	`, operator.SessionID, wrongProof[:]).Scan(&wrongProofOperatorID)
	var wrongProofError *pgconn.PgError
	if !errors.As(err, &wrongProofError) || wrongProofError.Code != "28000" {
		t.Fatalf("wrong Platform Operator session proof error = %v", err)
	}
	_ = wrongContext.Rollback()
	expiredAuthenticator := breakglass.NewAuthenticator(
		newRolePool(
			t,
			database.DSN,
			"vela_platform_operator_auth_login",
			"vela-platform-operator-auth-password",
		),
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://platform-identity.example.com",
			Subject:   "operator-requester",
			ExpiresAt: time.Now().UTC().Add(-time.Second),
		}},
	)
	if _, err := expiredAuthenticator.Authenticate(
		context.Background(), "expired-platform-oidc-token",
	); !errors.Is(err, breakglass.ErrInvalidOperatorCredential) {
		t.Fatalf("expired Platform Operator credential error = %v", err)
	}

	if _, err := database.Admin.Exec(`
		UPDATE platform_operator_oidc_bindings
		SET disabled_at = clock_timestamp()
		WHERE id = $1
	`, platformOperatorRequesterID); err != nil {
		t.Fatalf("disable Platform Operator: %v", err)
	}
	if _, err := authenticator.Authenticate(
		context.Background(), "new-platform-oidc-token",
	); !errors.Is(err, breakglass.ErrInvalidOperatorCredential) {
		t.Fatalf("disabled Platform Operator authentication error = %v", err)
	}
}

func TestBreakGlassRequiresDistinctApprovalAndRevocationIsPermanent(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "break-glass-target", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"customer support target"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit Break-glass target = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var target jobResponse
	if err := json.Unmarshal(accepted.Body, &target); err != nil {
		t.Fatalf("decode Break-glass target: %v", err)
	}

	if _, err := database.Admin.Exec(`
		INSERT INTO platform_operator_oidc_bindings (
			id, issuer, subject, display_name
		) VALUES
			($1, 'https://platform-identity.example.com', 'operator-requester', 'Requester'),
			($2, 'https://platform-identity.example.com', 'operator-approver', 'Approver'),
			($3, 'https://platform-identity.example.com', 'operator-third', 'Third Operator')
	`, platformOperatorRequesterID, platformOperatorApproverID, platformOperatorThirdID); err != nil {
		t.Fatalf("seed Platform Operator bindings: %v", err)
	}

	requesterSession, requesterProof := authenticatePlatformOperator(
		t, database.Admin, "operator-requester", "requester-token",
	)
	approverSession, approverProof := authenticatePlatformOperator(
		t, database.Admin, "operator-approver", "approver-token",
	)
	thirdSession, thirdProof := authenticatePlatformOperator(
		t, database.Admin, "operator-third", "third-operator-token",
	)
	expiredRequestID := uuid.New()
	if _, err := database.Admin.Exec(`
		WITH times AS (
			SELECT clock_timestamp() - interval '2 hours' AS requested_at
		)
		INSERT INTO break_glass_requests (
			id, organization_id, project_id, job_id, scopes, reason_code,
			ticket_reference, requested_duration_seconds, requester_operator_id,
			requester_session_id, idempotency_key, request_hash, requested_at,
			approval_deadline_at
		) SELECT
			$1, $2, $3, $4, ARRAY['ARTIFACT_READ']::break_glass_scope[],
			'CUSTOMER_SUPPORT', 'SUPPORT-EXPIRED-1', 600, $5, $6,
			'expired-approval-window', decode(repeat('01', 32), 'hex'),
			times.requested_at, times.requested_at + interval '1 hour'
		FROM times
	`,
		expiredRequestID,
		testOrganizationID,
		testProjectID,
		target.JobID,
		platformOperatorRequesterID,
		requesterSession,
	); err != nil {
		t.Fatalf("seed expired Break-glass request: %v", err)
	}
	expiredApproval := beginBreakGlassTransaction(
		t, database.Admin, approverSession, approverProof,
	)
	var expiredGrantID uuid.UUID
	err := expiredApproval.QueryRow(`
		SELECT grant_id
		FROM vela_approve_break_glass_request($1, $2)
	`, expiredRequestID, uuid.New()).Scan(&expiredGrantID)
	var expiredApprovalError *pgconn.PgError
	if !errors.As(err, &expiredApprovalError) || expiredApprovalError.Code != "55000" {
		t.Fatalf("expired Break-glass approval error = %v, want SQLSTATE 55000", err)
	}
	_ = expiredApproval.Rollback()
	var expiredApprovalEffects int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM break_glass_grants WHERE request_id = $1) +
			(SELECT count(*) FROM break_glass_events
			 WHERE request_id = $1 AND action = 'GRANT_APPROVED')
	`, expiredRequestID).Scan(&expiredApprovalEffects); err != nil {
		t.Fatalf("read expired Break-glass approval effects: %v", err)
	}
	if expiredApprovalEffects != 0 {
		t.Fatalf("expired Break-glass approval effects = %d", expiredApprovalEffects)
	}
	requestID := uuid.New()
	requestHash := sha256.Sum256([]byte("fixed Break-glass request"))
	withBreakGlassContext(t, database.Admin, requesterSession, requesterProof, func(tx *sql.Tx) {
		var returnedID uuid.UUID
		var replayed bool
		if err := tx.QueryRow(`
			SELECT request_id, replayed
			FROM vela_create_break_glass_request(
				$1, 'support-001', $2, $3, $4, $5,
				ARRAY['REQUEST_CONTENT_READ', 'ARTIFACT_READ']::break_glass_scope[],
				'CUSTOMER_SUPPORT', 'SUPPORT-1234', 3600
			)
		`, requestID, requestHash[:], testOrganizationID, testProjectID, target.JobID).Scan(
			&returnedID, &replayed,
		); err != nil {
			t.Fatalf("create Break-glass request: %v", err)
		}
		if returnedID != requestID || replayed {
			t.Fatalf("created Break-glass request = %s replayed=%t", returnedID, replayed)
		}
	})

	selfApproval := beginBreakGlassTransaction(
		t, database.Admin, requesterSession, requesterProof,
	)
	var selfApprovedGrantID uuid.UUID
	err = selfApproval.QueryRow(`
			SELECT grant_id
			FROM vela_approve_break_glass_request($1, $2)
		`, requestID, uuid.New()).Scan(&selfApprovedGrantID)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
		t.Fatalf("self-approval error = %v, want SQLSTATE 42501", err)
	}
	_ = selfApproval.Rollback()

	grantID := uuid.New()
	var grantExpiresAt time.Time
	withBreakGlassContext(t, database.Admin, approverSession, approverProof, func(tx *sql.Tx) {
		var returnedID uuid.UUID
		var replayed bool
		if err := tx.QueryRow(`
			SELECT grant_id, expires_at, replayed
			FROM vela_approve_break_glass_request($1, $2)
		`, requestID, grantID).Scan(&returnedID, &grantExpiresAt, &replayed); err != nil {
			t.Fatalf("approve Break-glass request: %v", err)
		}
		if returnedID != grantID || replayed {
			t.Fatalf("approved Break-glass grant = %s replayed=%t", returnedID, replayed)
		}
	})
	var approvedAt time.Time
	if err := database.Admin.QueryRow(`
		SELECT approved_at
		FROM break_glass_grants
		WHERE id = $1
	`, grantID).Scan(&approvedAt); err != nil {
		t.Fatalf("read Break-glass approval: %v", err)
	}
	if !grantExpiresAt.Equal(approvedAt.Add(time.Hour)) {
		t.Fatalf("grant expiry = %s, want %s", grantExpiresAt, approvedAt.Add(time.Hour))
	}
	withBreakGlassContext(t, database.Admin, approverSession, approverProof, func(tx *sql.Tx) {
		var replayedGrantID uuid.UUID
		var replayedExpiresAt time.Time
		var replayed bool
		if err := tx.QueryRow(`
			SELECT grant_id, expires_at, replayed
			FROM vela_approve_break_glass_request($1, $2)
		`, requestID, uuid.New()).Scan(
			&replayedGrantID,
			&replayedExpiresAt,
			&replayed,
		); err != nil {
			t.Fatalf("replay Break-glass approval: %v", err)
		}
		if replayedGrantID != grantID || !replayedExpiresAt.Equal(grantExpiresAt) || !replayed {
			t.Fatalf(
				"replayed Break-glass approval = grant %s expiry %s replayed=%t",
				replayedGrantID,
				replayedExpiresAt,
				replayed,
			)
		}
	})
	conflictingApproval := beginBreakGlassTransaction(
		t, database.Admin, thirdSession, thirdProof,
	)
	var conflictingGrantID uuid.UUID
	err = conflictingApproval.QueryRow(`
		SELECT grant_id
		FROM vela_approve_break_glass_request($1, $2)
	`, requestID, uuid.New()).Scan(&conflictingGrantID)
	var conflictingApprovalError *pgconn.PgError
	if !errors.As(err, &conflictingApprovalError) || conflictingApprovalError.Code != "55000" {
		t.Fatalf("conflicting Break-glass approver error = %v, want SQLSTATE 55000", err)
	}
	_ = conflictingApproval.Rollback()
	var approvalEvents int
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM break_glass_events
		WHERE request_id = $1 AND action = 'GRANT_APPROVED'
	`, requestID).Scan(&approvalEvents); err != nil {
		t.Fatalf("count Break-glass approval events: %v", err)
	}
	if approvalEvents != 2 {
		t.Fatalf("Break-glass approval events = %d, want 2 scoped events", approvalEvents)
	}
	unknownOutcome, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin unknown Break-glass outcome transaction: %v", err)
	}
	_, err = unknownOutcome.Exec(`
		INSERT INTO break_glass_events (
			id, organization_id, project_id, job_id, request_id, grant_id,
			operator_id, operator_session_id, action, scope, outcome_code, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8,
			'GRANT_APPROVED', NULL, 'UNRECOGNIZED', clock_timestamp()
		)
	`,
		uuid.New(),
		testOrganizationID,
		testProjectID,
		target.JobID,
		requestID,
		grantID,
		platformOperatorApproverID,
		approverSession,
	)
	_ = unknownOutcome.Rollback()
	var unknownOutcomeError *pgconn.PgError
	if !errors.As(err, &unknownOutcomeError) || unknownOutcomeError.Code != "22P02" {
		t.Fatalf("unknown Break-glass outcome error = %v, want SQLSTATE 22P02", err)
	}
	expiredGrantRequestID := uuid.New()
	lifecycleExpiredGrantID := uuid.New()
	if _, err := database.Admin.Exec(`
		WITH times AS (
			SELECT clock_timestamp() - interval '3 minutes' AS requested_at
		)
		INSERT INTO break_glass_requests (
			id, organization_id, project_id, job_id, scopes, reason_code,
			ticket_reference, requested_duration_seconds, requester_operator_id,
			requester_session_id, idempotency_key, request_hash, requested_at,
			approval_deadline_at
		) SELECT
			$1, $2, $3, $4, ARRAY['ARTIFACT_READ']::break_glass_scope[],
			'CUSTOMER_SUPPORT', 'SUPPORT-EXPIRED-GRANT', 60, $5, $6,
			'expired-grant-revocation', decode(repeat('02', 32), 'hex'),
			times.requested_at, times.requested_at + interval '1 hour'
		FROM times;
	`,
		expiredGrantRequestID,
		testOrganizationID,
		testProjectID,
		target.JobID,
		platformOperatorRequesterID,
		requesterSession,
	); err != nil {
		t.Fatalf("seed expired Break-glass grant request: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO break_glass_grants (
			id, request_id, organization_id, project_id, job_id,
			approver_operator_id, approver_session_id, approved_at, expires_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			clock_timestamp() - interval '2 minutes',
			clock_timestamp() - interval '1 minute'
		)
	`,
		lifecycleExpiredGrantID,
		expiredGrantRequestID,
		testOrganizationID,
		testProjectID,
		target.JobID,
		platformOperatorApproverID,
		approverSession,
	); err != nil {
		t.Fatalf("seed expired Break-glass grant: %v", err)
	}
	expiredRevocation := beginBreakGlassTransaction(
		t, database.Admin, thirdSession, thirdProof,
	)
	var expiredRevokedAt time.Time
	err = expiredRevocation.QueryRow(`
		SELECT revoked_at
		FROM vela_revoke_break_glass_grant($1)
	`, lifecycleExpiredGrantID).Scan(&expiredRevokedAt)
	var expiredRevocationError *pgconn.PgError
	if !errors.As(err, &expiredRevocationError) || expiredRevocationError.Code != "55000" {
		t.Fatalf("expired Break-glass revocation error = %v, want SQLSTATE 55000", err)
	}
	_ = expiredRevocation.Rollback()
	var expiredRevocationEffects int
	if err := database.Admin.QueryRow(`
		SELECT
			count(*) FILTER (WHERE target_grant.revoked_at IS NOT NULL) +
			count(event.id)
		FROM break_glass_grants AS target_grant
		LEFT JOIN break_glass_events AS event
		  ON event.grant_id = target_grant.id
		 AND event.action = 'GRANT_REVOKED'
		WHERE target_grant.id = $1
		GROUP BY target_grant.id
	`, lifecycleExpiredGrantID).Scan(&expiredRevocationEffects); err != nil {
		t.Fatalf("read expired Break-glass revocation effects: %v", err)
	}
	if expiredRevocationEffects != 0 {
		t.Fatalf("expired Break-glass revocation effects = %d", expiredRevocationEffects)
	}

	withBreakGlassContext(t, database.Admin, requesterSession, requesterProof, func(tx *sql.Tx) {
		var revokedAt time.Time
		var replayed bool
		if err := tx.QueryRow(`
			SELECT revoked_at, replayed
			FROM vela_revoke_break_glass_grant($1)
		`, grantID).Scan(&revokedAt, &replayed); err != nil {
			t.Fatalf("revoke Break-glass grant: %v", err)
		}
		if replayed || revokedAt.Before(approvedAt) {
			t.Fatalf("first revocation = %s replayed=%t", revokedAt, replayed)
		}
	})
	withBreakGlassContext(t, database.Admin, approverSession, approverProof, func(tx *sql.Tx) {
		var revokedAt time.Time
		var replayed bool
		if err := tx.QueryRow(`
			SELECT revoked_at, replayed
			FROM vela_revoke_break_glass_grant($1)
		`, grantID).Scan(&revokedAt, &replayed); err != nil {
			t.Fatalf("replay Break-glass revocation: %v", err)
		}
		if !replayed {
			t.Fatal("replayed Break-glass revocation was not identified")
		}
	})
	var revocationEvents int
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM break_glass_events
		WHERE grant_id = $1 AND action = 'GRANT_REVOKED'
	`, grantID).Scan(&revocationEvents); err != nil {
		t.Fatalf("count Break-glass revocation events: %v", err)
	}
	if revocationEvents != 2 {
		t.Fatalf("Break-glass revocation events = %d, want 2 scoped events", revocationEvents)
	}
	for _, mutation := range []struct {
		name      string
		statement string
		argument  uuid.UUID
	}{
		{
			name: "request",
			statement: `UPDATE break_glass_requests
				SET ticket_reference = 'SUPPORT-MUTATED' WHERE id = $1`,
			argument: requestID,
		},
		{
			name: "grant identity",
			statement: `UPDATE break_glass_grants
				SET expires_at = expires_at - interval '1 second' WHERE id = $1`,
			argument: grantID,
		},
		{
			name: "grant revocation",
			statement: `UPDATE break_glass_grants
				SET revoked_at = revoked_at + interval '1 second' WHERE id = $1`,
			argument: grantID,
		},
		{
			name: "event",
			statement: `UPDATE break_glass_events
				SET outcome_code = 'SIGNING_FAILED' WHERE grant_id = $1`,
			argument: grantID,
		},
		{
			name: "auth session",
			statement: `UPDATE platform_operator_auth_sessions
				SET expires_at = expires_at - interval '1 second' WHERE id = $1`,
			argument: requesterSession,
		},
	} {
		_, err := database.Admin.Exec(mutation.statement, mutation.argument)
		var immutableError *pgconn.PgError
		if !errors.As(err, &immutableError) || immutableError.Code != "55000" {
			t.Fatalf("mutate immutable Break-glass %s error = %v", mutation.name, err)
		}
	}
	for _, deletion := range []struct {
		name      string
		statement string
		argument  uuid.UUID
	}{
		{name: "event", statement: "DELETE FROM break_glass_events WHERE grant_id = $1", argument: grantID},
		{name: "grant", statement: "DELETE FROM break_glass_grants WHERE id = $1", argument: grantID},
		{name: "request", statement: "DELETE FROM break_glass_requests WHERE id = $1", argument: requestID},
	} {
		_, err := database.Admin.Exec(deletion.statement, deletion.argument)
		var immutableError *pgconn.PgError
		if !errors.As(err, &immutableError) || immutableError.Code != "55000" {
			t.Fatalf("delete immutable Break-glass %s error = %v", deletion.name, err)
		}
	}
	if _, err := database.Admin.Exec(`
		UPDATE platform_operator_oidc_bindings
		SET disabled_at = clock_timestamp()
		WHERE id = $1
	`, platformOperatorRequesterID); err != nil {
		t.Fatalf("disable immutable Platform Operator binding fixture: %v", err)
	}
	_, err = database.Admin.Exec(`
		UPDATE platform_operator_oidc_bindings
		SET disabled_at = NULL
		WHERE id = $1
	`, platformOperatorRequesterID)
	var bindingImmutableError *pgconn.PgError
	if !errors.As(err, &bindingImmutableError) || bindingImmutableError.Code != "55000" {
		t.Fatalf("restore disabled Platform Operator binding error = %v", err)
	}
}

func TestBreakGlassStateTransitionsRevalidateExactJobTarget(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "break-glass-transition-target", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"transition revalidation target"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit Break-glass transition target = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var target jobResponse
	if err := json.Unmarshal(accepted.Body, &target); err != nil {
		t.Fatalf("decode Break-glass transition target: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO platform_operator_oidc_bindings (
			id, issuer, subject, display_name
		) VALUES
			($1, 'https://platform-identity.example.com', 'transition-requester', 'Requester'),
			($2, 'https://platform-identity.example.com', 'transition-approver', 'Approver')
	`, platformOperatorRequesterID, platformOperatorApproverID); err != nil {
		t.Fatalf("seed transition-revalidation Platform Operators: %v", err)
	}
	requesterSession, requesterProof := authenticatePlatformOperator(
		t, database.Admin, "transition-requester", "transition-requester-token",
	)
	approverSession, approverProof := authenticatePlatformOperator(
		t, database.Admin, "transition-approver", "transition-approver-token",
	)
	requester := breakglass.NewOperatorForSession(
		uuid.MustParse(platformOperatorRequesterID), requesterSession, requesterProof,
	)
	approver := breakglass.NewOperatorForSession(
		uuid.MustParse(platformOperatorApproverID), approverSession, approverProof,
	)
	service, err := breakglass.NewService(newRolePool(
		t,
		database.DSN,
		"vela_break_glass_request_login",
		"vela-break-glass-request-password",
	))
	if err != nil {
		t.Fatalf("create transition-revalidation Break-glass service: %v", err)
	}
	input := breakglass.RequestInput{
		Target: breakglass.Target{
			OrganizationID: uuid.MustParse(testOrganizationID),
			ProjectID:      uuid.MustParse(testProjectID),
			JobID:          uuid.MustParse(target.JobID),
		},
		Scopes:                   []breakglass.Scope{breakglass.ScopeRequestContentRead},
		ReasonCode:               breakglass.ReasonServiceRecovery,
		TicketReference:          "SUPPORT-TARGET-1",
		RequestedDurationSeconds: 600,
	}
	pending, _, err := service.Request(
		context.Background(), requester, "transition-pending", input,
	)
	if err != nil {
		t.Fatalf("create pending transition-revalidation request: %v", err)
	}
	approved, _, err := service.Request(
		context.Background(), requester, "transition-approved", input,
	)
	if err != nil {
		t.Fatalf("create approved transition-revalidation request: %v", err)
	}
	approved, err = service.Approve(context.Background(), approver, approved.ID)
	if err != nil || approved.GrantID == nil {
		t.Fatalf("approve transition-revalidation request = %#v error=%v", approved, err)
	}

	removeTarget, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin forced target removal: %v", err)
	}
	if _, err := removeTarget.Exec("ALTER TABLE jobs DISABLE TRIGGER ALL"); err != nil {
		_ = removeTarget.Rollback()
		t.Fatalf("disable Job target constraints: %v", err)
	}
	if _, err := removeTarget.Exec("DELETE FROM jobs WHERE id = $1", target.JobID); err != nil {
		_ = removeTarget.Rollback()
		t.Fatalf("remove Job target fixture: %v", err)
	}
	if _, err := removeTarget.Exec("ALTER TABLE jobs ENABLE TRIGGER ALL"); err != nil {
		_ = removeTarget.Rollback()
		t.Fatalf("restore Job target constraints: %v", err)
	}
	if err := removeTarget.Commit(); err != nil {
		t.Fatalf("commit forced target removal: %v", err)
	}

	if _, err := service.Approve(
		context.Background(), approver, pending.ID,
	); !breakglass.IsFailure(err, breakglass.FailureNotFound) {
		t.Fatalf("approval after exact Job removal error = %v", err)
	}
	if _, err := service.Revoke(
		context.Background(), requester, *approved.GrantID,
	); !breakglass.IsFailure(err, breakglass.FailureNotFound) {
		t.Fatalf("revocation after exact Job removal error = %v", err)
	}
	var transitionEffects int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM break_glass_grants WHERE request_id = $1) +
			(SELECT count(*) FROM break_glass_events
			 WHERE request_id = $1 AND action = 'GRANT_APPROVED') +
			(SELECT count(*) FROM break_glass_events
			 WHERE grant_id = $2 AND action = 'GRANT_REVOKED') +
			(SELECT count(*) FROM break_glass_grants
			 WHERE id = $2 AND revoked_at IS NOT NULL)
	`, pending.ID, *approved.GrantID).Scan(&transitionEffects); err != nil {
		t.Fatalf("read target-revalidation transition effects: %v", err)
	}
	if transitionEffects != 0 {
		t.Fatalf("target-revalidation transition effects = %d, want 0", transitionEffects)
	}
}

func TestBreakGlassAuthorizationSerializesWithRevocation(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "break-glass-revocation-race", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"serialize authorization with revocation"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit Break-glass revocation-race target = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var target jobResponse
	if err := json.Unmarshal(accepted.Body, &target); err != nil {
		t.Fatalf("decode Break-glass revocation-race target: %v", err)
	}
	requesterID := uuid.New()
	approverID := uuid.New()
	revokerID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO platform_operator_oidc_bindings (
			id, issuer, subject, display_name
		) VALUES
			($1, 'https://platform-identity.example.com', 'race-requester', 'Race Requester'),
			($2, 'https://platform-identity.example.com', 'race-approver', 'Race Approver'),
			($3, 'https://platform-identity.example.com', 'race-revoker', 'Race Revoker')
	`, requesterID, approverID, revokerID); err != nil {
		t.Fatalf("seed Break-glass revocation-race operators: %v", err)
	}
	requesterSession, requesterProof := authenticatePlatformOperator(
		t, database.Admin, "race-requester", "race-requester-token",
	)
	approverSession, approverProof := authenticatePlatformOperator(
		t, database.Admin, "race-approver", "race-approver-token",
	)
	revokerSession, revokerProof := authenticatePlatformOperator(
		t, database.Admin, "race-revoker", "race-revoker-token",
	)
	requestID := uuid.New()
	requestHash := sha256.Sum256([]byte("Break-glass revocation-race request"))
	withBreakGlassContext(t, database.Admin, requesterSession, requesterProof, func(tx *sql.Tx) {
		var returnedID uuid.UUID
		var replayed bool
		if err := tx.QueryRow(`
			SELECT request_id, replayed
			FROM vela_create_break_glass_request(
				$1, 'revocation-race', $2, $3, $4, $5,
				ARRAY['REQUEST_CONTENT_READ']::break_glass_scope[],
				'CUSTOMER_SUPPORT', 'SUPPORT-RACE-1', 900
			)
		`, requestID, requestHash[:], testOrganizationID, testProjectID, target.JobID).Scan(
			&returnedID, &replayed,
		); err != nil || returnedID != requestID || replayed {
			t.Fatalf("create Break-glass revocation-race request = %s replayed=%t error=%v", returnedID, replayed, err)
		}
	})
	grantID := uuid.New()
	withBreakGlassContext(t, database.Admin, approverSession, approverProof, func(tx *sql.Tx) {
		var returnedID uuid.UUID
		var expiresAt time.Time
		var replayed bool
		if err := tx.QueryRow(`
			SELECT grant_id, expires_at, replayed
			FROM vela_approve_break_glass_request($1, $2)
		`, requestID, grantID).Scan(&returnedID, &expiresAt, &replayed); err != nil ||
			returnedID != grantID || replayed {
			t.Fatalf("approve Break-glass revocation-race request = %s replayed=%t error=%v", returnedID, replayed, err)
		}
	})

	const advisoryLockKey int64 = 170017
	if _, err := database.Admin.Exec(`
		CREATE FUNCTION vela_test_pause_break_glass_authorization() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.action = 'REQUEST_CONTENT_AUTHORIZED' THEN
				PERFORM pg_catalog.pg_advisory_xact_lock(170017);
			END IF;
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER vela_test_pause_break_glass_authorization
		BEFORE INSERT ON break_glass_events
		FOR EACH ROW EXECUTE FUNCTION vela_test_pause_break_glass_authorization();
	`); err != nil {
		t.Fatalf("install Break-glass authorization pause trigger: %v", err)
	}
	blocker, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Break-glass authorization blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	if _, err := blocker.Exec("SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		t.Fatalf("acquire Break-glass authorization blocker: %v", err)
	}
	type authorizationResult struct {
		authorized bool
		err        error
	}
	authorizationReady := make(chan struct{})
	authorizationResults := make(chan authorizationResult, 1)
	go func() {
		tx, err := database.Admin.Begin()
		if err != nil {
			close(authorizationReady)
			authorizationResults <- authorizationResult{err: err}
			return
		}
		defer func() { _ = tx.Rollback() }()
		var operatorID uuid.UUID
		var transactionTime time.Time
		err = tx.QueryRow(`
			SELECT operator_id, transaction_time
			FROM vela_set_break_glass_request_context($1, $2)
		`, requesterSession, requesterProof).Scan(&operatorID, &transactionTime)
		close(authorizationReady)
		if err != nil {
			authorizationResults <- authorizationResult{err: err}
			return
		}
		var result authorizationResult
		result.err = tx.QueryRow(`
			SELECT authorized
			FROM vela_authorize_break_glass_request_content($1)
		`, grantID).Scan(&result.authorized)
		if result.err == nil {
			result.err = tx.Commit()
		}
		authorizationResults <- result
	}()
	<-authorizationReady
	waitForRoleDatabaseLock(t, database.Admin, "postgres")

	type revocationResult struct {
		revokedAt time.Time
		replayed  bool
		err       error
	}
	ready := make(chan struct{})
	revocationResults := make(chan revocationResult, 1)
	go func() {
		tx, err := database.Admin.Begin()
		if err != nil {
			close(ready)
			revocationResults <- revocationResult{err: err}
			return
		}
		defer func() { _ = tx.Rollback() }()
		var operatorID uuid.UUID
		var transactionTime time.Time
		err = tx.QueryRow(`
			SELECT operator_id, transaction_time
			FROM vela_set_break_glass_request_context($1, $2)
		`, revokerSession, revokerProof).Scan(&operatorID, &transactionTime)
		close(ready)
		if err != nil {
			revocationResults <- revocationResult{err: err}
			return
		}
		var result revocationResult
		result.err = tx.QueryRow(`
			SELECT revoked_at, replayed
			FROM vela_revoke_break_glass_grant($1)
		`, grantID).Scan(&result.revokedAt, &result.replayed)
		if result.err == nil {
			result.err = tx.Commit()
		}
		revocationResults <- result
	}()
	<-ready
	var earlyRevocation *revocationResult
	select {
	case result := <-revocationResults:
		earlyRevocation = &result
	case <-time.After(250 * time.Millisecond):
	}
	var unlocked bool
	if err := blocker.QueryRow(
		"SELECT pg_advisory_unlock($1)", advisoryLockKey,
	).Scan(&unlocked); err != nil || !unlocked {
		t.Fatalf("release Break-glass authorization blocker = %t error=%v", unlocked, err)
	}
	if err := blocker.Commit(); err != nil {
		t.Fatalf("commit Break-glass authorization blocker: %v", err)
	}
	select {
	case result := <-authorizationResults:
		if result.err != nil || !result.authorized {
			t.Fatalf("serialized Break-glass authorization = %#v", result)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serialized Break-glass authorization did not finish")
	}
	if earlyRevocation != nil {
		t.Fatalf("Break-glass revocation crossed active authorization: %#v", *earlyRevocation)
	}
	select {
	case result := <-revocationResults:
		if result.err != nil || result.replayed || result.revokedAt.IsZero() {
			t.Fatalf("serialized Break-glass revocation = %#v", result)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serialized Break-glass revocation did not finish")
	}
	withBreakGlassContext(t, database.Admin, approverSession, approverProof, func(tx *sql.Tx) {
		var allowed bool
		var outcome string
		if err := tx.QueryRow(`
			SELECT authorized, outcome_code
			FROM vela_authorize_break_glass_request_content($1)
		`, grantID).Scan(&allowed, &outcome); err != nil {
			t.Fatalf("authorize content after serialized revocation: %v", err)
		}
		if allowed || outcome != "GRANT_REVOKED" {
			t.Fatalf("authorization after serialized revocation = %t outcome %s", allowed, outcome)
		}
	})
}

func TestBreakGlassServiceLifecycleUsesDedicatedRequestRole(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "break-glass-service-target", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"service lifecycle target"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit Break-glass service target = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var target jobResponse
	if err := json.Unmarshal(accepted.Body, &target); err != nil {
		t.Fatalf("decode Break-glass service target: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO platform_operator_oidc_bindings (
			id, issuer, subject, display_name
		) VALUES
			($1, 'https://platform-identity.example.com', 'operator-requester', 'Requester'),
			($2, 'https://platform-identity.example.com', 'operator-approver', 'Approver')
	`, platformOperatorRequesterID, platformOperatorApproverID); err != nil {
		t.Fatalf("seed Platform Operator bindings: %v", err)
	}
	requesterSession, requesterProof := authenticatePlatformOperator(
		t, database.Admin, "operator-requester", "requester-service-token",
	)
	approverSession, approverProof := authenticatePlatformOperator(
		t, database.Admin, "operator-approver", "approver-service-token",
	)
	requester := breakglass.NewOperatorForSession(
		uuid.MustParse(platformOperatorRequesterID), requesterSession, requesterProof,
	)
	approver := breakglass.NewOperatorForSession(
		uuid.MustParse(platformOperatorApproverID), approverSession, approverProof,
	)
	service, err := breakglass.NewService(newRolePool(
		t,
		database.DSN,
		"vela_break_glass_request_login",
		"vela-break-glass-request-password",
	))
	if err != nil {
		t.Fatalf("create Break-glass service: %v", err)
	}
	validInput := breakglass.RequestInput{
		Target: breakglass.Target{
			OrganizationID: uuid.MustParse(testOrganizationID),
			ProjectID:      uuid.MustParse(testProjectID),
			JobID:          uuid.MustParse(target.JobID),
		},
		Scopes:                   []breakglass.Scope{breakglass.ScopeArtifactRead},
		ReasonCode:               breakglass.ReasonCustomerSupport,
		TicketReference:          "SUPPORT-VALID",
		RequestedDurationSeconds: 900,
	}
	for _, invalid := range []struct {
		name  string
		input breakglass.RequestInput
	}{
		{name: "empty scope", input: func() breakglass.RequestInput {
			value := validInput
			value.Scopes = nil
			return value
		}()},
		{name: "duplicate scope", input: func() breakglass.RequestInput {
			value := validInput
			value.Scopes = []breakglass.Scope{
				breakglass.ScopeArtifactRead,
				breakglass.ScopeArtifactRead,
			}
			return value
		}()},
		{name: "unknown scope", input: func() breakglass.RequestInput {
			value := validInput
			value.Scopes = []breakglass.Scope{"CUSTOMER_DATA_READ"}
			return value
		}()},
		{name: "unknown reason", input: func() breakglass.RequestInput {
			value := validInput
			value.ReasonCode = "ROUTINE_INSPECTION"
			return value
		}()},
		{name: "unsafe ticket", input: func() breakglass.RequestInput {
			value := validInput
			value.TicketReference = "customer content"
			return value
		}()},
		{name: "duration below minimum", input: func() breakglass.RequestInput {
			value := validInput
			value.RequestedDurationSeconds = 59
			return value
		}()},
		{name: "duration above maximum", input: func() breakglass.RequestInput {
			value := validInput
			value.RequestedDurationSeconds = 3601
			return value
		}()},
		{name: "missing exact target", input: func() breakglass.RequestInput {
			value := validInput
			value.JobID = uuid.Nil
			return value
		}()},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			if _, _, err := service.Request(
				context.Background(),
				requester,
				"invalid-"+strings.ReplaceAll(invalid.name, " ", "-"),
				invalid.input,
			); !breakglass.IsFailure(err, breakglass.FailureInvalid) {
				t.Fatalf("invalid Break-glass request error = %v", err)
			}
		})
	}
	var invalidEffects int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM break_glass_requests
			 WHERE idempotency_key LIKE 'invalid-%') +
			(SELECT count(*) FROM break_glass_events
			 WHERE action = 'REQUEST_CREATED')
	`).Scan(&invalidEffects); err != nil {
		t.Fatalf("read invalid Break-glass request effects: %v", err)
	}
	if invalidEffects != 0 {
		t.Fatalf("invalid Break-glass request effects = %d", invalidEffects)
	}
	created, replayed, err := service.Request(
		context.Background(),
		requester,
		"service-lifecycle-001",
		breakglass.RequestInput{
			Target: breakglass.Target{
				OrganizationID: uuid.MustParse(testOrganizationID),
				ProjectID:      uuid.MustParse(testProjectID),
				JobID:          uuid.MustParse(target.JobID),
			},
			Scopes:                   []breakglass.Scope{breakglass.ScopeArtifactRead},
			ReasonCode:               breakglass.ReasonCustomerSupport,
			TicketReference:          "SUPPORT-2001",
			RequestedDurationSeconds: 900,
		},
	)
	if err != nil || replayed || created.State != breakglass.StatePending {
		t.Fatalf("create Break-glass request = %#v replayed=%t error=%v", created, replayed, err)
	}
	replayedRequest, replayed, err := service.Request(
		context.Background(),
		requester,
		"service-lifecycle-001",
		breakglass.RequestInput{
			Target: breakglass.Target{
				OrganizationID: uuid.MustParse(testOrganizationID),
				ProjectID:      uuid.MustParse(testProjectID),
				JobID:          uuid.MustParse(target.JobID),
			},
			Scopes:                   []breakglass.Scope{breakglass.ScopeArtifactRead},
			ReasonCode:               breakglass.ReasonCustomerSupport,
			TicketReference:          "SUPPORT-2001",
			RequestedDurationSeconds: 900,
		},
	)
	if err != nil || !replayed || replayedRequest.ID != created.ID {
		t.Fatalf("replay Break-glass request = %#v replayed=%t error=%v", replayedRequest, replayed, err)
	}
	if _, _, err := service.Request(
		context.Background(),
		requester,
		"service-lifecycle-001",
		breakglass.RequestInput{
			Target: breakglass.Target{
				OrganizationID: uuid.MustParse(testOrganizationID),
				ProjectID:      uuid.MustParse(testProjectID),
				JobID:          uuid.MustParse(target.JobID),
			},
			Scopes:                   []breakglass.Scope{breakglass.ScopeRequestContentRead},
			ReasonCode:               breakglass.ReasonCustomerSupport,
			TicketReference:          "SUPPORT-2001",
			RequestedDurationSeconds: 900,
		},
	); !breakglass.IsFailure(err, breakglass.FailureConflict) {
		t.Fatalf("conflicting Break-glass idempotency replay error = %v", err)
	}
	var requestCount, requestEventCount int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM break_glass_requests
			 WHERE requester_operator_id = $1 AND idempotency_key = 'service-lifecycle-001'),
			(SELECT count(*) FROM break_glass_events
			 WHERE request_id = $2 AND action = 'REQUEST_CREATED')
	`, requester.ID, created.ID).Scan(&requestCount, &requestEventCount); err != nil {
		t.Fatalf("read conflicting Break-glass replay effects: %v", err)
	}
	if requestCount != 1 || requestEventCount != 1 {
		t.Fatalf(
			"conflicting Break-glass replay effects = requests %d events %d",
			requestCount,
			requestEventCount,
		)
	}
	const concurrentRequestLockKey int64 = 170117
	if _, err := database.Admin.Exec(`
		CREATE FUNCTION vela_test_pause_break_glass_request_insert() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.idempotency_key = 'service-lifecycle-concurrent' THEN
				PERFORM pg_catalog.pg_advisory_xact_lock(170117);
			END IF;
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER vela_test_pause_break_glass_request_insert
		BEFORE INSERT ON break_glass_requests
		FOR EACH ROW EXECUTE FUNCTION vela_test_pause_break_glass_request_insert();
	`); err != nil {
		t.Fatalf("install concurrent Break-glass request pause trigger: %v", err)
	}
	requestBlocker, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin concurrent Break-glass request blocker: %v", err)
	}
	defer func() { _ = requestBlocker.Rollback() }()
	if _, err := requestBlocker.Exec(
		"SELECT pg_advisory_lock($1)", concurrentRequestLockKey,
	); err != nil {
		t.Fatalf("acquire concurrent Break-glass request blocker: %v", err)
	}
	type requestResult struct {
		request  breakglass.Request
		replayed bool
		err      error
	}
	concurrentResults := make(chan requestResult, 2)
	concurrentInput := breakglass.RequestInput{
		Target: breakglass.Target{
			OrganizationID: uuid.MustParse(testOrganizationID),
			ProjectID:      uuid.MustParse(testProjectID),
			JobID:          uuid.MustParse(target.JobID),
		},
		Scopes:                   []breakglass.Scope{breakglass.ScopeArtifactRead},
		ReasonCode:               breakglass.ReasonCustomerSupport,
		TicketReference:          "SUPPORT-CONCURRENT-1",
		RequestedDurationSeconds: 900,
	}
	for range 2 {
		go func() {
			request, replayed, requestErr := service.Request(
				context.Background(),
				requester,
				"service-lifecycle-concurrent",
				concurrentInput,
			)
			concurrentResults <- requestResult{
				request: request, replayed: replayed, err: requestErr,
			}
		}()
	}
	concurrencyDeadline := time.Now().Add(6 * time.Second)
	for {
		var waiting int
		if err := database.Admin.QueryRow(`
			SELECT count(*)
			FROM pg_catalog.pg_stat_activity
			WHERE usename = 'vela_break_glass_request_login'
			  AND wait_event_type = 'Lock'
		`).Scan(&waiting); err != nil {
			t.Fatalf("inspect concurrent Break-glass request locks: %v", err)
		}
		if waiting >= 2 {
			break
		}
		if !time.Now().Before(concurrencyDeadline) {
			t.Fatalf("concurrent Break-glass request lock waits = %d, want at least 2", waiting)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var requestUnlocked bool
	if err := requestBlocker.QueryRow(
		"SELECT pg_advisory_unlock($1)", concurrentRequestLockKey,
	).Scan(&requestUnlocked); err != nil || !requestUnlocked {
		t.Fatalf("release concurrent Break-glass request blocker = %t error=%v", requestUnlocked, err)
	}
	if err := requestBlocker.Commit(); err != nil {
		t.Fatalf("commit concurrent Break-glass request blocker: %v", err)
	}
	var concurrentRequestID uuid.UUID
	replayResults := map[bool]int{}
	for range 2 {
		select {
		case result := <-concurrentResults:
			if result.err != nil || result.request.ID == uuid.Nil {
				t.Fatalf("concurrent Break-glass request = %#v", result)
			}
			if concurrentRequestID == uuid.Nil {
				concurrentRequestID = result.request.ID
			} else if result.request.ID != concurrentRequestID {
				t.Fatalf(
					"concurrent Break-glass request IDs = %s and %s",
					concurrentRequestID,
					result.request.ID,
				)
			}
			replayResults[result.replayed]++
		case <-time.After(10 * time.Second):
			t.Fatal("concurrent Break-glass request did not finish")
		}
	}
	if replayResults[false] != 1 || replayResults[true] != 1 {
		t.Fatalf("concurrent Break-glass replay results = %#v", replayResults)
	}
	var concurrentRequests, concurrentEvents int
	if err := database.Admin.QueryRow(`
		SELECT
			count(DISTINCT request.id),
			count(event.id)
		FROM break_glass_requests AS request
		LEFT JOIN break_glass_events AS event
		  ON event.request_id = request.id
		 AND event.action = 'REQUEST_CREATED'
		WHERE request.requester_operator_id = $1
		  AND request.idempotency_key = 'service-lifecycle-concurrent'
	`, requester.ID).Scan(&concurrentRequests, &concurrentEvents); err != nil {
		t.Fatalf("read concurrent Break-glass request effects: %v", err)
	}
	if concurrentRequests != 1 || concurrentEvents != 1 {
		t.Fatalf(
			"concurrent Break-glass request effects = requests %d events %d",
			concurrentRequests,
			concurrentEvents,
		)
	}
	for _, mismatch := range []struct {
		name           string
		organizationID uuid.UUID
		projectID      uuid.UUID
		jobID          uuid.UUID
	}{
		{
			name: "Organization", organizationID: uuid.New(),
			projectID: uuid.MustParse(testProjectID), jobID: uuid.MustParse(target.JobID),
		},
		{
			name: "Project", organizationID: uuid.MustParse(testOrganizationID),
			projectID: uuid.New(), jobID: uuid.MustParse(target.JobID),
		},
		{
			name: "Job", organizationID: uuid.MustParse(testOrganizationID),
			projectID: uuid.MustParse(testProjectID), jobID: uuid.New(),
		},
	} {
		if _, _, err := service.Request(
			context.Background(),
			requester,
			"service-lifecycle-cross-tuple-"+strings.ToLower(mismatch.name),
			breakglass.RequestInput{
				Target: breakglass.Target{
					OrganizationID: mismatch.organizationID,
					ProjectID:      mismatch.projectID,
					JobID:          mismatch.jobID,
				},
				Scopes:                   []breakglass.Scope{breakglass.ScopeArtifactRead},
				ReasonCode:               breakglass.ReasonCustomerSupport,
				TicketReference:          "SUPPORT-2002",
				RequestedDurationSeconds: 900,
			},
		); !breakglass.IsFailure(err, breakglass.FailureNotFound) {
			t.Fatalf("cross-%s Break-glass request error = %v", mismatch.name, err)
		}
	}
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM break_glass_requests
		WHERE idempotency_key LIKE 'service-lifecycle-cross-tuple-%'
	`).Scan(&requestCount); err != nil {
		t.Fatalf("read cross-tuple Break-glass effects: %v", err)
	}
	if requestCount != 0 {
		t.Fatalf("cross-tuple Break-glass request effects = %d", requestCount)
	}
	if _, err := service.Approve(context.Background(), requester, created.ID); !breakglass.IsFailure(
		err, breakglass.FailureForbidden,
	) {
		t.Fatalf("self-approve through service error = %v", err)
	}
	approved, err := service.Approve(context.Background(), approver, created.ID)
	if err != nil || approved.State != breakglass.StateActive || approved.GrantID == nil ||
		approved.ApproverOperatorID == nil || *approved.ApproverOperatorID != approver.ID {
		t.Fatalf("approved Break-glass request = %#v error=%v", approved, err)
	}
	if _, err := service.GetArtifacts(
		context.Background(), requester, *approved.GrantID,
	); !breakglass.IsFailure(err, breakglass.FailureUnavailable) ||
		strings.Contains(strings.ToLower(err.Error()), "not configured") {
		t.Fatalf("unavailable Break-glass Artifact signer error = %v", err)
	}
	revoked, err := service.Revoke(context.Background(), requester, *approved.GrantID)
	if err != nil || revoked.State != breakglass.StateRevoked || revoked.RevokedAt == nil {
		t.Fatalf("revoked Break-glass request = %#v error=%v", revoked, err)
	}
	closedPool := newRolePool(
		t,
		database.DSN,
		"vela_break_glass_request_login",
		"vela-break-glass-request-password",
	)
	closedPool.Close()
	unavailableService, err := breakglass.NewService(closedPool)
	if err != nil {
		t.Fatalf("create unavailable Break-glass service: %v", err)
	}
	if _, _, err := unavailableService.Request(
		context.Background(),
		approver,
		"service-lifecycle-unavailable",
		breakglass.RequestInput{
			Target: breakglass.Target{
				OrganizationID: uuid.MustParse(testOrganizationID),
				ProjectID:      uuid.MustParse(testProjectID),
				JobID:          uuid.MustParse(target.JobID),
			},
			Scopes:                   []breakglass.Scope{breakglass.ScopeArtifactRead},
			ReasonCode:               breakglass.ReasonCustomerSupport,
			TicketReference:          "SUPPORT-2003",
			RequestedDurationSeconds: 900,
		},
	); !breakglass.IsFailure(err, breakglass.FailureUnavailable) ||
		strings.Contains(strings.ToLower(err.Error()), "closed pool") {
		t.Fatalf("unavailable Break-glass database error = %v", err)
	}
}

func TestBreakGlassAccessRevalidatesScopeGrantExpiryAndOperatorStatus(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "break-glass-revalidation-target", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"revalidation target"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit Break-glass revalidation target = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var target jobResponse
	if err := json.Unmarshal(accepted.Body, &target); err != nil {
		t.Fatalf("decode Break-glass revalidation target: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO platform_operator_oidc_bindings (
			id, issuer, subject, display_name
		) VALUES
			($1, 'https://platform-identity.example.com', 'operator-requester', 'Requester'),
			($2, 'https://platform-identity.example.com', 'operator-approver', 'Approver')
	`, platformOperatorRequesterID, platformOperatorApproverID); err != nil {
		t.Fatalf("seed revalidation Platform Operator bindings: %v", err)
	}
	requesterSession, requesterProof := authenticatePlatformOperator(
		t, database.Admin, "operator-requester", "requester-revalidation-token",
	)
	approverSession, approverProof := authenticatePlatformOperator(
		t, database.Admin, "operator-approver", "approver-revalidation-token",
	)
	requester := breakglass.NewOperatorForSession(
		uuid.MustParse(platformOperatorRequesterID), requesterSession, requesterProof,
	)
	approver := breakglass.NewOperatorForSession(
		uuid.MustParse(platformOperatorApproverID), approverSession, approverProof,
	)
	service, err := breakglass.NewService(newRolePool(
		t,
		database.DSN,
		"vela_break_glass_request_login",
		"vela-break-glass-request-password",
	))
	if err != nil {
		t.Fatalf("create Break-glass revalidation service: %v", err)
	}

	artifactOnly, _, err := service.Request(
		context.Background(),
		requester,
		"revalidation-artifact-only",
		breakglass.RequestInput{
			Target: breakglass.Target{
				OrganizationID: uuid.MustParse(testOrganizationID),
				ProjectID:      uuid.MustParse(testProjectID),
				JobID:          uuid.MustParse(target.JobID),
			},
			Scopes:                   []breakglass.Scope{breakglass.ScopeArtifactRead},
			ReasonCode:               breakglass.ReasonCustomerSupport,
			TicketReference:          "SUPPORT-5001",
			RequestedDurationSeconds: 600,
		},
	)
	if err != nil {
		t.Fatalf("create Artifact-only Break-glass request: %v", err)
	}
	artifactOnly, err = service.Approve(context.Background(), approver, artifactOnly.ID)
	if err != nil || artifactOnly.GrantID == nil {
		t.Fatalf("approve Artifact-only Break-glass request = %#v error=%v", artifactOnly, err)
	}
	if _, err := service.ReadRequestContent(
		context.Background(), requester, *artifactOnly.GrantID,
	); !breakglass.IsFailure(err, breakglass.FailureForbidden) {
		t.Fatalf("request-content access without scope error = %v", err)
	}
	var scopeDeniedEvents int
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM break_glass_events
		WHERE grant_id = $1
		  AND action = 'REQUEST_CONTENT_DENIED'
		  AND scope = 'REQUEST_CONTENT_READ'
		  AND outcome_code = 'SCOPE_DENIED'
	`, *artifactOnly.GrantID).Scan(&scopeDeniedEvents); err != nil {
		t.Fatalf("read missing-scope denial event: %v", err)
	}
	if scopeDeniedEvents != 1 {
		t.Fatalf("missing-scope denial events = %d, want 1", scopeDeniedEvents)
	}

	expiredRequestID := uuid.New()
	expiredGrantID := uuid.New()
	expiredRequestedAt := time.Now().UTC().Add(-3 * time.Hour)
	expiredApprovedAt := expiredRequestedAt.Add(30 * time.Minute)
	if _, err := database.Admin.Exec(`
		INSERT INTO break_glass_requests (
			id, organization_id, project_id, job_id, scopes, reason_code,
			ticket_reference, requested_duration_seconds, requester_operator_id,
			requester_session_id, idempotency_key, request_hash, requested_at,
			approval_deadline_at
		) VALUES (
			$1, $2, $3, $4, ARRAY['REQUEST_CONTENT_READ']::break_glass_scope[],
			'CUSTOMER_SUPPORT', 'SUPPORT-5002', 3600, $5, $6,
			'revalidation-expired-grant', decode(repeat('02', 32), 'hex'), $7, $8
		)
	`,
		expiredRequestID,
		testOrganizationID,
		testProjectID,
		target.JobID,
		platformOperatorRequesterID,
		requesterSession,
		expiredRequestedAt,
		expiredRequestedAt.Add(time.Hour),
	); err != nil {
		t.Fatalf("seed expired Break-glass request: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO break_glass_grants (
			id, request_id, organization_id, project_id, job_id,
			approver_operator_id, approver_session_id, approved_at, expires_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`,
		expiredGrantID,
		expiredRequestID,
		testOrganizationID,
		testProjectID,
		target.JobID,
		platformOperatorApproverID,
		approverSession,
		expiredApprovedAt,
		expiredApprovedAt.Add(time.Hour),
	); err != nil {
		t.Fatalf("seed expired Break-glass grant: %v", err)
	}
	if _, err := service.ReadRequestContent(
		context.Background(), requester, expiredGrantID,
	); !breakglass.IsFailure(err, breakglass.FailureForbidden) {
		t.Fatalf("request-content access with expired grant error = %v", err)
	}
	var expiredGrantEvents int
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM break_glass_events
		WHERE grant_id = $1
		  AND action = 'REQUEST_CONTENT_DENIED'
		  AND outcome_code = 'GRANT_EXPIRED'
	`, expiredGrantID).Scan(&expiredGrantEvents); err != nil {
		t.Fatalf("read expired-grant denial event: %v", err)
	}
	if expiredGrantEvents != 1 {
		t.Fatalf("expired-grant denial events = %d, want 1", expiredGrantEvents)
	}
	if _, err := service.Revoke(
		context.Background(), requester, expiredGrantID,
	); !breakglass.IsFailure(err, breakglass.FailureForbidden) {
		t.Fatalf("expired Break-glass grant revocation error = %v, want forbidden", err)
	}

	active, _, err := service.Request(
		context.Background(),
		requester,
		"revalidation-disabled-operator",
		breakglass.RequestInput{
			Target: breakglass.Target{
				OrganizationID: uuid.MustParse(testOrganizationID),
				ProjectID:      uuid.MustParse(testProjectID),
				JobID:          uuid.MustParse(target.JobID),
			},
			Scopes:                   []breakglass.Scope{breakglass.ScopeRequestContentRead},
			ReasonCode:               breakglass.ReasonCustomerSupport,
			TicketReference:          "SUPPORT-5003",
			RequestedDurationSeconds: 600,
		},
	)
	if err != nil {
		t.Fatalf("create disabled-operator Break-glass request: %v", err)
	}
	active, err = service.Approve(context.Background(), approver, active.ID)
	if err != nil || active.GrantID == nil {
		t.Fatalf("approve disabled-operator Break-glass request = %#v error=%v", active, err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE platform_operator_oidc_bindings
		SET disabled_at = clock_timestamp()
		WHERE id = $1
	`, requester.ID); err != nil {
		t.Fatalf("disable active Platform Operator: %v", err)
	}
	if _, err := service.ReadRequestContent(
		context.Background(), requester, *active.GrantID,
	); !breakglass.IsFailure(err, breakglass.FailureUnauthorized) {
		t.Fatalf("disabled Platform Operator access error = %v", err)
	}
}

func TestBreakGlassContentAccessIsScopedAuditedAndGrantBounded(t *testing.T) {
	fixture := newStartFixture(t, "break-glass-content-access", 17)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start Break-glass target = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization Break-glass target = %#v error=%v", plan, err)
	}
	completionService := visibleCompletionService(t, fixture.database.DSN)
	artifactIDs := uploadAndVerifyFinalizationPlan(
		t, completionService, fixture.worker, fixture.credentials, plan,
	)
	completed, err := completionService.CompleteVisibleCompletion(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		workercontrol.VisibleCompletionCandidate{
			CompletionID:       uuid.New(),
			ExpectedJobVersion: plan.JobVersion,
			ArtifactIDs:        artifactIDs,
		},
	)
	if err != nil || completed.Decision != workercontrol.VisibleCompletionCommitted {
		t.Fatalf("CompleteVisibleCompletion Break-glass target = %#v error=%v", completed, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO platform_operator_oidc_bindings (
			id, issuer, subject, display_name
		) VALUES
			($1, 'https://platform-identity.example.com', 'operator-requester', 'Requester'),
			($2, 'https://platform-identity.example.com', 'operator-approver', 'Approver')
	`, platformOperatorRequesterID, platformOperatorApproverID); err != nil {
		t.Fatalf("seed Platform Operator bindings: %v", err)
	}
	requesterSession, requesterProof := authenticatePlatformOperator(
		t, fixture.database.Admin, "operator-requester", "requester-content-token",
	)
	approverSession, approverProof := authenticatePlatformOperator(
		t, fixture.database.Admin, "operator-approver", "approver-content-token",
	)
	requester := breakglass.NewOperatorForSession(
		uuid.MustParse(platformOperatorRequesterID), requesterSession, requesterProof,
	)
	approver := breakglass.NewOperatorForSession(
		uuid.MustParse(platformOperatorApproverID), approverSession, approverProof,
	)
	signer := &grantBoundedArtifactSigner{}
	service, err := breakglass.NewService(
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_break_glass_request_login",
			"vela-break-glass-request-password",
		),
		signer,
	)
	if err != nil {
		t.Fatalf("create Break-glass content service: %v", err)
	}
	unknownContentGrantID := uuid.New()
	if _, err := service.ReadRequestContent(
		context.Background(), requester, unknownContentGrantID,
	); !breakglass.IsFailure(err, breakglass.FailureNotFound) {
		t.Fatalf("unknown request-content grant error = %v", err)
	}
	unknownArtifactGrantID := uuid.New()
	if _, err := service.GetArtifacts(
		context.Background(), requester, unknownArtifactGrantID,
	); !breakglass.IsFailure(err, breakglass.FailureNotFound) {
		t.Fatalf("unknown Artifact grant error = %v", err)
	}
	var unknownDenials int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*)
		FROM break_glass_denial_events
		WHERE operator_id = $1
		  AND (
			(attempted_grant_id = $2
			 AND action = 'REQUEST_CONTENT_DENIED'
			 AND scope = 'REQUEST_CONTENT_READ')
			OR
			(attempted_grant_id = $3
			 AND action = 'ARTIFACT_DENIED'
			 AND scope = 'ARTIFACT_READ')
		  )
		  AND outcome_code = 'TARGET_NOT_FOUND'
	`, requester.ID, unknownContentGrantID, unknownArtifactGrantID).Scan(&unknownDenials); err != nil {
		t.Fatalf("read unknown-grant denial evidence: %v", err)
	}
	if unknownDenials != 2 {
		t.Fatalf("unknown-grant denial events = %d, want 2", unknownDenials)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE break_glass_denial_events
		SET attempted_grant_id = $2
		WHERE attempted_grant_id = $1
	`, unknownContentGrantID, uuid.New()); err == nil {
		t.Fatal("immutable unknown-grant denial evidence accepted mutation")
	} else {
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != "55000" {
			t.Fatalf("unknown-grant denial evidence mutation error = %v", err)
		}
	}
	created, _, err := service.Request(
		context.Background(),
		requester,
		"content-access-001",
		breakglass.RequestInput{
			Target: breakglass.Target{
				OrganizationID: uuid.MustParse(testOrganizationID),
				ProjectID:      uuid.MustParse(testProjectID),
				JobID:          fixture.assignment.JobID,
			},
			Scopes: []breakglass.Scope{
				breakglass.ScopeRequestContentRead,
				breakglass.ScopeArtifactRead,
			},
			ReasonCode:               breakglass.ReasonCustomerSupport,
			TicketReference:          "SUPPORT-3001",
			RequestedDurationSeconds: 600,
		},
	)
	if err != nil {
		t.Fatalf("create Break-glass content request: %v", err)
	}
	approved, err := service.Approve(context.Background(), approver, created.ID)
	if err != nil || approved.GrantID == nil || approved.ExpiresAt == nil {
		t.Fatalf("approve Break-glass content request = %#v error=%v", approved, err)
	}
	content, err := service.ReadRequestContent(
		context.Background(), requester, *approved.GrantID,
	)
	if err != nil || content.JobID != fixture.assignment.JobID ||
		!bytes.Contains(content.RequestContent, []byte("exercise Assignment")) {
		t.Fatalf("Break-glass request content = %#v error=%v", content, err)
	}
	artifacts, err := service.GetArtifacts(
		context.Background(), requester, *approved.GrantID,
	)
	if err != nil || artifacts.ID != completed.ArtifactSetID ||
		len(artifacts.Artifacts) != len(completed.Artifacts) ||
		len(signer.calls) != len(completed.Artifacts) {
		t.Fatalf("Break-glass Artifacts = %#v calls=%#v error=%v", artifacts, signer.calls, err)
	}
	for _, call := range signer.calls {
		if !call.notAfter.Equal(*approved.ExpiresAt) {
			t.Fatalf("Break-glass signer deadline = %s, want grant expiry %s", call.notAfter, *approved.ExpiresAt)
		}
	}
	if _, err := service.Revoke(context.Background(), approver, *approved.GrantID); err != nil {
		t.Fatalf("revoke Break-glass content grant: %v", err)
	}
	if _, err := service.ReadRequestContent(
		context.Background(), requester, *approved.GrantID,
	); !breakglass.IsFailure(err, breakglass.FailureForbidden) {
		t.Fatalf("request content after revocation error = %v", err)
	}
	if _, err := service.GetArtifacts(
		context.Background(), requester, *approved.GrantID,
	); !breakglass.IsFailure(err, breakglass.FailureForbidden) {
		t.Fatalf("Artifact access after revocation error = %v", err)
	}
	var allowed, denied int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			count(*) FILTER (WHERE action IN (
				'REQUEST_CONTENT_AUTHORIZED', 'ARTIFACT_AUTHORIZED', 'ARTIFACT_DELIVERED'
			)),
			count(*) FILTER (WHERE action IN ('REQUEST_CONTENT_DENIED', 'ARTIFACT_DENIED'))
		FROM break_glass_events
		WHERE grant_id = $1
	`, *approved.GrantID).Scan(&allowed, &denied); err != nil {
		t.Fatalf("read Break-glass access events: %v", err)
	}
	if allowed != 3 || denied != 2 {
		t.Fatalf("Break-glass access events = allowed %d denied %d", allowed, denied)
	}

	auditorID := uuid.New()
	const auditorSubject = "break-glass-organization-auditor"
	seedHumanRoleFixture(
		t,
		fixture.database.Admin,
		auditorID,
		auditorSubject,
		[]string{"OrganizationAuditor"},
		nil,
	)
	auditorAuthenticator := newHumanMembershipAuthenticator(
		t,
		fixture.database,
		newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_human_auth_login",
			"vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   auditorSubject,
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	auditor, err := auditorAuthenticator.Authenticate(
		context.Background(), "break-glass-auditor-token",
	)
	if err != nil {
		t.Fatalf("authenticate Break-glass OrganizationAuditor: %v", err)
	}
	auditor, ok := auditor.ForOrganization(uuid.MustParse(testOrganizationID))
	if !ok || !auditor.HasScope(identity.ScopeOrganizationAuditRead) {
		t.Fatalf("Break-glass OrganizationAuditor scopes = %v", auditor.Scopes)
	}
	reportingService, err := organizationreporting.NewService(
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_organization_billing_request_login",
			"vela-organization-billing-request-password",
		),
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_organization_audit_request_login",
			"vela-organization-audit-request-password",
		),
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_break_glass_audit_request_login",
			"vela-break-glass-audit-request-password",
		),
	)
	if err != nil {
		t.Fatalf("create Break-glass Organization reporting service: %v", err)
	}
	auditEvents, err := reportingService.ListAuditEvents(
		context.Background(), auditor, uuid.MustParse(testOrganizationID), 100,
	)
	if err != nil {
		t.Fatalf("list Break-glass Organization audit events: %v", err)
	}
	expectedActions := map[string]struct {
		found       bool
		outcomeCode string
	}{
		"REQUEST_CREATED/REQUEST_CONTENT_READ": {
			outcomeCode: "CREATED",
		},
		"REQUEST_CREATED/ARTIFACT_READ": {
			outcomeCode: "CREATED",
		},
		"GRANT_APPROVED/REQUEST_CONTENT_READ": {
			outcomeCode: "APPROVED",
		},
		"GRANT_APPROVED/ARTIFACT_READ": {
			outcomeCode: "APPROVED",
		},
		"REQUEST_CONTENT_AUTHORIZED/REQUEST_CONTENT_READ": {
			outcomeCode: "ALLOWED",
		},
		"ARTIFACT_AUTHORIZED/ARTIFACT_READ": {
			outcomeCode: "ALLOWED",
		},
		"ARTIFACT_DELIVERED/ARTIFACT_READ": {
			outcomeCode: "DELIVERED",
		},
		"GRANT_REVOKED/REQUEST_CONTENT_READ": {
			outcomeCode: "REVOKED",
		},
		"GRANT_REVOKED/ARTIFACT_READ": {
			outcomeCode: "REVOKED",
		},
		"REQUEST_CONTENT_DENIED/REQUEST_CONTENT_READ": {
			outcomeCode: "GRANT_REVOKED",
		},
		"ARTIFACT_DENIED/ARTIFACT_READ": {
			outcomeCode: "GRANT_REVOKED",
		},
	}
	breakGlassAuditEvents := make([]organizationreporting.AuditEvent, 0, len(expectedActions))
	for _, event := range auditEvents {
		if event.Source != "BREAK_GLASS" {
			continue
		}
		breakGlassAuditEvents = append(breakGlassAuditEvents, event)
		if event.Scope == nil {
			t.Fatalf("Break-glass Organization audit event lacks scope = %#v", event)
		}
		key := event.Action + "/" + *event.Scope
		expected, ok := expectedActions[key]
		if !ok {
			t.Fatalf("unexpected Break-glass Organization audit action = %#v", event)
		}
		expected.found = true
		expectedActions[key] = expected
		if event.OutcomeCode == nil || *event.OutcomeCode != expected.outcomeCode {
			t.Fatalf("Break-glass Organization audit scope/outcome = %#v", event)
		}
		if event.ProjectID == nil || *event.ProjectID != uuid.MustParse(testProjectID) ||
			event.TargetKind != "JOB" || event.TargetID != fixture.assignment.JobID ||
			(event.ActorPrincipalID != requester.ID && event.ActorPrincipalID != approver.ID) ||
			(event.ActorSessionID != requesterSession && event.ActorSessionID != approverSession) {
			t.Fatalf("unsafe or misattributed Break-glass Organization audit event = %#v", event)
		}
	}
	for action, expected := range expectedActions {
		if !expected.found {
			t.Fatalf("Break-glass Organization audit event %s is missing: %#v", action, auditEvents)
		}
	}
	safeProjection, err := json.Marshal(breakGlassAuditEvents)
	if err != nil {
		t.Fatalf("encode Break-glass Organization audit projection: %v", err)
	}
	for _, forbidden := range [][]byte{
		[]byte("SUPPORT-3001"),
		[]byte("exercise Assignment"),
		[]byte("artifacts.example.com"),
	} {
		if bytes.Contains(safeProjection, forbidden) {
			t.Fatalf("Break-glass Organization audit projection leaked %q: %s", forbidden, safeProjection)
		}
	}
	for _, call := range signer.calls {
		if bytes.Contains(safeProjection, []byte(call.objectKey)) ||
			bytes.Contains(safeProjection, []byte(call.versionID)) {
			t.Fatalf("Break-glass Organization audit projection leaked storage identity: %s", safeProjection)
		}
	}
}

func TestBreakGlassContentLifecycleAndSigningFailuresRemainFailClosed(t *testing.T) {
	fixture := newStartFixture(t, "break-glass-content-lifecycle", 19)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start Break-glass lifecycle target = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization Break-glass lifecycle target = %#v error=%v", plan, err)
	}
	completionService := visibleCompletionService(t, fixture.database.DSN)
	artifactIDs := uploadAndVerifyFinalizationPlan(
		t, completionService, fixture.worker, fixture.credentials, plan,
	)
	completed, err := completionService.CompleteVisibleCompletion(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		workercontrol.VisibleCompletionCandidate{
			CompletionID:       uuid.New(),
			ExpectedJobVersion: plan.JobVersion,
			ArtifactIDs:        artifactIDs,
		},
	)
	if err != nil || completed.Decision != workercontrol.VisibleCompletionCommitted {
		t.Fatalf("CompleteVisibleCompletion Break-glass lifecycle target = %#v error=%v", completed, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO platform_operator_oidc_bindings (
			id, issuer, subject, display_name
		) VALUES
			($1, 'https://platform-identity.example.com', 'operator-requester', 'Requester'),
			($2, 'https://platform-identity.example.com', 'operator-approver', 'Approver')
	`, platformOperatorRequesterID, platformOperatorApproverID); err != nil {
		t.Fatalf("seed lifecycle Platform Operator bindings: %v", err)
	}
	requesterSession, requesterProof := authenticatePlatformOperator(
		t, fixture.database.Admin, "operator-requester", "requester-lifecycle-token",
	)
	approverSession, approverProof := authenticatePlatformOperator(
		t, fixture.database.Admin, "operator-approver", "approver-lifecycle-token",
	)
	requester := breakglass.NewOperatorForSession(
		uuid.MustParse(platformOperatorRequesterID), requesterSession, requesterProof,
	)
	approver := breakglass.NewOperatorForSession(
		uuid.MustParse(platformOperatorApproverID), approverSession, approverProof,
	)
	requestPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_break_glass_request_login",
		"vela-break-glass-request-password",
	)
	failingService, err := breakglass.NewService(
		requestPool,
		failingArtifactSigner{err: errors.New("injected exact-version signing failure")},
	)
	if err != nil {
		t.Fatalf("create failing Break-glass Artifact service: %v", err)
	}
	created, _, err := failingService.Request(
		context.Background(),
		requester,
		"content-lifecycle-001",
		breakglass.RequestInput{
			Target: breakglass.Target{
				OrganizationID: uuid.MustParse(testOrganizationID),
				ProjectID:      uuid.MustParse(testProjectID),
				JobID:          fixture.assignment.JobID,
			},
			Scopes: []breakglass.Scope{
				breakglass.ScopeRequestContentRead,
				breakglass.ScopeArtifactRead,
			},
			ReasonCode:               breakglass.ReasonCustomerSupport,
			TicketReference:          "SUPPORT-6001",
			RequestedDurationSeconds: 900,
		},
	)
	if err != nil {
		t.Fatalf("create lifecycle Break-glass request: %v", err)
	}
	approved, err := failingService.Approve(context.Background(), approver, created.ID)
	if err != nil || approved.GrantID == nil {
		t.Fatalf("approve lifecycle Break-glass request = %#v error=%v", approved, err)
	}
	if _, err := failingService.GetArtifacts(
		context.Background(), requester, *approved.GrantID,
	); !breakglass.IsFailure(err, breakglass.FailureUnavailable) ||
		strings.Contains(err.Error(), "injected exact-version signing failure") {
		t.Fatalf("injected Break-glass signing failure error = %v", err)
	}
	var signingFailureEvents int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*)
		FROM break_glass_events
		WHERE grant_id = $1
		  AND action = 'ARTIFACT_DELIVERY_FAILED'
		  AND scope = 'ARTIFACT_READ'
		  AND outcome_code = 'SIGNING_FAILED'
	`, *approved.GrantID).Scan(&signingFailureEvents); err != nil {
		t.Fatalf("read Break-glass signing-failure event: %v", err)
	}
	if signingFailureEvents != 1 {
		t.Fatalf("Break-glass signing-failure events = %d, want 1", signingFailureEvents)
	}

	service, err := breakglass.NewService(requestPool, &grantBoundedArtifactSigner{})
	if err != nil {
		t.Fatalf("create lifecycle Break-glass service: %v", err)
	}
	var lifecycleArtifactID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT artifact_id
		FROM artifact_set_items
		WHERE artifact_set_id = $1
		ORDER BY kind, ordinal
		LIMIT 1
	`, completed.ArtifactSetID).Scan(&lifecycleArtifactID); err != nil {
		t.Fatalf("select lifecycle Artifact: %v", err)
	}
	lifecycleRaceSigner := &grantBoundedArtifactSigner{
		beforeFirstSign: func() {
			result, updateErr := fixture.database.Admin.Exec(`
				UPDATE artifacts SET state = 'EXPIRED' WHERE id = $1
			`, lifecycleArtifactID)
			if updateErr != nil {
				t.Fatalf("expire Artifact during Break-glass signing: %v", updateErr)
			}
			if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
				t.Fatalf("expire Artifact during Break-glass signing rows = %d error=%v", rows, rowsErr)
			}
		},
	}
	lifecycleRaceService, err := breakglass.NewService(requestPool, lifecycleRaceSigner)
	if err != nil {
		t.Fatalf("create lifecycle-race Break-glass service: %v", err)
	}
	if _, err := lifecycleRaceService.GetArtifacts(
		context.Background(), requester, *approved.GrantID,
	); !breakglass.IsFailure(err, breakglass.FailureForbidden) {
		t.Fatalf("Break-glass delivery after Artifact lifecycle race error = %v", err)
	}
	if !lifecycleRaceSigner.firstSignHookRan {
		t.Fatal("Break-glass lifecycle race did not execute during signing")
	}
	var lifecycleRaceEvents, lifecycleRaceDeliveries int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			count(*) FILTER (
				WHERE action = 'ARTIFACT_DELIVERY_FAILED'
				  AND outcome_code = 'CONTENT_UNAVAILABLE'
			),
			count(*) FILTER (WHERE action = 'ARTIFACT_DELIVERED')
		FROM break_glass_events
		WHERE grant_id = $1
	`, *approved.GrantID).Scan(&lifecycleRaceEvents, &lifecycleRaceDeliveries); err != nil {
		t.Fatalf("read Break-glass lifecycle-race delivery evidence: %v", err)
	}
	if lifecycleRaceEvents != 1 || lifecycleRaceDeliveries != 0 {
		t.Fatalf(
			"Break-glass lifecycle-race events = failed %d delivered %d, want 1/0",
			lifecycleRaceEvents,
			lifecycleRaceDeliveries,
		)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE artifacts SET state = 'COMMITTED' WHERE id = $1
	`, lifecycleArtifactID); err != nil {
		t.Fatalf("restore lifecycle Artifact after signing race: %v", err)
	}

	authorizationTx := beginBreakGlassTransaction(
		t, fixture.database.Admin, requesterSession, requesterProof,
	)
	defer func() { _ = authorizationTx.Rollback() }()
	var deliveryAuthorizationEventID uuid.UUID
	var deliveryAuthorized bool
	if err := authorizationTx.QueryRow(`
		SELECT authorized, event_id
		FROM vela_authorize_break_glass_artifacts($1)
		LIMIT 1
	`, *approved.GrantID).Scan(
		&deliveryAuthorized,
		&deliveryAuthorizationEventID,
	); err != nil {
		t.Fatalf("authorize Break-glass Artifact lock fixture: %v", err)
	}
	if !deliveryAuthorized || deliveryAuthorizationEventID == uuid.Nil {
		t.Fatalf(
			"Break-glass Artifact lock authorization = authorized %t event %s",
			deliveryAuthorized,
			deliveryAuthorizationEventID,
		)
	}
	if err := authorizationTx.Commit(); err != nil {
		t.Fatalf("commit Break-glass Artifact lock authorization: %v", err)
	}

	deliveryTx := beginBreakGlassTransaction(
		t, fixture.database.Admin, requesterSession, requesterProof,
	)
	defer func() { _ = deliveryTx.Rollback() }()
	var delivered bool
	var deliveryOutcome string
	var deliveryEventID uuid.UUID
	if err := deliveryTx.QueryRow(`
		SELECT delivered, outcome_code, event_id
		FROM vela_record_break_glass_artifact_delivery($1, true)
	`, deliveryAuthorizationEventID).Scan(
		&delivered,
		&deliveryOutcome,
		&deliveryEventID,
	); err != nil {
		t.Fatalf("record Break-glass Artifact lock delivery: %v", err)
	}
	if !delivered || deliveryOutcome != "DELIVERED" || deliveryEventID == uuid.Nil {
		t.Fatalf(
			"Break-glass Artifact lock delivery = delivered %t outcome %s event %s",
			delivered,
			deliveryOutcome,
			deliveryEventID,
		)
	}
	internalConnection := newRoleConnection(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	type lifecycleMutationResult struct {
		rows int64
		err  error
	}
	lifecycleMutationResults := make(chan lifecycleMutationResult, 1)
	go func() {
		command, updateErr := internalConnection.Exec(context.Background(), `
			UPDATE artifacts SET state = 'EXPIRED' WHERE id = $1
		`, lifecycleArtifactID)
		lifecycleMutationResults <- lifecycleMutationResult{
			rows: command.RowsAffected(),
			err:  updateErr,
		}
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_internal_login")
	if err := deliveryTx.Commit(); err != nil {
		t.Fatalf("commit Break-glass Artifact delivery while lifecycle waits: %v", err)
	}
	mutation := <-lifecycleMutationResults
	if mutation.err != nil || mutation.rows != 1 {
		t.Fatalf(
			"Artifact lifecycle mutation after Break-glass delivery = rows %d error=%v",
			mutation.rows,
			mutation.err,
		)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE artifacts SET state = 'COMMITTED' WHERE id = $1
	`, lifecycleArtifactID); err != nil {
		t.Fatalf("restore lifecycle Artifact after delivery lock fixture: %v", err)
	}
	for _, state := range []string{"EXPIRED", "DELETED"} {
		if _, err := fixture.database.Admin.Exec(`
			UPDATE artifacts SET state = $2::artifact_state WHERE id = $1
		`, lifecycleArtifactID, state); err != nil {
			t.Fatalf("mark lifecycle Artifact %s: %v", state, err)
		}
		if _, err := service.GetArtifacts(
			context.Background(), requester, *approved.GrantID,
		); !breakglass.IsFailure(err, breakglass.FailureForbidden) {
			t.Fatalf("Break-glass access with %s Artifact error = %v", state, err)
		}
		var unavailableEvents int
		if err := fixture.database.Admin.QueryRow(`
			SELECT count(*)
			FROM break_glass_events
			WHERE grant_id = $1
			  AND action = 'ARTIFACT_DENIED'
			  AND outcome_code = 'CONTENT_UNAVAILABLE'
		`, *approved.GrantID).Scan(&unavailableEvents); err != nil {
			t.Fatalf("read %s Artifact denial event: %v", state, err)
		}
		if unavailableEvents == 0 {
			t.Fatalf("%s Artifact denial event is missing", state)
		}
		if _, err := fixture.database.Admin.Exec(`
			UPDATE artifacts SET state = 'COMMITTED' WHERE id = $1
		`, lifecycleArtifactID); err != nil {
			t.Fatalf("restore lifecycle Artifact after %s fixture: %v", state, err)
		}
	}
	if _, err := fixture.database.Admin.Exec(`
		ALTER TABLE artifact_sets DISABLE TRIGGER artifact_sets_immutable;
		UPDATE artifact_sets
		SET committed_at = clock_timestamp() - interval '2 hours',
			retention_expires_at = clock_timestamp() - interval '1 hour'
		WHERE id = '` + completed.ArtifactSetID.String() + `';
		ALTER TABLE artifact_sets ENABLE TRIGGER artifact_sets_immutable;
	`); err != nil {
		t.Fatalf("expire lifecycle ArtifactSet fixture: %v", err)
	}
	if _, err := service.GetArtifacts(
		context.Background(), requester, *approved.GrantID,
	); !breakglass.IsFailure(err, breakglass.FailureForbidden) {
		t.Fatalf("Break-glass access with expired ArtifactSet error = %v", err)
	}

	tombstone, err := fixture.database.Admin.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin request-content tombstone fixture: %v", err)
	}
	defer func() { _ = tombstone.Rollback() }()
	if _, err := tombstone.Exec("SET LOCAL ROLE vela_retention_owner"); err != nil {
		t.Fatalf("assume retention owner for tombstone fixture: %v", err)
	}
	if _, err := tombstone.Exec(`
		UPDATE jobs
		SET request_content = '{"deleted":true}'::jsonb,
			request_content_deleted_at = clock_timestamp(),
			updated_at = clock_timestamp()
		WHERE id = $1
	`, fixture.assignment.JobID); err != nil {
		t.Fatalf("tombstone Break-glass request content fixture: %v", err)
	}
	if err := tombstone.Commit(); err != nil {
		t.Fatalf("commit request-content tombstone fixture: %v", err)
	}
	if _, err := service.ReadRequestContent(
		context.Background(), requester, *approved.GrantID,
	); !breakglass.IsFailure(err, breakglass.FailureForbidden) {
		t.Fatalf("Break-glass access to deleted request content error = %v", err)
	}
	var deletedContentEvents int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*)
		FROM break_glass_events
		WHERE grant_id = $1
		  AND action = 'REQUEST_CONTENT_DENIED'
		  AND outcome_code = 'CONTENT_DELETED'
	`, *approved.GrantID).Scan(&deletedContentEvents); err != nil {
		t.Fatalf("read deleted request-content denial event: %v", err)
	}
	if deletedContentEvents != 1 {
		t.Fatalf("deleted request-content denial events = %d, want 1", deletedContentEvents)
	}
}

func TestBreakGlassHTTPUsesPlatformIdentityAndHidesStorageIdentity(t *testing.T) {
	fixture := newStartFixture(t, "break-glass-http", 18)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start Break-glass HTTP target = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization Break-glass HTTP target = %#v error=%v", plan, err)
	}
	completionService := visibleCompletionService(t, fixture.database.DSN)
	artifactIDs := uploadAndVerifyFinalizationPlan(
		t, completionService, fixture.worker, fixture.credentials, plan,
	)
	completed, err := completionService.CompleteVisibleCompletion(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		workercontrol.VisibleCompletionCandidate{
			CompletionID:       uuid.New(),
			ExpectedJobVersion: plan.JobVersion,
			ArtifactIDs:        artifactIDs,
		},
	)
	if err != nil || completed.Decision != workercontrol.VisibleCompletionCommitted {
		t.Fatalf("CompleteVisibleCompletion Break-glass HTTP target = %#v error=%v", completed, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO platform_operator_oidc_bindings (
			id, issuer, subject, display_name
		) VALUES
			($1, 'https://platform-identity.example.com', 'operator-requester', 'Requester'),
			($2, 'https://platform-identity.example.com', 'operator-approver', 'Approver')
	`, platformOperatorRequesterID, platformOperatorApproverID); err != nil {
		t.Fatalf("seed Platform Operator HTTP bindings: %v", err)
	}
	operatorVerifier := tokenOIDCVerifier{
		"operator-requester-http-token": {
			Issuer:    "https://platform-identity.example.com",
			Subject:   "operator-requester",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		},
		"operator-approver-http-token": {
			Issuer:    "https://platform-identity.example.com",
			Subject:   "operator-approver",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		},
	}
	humanVerifier := tokenOIDCVerifier{}
	for _, human := range []struct {
		role              string
		organizationRoles []string
		projectRoles      map[string][]string
	}{
		{role: "OrganizationOwner", organizationRoles: []string{"OrganizationOwner"}},
		{role: "BillingAdmin", organizationRoles: []string{"BillingAdmin"}},
		{role: "OrganizationAuditor", organizationRoles: []string{"OrganizationAuditor"}},
		{role: "ProjectAdmin", projectRoles: map[string][]string{testProjectID: {"ProjectAdmin"}}},
		{role: "Developer", projectRoles: map[string][]string{testProjectID: {"Developer"}}},
		{role: "ProjectViewer", projectRoles: map[string][]string{testProjectID: {"ProjectViewer"}}},
	} {
		subject := "break-glass-http-" + strings.ToLower(human.role)
		seedHumanRoleFixture(
			t,
			fixture.database.Admin,
			uuid.New(),
			subject,
			human.organizationRoles,
			human.projectRoles,
		)
		humanVerifier[subject+"-token"] = identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   subject,
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}
	}
	httpSigner := &grantBoundedArtifactSigner{}
	breakGlassService, err := breakglass.NewService(
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_break_glass_request_login",
			"vela-break-glass-request-password",
		),
		httpSigner,
	)
	if err != nil {
		t.Fatalf("create Break-glass HTTP service: %v", err)
	}
	authPool := newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password")
	humanAuthPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_human_auth_login",
		"vela-human-auth-password",
	)
	humanMembershipAuthPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_human_membership_auth_login",
		"vela-human-membership-auth-password",
	)
	platformAuthPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_platform_operator_auth_login",
		"vela-platform-operator-auth-password",
	)
	requestPool := newRolePool(
		t, fixture.database.DSN, "vela_request_login", "vela-request-password",
	)
	artifactPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_artifact_request_login",
		"vela-artifact-request-password",
	)
	cancelPool := newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password")
	internalPool := newRolePool(
		t, fixture.database.DSN, "vela_internal_login", "vela-internal-password",
	)
	webhookRequestPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_webhook_request_login",
		"vela-webhook-request-password",
	)
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator: identity.NewAuthenticatorWithHumanMembershipOIDC(
			authPool,
			humanAuthPool,
			humanMembershipAuthPool,
			testCredentialPepper,
			humanVerifier,
		),
		PlatformAuthenticator:  breakglass.NewAuthenticator(platformAuthPool, operatorVerifier),
		BreakGlass:             breakGlassService,
		Remediation:            &remediation.Service{},
		IdentityAdministration: &identity.AdministrationService{},
		OrganizationReporting:  &organizationreporting.Service{},
		Retention:              &retention.Service{},
		Admission:              admission.NewLegacyService(requestPool),
		Cancellation:           cancellation.NewService(cancelPool, internalPool),
		Artifacts:              artifactaccess.NewService(artifactPool, &recordingArtifactSigner{}),
		Webhooks:               testWebhookService(t, webhookRequestPool),
	})
	if err != nil {
		t.Fatalf("create Break-glass HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	requestBody := []byte(fmt.Sprintf(`{
		"organization_id":%q,
		"project_id":%q,
		"job_id":%q,
		"scopes":["REQUEST_CONTENT_READ","ARTIFACT_READ"],
		"reason_code":"CUSTOMER_SUPPORT",
		"ticket_reference":"SUPPORT-4001",
		"requested_duration_seconds":600
	}`, testOrganizationID, testProjectID, fixture.assignment.JobID))
	customerResponse := doBreakGlassHTTP(
		t,
		http.MethodPost,
		server.URL+"/v1/platform/break-glass/requests",
		testBearerCredential(),
		"http-request-001",
		requestBody,
	)
	if customerResponse.StatusCode != http.StatusUnauthorized {
		t.Fatalf(
			"customer credential on platform path = %d body=%s",
			customerResponse.StatusCode,
			customerResponse.Body,
		)
	}
	for token := range humanVerifier {
		humanResponse := doBreakGlassHTTP(
			t,
			http.MethodPost,
			server.URL+"/v1/platform/break-glass/requests",
			token,
			"http-human-denied-"+token,
			requestBody,
		)
		if humanResponse.StatusCode != http.StatusUnauthorized {
			t.Fatalf(
				"customer Human token %q on platform path = %d body=%s",
				token,
				humanResponse.StatusCode,
				humanResponse.Body,
			)
		}
	}
	platformOnCustomer := doBreakGlassHTTP(
		t,
		http.MethodGet,
		server.URL+"/v1/projects/"+testProjectID+"/jobs/"+fixture.assignment.JobID.String(),
		"operator-requester-http-token",
		"",
		nil,
	)
	if platformOnCustomer.StatusCode != http.StatusUnauthorized {
		t.Fatalf(
			"Platform Operator token on customer path = %d body=%s",
			platformOnCustomer.StatusCode,
			platformOnCustomer.Body,
		)
	}
	createdResponse := doBreakGlassHTTP(
		t,
		http.MethodPost,
		server.URL+"/v1/platform/break-glass/requests",
		"operator-requester-http-token",
		"http-request-001",
		requestBody,
	)
	if createdResponse.StatusCode != http.StatusCreated {
		t.Fatalf(
			"create Break-glass HTTP request = %d body=%s",
			createdResponse.StatusCode,
			createdResponse.Body,
		)
	}
	var projection struct {
		RequestID uuid.UUID  `json:"request_id"`
		GrantID   *uuid.UUID `json:"grant_id"`
		State     string     `json:"state"`
	}
	if err := json.Unmarshal(createdResponse.Body, &projection); err != nil {
		t.Fatalf("decode Break-glass HTTP request: %v", err)
	}
	replayedResponse := doBreakGlassHTTP(
		t,
		http.MethodPost,
		server.URL+"/v1/platform/break-glass/requests",
		"operator-requester-http-token",
		"http-request-001",
		requestBody,
	)
	if replayedResponse.StatusCode != http.StatusOK {
		t.Fatalf(
			"replay Break-glass HTTP request = %d body=%s",
			replayedResponse.StatusCode,
			replayedResponse.Body,
		)
	}
	var replayedProjection struct {
		RequestID uuid.UUID `json:"request_id"`
	}
	if err := json.Unmarshal(replayedResponse.Body, &replayedProjection); err != nil ||
		replayedProjection.RequestID != projection.RequestID {
		t.Fatalf("replayed Break-glass HTTP projection = %#v error=%v", replayedProjection, err)
	}
	conflictingBody := bytes.Replace(
		requestBody,
		[]byte("SUPPORT-4001"),
		[]byte("SUPPORT-4002"),
		1,
	)
	conflictingResponse := doBreakGlassHTTP(
		t,
		http.MethodPost,
		server.URL+"/v1/platform/break-glass/requests",
		"operator-requester-http-token",
		"http-request-001",
		conflictingBody,
	)
	if conflictingResponse.StatusCode != http.StatusConflict {
		t.Fatalf(
			"conflicting Break-glass HTTP replay = %d body=%s",
			conflictingResponse.StatusCode,
			conflictingResponse.Body,
		)
	}
	selfApproval := doBreakGlassHTTP(
		t,
		http.MethodPost,
		server.URL+"/v1/platform/break-glass/requests/"+
			projection.RequestID.String()+"/approval",
		"operator-requester-http-token",
		"",
		nil,
	)
	if selfApproval.StatusCode != http.StatusForbidden {
		t.Fatalf("self-approval HTTP = %d body=%s", selfApproval.StatusCode, selfApproval.Body)
	}
	approvedResponse := doBreakGlassHTTP(
		t,
		http.MethodPost,
		server.URL+"/v1/platform/break-glass/requests/"+
			projection.RequestID.String()+"/approval",
		"operator-approver-http-token",
		"",
		nil,
	)
	if approvedResponse.StatusCode != http.StatusOK {
		t.Fatalf(
			"approve Break-glass HTTP = %d body=%s",
			approvedResponse.StatusCode,
			approvedResponse.Body,
		)
	}
	if err := json.Unmarshal(approvedResponse.Body, &projection); err != nil || projection.GrantID == nil {
		t.Fatalf("decode approved Break-glass HTTP response = %#v error=%v", projection, err)
	}
	statusResponse := doBreakGlassHTTP(
		t,
		http.MethodGet,
		server.URL+"/v1/platform/break-glass/requests/"+projection.RequestID.String(),
		"operator-requester-http-token",
		"",
		nil,
	)
	if statusResponse.StatusCode != http.StatusOK ||
		!bytes.Contains(statusResponse.Body, []byte(`"state":"ACTIVE"`)) {
		t.Fatalf(
			"get active Break-glass HTTP request = %d body=%s",
			statusResponse.StatusCode,
			statusResponse.Body,
		)
	}
	contentResponse := doBreakGlassHTTP(
		t,
		http.MethodGet,
		server.URL+"/v1/platform/break-glass/grants/"+
			projection.GrantID.String()+"/request-content",
		"operator-requester-http-token",
		"",
		nil,
	)
	if contentResponse.StatusCode != http.StatusOK ||
		!bytes.Contains(contentResponse.Body, []byte("exercise Assignment")) {
		t.Fatalf(
			"Break-glass request-content HTTP = %d body=%s",
			contentResponse.StatusCode,
			contentResponse.Body,
		)
	}
	artifactResponse := doBreakGlassHTTP(
		t,
		http.MethodGet,
		server.URL+"/v1/platform/break-glass/grants/"+
			projection.GrantID.String()+"/artifacts",
		"operator-requester-http-token",
		"",
		nil,
	)
	if artifactResponse.StatusCode != http.StatusOK ||
		bytes.Contains(artifactResponse.Body, []byte("object_key")) ||
		bytes.Contains(artifactResponse.Body, []byte("object_version_id")) ||
		!bytes.Contains(artifactResponse.Body, []byte("download_url_expires_at")) {
		t.Fatalf(
			"Break-glass Artifact HTTP = %d body=%s",
			artifactResponse.StatusCode,
			artifactResponse.Body,
		)
	}
	httpSigner.err = errors.New("injected HTTP signing dependency detail")
	signingFailureResponse := doBreakGlassHTTP(
		t,
		http.MethodGet,
		server.URL+"/v1/platform/break-glass/grants/"+
			projection.GrantID.String()+"/artifacts",
		"operator-requester-http-token",
		"",
		nil,
	)
	if signingFailureResponse.StatusCode != http.StatusServiceUnavailable ||
		bytes.Contains(signingFailureResponse.Body, []byte("injected HTTP signing dependency detail")) ||
		!bytes.Contains(signingFailureResponse.Body, []byte(`"code":"service_unavailable"`)) {
		t.Fatalf(
			"Break-glass HTTP signing dependency failure = %d body=%s",
			signingFailureResponse.StatusCode,
			signingFailureResponse.Body,
		)
	}
	revokedResponse := doBreakGlassHTTP(
		t,
		http.MethodPost,
		server.URL+"/v1/platform/break-glass/grants/"+projection.GrantID.String()+"/revocation",
		"operator-approver-http-token",
		"",
		nil,
	)
	if revokedResponse.StatusCode != http.StatusOK ||
		!bytes.Contains(revokedResponse.Body, []byte(`"state":"REVOKED"`)) {
		t.Fatalf(
			"revoke Break-glass HTTP grant = %d body=%s",
			revokedResponse.StatusCode,
			revokedResponse.Body,
		)
	}
	contentAfterRevocation := doBreakGlassHTTP(
		t,
		http.MethodGet,
		server.URL+"/v1/platform/break-glass/grants/"+
			projection.GrantID.String()+"/request-content",
		"operator-requester-http-token",
		"",
		nil,
	)
	if contentAfterRevocation.StatusCode != http.StatusForbidden {
		t.Fatalf(
			"Break-glass request content after HTTP revocation = %d body=%s",
			contentAfterRevocation.StatusCode,
			contentAfterRevocation.Body,
		)
	}
}

func TestBreakGlassMigrationRoundTripRestoresOrganizationAuditProjection(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")

	var schema17Definition string
	if err := database.Admin.QueryRow(`
		SELECT pg_catalog.pg_get_functiondef(
			'vela_list_organization_audit_events(uuid,integer)'::regprocedure
		)
	`).Scan(&schema17Definition); err != nil {
		t.Fatalf("read schema 17 Organization audit projection: %v", err)
	}
	if !strings.Contains(schema17Definition, "break_glass_events") {
		t.Fatalf("schema 17 Organization audit projection lacks Break-glass source:\n%s", schema17Definition)
	}
	var schema17V2Definition string
	if err := database.Admin.QueryRow(`
		SELECT pg_catalog.pg_get_functiondef(
			'vela_list_organization_audit_events_v2(uuid,integer)'::regprocedure
		)
	`).Scan(&schema17V2Definition); err != nil {
		t.Fatalf("read schema 17 Organization audit v2 projection: %v", err)
	}

	if err := goose.DownTo(database.Admin, migrations, 16); err != nil {
		t.Fatalf("migrate empty Break-glass schema to 16: %v", err)
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 16 {
		t.Fatalf("Break-glass Down version = %d error=%v", version, err)
	}
	var schema16Definition string
	if err := database.Admin.QueryRow(`
		SELECT pg_catalog.pg_get_functiondef(
			'vela_list_organization_audit_events(uuid,integer)'::regprocedure
		)
	`).Scan(&schema16Definition); err != nil {
		t.Fatalf("read restored schema 16 Organization audit projection: %v", err)
	}
	if strings.Contains(schema16Definition, "break_glass") {
		t.Fatalf("schema 16 Organization audit projection retained Break-glass dependency:\n%s", schema16Definition)
	}
	var removedSurface bool
	if err := database.Admin.QueryRow(`
		SELECT
				to_regclass('public.break_glass_requests') IS NULL
				AND to_regclass('public.break_glass_grants') IS NULL
				AND to_regclass('public.break_glass_events') IS NULL
				AND to_regclass('public.break_glass_denial_events') IS NULL
			AND to_regprocedure(
				'vela_authenticate_platform_operator_oidc(text,text,bytea,timestamptz)'
			) IS NULL
			AND to_regprocedure('vela_set_break_glass_request_context(uuid,bytea)') IS NULL
			AND to_regprocedure(
				'vela_list_organization_audit_events_v2(uuid,integer)'
			) IS NULL
	`).Scan(&removedSurface); err != nil {
		t.Fatalf("inspect removed Break-glass schema surface: %v", err)
	}
	if !removedSurface {
		t.Fatal("Break-glass Down retained schema 17 tables or functions")
	}

	if err := goose.UpTo(database.Admin, migrations, 17); err != nil {
		t.Fatalf("restore Break-glass schema 17: %v", err)
	}
	version, err = goose.GetDBVersion(database.Admin)
	if err != nil || version != 17 {
		t.Fatalf("Break-glass first Up version = %d error=%v", version, err)
	}
	var restoredSchema17Definition string
	if err := database.Admin.QueryRow(`
		SELECT pg_catalog.pg_get_functiondef(
			'vela_list_organization_audit_events(uuid,integer)'::regprocedure
		)
	`).Scan(&restoredSchema17Definition); err != nil {
		t.Fatalf("read restored schema 17 Organization audit projection: %v", err)
	}
	if restoredSchema17Definition != schema17Definition {
		t.Fatal("Break-glass Up did not restore the exact schema 17 Organization audit projection")
	}
	var restoredSchema17V2Definition string
	if err := database.Admin.QueryRow(`
		SELECT pg_catalog.pg_get_functiondef(
			'vela_list_organization_audit_events_v2(uuid,integer)'::regprocedure
		)
	`).Scan(&restoredSchema17V2Definition); err != nil {
		t.Fatalf("read restored schema 17 Organization audit v2 projection: %v", err)
	}
	if restoredSchema17V2Definition != schema17V2Definition {
		t.Fatal("Break-glass Up did not restore the exact schema 17 Organization audit v2 projection")
	}

	if err := goose.DownTo(database.Admin, migrations, 16); err != nil {
		t.Fatalf("repeat Break-glass Down to schema 16: %v", err)
	}
	var repeatedSchema16Definition string
	if err := database.Admin.QueryRow(`
		SELECT pg_catalog.pg_get_functiondef(
			'vela_list_organization_audit_events(uuid,integer)'::regprocedure
		)
	`).Scan(&repeatedSchema16Definition); err != nil {
		t.Fatalf("read repeated schema 16 Organization audit projection: %v", err)
	}
	if repeatedSchema16Definition != schema16Definition {
		t.Fatal("Break-glass Down did not restore the exact prior Organization audit projection")
	}
	if err := goose.UpTo(database.Admin, migrations, 17); err != nil {
		t.Fatalf("repeat Break-glass Up to schema 17: %v", err)
	}
	version, err = goose.GetDBVersion(database.Admin)
	if err != nil || version != 17 {
		t.Fatalf("Break-glass final Up version = %d error=%v", version, err)
	}
}

func TestBreakGlassMigrationDownRefusesDurableIdentityEvidence(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	if _, err := database.Admin.Exec(`
		INSERT INTO platform_operator_oidc_bindings (
			id, issuer, subject, display_name
		) VALUES (
			$1, 'https://platform-identity.example.com',
			'operator-durable-migration-evidence', 'Durable Operator'
		)
	`, uuid.New()); err != nil {
		t.Fatalf("seed durable Platform Operator identity evidence: %v", err)
	}
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err := goose.DownTo(database.Admin, migrations, 16)
	if err == nil || !strings.Contains(err.Error(), "break_glass_contract_requires_empty_evidence") {
		t.Fatalf("Break-glass Down with durable evidence error = %v", err)
	}
	version, versionErr := goose.GetDBVersion(database.Admin)
	if versionErr != nil || version != 17 {
		t.Fatalf("Break-glass durable-evidence version = %d error=%v", version, versionErr)
	}
	var bindingCount int
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM platform_operator_oidc_bindings
	`).Scan(&bindingCount); err != nil {
		t.Fatalf("read preserved Platform Operator binding: %v", err)
	}
	if bindingCount != 1 {
		t.Fatalf("preserved Platform Operator bindings = %d, want 1", bindingCount)
	}
}

func TestBreakGlassMigrationDownSerializesConcurrentIdentityEvidence(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	writer, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin concurrent Platform Operator identity writer: %v", err)
	}
	defer func() { _ = writer.Rollback() }()
	operatorID := uuid.New()
	if _, err := writer.Exec(`
		INSERT INTO platform_operator_oidc_bindings (
			id, issuer, subject, display_name
		) VALUES (
			$1, 'https://platform-identity.example.com',
			'operator-concurrent-migration-evidence', 'Concurrent Operator'
		)
	`, operatorID); err != nil {
		t.Fatalf("write concurrent Platform Operator identity evidence: %v", err)
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	downErrors := make(chan error, 1)
	go func() {
		downErrors <- goose.DownTo(database.Admin, migrations, 16)
	}()
	waitForRoleDatabaseLock(t, database.Admin, "postgres")
	if err := writer.Commit(); err != nil {
		t.Fatalf("commit concurrent Platform Operator identity evidence: %v", err)
	}

	select {
	case downErr := <-downErrors:
		var postgresError *pgconn.PgError
		if !errors.As(downErr, &postgresError) || postgresError.Code != "55000" ||
			postgresError.ConstraintName != "break_glass_contract_requires_empty_evidence" {
			t.Fatalf(
				"concurrent Break-glass migration Down error = %v, want named SQLSTATE 55000",
				downErr,
			)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent Break-glass migration Down did not finish")
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 17 {
		t.Fatalf("Break-glass concurrent-evidence version = %d error=%v", version, err)
	}
	var preserved bool
	if err := database.Admin.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM platform_operator_oidc_bindings WHERE id = $1
		)
	`, operatorID).Scan(&preserved); err != nil {
		t.Fatalf("read concurrent Platform Operator identity evidence: %v", err)
	}
	if !preserved {
		t.Fatal("concurrent Platform Operator identity evidence was not preserved")
	}
}

type tokenOIDCVerifier map[string]identity.OIDCIdentity

func (verifier tokenOIDCVerifier) Verify(
	_ context.Context,
	token string,
) (identity.OIDCIdentity, error) {
	verified, ok := verifier[token]
	if !ok {
		return identity.OIDCIdentity{}, identity.ErrInvalidOIDCToken
	}
	return verified, nil
}

func doBreakGlassHTTP(
	t *testing.T,
	method string,
	url string,
	token string,
	idempotencyKey string,
	body []byte,
) httpResult {
	t.Helper()
	request, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create Break-glass HTTP request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	if len(body) > 0 {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("perform Break-glass HTTP request: %v", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read Break-glass HTTP response: %v", err)
	}
	return httpResult{
		StatusCode: response.StatusCode,
		Header:     response.Header.Clone(),
		Body:       responseBody,
	}
}

type grantBoundedSignerCall struct {
	objectKey string
	versionID string
	notAfter  time.Time
}

type grantBoundedArtifactSigner struct {
	calls            []grantBoundedSignerCall
	err              error
	beforeFirstSign  func()
	firstSignHookRan bool
}

type failingArtifactSigner struct {
	err error
}

func (signer failingArtifactSigner) PresignExactVersionUntil(
	context.Context,
	string,
	string,
	time.Time,
) (artifactstore.SignedRead, error) {
	return artifactstore.SignedRead{}, signer.err
}

func (signer *grantBoundedArtifactSigner) PresignExactVersionUntil(
	_ context.Context,
	objectKey string,
	versionID string,
	notAfter time.Time,
) (artifactstore.SignedRead, error) {
	signer.calls = append(signer.calls, grantBoundedSignerCall{
		objectKey: objectKey, versionID: versionID, notAfter: notAfter,
	})
	if signer.beforeFirstSign != nil && !signer.firstSignHookRan {
		signer.firstSignHookRan = true
		signer.beforeFirstSign()
	}
	if signer.err != nil {
		return artifactstore.SignedRead{}, signer.err
	}
	issuedAt := notAfter.Add(-10 * time.Minute)
	return artifactstore.SignedRead{
		URL:       "https://artifacts.example.com/exact/" + uuid.NewString(),
		IssuedAt:  issuedAt,
		ExpiresAt: notAfter,
	}, nil
}

func authenticatePlatformOperator(
	t *testing.T,
	database *sql.DB,
	subject string,
	token string,
) (uuid.UUID, []byte) {
	t.Helper()
	proof := sha256.Sum256([]byte(token))
	var operatorID, sessionID uuid.UUID
	var expiresAt time.Time
	if err := database.QueryRow(`
		SELECT operator_id, session_id, expires_at
		FROM vela_authenticate_platform_operator_oidc($1, $2, $3, $4)
	`,
		"https://platform-identity.example.com",
		subject,
		proof[:],
		time.Now().UTC().Add(time.Hour),
	).Scan(&operatorID, &sessionID, &expiresAt); err != nil {
		t.Fatalf("authenticate Platform Operator %q: %v", subject, err)
	}
	if operatorID == uuid.Nil || sessionID == uuid.Nil || expiresAt.After(time.Now().UTC().Add(16*time.Minute)) {
		t.Fatalf("Platform Operator session = operator %s session %s expires %s", operatorID, sessionID, expiresAt)
	}
	return sessionID, proof[:]
}

func withBreakGlassContext(
	t *testing.T,
	database *sql.DB,
	sessionID uuid.UUID,
	proof []byte,
	action func(*sql.Tx),
) {
	t.Helper()
	tx := beginBreakGlassTransaction(t, database, sessionID, proof)
	defer func() { _ = tx.Rollback() }()
	action(tx)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit Break-glass transaction: %v", err)
	}
}

func beginBreakGlassTransaction(
	t *testing.T,
	database *sql.DB,
	sessionID uuid.UUID,
	proof []byte,
) *sql.Tx {
	t.Helper()
	tx, err := database.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin Break-glass transaction: %v", err)
	}
	var operatorID uuid.UUID
	var transactionTime time.Time
	if err := tx.QueryRow(`
		SELECT operator_id, transaction_time
		FROM vela_set_break_glass_request_context($1, $2)
	`, sessionID, proof).Scan(&operatorID, &transactionTime); err != nil {
		t.Fatalf("establish Break-glass context: %v", err)
	}
	if operatorID == uuid.Nil || transactionTime.IsZero() {
		t.Fatalf("Break-glass context = operator %s time %s", operatorID, transactionTime)
	}
	return tx
}
