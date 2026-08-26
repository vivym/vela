//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/artifactaccess"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/debugdump"
	"github.com/vivym/vela/internal/httpapi"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/organizationreporting"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/webhook"
	"github.com/vivym/vela/internal/workercontrol"
)

func TestProjectAdminListsAndReadsExactDebugDumpOverProductionHTTP(t *testing.T) {
	fixture := newAssignmentFixture(t, "debug-dump-read", 7)
	authorizationID := seedActiveDebugDumpAuthorization(
		t, fixture.database, fixture.candidate.JobID,
	)
	dumpID, objectKey, versionID, content, digest := persistAvailableDebugDump(
		t, fixture, authorizationID,
	)

	const readerSubject = "debug-dump-reader"
	seedHumanRoleFixture(
		t,
		fixture.database.Admin,
		uuid.New(),
		readerSubject,
		[]string{"OrganizationAuditor"},
		map[string][]string{testProjectID: {"ProjectAdmin"}},
	)
	seedSecondProjectAndPool(t, fixture.database.Admin)
	seedOtherOrganization(t, fixture.database.Admin)
	authenticator := newHumanMembershipAuthenticator(
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
			Subject:   readerSubject,
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	requestPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_debug_dump_request_login",
		"vela-debug-dump-request-password",
	)
	signer := &debugDumpExactSigner{}
	debugService, err := debugdump.NewService(requestPool, signer)
	if err != nil {
		t.Fatalf("create debug dump read service: %v", err)
	}
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          authenticator,
		IdentityAdministration: &identity.AdministrationService{},
		OrganizationReporting:  &organizationreporting.Service{},
		Retention:              &retention.Service{},
		DebugDumps:             debugService,
		Admission:              &admission.Service{},
		Cancellation:           &cancellation.Service{},
		Artifacts:              &artifactaccess.Service{},
		Webhooks:               &webhook.Service{},
	})
	if err != nil {
		t.Fatalf("create debug dump read HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	basePath := fmt.Sprintf(
		"/v1/projects/%s/jobs/%s/debug-dump-authorizations/%s/dumps",
		testProjectID,
		fixture.candidate.JobID,
		authorizationID,
	)

	listStatus, listBody := debugDumpGET(
		t, server, basePath, "debug-dump-reader-token",
	)
	if listStatus != http.StatusOK || bytes.Contains(listBody, []byte("object_key")) ||
		bytes.Contains(listBody, []byte("object_version")) {
		t.Fatalf("debug dump list status/body = %d %s", listStatus, listBody)
	}
	var listed struct {
		Dumps []struct {
			DebugDumpID     string `json:"debug_dump_id"`
			AuthorizationID string `json:"authorization_id"`
			State           string `json:"state"`
			SizeBytes       int64  `json:"size_bytes"`
			SHA256          string `json:"sha256"`
		} `json:"dumps"`
	}
	if err := json.Unmarshal(listBody, &listed); err != nil {
		t.Fatalf("decode debug dump list: %v", err)
	}
	if len(listed.Dumps) != 1 || listed.Dumps[0].DebugDumpID != dumpID.String() ||
		listed.Dumps[0].AuthorizationID != authorizationID.String() ||
		listed.Dumps[0].State != "AVAILABLE" ||
		listed.Dumps[0].SizeBytes != int64(len(content)) ||
		listed.Dumps[0].SHA256 != fmt.Sprintf("%x", digest) {
		t.Fatalf("debug dump list = %#v", listed)
	}

	readPath := basePath + "/" + dumpID.String()
	readStatus, readBody := debugDumpGET(
		t, server, readPath, "debug-dump-reader-token",
	)
	if readStatus != http.StatusOK || bytes.Contains(readBody, []byte("object_key")) ||
		bytes.Contains(readBody, []byte("object_version")) {
		t.Fatalf("debug dump read status/body = %d %s", readStatus, readBody)
	}
	var download struct {
		DebugDumpID          string    `json:"debug_dump_id"`
		AuthorizationID      string    `json:"authorization_id"`
		DownloadURL          string    `json:"download_url"`
		DownloadURLExpiresAt time.Time `json:"download_url_expires_at"`
		SizeBytes            int64     `json:"size_bytes"`
		SHA256               string    `json:"sha256"`
	}
	if err := json.Unmarshal(readBody, &download); err != nil {
		t.Fatalf("decode debug dump download: %v", err)
	}
	if download.DebugDumpID != dumpID.String() ||
		download.AuthorizationID != authorizationID.String() ||
		download.DownloadURL == "" || download.DownloadURLExpiresAt.IsZero() ||
		download.SizeBytes != int64(len(content)) ||
		download.SHA256 != fmt.Sprintf("%x", digest) || len(signer.calls) != 1 ||
		signer.calls[0].objectKey != objectKey || signer.calls[0].versionID != versionID {
		t.Fatalf("debug dump download/signer = %#v / %#v", download, signer.calls)
	}

	serviceStatus, _ := debugDumpGET(t, server, basePath, testBearerCredential())
	projectStatus, _ := debugDumpGET(
		t,
		server,
		fmt.Sprintf(
			"/v1/projects/%s/jobs/%s/debug-dump-authorizations/%s/dumps",
			testProjectTwoID,
			fixture.candidate.JobID,
			authorizationID,
		),
		"debug-dump-reader-token",
	)
	organizationStatus, _ := debugDumpGET(
		t,
		server,
		fmt.Sprintf(
			"/v1/projects/%s/jobs/%s/debug-dump-authorizations/%s/dumps",
			testOtherProjectID,
			fixture.candidate.JobID,
			authorizationID,
		),
		"debug-dump-reader-token",
	)
	if serviceStatus != http.StatusForbidden || projectStatus != http.StatusNotFound ||
		organizationStatus != http.StatusNotFound {
		t.Fatalf(
			"service/cross-Project/cross-Organization statuses = %d/%d/%d",
			serviceStatus,
			projectStatus,
			organizationStatus,
		)
	}

	const deliveryFunction = `vela_record_debug_dump_delivery(
		uuid, uuid, uuid, uuid, text, boolean
	)`
	signer.invalidReceipt = true
	signer.beforeSign = func() {
		if _, revokeErr := fixture.database.Admin.Exec(
			"REVOKE EXECUTE ON FUNCTION " + deliveryFunction + " FROM vela_debug_dump_request",
		); revokeErr != nil {
			t.Fatalf("revoke debug dump delivery evidence function: %v", revokeErr)
		}
	}
	invalidStatus, invalidBody := debugDumpGET(
		t, server, readPath, "debug-dump-reader-token",
	)
	if invalidStatus != http.StatusServiceUnavailable ||
		!bytes.Contains(invalidBody, []byte("delivery evidence is unavailable")) ||
		bytes.Contains(invalidBody, []byte("download_url")) {
		t.Fatalf("invalid signer receipt status/body = %d %s", invalidStatus, invalidBody)
	}
	if _, err := fixture.database.Admin.Exec(
		"GRANT EXECUTE ON FUNCTION " + deliveryFunction + " TO vela_debug_dump_request",
	); err != nil {
		t.Fatalf("restore debug dump delivery evidence function: %v", err)
	}
	t.Cleanup(func() {
		_, _ = fixture.database.Admin.Exec(
			"GRANT EXECUTE ON FUNCTION " + deliveryFunction + " TO vela_debug_dump_request",
		)
	})
	signer.invalidReceipt = false

	actor, err := authenticator.Authenticate(
		context.Background(), "debug-dump-reader-token",
	)
	if err != nil {
		t.Fatalf("authenticate debug dump reader for race: %v", err)
	}
	actor, ok := actor.ForProject(uuid.MustParse(testProjectID))
	if !ok {
		t.Fatal("debug dump reader lacks Project authority")
	}
	signer.beforeSign = func() {
		if _, revokeErr := debugService.RevokeAuthorization(
			context.Background(),
			actor,
			uuid.MustParse(testProjectID),
			fixture.candidate.JobID,
			authorizationID,
			"debug-dump-read-race-revoke",
		); revokeErr != nil {
			t.Fatalf("revoke authorization during signing: %v", revokeErr)
		}
	}
	raceStatus, raceBody := debugDumpGET(
		t, server, readPath, "debug-dump-reader-token",
	)
	if raceStatus != http.StatusForbidden || bytes.Contains(raceBody, []byte("download_url")) {
		t.Fatalf("debug dump signing race status/body = %d %s", raceStatus, raceBody)
	}
	deniedStatus, deniedBody := debugDumpGET(
		t, server, readPath, "debug-dump-reader-token",
	)
	if deniedStatus != http.StatusForbidden ||
		bytes.Contains(deniedBody, []byte("download_url")) || len(signer.calls) != 3 {
		t.Fatalf("revoked debug dump read status/body/calls = %d %s / %d", deniedStatus, deniedBody, len(signer.calls))
	}

	var readAuthorized, readDenied, delivered, deliveryFailed, revoked int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			count(*) FILTER (WHERE action = 'READ_AUTHORIZED'),
			count(*) FILTER (WHERE action = 'READ_DENIED'),
			count(*) FILTER (WHERE action = 'DELIVERED'),
			count(*) FILTER (WHERE action = 'DELIVERY_FAILED'),
			count(*) FILTER (WHERE action = 'REVOKED')
		FROM debug_dump_events
		WHERE authorization_id = $1 AND debug_dump_id IS NOT DISTINCT FROM $2
	`, authorizationID, dumpID).Scan(
		&readAuthorized,
		&readDenied,
		&delivered,
		&deliveryFailed,
		&revoked,
	); err != nil {
		t.Fatalf("read debug dump delivery audit: %v", err)
	}
	if readAuthorized != 3 || readDenied != 1 || delivered != 1 ||
		deliveryFailed != 1 || revoked != 0 {
		t.Fatalf(
			"debug dump read authorized/denied/delivery/revocation events = %d/%d/%d/%d/%d",
			readAuthorized,
			readDenied,
			delivered,
			deliveryFailed,
			revoked,
		)
	}
	var revocationEvents int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*) FROM debug_dump_events
		WHERE authorization_id = $1 AND debug_dump_id IS NULL AND action = 'REVOKED'
	`, authorizationID).Scan(&revocationEvents); err != nil {
		t.Fatalf("read debug dump revocation audit: %v", err)
	}
	if revocationEvents != 1 {
		t.Fatalf("debug dump revocation events = %d, want 1", revocationEvents)
	}
	reportingService, err := newOrganizationReportingService(t, fixture.database.DSN)
	if err != nil {
		t.Fatalf("create debug dump Organization audit service: %v", err)
	}
	auditor, err := authenticator.Authenticate(
		context.Background(), "debug-dump-reader-token",
	)
	if err != nil {
		t.Fatalf("authenticate debug dump OrganizationAuditor: %v", err)
	}
	auditor, ok = auditor.ForOrganization(uuid.MustParse(testOrganizationID))
	if !ok {
		t.Fatal("debug dump reader lacks Organization audit authority")
	}
	events, err := reportingService.ListAuditEvents(
		context.Background(), auditor, uuid.MustParse(testOrganizationID), 100,
	)
	if err != nil {
		t.Fatalf("list debug dump Organization audit events: %v", err)
	}
	wantActions := map[string]bool{
		"UPLOAD_CLAIMED":  false,
		"UPLOADED":        false,
		"READ_AUTHORIZED": false,
		"READ_DENIED":     false,
		"DELIVERED":       false,
		"DELIVERY_FAILED": false,
		"REVOKED":         false,
	}
	for _, event := range events {
		if event.Source != "DEBUG_DUMP" {
			continue
		}
		if _, wanted := wantActions[event.Action]; wanted {
			wantActions[event.Action] = true
		}
		if event.TargetKind != "DEBUG_DUMP" &&
			event.TargetKind != "DEBUG_DUMP_AUTHORIZATION" {
			t.Fatalf("debug dump Organization audit target = %#v", event)
		}
	}
	for action, found := range wantActions {
		if !found {
			t.Fatalf("Organization audit projection lacks debug dump action %s: %#v", action, events)
		}
	}
}

func persistAvailableDebugDump(
	t *testing.T,
	fixture assignmentFixture,
	authorizationID uuid.UUID,
) (uuid.UUID, string, string, []byte, [sha256.Size]byte) {
	t.Helper()
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("Acquire debug dump read Assignment: %v", err)
	}
	credentials := leaseCredentials(assignment)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start debug dump read Assignment = %#v error=%v", started, err)
	}
	content := []byte(`{"attempt_id":"readable","schema_version":1}`)
	digest := sha256.Sum256(content)
	dumpID := uuid.New()
	claimID := uuid.New()
	claim, err := fixture.service.ClaimDebugDumpUpload(
		context.Background(),
		fixture.worker,
		credentials,
		workercontrol.DebugDumpUploadIntent{
			DebugDumpID: dumpID, AuthorizationID: authorizationID,
			SizeBytes: int64(len(content)), SHA256: digest,
			ContentType: "application/vnd.vela.debug-dump+json",
		},
		claimID,
	)
	if err != nil || claim.Decision != workercontrol.DebugDumpUploadClaimGranted {
		t.Fatalf("Claim readable debug dump = %#v error=%v", claim, err)
	}
	if session, err := fixture.service.RecordDebugDumpMultipartSession(
		context.Background(), fixture.worker, credentials, dumpID, claimID, "read-upload-1",
	); err != nil || session.Decision != workercontrol.DebugDumpMultipartSessionRecorded {
		t.Fatalf("Record readable debug dump session = %#v error=%v", session, err)
	}
	parts := []workercontrol.DebugDumpUploadPart{{
		Number: 1, ETag: "read-etag-1", SizeBytes: int64(len(content)),
		ChecksumSHA256: base64.StdEncoding.EncodeToString(digest[:]),
	}}
	if intent, err := fixture.service.RecordDebugDumpCompletionIntent(
		context.Background(),
		fixture.worker,
		credentials,
		dumpID,
		workercontrol.DebugDumpUploadReport{
			SizeBytes: int64(len(content)), SHA256: digest,
			ContentType: "application/vnd.vela.debug-dump+json", CompletedParts: parts,
		},
	); err != nil || intent.Decision != workercontrol.DebugDumpCompletionIntentRecorded {
		t.Fatalf("Record readable debug dump intent = %#v error=%v", intent, err)
	}
	const versionID = "read-version-1"
	if completed, err := fixture.service.RecordDebugDumpUploaded(
		context.Background(),
		fixture.worker,
		credentials,
		dumpID,
		workercontrol.DebugDumpUploadReport{
			ObjectVersionID: versionID, SizeBytes: int64(len(content)), SHA256: digest,
			ContentType: "application/vnd.vela.debug-dump+json", CompletedParts: parts,
		},
	); err != nil || completed.Decision != workercontrol.DebugDumpUploadRecorded {
		t.Fatalf("Record readable debug dump = %#v error=%v", completed, err)
	}
	return dumpID, claim.ObjectKey, versionID, content, digest
}

func debugDumpGET(
	t *testing.T,
	server *httptest.Server,
	path string,
	token string,
) (int, []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
	if err != nil {
		t.Fatalf("create debug dump GET: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("execute debug dump GET: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read debug dump GET response: %v", err)
	}
	return response.StatusCode, body
}

type debugDumpSignerCall struct {
	objectKey string
	versionID string
	notAfter  time.Time
}

type debugDumpExactSigner struct {
	calls          []debugDumpSignerCall
	beforeSign     func()
	invalidReceipt bool
}

func (signer *debugDumpExactSigner) PresignExactVersionUntil(
	_ context.Context,
	objectKey string,
	versionID string,
	notAfter time.Time,
) (artifactstore.SignedRead, error) {
	signer.calls = append(signer.calls, debugDumpSignerCall{
		objectKey: objectKey, versionID: versionID, notAfter: notAfter,
	})
	if signer.beforeSign != nil {
		hook := signer.beforeSign
		signer.beforeSign = nil
		hook()
	}
	if signer.invalidReceipt {
		return artifactstore.SignedRead{}, nil
	}
	issuedAt := time.Now().UTC()
	expiresAt := issuedAt.Add(5 * time.Minute)
	if notAfter.Before(expiresAt) {
		expiresAt = notAfter
	}
	return artifactstore.SignedRead{
		URL:       "https://debug-dumps.example.com/exact/" + uuid.NewString(),
		IssuedAt:  issuedAt,
		ExpiresAt: expiresAt,
	}, nil
}
