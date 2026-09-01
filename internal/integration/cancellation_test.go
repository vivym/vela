//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/identity"
)

func TestQueuedJobCancellationReleasesCreditWithoutCharge(t *testing.T) {
	server, admin := newAdmissionServer(t)
	if _, err := admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	accepted := submitJob(t, server.URL, "cancel-queued", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"cancel this Accepted Job before Billable Start"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Accepted Job: %v", err)
	}

	canceled := cancelJob(t, server.URL, testProjectID, job.JobID, testBearerCredential())
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200; body=%s", canceled.StatusCode, canceled.Body)
	}
	var result cancelResponse
	if err := json.Unmarshal(canceled.Body, &result); err != nil {
		t.Fatalf("decode cancellation response: %v", err)
	}
	if result.CancellationID == "" || result.JobID != job.JobID ||
		result.Decision != "CANCELED" || result.State != "CANCELED" ||
		result.JobVersion != 3 || result.Billable || result.Charge != nil ||
		result.DecidedAt.IsZero() {
		t.Fatalf("cancellation result = %#v", result)
	}

	var (
		state                                          string
		jobVersion, projectQueued, readyQueue          int64
		reservedMinor, unsettledPostedMinor            int64
		reservationState                               string
		cancellationCount, chargeCount, canceledEvents int64
	)
	if err := admin.QueryRow(`
		SELECT
			j.state,
			j.version,
			p.queued_count,
			(SELECT count(*) FROM stage_ready_queue_entries
			 WHERE project_id = j.project_id),
			oca.reserved_minor,
			oca.unsettled_posted_minor,
			cr.state,
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = j.id),
			(SELECT count(*) FROM charges WHERE job_id = j.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id AND event_type = 'job.canceled')
		FROM jobs AS j
		JOIN projects AS p ON p.id = j.project_id
		JOIN organization_credit_accounts AS oca ON oca.organization_id = j.organization_id
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		WHERE j.id = $1
	`, job.JobID).Scan(
		&state,
		&jobVersion,
		&projectQueued,
		&readyQueue,
		&reservedMinor,
		&unsettledPostedMinor,
		&reservationState,
		&cancellationCount,
		&chargeCount,
		&canceledEvents,
	); err != nil {
		t.Fatalf("read queued cancellation state: %v", err)
	}
	if state != "CANCELED" || jobVersion != 3 || projectQueued != 0 || readyQueue != 0 ||
		reservedMinor != 0 || unsettledPostedMinor != 0 || reservationState != "RELEASED" ||
		cancellationCount != 1 || chargeCount != 0 || canceledEvents != 1 {
		t.Fatalf(
			"queued cancellation state = state %s version %d project/ready %d/%d credit %d/%d reservation %s decisions/charges/events %d/%d/%d",
			state,
			jobVersion,
			projectQueued,
			readyQueue,
			reservedMinor,
			unsettledPostedMinor,
			reservationState,
			cancellationCount,
			chargeCount,
			canceledEvents,
		)
	}
}

func TestCancellationCredentialChangeAfterAuthenticationFailsClosedWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		name        string
		mutation    string
		wantFailure cancellation.FailureCode
	}{
		{
			name:        "scope removed",
			mutation:    "scopes = ARRAY['jobs:submit', 'jobs:read']",
			wantFailure: cancellation.FailureForbidden,
		},
		{
			name:        "credential revoked",
			mutation:    "revoked_at = clock_timestamp()",
			wantFailure: cancellation.FailureUnauthorized,
		},
		{
			name:        "credential expired",
			mutation:    "expires_at = clock_timestamp() - interval '1 second'",
			wantFailure: cancellation.FailureUnauthorized,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := newPostgres(t)
			applyFoundation(t, database.Admin)
			seedAdmissionFixture(t, database.Admin)
			server := admissionServerForDatabase(t, database)
			accepted := submitJob(t, server.URL, "cancel-credential-changed", []byte(`{
				"model":"minimax-h3",
				"generation_preset":"balanced",
				"service_class":"standard",
				"output_spec":"video-1080p-5s-24fps",
				"generation_count":1,
				"prompt":"credential changes must fail closed in cancellation transaction"
			}`))
			if accepted.StatusCode != http.StatusAccepted {
				t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
			}
			var job jobResponse
			if err := json.Unmarshal(accepted.Body, &job); err != nil {
				t.Fatalf("decode Accepted Job: %v", err)
			}
			if _, err := database.Admin.Exec(`
				UPDATE credentials
				SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
				WHERE id = $1
			`, testCredentialID); err != nil {
				t.Fatalf("grant cancellation scope before authentication: %v", err)
			}
			authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
			principal, err := identity.NewAuthenticator(authPool, testCredentialPepper).Authenticate(
				context.Background(), testBearerCredential(),
			)
			if err != nil {
				t.Fatalf("authenticate before credential mutation: %v", err)
			}
			if _, err := database.Admin.Exec(
				"UPDATE credentials SET "+test.mutation+" WHERE id = $1",
				testCredentialID,
			); err != nil {
				t.Fatalf("mutate authenticated credential: %v", err)
			}
			cancelPool := newRolePool(t, database.DSN, "vela_cancel_login", "vela-cancel-password")
			internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
			_, err = cancellation.NewService(cancelPool, internalPool).Cancel(
				context.Background(),
				principal,
				uuid.MustParse(testProjectID),
				uuid.MustParse(job.JobID),
			)
			var failure *cancellation.Failure
			if !errors.As(err, &failure) || failure.Code != test.wantFailure {
				t.Fatalf("cancellation error = %v, want %s", err, test.wantFailure)
			}

			var jobState, reservationState string
			var jobVersion, projectQueued, readyQueue int64
			var reservedMinor, unsettledPostedMinor int64
			var decisionCount, chargeCount, outboxCount int64
			if err := database.Admin.QueryRow(`
				SELECT
						job.state,
						job.version,
						project.queued_count,
						(SELECT count(*) FROM stage_ready_queue_entries
						 WHERE project_id = job.project_id),
					reservation.state,
					credit.reserved_minor,
					credit.unsettled_posted_minor,
					(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
					(SELECT count(*) FROM charges WHERE job_id = job.id),
					(SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id)
				FROM jobs AS job
				JOIN projects AS project ON project.id = job.project_id
				JOIN credit_reservations AS reservation ON reservation.job_id = job.id
				JOIN organization_credit_accounts AS credit
				  ON credit.organization_id = job.organization_id
				WHERE job.id = $1
			`, job.JobID).Scan(
				&jobState,
				&jobVersion,
				&projectQueued,
				&readyQueue,
				&reservationState,
				&reservedMinor,
				&unsettledPostedMinor,
				&decisionCount,
				&chargeCount,
				&outboxCount,
			); err != nil {
				t.Fatalf("read rejected cancellation state: %v", err)
			}
			if jobState != "QUEUED" || jobVersion != 2 || projectQueued != 1 || readyQueue != 1 ||
				reservationState != "RESERVED" || reservedMinor != 1250 || unsettledPostedMinor != 0 ||
				decisionCount != 0 || chargeCount != 0 || outboxCount != 1 {
				t.Fatalf(
					"rejected cancellation state = job %s/%d queued project/ready %d/%d reservation %s credit %d/%d decisions/charges/outbox %d/%d/%d",
					jobState,
					jobVersion,
					projectQueued,
					readyQueue,
					reservationState,
					reservedMinor,
					unsettledPostedMinor,
					decisionCount,
					chargeCount,
					outboxCount,
				)
			}
		})
	}
}

func TestCancellationHTTPCredentialChangeAfterAuthenticationFailsClosed(t *testing.T) {
	const authenticationPauseLock int64 = 580007
	for _, test := range []struct {
		name       string
		mutation   string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "scope removed",
			mutation:   "scopes = ARRAY['jobs:submit', 'jobs:read']",
			wantStatus: http.StatusForbidden,
			wantCode:   "forbidden",
		},
		{
			name:       "credential revoked",
			mutation:   "revoked_at = clock_timestamp()",
			wantStatus: http.StatusUnauthorized,
			wantCode:   "unauthorized",
		},
		{
			name:       "credential expired",
			mutation:   "expires_at = clock_timestamp() - interval '1 second'",
			wantStatus: http.StatusUnauthorized,
			wantCode:   "unauthorized",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := newPostgres(t)
			applyFoundation(t, database.Admin)
			seedAdmissionFixture(t, database.Admin)
			server := admissionServerForDatabase(t, database)
			accepted := submitJob(t, server.URL, "cancel-http-credential-changed", []byte(`{
				"model":"minimax-h3",
				"generation_preset":"balanced",
				"service_class":"standard",
				"output_spec":"video-1080p-5s-24fps",
				"generation_count":1,
				"prompt":"HTTP cancellation must revalidate authenticated credentials"
			}`))
			if accepted.StatusCode != http.StatusAccepted {
				t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
			}
			var job jobResponse
			if err := json.Unmarshal(accepted.Body, &job); err != nil {
				t.Fatalf("decode Accepted Job: %v", err)
			}
			if _, err := database.Admin.Exec(`
				UPDATE credentials
				SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
				WHERE id = $1
			`, testCredentialID); err != nil {
				t.Fatalf("grant cancellation scope before HTTP authentication: %v", err)
			}
			installPausedCredentialAuthenticator(t, database.Admin, authenticationPauseLock)

			blocker, err := database.Admin.Conn(context.Background())
			if err != nil {
				t.Fatalf("open authentication pause connection: %v", err)
			}
			defer blocker.Close()
			if _, err := blocker.ExecContext(
				context.Background(), "SELECT pg_advisory_lock($1)", authenticationPauseLock,
			); err != nil {
				t.Fatalf("acquire authentication pause lock: %v", err)
			}
			lockHeld := true
			defer func() {
				if lockHeld {
					_, _ = blocker.ExecContext(
						context.Background(), "SELECT pg_advisory_unlock($1)", authenticationPauseLock,
					)
				}
			}()

			type cancelCall struct {
				result httpResult
				err    error
			}
			cancelResult := make(chan cancelCall, 1)
			go func() {
				result, cancelErr := doCancelJob(
					server.URL,
					testProjectID,
					job.JobID,
					testBearerCredential(),
				)
				cancelResult <- cancelCall{result: result, err: cancelErr}
			}()
			waitForRoleDatabaseLock(t, database.Admin, "vela_auth_login")

			if _, err := database.Admin.Exec(
				"UPDATE credentials SET "+test.mutation+" WHERE id = $1",
				testCredentialID,
			); err != nil {
				t.Fatalf("mutate HTTP-authenticated credential: %v", err)
			}
			var unlocked bool
			if err := blocker.QueryRowContext(
				context.Background(), "SELECT pg_advisory_unlock($1)", authenticationPauseLock,
			).Scan(&unlocked); err != nil {
				t.Fatalf("release authentication pause lock: %v", err)
			}
			if !unlocked {
				t.Fatal("authentication pause lock was not held")
			}
			lockHeld = false

			var response httpResult
			select {
			case call := <-cancelResult:
				if call.err != nil {
					t.Fatalf("cancel Job after credential mutation: %v", call.err)
				}
				response = call.result
			case <-time.After(10 * time.Second):
				t.Fatal("HTTP cancellation did not return after authentication resumed")
			}
			if response.StatusCode != test.wantStatus {
				t.Fatalf(
					"post-authentication credential mutation status = %d, want %d; body=%s",
					response.StatusCode,
					test.wantStatus,
					response.Body,
				)
			}
			if contentType := response.Header.Get("Content-Type"); contentType != "application/json" {
				t.Fatalf("post-authentication credential mutation content type = %q", contentType)
			}
			var responseError struct {
				Code string `json:"code"`
			}
			if err := json.Unmarshal(response.Body, &responseError); err != nil {
				t.Fatalf("decode credential mutation response: %v", err)
			}
			if responseError.Code != test.wantCode {
				t.Fatalf("credential mutation error code = %q, want %q", responseError.Code, test.wantCode)
			}

			var jobState, reservationState string
			var jobVersion, projectQueued, readyQueue int64
			var reservedMinor, unsettledPostedMinor int64
			var decisionCount, chargeCount, outboxCount int64
			if err := database.Admin.QueryRow(`
				SELECT
						job.state,
						job.version,
						project.queued_count,
						(SELECT count(*) FROM stage_ready_queue_entries
						 WHERE project_id = job.project_id),
					reservation.state,
					credit.reserved_minor,
					credit.unsettled_posted_minor,
					(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
					(SELECT count(*) FROM charges WHERE job_id = job.id),
					(SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id)
				FROM jobs AS job
				JOIN projects AS project ON project.id = job.project_id
				JOIN credit_reservations AS reservation ON reservation.job_id = job.id
				JOIN organization_credit_accounts AS credit
				  ON credit.organization_id = job.organization_id
				WHERE job.id = $1
			`, job.JobID).Scan(
				&jobState,
				&jobVersion,
				&projectQueued,
				&readyQueue,
				&reservationState,
				&reservedMinor,
				&unsettledPostedMinor,
				&decisionCount,
				&chargeCount,
				&outboxCount,
			); err != nil {
				t.Fatalf("read HTTP credential mutation state: %v", err)
			}
			if jobState != "QUEUED" || jobVersion != 2 || projectQueued != 1 || readyQueue != 1 ||
				reservationState != "RESERVED" || reservedMinor != 1250 || unsettledPostedMinor != 0 ||
				decisionCount != 0 || chargeCount != 0 || outboxCount != 1 {
				t.Fatalf(
					"HTTP credential mutation state = job %s/%d queued project/ready %d/%d reservation %s credit %d/%d decisions/charges/outbox %d/%d/%d",
					jobState,
					jobVersion,
					projectQueued,
					readyQueue,
					reservationState,
					reservedMinor,
					unsettledPostedMinor,
					decisionCount,
					chargeCount,
					outboxCount,
				)
			}
		})
	}
}

func TestCancellationHTTPAuthorizationAndVisibilityFailClosed(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "cancel-http-authorization", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"authorization failures must not mutate cancellation state"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode authorization fixture Job: %v", err)
	}

	missingScope := cancelJob(
		t, server.URL, testProjectID, job.JobID, testBearerCredential(),
	)
	if missingScope.StatusCode != http.StatusForbidden {
		t.Fatalf("missing cancellation scope status = %d, want 403; body=%s", missingScope.StatusCode, missingScope.Body)
	}
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}

	wrongProject := cancelJob(
		t, server.URL, uuid.NewString(), job.JobID, testBearerCredential(),
	)
	if wrongProject.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong Project cancellation status = %d, want 403; body=%s", wrongProject.StatusCode, wrongProject.Body)
	}
	unknownJob := cancelJob(
		t, server.URL, testProjectID, uuid.NewString(), testBearerCredential(),
	)
	if unknownJob.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown Job cancellation status = %d, want 404; body=%s", unknownJob.StatusCode, unknownJob.Body)
	}

	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET expires_at = clock_timestamp() - interval '1 second'
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("expire cancellation credential: %v", err)
	}
	expired := cancelJob(
		t, server.URL, testProjectID, job.JobID, testBearerCredential(),
	)
	if expired.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired cancellation credential status = %d, want 401; body=%s", expired.StatusCode, expired.Body)
	}
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET expires_at = clock_timestamp() + interval '1 hour',
			revoked_at = clock_timestamp()
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("revoke cancellation credential: %v", err)
	}
	revoked := cancelJob(
		t, server.URL, testProjectID, job.JobID, testBearerCredential(),
	)
	if revoked.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked cancellation credential status = %d, want 401; body=%s", revoked.StatusCode, revoked.Body)
	}

	var jobState, reservationState string
	var queuedCount, reservedMinor, decisionCount, chargeCount int64
	if err := database.Admin.QueryRow(`
		SELECT
			job.state,
			project.queued_count,
			reservation.state,
			credit.reserved_minor,
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
			(SELECT count(*) FROM charges WHERE job_id = job.id)
		FROM jobs AS job
		JOIN projects AS project ON project.id = job.project_id
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN organization_credit_accounts AS credit
		  ON credit.organization_id = job.organization_id
		WHERE job.id = $1
	`, job.JobID).Scan(
		&jobState,
		&queuedCount,
		&reservationState,
		&reservedMinor,
		&decisionCount,
		&chargeCount,
	); err != nil {
		t.Fatalf("read authorization failure state: %v", err)
	}
	if jobState != "QUEUED" || queuedCount != 1 || reservationState != "RESERVED" ||
		reservedMinor != 1250 || decisionCount != 0 || chargeCount != 0 {
		t.Fatalf(
			"authorization failure state = job %s queued %d reservation %s credit %d decisions/charges %d/%d",
			jobState,
			queuedCount,
			reservationState,
			reservedMinor,
			decisionCount,
			chargeCount,
		)
	}
}

func installPausedCredentialAuthenticator(t *testing.T, db *sql.DB, advisoryLockKey int64) {
	t.Helper()
	if _, err := db.Exec(fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION vela_authenticate_service_credential(p_credential_id uuid)
		RETURNS TABLE (
			organization_id uuid,
			project_id uuid,
			principal_id uuid,
			secret_digest bytea,
			scopes text[],
			expires_at timestamptz,
			revoked_at timestamptz
		)
		LANGUAGE plpgsql
		SECURITY DEFINER
		SET search_path = pg_catalog, public
		AS $function$
		DECLARE
			v_organization_id uuid;
			v_project_id uuid;
			v_principal_id uuid;
			v_secret_digest bytea;
			v_scopes text[];
			v_expires_at timestamptz;
			v_revoked_at timestamptz;
		BEGIN
			SELECT
				credential.organization_id,
				credential.project_id,
				credential.principal_id,
				credential.secret_digest,
				credential.scopes,
				credential.expires_at,
				credential.revoked_at
			INTO
				v_organization_id,
				v_project_id,
				v_principal_id,
				v_secret_digest,
				v_scopes,
				v_expires_at,
				v_revoked_at
			FROM public.credentials AS credential
			WHERE credential.id = p_credential_id
			  AND credential.revoked_at IS NULL
			  AND credential.expires_at > clock_timestamp();

			IF FOUND THEN
				PERFORM pg_catalog.pg_advisory_xact_lock(%d);
				RETURN QUERY SELECT
					v_organization_id,
					v_project_id,
					v_principal_id,
					v_secret_digest,
					v_scopes,
					v_expires_at,
					v_revoked_at;
			END IF;
		END
		$function$;
	`, advisoryLockKey)); err != nil {
		t.Fatalf("install paused credential authenticator: %v", err)
	}
}

func TestSucceededStageGraphCancellationReturnsTerminalWithoutDecisionHistory(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "cancel-stage-graph-succeeded-replay")
	server := admissionServerForDatabase(t, fixture.database)
	admin := fixture.database.Admin
	if _, err := admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	jobID := fixture.assignment.JobID

	first := cancelJob(t, server.URL, testProjectID, jobID.String(), testBearerCredential())
	replayed := cancelJob(t, server.URL, testProjectID, jobID.String(), testBearerCredential())
	if first.StatusCode != http.StatusOK || replayed.StatusCode != http.StatusOK {
		t.Fatalf(
			"succeeded Stage graph cancellation statuses = %d/%d, want 200/200; bodies=%s / %s",
			first.StatusCode,
			replayed.StatusCode,
			first.Body,
			replayed.Body,
		)
	}
	var firstResult, replayedResult cancelResponse
	if err := json.Unmarshal(first.Body, &firstResult); err != nil {
		t.Fatalf("decode succeeded Stage graph response: %v", err)
	}
	if err := json.Unmarshal(replayed.Body, &replayedResult); err != nil {
		t.Fatalf("decode replayed succeeded Stage graph response: %v", err)
	}
	if firstResult.CancellationID != uuid.Nil.String() ||
		replayedResult.CancellationID != uuid.Nil.String() ||
		firstResult.Decision != "ALREADY_SUCCEEDED" || replayedResult.Decision != "ALREADY_SUCCEEDED" ||
		firstResult.State != "SUCCEEDED" || replayedResult.State != "SUCCEEDED" ||
		firstResult.JobVersion <= 0 || replayedResult.JobVersion != firstResult.JobVersion ||
		firstResult.Billable || replayedResult.Billable ||
		firstResult.Charge != nil || replayedResult.Charge != nil ||
		firstResult.DecidedAt.IsZero() || !firstResult.DecidedAt.Equal(replayedResult.DecidedAt) {
		t.Fatalf("succeeded Stage graph first/replayed response = %#v / %#v", firstResult, replayedResult)
	}

	var state string
	var version, decisions, charges, cancellationEvents int64
	if err := admin.QueryRow(`
		SELECT
			j.state,
			j.version,
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = j.id),
			(SELECT count(*) FROM charges WHERE job_id = j.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id
			   AND event_type IN ('job.cancel_requested', 'job.canceling', 'job.canceled'))
		FROM jobs AS j
		WHERE j.id = $1
	`, jobID).Scan(
		&state,
		&version,
		&decisions,
		&charges,
		&cancellationEvents,
	); err != nil {
		t.Fatalf("read succeeded Stage graph terminal state: %v", err)
	}
	if state != "SUCCEEDED" || version != firstResult.JobVersion ||
		decisions != 0 || charges != 1 || cancellationEvents != 0 {
		t.Fatalf(
			"succeeded Stage graph state = %s/%d history decisions/charges/cancel-events %d/%d/%d",
			state,
			version,
			decisions,
			charges,
			cancellationEvents,
		)
	}
}

func TestConcurrentQueuedCancellationReplaysOneDecision(t *testing.T) {
	server, admin := newAdmissionServer(t)
	if _, err := admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	accepted := submitJob(t, server.URL, "cancel-concurrent-queued", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"concurrent cancellation must replay one decision"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Accepted Job: %v", err)
	}

	start := make(chan struct{})
	results := make(chan httpResult, 2)
	errorsChannel := make(chan error, 2)
	var waitGroup sync.WaitGroup
	for range 2 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			result, err := doCancelJob(
				server.URL,
				testProjectID,
				job.JobID,
				testBearerCredential(),
			)
			if err != nil {
				errorsChannel <- err
				return
			}
			results <- result
		}()
	}
	close(start)
	waitGroup.Wait()
	close(results)
	close(errorsChannel)
	for err := range errorsChannel {
		t.Fatalf("concurrent queued cancellation request: %v", err)
	}
	var responses []cancelResponse
	for result := range results {
		if result.StatusCode != http.StatusOK {
			t.Fatalf("concurrent queued cancellation status = %d, want 200; body=%s", result.StatusCode, result.Body)
		}
		var response cancelResponse
		if err := json.Unmarshal(result.Body, &response); err != nil {
			t.Fatalf("decode concurrent queued cancellation: %v", err)
		}
		responses = append(responses, response)
	}
	if len(responses) != 2 || responses[0].CancellationID == "" ||
		responses[0].CancellationID != responses[1].CancellationID ||
		responses[0].Decision != "CANCELED" || responses[1].Decision != "CANCELED" ||
		responses[0].JobVersion != 3 || responses[1].JobVersion != 3 ||
		!responses[0].DecidedAt.Equal(responses[1].DecidedAt) {
		t.Fatalf("concurrent queued cancellation responses = %#v", responses)
	}

	var decisionCount, chargeCount, canceledEvents, reservedMinor int64
	if err := admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = $1),
			(SELECT count(*) FROM charges WHERE job_id = $1),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = $1 AND event_type = 'job.canceled'),
			(SELECT reserved_minor FROM organization_credit_accounts WHERE organization_id = $2)
	`, job.JobID, testOrganizationID).Scan(
		&decisionCount,
		&chargeCount,
		&canceledEvents,
		&reservedMinor,
	); err != nil {
		t.Fatalf("read concurrent queued cancellation state: %v", err)
	}
	if decisionCount != 1 || chargeCount != 0 || canceledEvents != 1 || reservedMinor != 0 {
		t.Fatalf(
			"concurrent queued cancellation state = decisions/charges/events/credit %d/%d/%d/%d",
			decisionCount,
			chargeCount,
			canceledEvents,
			reservedMinor,
		)
	}
}

type cancelResponse struct {
	CancellationID string          `json:"cancellation_id"`
	JobID          string          `json:"job_id"`
	Decision       string          `json:"decision"`
	State          string          `json:"state"`
	JobVersion     int64           `json:"job_version"`
	Billable       bool            `json:"billable"`
	Charge         *chargeResponse `json:"charge"`
	DecidedAt      time.Time       `json:"decided_at"`
}

type chargeResponse struct {
	ChargeID string    `json:"charge_id"`
	Amount   int64     `json:"amount_minor"`
	Currency string    `json:"currency"`
	Reason   string    `json:"reason"`
	PostedAt time.Time `json:"posted_at"`
}

func cancelJob(
	t *testing.T,
	serverURL, projectID, jobID, bearerCredential string,
) httpResult {
	t.Helper()
	result, err := doCancelJob(serverURL, projectID, jobID, bearerCredential)
	if err != nil {
		t.Fatalf("cancel Job: %v", err)
	}
	return result
}

func doCancelJob(
	serverURL, projectID, jobID, bearerCredential string,
) (httpResult, error) {
	request, err := http.NewRequest(
		http.MethodPost,
		serverURL+"/v1/projects/"+projectID+"/jobs/"+jobID+"/cancel",
		nil,
	)
	if err != nil {
		return httpResult{}, fmt.Errorf("create cancellation request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+bearerCredential)
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return httpResult{}, fmt.Errorf("send cancellation request: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return httpResult{}, fmt.Errorf("read cancellation response: %w", err)
	}
	return httpResult{StatusCode: response.StatusCode, Header: response.Header.Clone(), Body: body}, nil
}
