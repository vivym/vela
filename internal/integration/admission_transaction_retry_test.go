//go:build integration

package integration_test

import (
	"fmt"
	"testing"
)

func TestAdmissionRetriesConfirmedTransactionAbortWithoutDuplicateEffects(t *testing.T) {
	for _, code := range []string{"40P01", "40001"} {
		t.Run(code, func(t *testing.T) {
			server, admin := newAdmissionServer(t)
			_, err := admin.Exec(fmt.Sprintf(`
CREATE SEQUENCE admission_abort_probe;
CREATE FUNCTION admission_abort_probe() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
BEGIN
 IF nextval('admission_abort_probe') <= 2 THEN RAISE EXCEPTION 'injected transaction abort' USING ERRCODE='%s'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER admission_abort_probe AFTER INSERT ON idempotency_results FOR EACH ROW EXECUTE FUNCTION admission_abort_probe();`, code))
			if err != nil {
				t.Fatal(err)
			}
			body := []byte(`{"model":"minimax-h3","generation_preset":"balanced","service_class":"standard","output_spec":"video-1080p-5s-24fps","generation_count":1,"prompt":"confirmed abort retry"}`)
			before := readStageAdmissionEffectCounts(t, admin)
			response := submitJob(t, server.URL, "admission-abort-retry", body)
			if response.StatusCode != 202 {
				t.Fatalf("confirmed transaction abort leaked to API: status=%d body=%s", response.StatusCode, response.Body)
			}
			var tries int
			if err := admin.QueryRow(`SELECT last_value FROM admission_abort_probe`).Scan(&tries); err != nil {
				t.Fatal(err)
			}
			if tries != 3 {
				t.Fatalf("attempts=%d want=3", tries)
			}
			after := readStageAdmissionEffectCounts(t, admin)
			for _, i := range []int{2, 3, 4, 5, 8, 9} {
				if after[i] != before[i]+1 {
					t.Fatalf("duplicate/missing admission effect index=%d before=%v after=%v", i, before, after)
				}
			}
			replay := submitJob(t, server.URL, "admission-abort-retry", body)
			if replay.StatusCode != 202 {
				t.Fatalf("idempotency replay: %d %s", replay.StatusCode, replay.Body)
			}
			if got := readStageAdmissionEffectCounts(t, admin); got != after {
				t.Fatalf("replay changed effects: %v -> %v", after, got)
			}
		})
	}
}

func TestAdmissionTransactionRetryIsBoundedAndRequiresConfirmedAbort(t *testing.T) {
	for _, tc := range []struct {
		name, code    string
		status, tries int
	}{{"persistent-deadlock", "40P01", 503, 3}, {"constraint-error", "23514", 500, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			server, admin := newAdmissionServer(t)
			_, err := admin.Exec(fmt.Sprintf(`CREATE SEQUENCE admission_abort_probe; CREATE FUNCTION admission_abort_probe() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$ BEGIN PERFORM nextval('admission_abort_probe'); RAISE EXCEPTION 'injected persistent abort' USING ERRCODE='%s'; END $$; CREATE TRIGGER admission_abort_probe AFTER INSERT ON idempotency_results FOR EACH ROW EXECUTE FUNCTION admission_abort_probe();`, tc.code))
			if err != nil {
				t.Fatal(err)
			}
			before := readStageAdmissionEffectCounts(t, admin)
			body := []byte(`{"model":"minimax-h3","generation_preset":"balanced","service_class":"standard","output_spec":"video-1080p-5s-24fps","generation_count":1,"prompt":"bounded abort retry"}`)
			response := submitJob(t, server.URL, "admission-abort-bounded", body)
			if response.StatusCode != tc.status {
				t.Fatalf("status=%d body=%s want=%d", response.StatusCode, response.Body, tc.status)
			}
			var tries int
			if err := admin.QueryRow(`SELECT last_value FROM admission_abort_probe`).Scan(&tries); err != nil {
				t.Fatal(err)
			}
			if tries != tc.tries {
				t.Fatalf("attempts=%d want=%d", tries, tc.tries)
			}
			if after := readStageAdmissionEffectCounts(t, admin); after != before {
				t.Fatalf("aborted admission changed effects: %v -> %v", before, after)
			}
		})
	}
}
