//go:build integration

package integration_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestAdmissionRejectsUnsupportedNativeSamplingWithoutEffects(t *testing.T) {
	server, admin := newAdmissionServer(t)
	response := submitJob(t, server.URL, "unsupported-native-sampling", []byte(`{"model":"minimax-h3","generation_preset":"fast","service_class":"standard","output_spec":"h3-native-av-1344x768-5s-24fps","generation_count":1,"prompt":"lake","h3":{}}`))
	if response.StatusCode != http.StatusBadRequest || !strings.Contains(string(response.Body), "lossless") {
		t.Fatalf("response=%d %s", response.StatusCode, response.Body)
	}
	var jobs, reservations, idempotency int
	if err := admin.QueryRow(`SELECT (SELECT count(*) FROM jobs),(SELECT count(*) FROM credit_reservations),(SELECT count(*) FROM idempotency_results)`).Scan(&jobs, &reservations, &idempotency); err != nil {
		t.Fatal(err)
	}
	if jobs != 0 || reservations != 0 || idempotency != 0 {
		t.Fatalf("rejected request left jobs/reservations/idempotency=%d/%d/%d", jobs, reservations, idempotency)
	}
}

func TestNativeH3OutputContractRejectsWithoutEffectsAndAcceptsDefaults(t *testing.T) {
	server, admin := newAdmissionServer(t)
	// Add a real native SKU so invalid_sku cannot mask a missing request guard.
	if _, err := admin.Exec(`
 INSERT INTO output_specs(id,stable_id,revision,state,width,height,duration_milliseconds,frame_rate_milli,codec,media_contract)
 VALUES ('99000000-0000-0000-0000-000000000001','h3-native-av-1344x768-5s-24fps',1,'ACTIVE',1344,768,5000,24000,'h264','h3-native-av-v1');
 INSERT INTO rate_card_lines(id,rate_card_revision_id,model_revision_id,generation_preset_revision_id,service_class_revision_id,output_spec_id,unit_amount_minor,currency)
 SELECT '99000000-0000-0000-0000-000000000002',rate_card_revision_id,model_revision_id,generation_preset_revision_id,service_class_revision_id,'99000000-0000-0000-0000-000000000001',unit_amount_minor,currency FROM rate_card_lines WHERE id='00000000-0000-0000-0000-000000000017';
 INSERT INTO profile_certifications(id,model_revision_id,generation_preset_revision_id,output_spec_id,execution_profile_revision_id,state,evidence_digest,certified_at)
 SELECT '99000000-0000-0000-0000-000000000003',model_revision_id,generation_preset_revision_id,'99000000-0000-0000-0000-000000000001',execution_profile_revision_id,'ACTIVE',evidence_digest,certified_at FROM profile_certifications WHERE state='ACTIVE' LIMIT 1;
 `); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		count   int
		target  string
		message string
	}{
		{"batch", 2, `{}`, "generation_count=1"},
		{"ten-seconds", 1, `{"duration_seconds":10}`, "duration_seconds=5"},
		{"four-seconds", 1, `{"duration_seconds":4}`, "duration_seconds=5"},
		{"portrait", 1, `{"aspect_ratio":"9:16"}`, "aspect_ratio=16:9"},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := []byte(fmt.Sprintf(`{"model":"minimax-h3","generation_preset":"balanced","service_class":"standard","output_spec":"h3-native-av-1344x768-5s-24fps","generation_count":%d,"prompt":"lake","h3":{"target":%s,"sampling":{"num_inference_steps":20,"quality":"lossless"}}}`, test.count, test.target))
			response := submitJob(t, server.URL, "native-contract-"+test.name, body)
			if response.StatusCode != http.StatusBadRequest || !strings.Contains(string(response.Body), `"invalid_request"`) || !strings.Contains(string(response.Body), test.message) {
				t.Fatalf("response=%d %s", response.StatusCode, response.Body)
			}
			assertNoAdmissionEffects(t, admin)
			var keys int
			if err := admin.QueryRow(`SELECT count(*) FROM idempotency_results`).Scan(&keys); err != nil || keys != 0 {
				t.Fatalf("idempotency=%d err=%v", keys, err)
			}
		})
	}
	body := []byte(`{"model":"minimax-h3","generation_preset":"balanced","service_class":"standard","output_spec":"h3-native-av-1344x768-5s-24fps","generation_count":1,"prompt":"lake","h3":{"sampling":{"num_inference_steps":20,"quality":"lossless"}}}`)
	accepted := submitJob(t, server.URL, "native-valid", body)
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("valid native request=%d %s", accepted.StatusCode, accepted.Body)
	}
	replay := submitJob(t, server.URL, "native-valid", body)
	if replay.StatusCode != http.StatusAccepted || string(replay.Body) != string(accepted.Body) {
		t.Fatalf("replay=%d %s accepted=%s", replay.StatusCode, replay.Body, accepted.Body)
	}
}
