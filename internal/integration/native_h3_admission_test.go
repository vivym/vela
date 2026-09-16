//go:build integration

package integration_test

import (
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
