package httpapi

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/vivym/vela/internal/identity"
)

func TestServiceCredentialProjectionPreservesPermanentAndFiniteExpiry(t *testing.T) {
	expiry := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name   string
		expiry *time.Time
		want   string
	}{
		{name: "permanent", want: "null"},
		{name: "finite", expiry: &expiry, want: `"2026-10-01T00:00:00Z"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			wire, err := json.Marshal(toAPIServiceCredential(identity.Credential{ExpiresAt: test.expiry}))
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(wire, &fields); err != nil {
				t.Fatal(err)
			}
			if string(fields["expires_at"]) != test.want {
				t.Fatalf("expires_at=%s, want %s", fields["expires_at"], test.want)
			}
		})
	}
}
