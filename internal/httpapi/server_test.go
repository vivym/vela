package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/artifactaccess"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/webhook"
)

func TestAuthenticationFailurePreservesServiceContractAndSupportsHumanLanguage(t *testing.T) {
	handler, err := NewHandler(Config{
		Authenticator:          identity.NewAuthenticator(nil, []byte("test-credential-pepper")),
		IdentityAdministration: &identity.AdministrationService{},
		Admission:              &admission.Service{},
		Cancellation:           &cancellation.Service{},
		Artifacts:              &artifactaccess.Service{},
		Webhooks:               &webhook.Service{},
	})
	if err != nil {
		t.Fatalf("create HTTP handler: %v", err)
	}

	for _, test := range []struct {
		name          string
		authorization string
		wantMessage   string
		wantBody      string
	}{
		{
			name: "missing bearer credential", wantMessage: "valid bearer credential is required",
		},
		{
			name: "invalid Human bearer credential", authorization: "Bearer invalid-human-token",
			wantMessage: "valid bearer credential is required",
		},
		{
			name: "invalid Service bearer credential", authorization: "Bearer vla_invalid",
			wantMessage: "valid Service Principal credential is required",
			wantBody:    "{\"code\":\"unauthorized\",\"message\":\"valid Service Principal credential is required\"}\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(
				http.MethodGet,
				"/v1/projects/00000000-0000-0000-0000-000000000001/jobs/"+
					"00000000-0000-0000-0000-000000000002",
				nil,
			)
			if test.authorization != "" {
				request.Header.Set("Authorization", test.authorization)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != http.StatusUnauthorized {
				t.Fatalf("authentication status = %d, want 401", response.Code)
			}
			var failure struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil {
				t.Fatalf("decode authentication failure: %v", err)
			}
			if failure.Code != "unauthorized" || failure.Message != test.wantMessage {
				t.Fatalf("authentication failure = %#v", failure)
			}
			if test.wantBody != "" && response.Body.String() != test.wantBody {
				t.Fatalf("Service authentication response body = %q, want %q", response.Body.String(), test.wantBody)
			}
			if !strings.HasPrefix(strings.TrimPrefix(test.authorization, "Bearer "), "vla_") &&
				strings.Contains(strings.ToLower(failure.Message), "service principal") {
				t.Fatalf("authentication failure exposes Service-only language: %q", failure.Message)
			}
		})
	}
}
