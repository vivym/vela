package debugdump

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/identity"
)

func TestAuthorizeRejectsNonHumanAndCrossProjectAuthorityBeforePersistence(t *testing.T) {
	t.Parallel()

	projectID := uuid.New()
	jobID := uuid.New()
	tests := []struct {
		name  string
		actor identity.Principal
		code  FailureCode
	}{
		{
			name: "missing credential",
			actor: identity.Principal{
				Kind: identity.PrincipalKindHuman, ProjectID: projectID,
				OrganizationID: uuid.New(), PrincipalID: uuid.New(),
				Scopes: []string{identity.ScopeDebugDumpsManage},
			},
			code: FailureUnauthorized,
		},
		{
			name: "service principal",
			actor: identity.Principal{
				Kind: identity.PrincipalKindService, CredentialID: uuid.New(),
				ProjectID: projectID, OrganizationID: uuid.New(), PrincipalID: uuid.New(),
				Scopes: []string{identity.ScopeDebugDumpsManage},
			},
			code: FailureForbidden,
		},
		{
			name: "cross project human",
			actor: identity.Principal{
				Kind: identity.PrincipalKindHuman, CredentialID: uuid.New(),
				ProjectID: uuid.New(), OrganizationID: uuid.New(), PrincipalID: uuid.New(),
				Scopes: []string{identity.ScopeDebugDumpsManage},
			},
			code: FailureNotFound,
		},
		{
			name: "scope absent",
			actor: identity.Principal{
				Kind: identity.PrincipalKindHuman, CredentialID: uuid.New(),
				ProjectID: projectID, OrganizationID: uuid.New(), PrincipalID: uuid.New(),
			},
			code: FailureForbidden,
		},
	}

	service := &Service{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := service.Authorize(
				context.Background(), test.actor, projectID, jobID,
				"debug-auth-key", PurposeCustomerSupport,
			)
			var failure *Failure
			if !errors.As(err, &failure) || failure.Code != test.code {
				t.Fatalf("Authorize error = %v, want %s", err, test.code)
			}
		})
	}
}
