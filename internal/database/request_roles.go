package database

import (
	"context"
	"fmt"
)

type RequestRoleSurface string

const (
	RequestRoleSurfaceServiceAuthentication RequestRoleSurface = "service_authentication"
	RequestRoleSurfaceJobSubmit             RequestRoleSurface = "job_submit"
	RequestRoleSurfaceJobRead               RequestRoleSurface = "job_read"
	RequestRoleSurfaceArtifactRead          RequestRoleSurface = "artifact_read"
)

type RequestRoleObservation struct {
	Surface       RequestRoleSurface
	DatabaseLogin string
	DatabaseRole  Role
}

type RequestRoleObserver interface {
	ObserveRequestRole(context.Context, RequestRoleObservation)
}

type requestRoleObservers []RequestRoleObserver

func (observers requestRoleObservers) ObserveRequestRole(
	ctx context.Context,
	observation RequestRoleObservation,
) {
	for _, observer := range observers {
		observer.ObserveRequestRole(ctx, observation)
	}
}

func CombineRequestRoleObservers(observers ...RequestRoleObserver) RequestRoleObserver {
	combined := make(requestRoleObservers, 0, len(observers))
	for _, observer := range observers {
		if observer != nil {
			combined = append(combined, observer)
		}
	}
	switch len(combined) {
	case 0:
		return nil
	case 1:
		return combined[0]
	default:
		return combined
	}
}

func VerifyRequestRole(databaseLogin string, databaseRole Role, member bool) error {
	if databaseLogin == "" {
		return fmt.Errorf("database request login is empty for role %q", databaseRole)
	}
	if _, supported := roleDescriptors[databaseRole]; !supported {
		return fmt.Errorf("unsupported database request role %q", databaseRole)
	}
	expectedLogin := string(databaseRole) + "_login"
	if databaseLogin != expectedLogin {
		return fmt.Errorf(
			"database request login %q does not match required login %q for role %q",
			databaseLogin,
			expectedLogin,
			databaseRole,
		)
	}
	if !member {
		return fmt.Errorf(
			"database request login %q is not a member of required role %q",
			databaseLogin,
			databaseRole,
		)
	}
	return nil
}
