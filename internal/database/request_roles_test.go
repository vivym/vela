package database

import (
	"context"
	"strings"
	"testing"
)

type recordingRequestRoleObserver struct {
	observations []RequestRoleObservation
}

func (observer *recordingRequestRoleObserver) ObserveRequestRole(
	_ context.Context,
	observation RequestRoleObservation,
) {
	observer.observations = append(observer.observations, observation)
}

func TestVerifyRequestRoleFailsClosed(t *testing.T) {
	tests := []struct {
		name     string
		login    string
		role     Role
		member   bool
		wantText string
	}{
		{name: "empty login", role: RoleRequest, member: true, wantText: "login is empty"},
		{name: "unsupported role", login: "vela_request_login", role: Role("unknown"), member: true, wantText: "unsupported"},
		{name: "mismatched login", login: "vela_internal_login", role: RoleRequest, member: true, wantText: "does not match"},
		{name: "membership revoked", login: "vela_request_login", role: RoleRequest, member: false, wantText: "not a member"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := VerifyRequestRole(test.login, test.role, test.member)
			if err == nil || !strings.Contains(err.Error(), test.wantText) {
				t.Fatalf("VerifyRequestRole error = %v, want %q", err, test.wantText)
			}
		})
	}
	if err := VerifyRequestRole("vela_request_login", RoleRequest, true); err != nil {
		t.Fatalf("verified request role rejected: %v", err)
	}
}

func TestCombineRequestRoleObserversFansOutAndIgnoresNil(t *testing.T) {
	first := &recordingRequestRoleObserver{}
	second := &recordingRequestRoleObserver{}
	combined := CombineRequestRoleObservers(first, nil, second)
	if combined == nil {
		t.Fatal("combined observer is nil")
	}
	observation := RequestRoleObservation{
		Surface:       RequestRoleSurfaceJobRead,
		DatabaseLogin: "vela_request_login",
		DatabaseRole:  RoleRequest,
	}
	combined.ObserveRequestRole(context.Background(), observation)
	for name, observer := range map[string]*recordingRequestRoleObserver{
		"first":  first,
		"second": second,
	} {
		if len(observer.observations) != 1 || observer.observations[0] != observation {
			t.Fatalf("%s observations = %#v, want %#v", name, observer.observations, observation)
		}
	}
	if CombineRequestRoleObservers(nil) != nil {
		t.Fatal("nil-only observer combination is non-nil")
	}
}
