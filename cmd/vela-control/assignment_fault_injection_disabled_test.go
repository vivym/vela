//go:build !vela_lab_fault_injection

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/workercontrol"
)

func TestConfigureSchedulerAssignmentCoordinatorRejectsLabFaultWithoutBuildTag(t *testing.T) {
	t.Setenv(labAssignmentFaultPhaseEnv, assignmentPostCommitPreResponse)
	_, err := configureSchedulerAssignmentCoordinator(disabledTestAssignmentCoordinator{})
	if err == nil || !strings.Contains(err.Error(), "vela_lab_fault_injection") {
		t.Fatalf("configure lab Assignment fault without build tag error = %v", err)
	}
}

func TestLabAssignmentFaultReadCommandRejectsBinaryWithoutBuildTag(t *testing.T) {
	handled, err := runLabAssignmentFaultCommand(
		[]string{labAssignmentFaultReadMarkerArg},
		&bytes.Buffer{},
	)
	if !handled || err == nil || !strings.Contains(err.Error(), "vela_lab_fault_injection") {
		t.Fatalf("Assignment fault command handled = %t, error = %v", handled, err)
	}
}

type disabledTestAssignmentCoordinator struct{}

func (disabledTestAssignmentCoordinator) Acquire(
	context.Context,
	workercontrol.AuthenticatedWorker,
	int64,
	*workercontrol.AssignmentCandidate,
) (workercontrol.Assignment, error) {
	return workercontrol.Assignment{}, nil
}
