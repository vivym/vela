package stageworkeragent

import (
	"context"
	"errors"

	"github.com/vivym/vela/internal/journalbinding"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// InspectJournalBinding observes held identity without admitting execution or
// requiring resolved Runtime routes, empty history or completed writer drain.
func (gate *FileAssignmentAdmission) InspectJournalBinding(ctx context.Context) (*velav1.WorkerBootstrapBinding, error) {
	if gate == nil || ctx == nil {
		return nil, ErrAdmissionClosed
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return nil, err
	}
	if gate.registryBinding == nil {
		return nil, errors.New("assignment journal has no Registry binding")
	}
	if err := gate.verifyRegistryJournal(); err != nil {
		gate.failed = err
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return proto.Clone(gate.registryBinding).(*velav1.WorkerBootstrapBinding), nil
}

func (gate *FileAssignmentAdmission) verifyRegistryJournal() error {
	var memberEpoch int64
	for _, member := range gate.scope.Members {
		if member.ID == gate.scope.WorkerMemberID {
			memberEpoch = member.Epoch
		}
	}
	return gate.registryVerifier.VerifyJournal(gate.registryBinding, journalbinding.WorkerJournal, journalbinding.Journal{
		WorkerInstanceID: gate.scope.WorkerInstanceID, WorkerInstanceEpoch: gate.scope.WorkerInstanceEpoch,
		WorkerMemberID: gate.scope.WorkerMemberID, WorkerMemberEpoch: memberEpoch, JournalID: gate.state.ID, Scope: gate.scopeDigest,
	})
}
