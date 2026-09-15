//go:build !linux

package stageworkeragent

import (
	"context"
	"errors"

	"github.com/vivym/vela/internal/workerjournalwire"
)

var (
	ErrRemoteWorkerJournalRejected  = errors.New("node rejected Worker journal operation")
	ErrRemoteWorkerJournalUncertain = errors.New("worker journal mutation outcome is uncertain")
	ErrRemoteWorkerJournalClosed    = errors.New("worker journal client is closed")
)

type RemoteWorkerJournalConfig struct {
	Socket            string
	PIDFDBrokerSocket string
	Identity          workerjournalwire.Identity
}

type RemoteInputTransferJournal struct{}
type RemoteMaterializationJournal struct{}

func NewRemoteInputTransferJournal(RemoteWorkerJournalConfig) (*RemoteInputTransferJournal, error) {
	return nil, errors.New("node-owned Worker journal transport requires Linux")
}
func NewRemoteMaterializationJournal(RemoteWorkerJournalConfig) (*RemoteMaterializationJournal, error) {
	return nil, errors.New("node-owned Worker journal transport requires Linux")
}
func (*RemoteInputTransferJournal) Load(context.Context, [32]byte) (InputTransferJournalRecord, bool, error) {
	return InputTransferJournalRecord{}, false, errors.New("node-owned Worker journal transport requires Linux")
}
func (*RemoteInputTransferJournal) PutPending(context.Context, InputTransferJournalRecord) error {
	return errors.New("node-owned Worker journal transport requires Linux")
}
func (*RemoteInputTransferJournal) MarkConsumed(context.Context, InputTransferJournalRecord) error {
	return errors.New("node-owned Worker journal transport requires Linux")
}
func (*RemoteInputTransferJournal) Close() error { return nil }
func (*RemoteMaterializationJournal) EnsureCapacity(context.Context) error {
	return errors.New("node-owned Worker journal transport requires Linux")
}
func (*RemoteMaterializationJournal) Put(context.Context, PendingMaterialization) error {
	return errors.New("node-owned Worker journal transport requires Linux")
}
func (*RemoteMaterializationJournal) List(context.Context) ([]PendingMaterialization, error) {
	return nil, errors.New("node-owned Worker journal transport requires Linux")
}
func (*RemoteMaterializationJournal) Delete(context.Context, string) error {
	return errors.New("node-owned Worker journal transport requires Linux")
}
func (*RemoteMaterializationJournal) Close() error { return nil }

var _ InputTransferJournal = (*RemoteInputTransferJournal)(nil)
var _ MaterializationJournal = (*RemoteMaterializationJournal)(nil)
