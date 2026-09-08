package nodeagent

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

const (
	runtimeStartupLedgerName     = "runtime-startups.jsonl"
	maxRuntimeStartupRecords     = 1024
	maxRuntimeStartupLedgerBytes = 8 << 20
)

var (
	ErrRuntimeStartupLedger   = errors.New("runtime startup ledger is unavailable, changed or incomplete")
	ErrRuntimeStartupRecorded = errors.New("runtime journal already has a recorded startup; no new owner may register")
)

// RuntimeStartupRecord binds an authenticated declaration to an independently
// retained namespace owner. It is not proof of the actual journal lock,
// effective launch, current activation, or authorization to start a backend.
type RuntimeStartupRecord struct {
	OperationID        uuid.UUID                          `json:"operation_id"`
	Request            modelruntime.BackendStartupRequest `json:"request"`
	RegistryBinding    []byte                             `json:"registry_binding"`
	Owner              RuntimeContainerCallerObservation  `json:"owner"`
	PodResourceVersion string                             `json:"pod_resource_version"`
	RetainedAt         time.Time                          `json:"retained_at"`
	RecordedAt         time.Time                          `json:"recorded_at"`
	Remote             *RuntimeStartupRemoteIntent        `json:"remote,omitempty"`
}

// RuntimeStartupExit retains exact-owner kernel exit across Node restart.
// It neither retires the backend nor grants replacement/device release.
type RuntimeStartupExit struct {
	OperationID uuid.UUID                       `json:"operation_id"`
	JournalID   uuid.UUID                       `json:"journal_id"`
	Observation RuntimeNamespaceExitObservation `json:"observation"`
}

type runtimeStartupFileIdentity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

type runtimeStartupHeader struct {
	SchemaVersion int                        `json:"schema_version"`
	ID            uuid.UUID                  `json:"ledger_id"`
	NodeIdentity  string                     `json:"node_identity"`
	Root          runtimeStartupFileIdentity `json:"root"`
	File          runtimeStartupFileIdentity `json:"file"`
}

type runtimeStartupEntry struct {
	Startup     *RuntimeStartupRecord            `json:"startup,omitempty"`
	Exit        *RuntimeStartupExit              `json:"exit,omitempty"`
	Reservation *RuntimeStartupReservationRecord `json:"reservation,omitempty"`
}

// RuntimeStartupLedger owns one root-only append journal and its lifetime lock.
// Initialization is explicit and only accepts an empty pre-existing directory.
// Lost files are never recreated on recovery. No method issues a startup grant.
type RuntimeStartupLedger struct {
	mu           sync.Mutex
	path         string
	root         *os.Root
	file         *os.File
	header       runtimeStartupHeader
	digest       [sha256.Size]byte
	size         int64
	failed       error
	closed       bool
	starts       map[uuid.UUID]RuntimeStartupRecord
	exits        map[uuid.UUID]RuntimeStartupExit
	owners       map[uuid.UUID]*RuntimeNamespaceOwner
	reservations map[uuid.UUID]RuntimeStartupReservationRecord
	boundary     func(string) error
}

func OpenRuntimeStartupLedger(ctx context.Context, directory, nodeIdentity string, initialize bool) (*RuntimeStartupLedger, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if os.Geteuid() != 0 || !validText(nodeIdentity, maxIdentityText) || !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return nil, ErrRuntimeStartupLedger
	}
	anchor, err := securefile.OpenTrustedDirectory(directory)
	if err != nil {
		return nil, err
	}
	defer func() { _ = anchor.Close() }()
	info, err := anchor.Stat()
	if err != nil || !runtimeStartupPrivate(info, true) {
		return nil, ErrRuntimeStartupLedger
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	ledger := &RuntimeStartupLedger{path: directory, root: root,
		starts: make(map[uuid.UUID]RuntimeStartupRecord), exits: make(map[uuid.UUID]RuntimeStartupExit), owners: make(map[uuid.UUID]*RuntimeNamespaceOwner),
		reservations: make(map[uuid.UUID]RuntimeStartupReservationRecord)}
	success := false
	defer func() {
		if !success {
			_ = ledger.Close()
		}
	}()
	opened, err := root.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		return nil, ErrRuntimeStartupLedger
	}
	flags := os.O_RDWR | os.O_APPEND | unix.O_NOFOLLOW | unix.O_NONBLOCK
	if initialize {
		// A failed or partial initialization cannot be retried over existing files.
		names, err := anchor.Readdirnames(1)
		if err != nil && !errors.Is(err, io.EOF) || len(names) != 0 {
			return nil, ErrRuntimeStartupLedger
		}
		flags |= os.O_CREATE | os.O_EXCL
	}
	ledger.file, err = root.OpenFile(runtimeStartupLedgerName, flags, 0o600)
	if err != nil {
		return nil, err
	}
	fileInfo, err := ledger.file.Stat()
	if err != nil || !runtimeStartupPrivate(fileInfo, false) {
		return nil, ErrRuntimeStartupLedger
	}
	if err := unix.Flock(int(ledger.file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, err
	}
	if initialize {
		ledger.header = runtimeStartupHeader{SchemaVersion: 2, ID: uuid.New(), NodeIdentity: nodeIdentity,
			Root: runtimeStartupIdentity(info), File: runtimeStartupIdentity(fileInfo)}
		wire, err := json.Marshal(ledger.header)
		if err != nil {
			return nil, err
		}
		if _, err := ledger.file.Write(append(wire, '\n')); err != nil {
			return nil, err
		}
	}
	document, err := ledger.read()
	if err != nil {
		return nil, err
	}
	lines := bufio.NewScanner(bytes.NewReader(document))
	lines.Buffer(make([]byte, 4096), maxLedgerRecordBytes)
	if !lines.Scan() || decodeRuntimeStartupLine(lines.Bytes(), &ledger.header) != nil || (ledger.header.SchemaVersion != 1 && ledger.header.SchemaVersion != 2) || ledger.header.ID == uuid.Nil ||
		ledger.header.NodeIdentity != nodeIdentity || ledger.header.Root != runtimeStartupIdentity(info) || ledger.header.File != runtimeStartupIdentity(fileInfo) {
		return nil, ErrRuntimeStartupLedger
	}
	for lines.Scan() {
		var entry runtimeStartupEntry
		if err := decodeRuntimeStartupLine(lines.Bytes(), &entry); err != nil {
			return nil, err
		}
		if err := ledger.apply(entry); err != nil {
			return nil, err
		}
	}
	if err := lines.Err(); err != nil {
		return nil, err
	}
	ledger.size, ledger.digest = int64(len(document)), sha256.Sum256(document)
	// Reconfirm durability if a previous process died after append or file sync.
	if err := errors.Join(ledger.file.Sync(), anchor.Sync(), context.Cause(ctx), ledger.check()); err != nil {
		return nil, err
	}
	success = true
	return ledger, nil
}

// Record retains the original owner before appending the startup association.
// Exact replay and changed callers both reject. A returned record is evidence,
// never a token that can be exchanged for permission by this API.
func (ledger *RuntimeStartupLedger) Record(ctx context.Context, plan *RuntimeLaunchPlan, pods RuntimeLaunchPodReader, observer *RuntimeContainerObserver, caller *RuntimeCaller) (RuntimeStartupRecord, error) {
	return ledger.record(ctx, plan, pods, observer, caller, nil)
}

func (ledger *RuntimeStartupLedger) record(ctx context.Context, plan *RuntimeLaunchPlan, pods RuntimeLaunchPodReader, observer *RuntimeContainerObserver, caller *RuntimeCaller, remote *RuntimeStartupRemoteIntent) (RuntimeStartupRecord, error) {
	if err := contextError(ctx); err != nil {
		return RuntimeStartupRecord{}, err
	}
	if ledger == nil || plan == nil || plan.binding == nil || caller == nil || observer == nil {
		return RuntimeStartupRecord{}, ErrRuntimeStartupLedger
	}
	payload := caller.Payload()
	request, err := modelruntime.ParseBackendStartupRequest(payload)
	if err != nil {
		return RuntimeStartupRecord{}, err
	}
	binding, err := proto.MarshalOptions{Deterministic: true}.Marshal(plan.binding)
	if err != nil || request.NodeIdentity != plan.binding.Claim.NodeIdentity || request.JournalID.String() != plan.binding.Pair.RuntimeJournalId ||
		!bytes.Equal(request.JournalScope[:], plan.binding.Pair.RuntimeScope) || request.RegistryBindingDigest != sha256.Sum256(binding) || request.LaunchDigest != sha256.Sum256(plan.manifest) {
		return RuntimeStartupRecord{}, errors.Join(ErrRuntimeLaunchPlan, err)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if err := errors.Join(ledger.check(), context.Cause(ctx)); err != nil {
		return RuntimeStartupRecord{}, err
	}
	if request.NodeIdentity != ledger.header.NodeIdentity || len(ledger.starts) >= maxRuntimeStartupRecords {
		return RuntimeStartupRecord{}, ErrRuntimeStartupLedger
	}
	if remote != nil && ledger.header.SchemaVersion != 2 {
		return RuntimeStartupRecord{}, ErrRuntimeStartupLedger
	}
	if _, exists := ledger.starts[request.JournalID]; exists {
		return RuntimeStartupRecord{}, ErrRuntimeStartupRecorded
	}
	observation, err := observer.observePlannedCaller(ctx, plan, pods, caller, payload)
	if err != nil {
		return RuntimeStartupRecord{}, err
	}
	owner, err := observer.RetainNamespaceOwner(ctx, observation.Caller.Container.Target, caller)
	if err != nil {
		return RuntimeStartupRecord{}, err
	}
	retained := false
	defer func() {
		if !retained {
			_ = owner.Close()
		}
	}()
	previous := observation.Caller.Process
	previous.ObservedAt = owner.owner.Process.ObservedAt
	if previous != owner.owner.Process || owner.owner.Container.Target != observation.Caller.Container.Target {
		return RuntimeStartupRecord{}, ErrRuntimeNamespaceOwnerLost
	}
	record := RuntimeStartupRecord{OperationID: uuid.New(), Request: request, RegistryBinding: binding, Owner: owner.owner,
		PodResourceVersion: observation.PodResourceVersion, RetainedAt: owner.retainedAt, RecordedAt: time.Now().UTC()}
	if remote != nil {
		copyRemote := *remote
		copyRemote.Epochs = slices.Clone(remote.Epochs)
		copyRemote.Executable = observation.Executable
		record.Remote = &copyRemote
	}
	if err := ledger.append(ctx, runtimeStartupEntry{Startup: &record}); err != nil {
		return RuntimeStartupRecord{}, err
	}
	ledger.owners[request.JournalID] = owner
	retained = true
	return cloneRuntimeStartup(record), nil
}

func (ledger *RuntimeStartupLedger) Inspect(ctx context.Context, journalID uuid.UUID) (RuntimeStartupRecord, error) {
	if err := contextError(ctx); err != nil {
		return RuntimeStartupRecord{}, err
	}
	if ledger == nil {
		return RuntimeStartupRecord{}, ErrRuntimeStartupLedger
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if err := errors.Join(ledger.check(), context.Cause(ctx)); err != nil {
		return RuntimeStartupRecord{}, err
	}
	record, ok := ledger.starts[journalID]
	if !ok {
		return RuntimeStartupRecord{}, os.ErrNotExist
	}
	return cloneRuntimeStartup(record), nil
}

// RecordExit persists only an exit event from this ledger's original retained
// pidfd. After restart, unresolved entries cannot reconstruct that handle from
// serialized process IDs or absent metadata. Already durable exits are readable.
func (ledger *RuntimeStartupLedger) RecordExit(ctx context.Context, journalID uuid.UUID) (RuntimeStartupExit, error) {
	if err := contextError(ctx); err != nil {
		return RuntimeStartupExit{}, err
	}
	if ledger == nil {
		return RuntimeStartupExit{}, ErrRuntimeStartupLedger
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if err := errors.Join(ledger.check(), context.Cause(ctx)); err != nil {
		return RuntimeStartupExit{}, err
	}
	if exit, ok := ledger.exits[journalID]; ok {
		return exit, nil
	}
	owner := ledger.owners[journalID]
	observed, err := owner.ObserveExit(ctx)
	if err != nil {
		return RuntimeStartupExit{}, err
	}
	exit := RuntimeStartupExit{OperationID: ledger.starts[journalID].OperationID, JournalID: journalID, Observation: observed}
	if err := ledger.append(ctx, runtimeStartupEntry{Exit: &exit}); err != nil {
		return RuntimeStartupExit{}, err
	}
	_ = owner.Close()
	delete(ledger.owners, journalID)
	return exit, nil
}

func cloneRuntimeStartup(record RuntimeStartupRecord) RuntimeStartupRecord {
	record.RegistryBinding = slices.Clone(record.RegistryBinding)
	if record.Remote != nil {
		copyRemote := *record.Remote
		copyRemote.Epochs = slices.Clone(record.Remote.Epochs)
		record.Remote = &copyRemote
	}
	return record
}

func (ledger *RuntimeStartupLedger) append(ctx context.Context, entry runtimeStartupEntry) (err error) {
	if err := errors.Join(ledger.check(), context.Cause(ctx)); err != nil {
		return err
	}
	wire, err := json.Marshal(entry)
	if err != nil || len(wire) >= maxLedgerRecordBytes || ledger.size+int64(len(wire))+1 > maxRuntimeStartupLedgerBytes {
		return errors.Join(ErrRuntimeStartupLedger, err)
	}
	reserved, err := ledger.reservedExitBytes(entry)
	if err != nil || ledger.size+int64(len(wire))+1+reserved > maxRuntimeStartupLedgerBytes {
		return errors.Join(ErrRuntimeStartupLedger, err)
	}
	// Any uncertain append poisons this live handle. Recovery must parse every
	// byte and reconfirm fsync before using even a fully written previous record.
	defer func() {
		if err != nil {
			ledger.failed = err
		}
	}()
	if err = ledger.checkpoint("before-append"); err != nil {
		return err
	}
	wire = append(wire, '\n')
	if n, writeErr := ledger.file.Write(wire); writeErr != nil || n != len(wire) {
		return errors.Join(io.ErrShortWrite, writeErr)
	}
	if err = ledger.checkpoint("after-append"); err != nil {
		return err
	}
	if err = ledger.file.Sync(); err != nil {
		return err
	}
	if err = ledger.checkpoint("after-sync"); err != nil {
		return err
	}
	document, err := ledger.read()
	if err != nil || int64(len(document)) != ledger.size+int64(len(wire)) || sha256.Sum256(document[:min(len(document), int(ledger.size))]) != ledger.digest || !bytes.HasSuffix(document, wire) {
		return errors.Join(ErrRuntimeStartupLedger, err)
	}
	ledger.size, ledger.digest = int64(len(document)), sha256.Sum256(document)
	if err = errors.Join(ledger.check(), context.Cause(ctx)); err != nil {
		return err
	}
	return ledger.apply(entry)
}

// Keep enough journal space for each registered owner's eventual exit, even
// when new registrations exhaust the journal's total byte budget.
func (ledger *RuntimeStartupLedger) reservedExitBytes(next runtimeStartupEntry) (int64, error) {
	var total int64
	reserve := func(record RuntimeStartupRecord) error {
		wire, err := json.Marshal(runtimeStartupEntry{Exit: &RuntimeStartupExit{OperationID: record.OperationID, JournalID: record.Request.JournalID,
			Observation: RuntimeNamespaceExitObservation{Owner: record.Owner, RetainedAt: record.RetainedAt,
				ObservedAt: time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)}}})
		if err != nil || len(wire) >= maxLedgerRecordBytes {
			return errors.Join(ErrRuntimeStartupLedger, err)
		}
		total += int64(len(wire)) + 1
		if record.Remote != nil {
			_, recorded := ledger.reservations[record.Request.JournalID]
			if !recorded && (next.Reservation == nil || next.Reservation.JournalID != record.Request.JournalID) {
				reserved := RuntimeStartupReservationRecord{OperationID: record.OperationID, JournalID: record.Request.JournalID,
					RequestDigest: [sha256.Size]byte{255}, ReservedAt: time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
					RecordedAt: time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)}
				// Worst-case decimal JSON encoding for the fixed digest array.
				for i := range reserved.RequestDigest {
					reserved.RequestDigest[i] = 255
				}
				wire, err := json.Marshal(runtimeStartupEntry{Reservation: &reserved})
				if err != nil || len(wire) >= maxLedgerRecordBytes {
					return errors.Join(ErrRuntimeStartupLedger, err)
				}
				total += int64(len(wire)) + 1
			}
		}
		return nil
	}
	for id, record := range ledger.starts {
		if _, exited := ledger.exits[id]; exited || next.Exit != nil && next.Exit.JournalID == id {
			continue
		}
		if err := reserve(record); err != nil {
			return 0, err
		}
	}
	if next.Startup != nil {
		if err := reserve(*next.Startup); err != nil {
			return 0, err
		}
	}
	return total, nil
}

func (ledger *RuntimeStartupLedger) checkpoint(phase string) error {
	if ledger.boundary != nil {
		return ledger.boundary(phase)
	}
	return nil
}

func (ledger *RuntimeStartupLedger) check() (err error) {
	if ledger == nil {
		return ErrRuntimeStartupLedger
	}
	defer func() {
		if err != nil && ledger.failed == nil {
			ledger.failed = err
		}
	}()
	if ledger.closed || ledger.failed != nil || ledger.root == nil || ledger.file == nil {
		return errors.Join(ErrRuntimeStartupLedger, ledger.failed)
	}
	anchor, err := securefile.OpenTrustedDirectory(ledger.path)
	if err != nil {
		return err
	}
	defer func() { _ = anchor.Close() }()
	info, err := anchor.Stat()
	if err != nil || !runtimeStartupPrivate(info, true) || runtimeStartupIdentity(info) != ledger.header.Root {
		return ErrRuntimeStartupLedger
	}
	info, err = ledger.root.Stat(".")
	if err != nil || !runtimeStartupPrivate(info, true) || runtimeStartupIdentity(info) != ledger.header.Root {
		return ErrRuntimeStartupLedger
	}
	info, err = ledger.root.Lstat(runtimeStartupLedgerName)
	if err != nil || !runtimeStartupPrivate(info, false) || runtimeStartupIdentity(info) != ledger.header.File {
		return ErrRuntimeStartupLedger
	}
	document, err := ledger.read()
	if err != nil || int64(len(document)) != ledger.size || sha256.Sum256(document) != ledger.digest {
		return errors.Join(ErrRuntimeStartupLedger, err)
	}
	return nil
}

func (ledger *RuntimeStartupLedger) read() ([]byte, error) {
	info, err := ledger.file.Stat()
	if err != nil || !runtimeStartupPrivate(info, false) || info.Size() <= 0 || info.Size() > maxRuntimeStartupLedgerBytes {
		return nil, ErrRuntimeStartupLedger
	}
	document := make([]byte, info.Size())
	if _, err := ledger.file.ReadAt(document, 0); err != nil || document[len(document)-1] != '\n' || bytes.ContainsRune(document, '\r') {
		return nil, errors.Join(ErrRuntimeStartupLedger, err)
	}
	current, err := ledger.file.Stat()
	if err != nil || current.Size() != info.Size() || current.ModTime() != info.ModTime() || !runtimeStartupPrivate(current, false) {
		return nil, ErrRuntimeStartupLedger
	}
	return document, nil
}

func decodeRuntimeStartupLine(document []byte, value any) error {
	if len(document) == 0 || len(document) >= maxLedgerRecordBytes || strictjson.RejectDuplicateKeys(document) != nil {
		return ErrRuntimeStartupLedger
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return errors.Join(ErrRuntimeStartupLedger, err)
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(canonical, document) {
		return errors.Join(ErrRuntimeStartupLedger, err)
	}
	return nil
}

func runtimeStartupPrivate(info os.FileInfo, directory bool) bool {
	if info == nil || info.Mode().Perm() != 0o600 && !directory || directory && info.Mode().Perm() != 0o700 ||
		info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 || directory != info.IsDir() || !directory && !info.Mode().IsRegular() {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == 0 && stat.Gid == 0 && (directory || stat.Nlink == 1)
}

func runtimeStartupIdentity(info os.FileInfo) runtimeStartupFileIdentity {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return runtimeStartupFileIdentity{}
	}
	return runtimeStartupFileIdentity{Device: uint64(stat.Dev), Inode: stat.Ino}
}

func (ledger *RuntimeStartupLedger) Close() error {
	if ledger == nil {
		return nil
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.closed {
		return nil
	}
	ledger.closed = true
	var err error
	for _, owner := range ledger.owners {
		err = errors.Join(err, owner.Close())
	}
	ledger.owners = nil
	if ledger.file != nil {
		err = errors.Join(err, ledger.file.Close())
		ledger.file = nil
	}
	if ledger.root != nil {
		err = errors.Join(err, ledger.root.Close())
		ledger.root = nil
	}
	return err
}

func (ledger *RuntimeStartupLedger) apply(entry runtimeStartupEntry) error {
	count := 0
	if entry.Startup != nil {
		count++
	}
	if entry.Exit != nil {
		count++
	}
	if entry.Reservation != nil {
		count++
	}
	if count != 1 {
		return ErrRuntimeStartupLedger
	}
	if entry.Reservation != nil {
		return ledger.applyReservation(*entry.Reservation)
	}
	if record := entry.Startup; record != nil {
		if record.Remote != nil && ledger.header.SchemaVersion != 2 {
			return ErrRuntimeStartupLedger
		}
		if err := validateRuntimeStartupRecord(*record, ledger.header.NodeIdentity); err != nil {
			return err
		}
		if _, exists := ledger.starts[record.Request.JournalID]; exists || len(ledger.starts) >= maxRuntimeStartupRecords {
			return ErrRuntimeStartupLedger
		}
		for _, prior := range ledger.starts {
			if prior.OperationID == record.OperationID {
				return ErrRuntimeStartupLedger
			}
		}
		ledger.starts[record.Request.JournalID] = cloneRuntimeStartup(*record)
		return nil
	}
	exit := *entry.Exit
	record, exists := ledger.starts[exit.JournalID]
	_, duplicate := ledger.exits[exit.JournalID]
	if !exists || duplicate || record.OperationID != exit.OperationID || exit.Observation.Owner != record.Owner ||
		exit.Observation.RetainedAt != record.RetainedAt || exit.Observation.ObservedAt.Before(record.RecordedAt) {
		return ErrRuntimeStartupLedger
	}
	ledger.exits[exit.JournalID] = exit
	return nil
}

func validateRuntimeStartupRecord(record RuntimeStartupRecord, node string) error {
	binding := &velav1.WorkerBootstrapBinding{}
	if err := proto.Unmarshal(record.RegistryBinding, binding); err != nil {
		return errors.Join(ErrRuntimeStartupLedger, err)
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(binding)
	_, shapeErr := journalbinding.Encode(binding)
	if err != nil || shapeErr != nil || !bytes.Equal(canonical, record.RegistryBinding) || binding.GetClaim().GetNodeIdentity() != node ||
		binding.GetPair().GetRuntimeJournalId() != record.Request.JournalID.String() || !bytes.Equal(binding.GetPair().GetRuntimeScope(), record.Request.JournalScope[:]) {
		return errors.Join(ErrRuntimeStartupLedger, err, shapeErr)
	}
	if record.Request.Validate() != nil || record.Request.NodeIdentity != node || record.OperationID == uuid.Nil ||
		len(record.RegistryBinding) == 0 || record.Request.RegistryBindingDigest != sha256.Sum256(record.RegistryBinding) ||
		record.Owner.SchemaVersion != 1 || record.Owner.Process.BootID == uuid.Nil || record.Owner.Process.HostPID <= 0 ||
		record.Owner.Process.NamespacePID != 1 || record.Owner.Process.NamespaceDepth < 2 || record.Owner.Process.UID == 0 || record.Owner.Process.GID == 0 ||
		record.Owner.Container.Target.Validate() != nil || record.Owner.Container.NodeIdentity != node || !supportedRuntimeCallerContainer(record.Owner.Container, record.Owner.Process) ||
		!validText(record.PodResourceVersion, 253) || record.RetainedAt.IsZero() || record.RecordedAt.Before(record.RetainedAt) {
		return fmt.Errorf("%w: invalid retained startup record", ErrRuntimeStartupLedger)
	}
	if record.Remote != nil {
		return validateRemoteStartupRecord(record)
	}
	return nil
}
