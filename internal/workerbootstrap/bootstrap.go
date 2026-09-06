// Package workerbootstrap coordinates one-time local journal preparation with
// Registry authority. It starts no backend and grants no serving readiness.
package workerbootstrap

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/authoritypolicy"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/workerjournal"
)

var ErrIncomplete = errors.New("worker bootstrap is incomplete; independent reconciliation is required")

type Authority interface {
	ClaimWorkerBootstrap(context.Context, fleet.WorkerBootstrapRequest) (fleet.WorkerBootstrapClaim, error)
	RecordWorkerBootstrapReceipt(context.Context, fleet.WorkerBootstrapReceipt) (time.Time, error)
}

type Config struct {
	Bundle        fleetcontroller.WorkerBundleActuation
	Launch        modelruntime.LaunchManifest
	NodeIdentity  string
	ActorIdentity string
	// ScratchDirectory is the local mount of Launch's scratch root. The existing
	// private bootstrap, worker-admission, runtime-admission, inputs and outputs
	// children must be owned by the effective user. Preparation creates no dirs.
	ScratchDirectory string
	MaxRecords       int
	Validator        *stageauthority.Validator
}

type Result struct {
	RequestID  uuid.UUID
	Worker     stageworkeragent.AssignmentJournalStatus
	Runtime    modelruntime.ExecutionJournalStatus
	RecordedAt time.Time
}

type preparation struct {
	config   Config
	request  fleet.WorkerBootstrapRequest
	launch   modelruntime.LaunchManifest
	worker   stageworkeragent.AssignmentAdmissionConfig
	digest   [sha256.Size]byte
	launchID [sha256.Size]byte
}

// Prepare allows initialization only after creating a durable local operation
// and receiving a fresh committed claim in this invocation. A retained operation
// never repeats initialization, even when its claim response or files were lost.
// Complete pairs are independently recovered before every receipt replay.
func Prepare(ctx context.Context, config Config, authority Authority) (Result, error) {
	return prepare(ctx, config, authority, nil)
}

// boundary is package-private fault injection at durable lifecycle boundaries.
func prepare(ctx context.Context, config Config, authority Authority, boundary func(string) error) (result Result, err error) {
	if ctx == nil || authority == nil {
		return Result{}, errors.New("worker bootstrap requires context and Registry authority")
	}
	p, err := bind(config)
	if err != nil {
		return Result{}, err
	}
	checkpoint := func(name string) error {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if boundary != nil {
			return boundary(name)
		}
		return nil
	}
	if err := checkpoint("preflight"); err != nil {
		return Result{}, err
	}
	local, err := openOperation(p, true)
	if err != nil {
		return Result{}, err
	}
	defer func() {
		err = errors.Join(err, local.close())
		if err != nil {
			result = Result{}
		}
	}()
	p.request.RequestID = local.operation.RequestID
	if err := checkpoint("operation-durable"); err != nil {
		return Result{}, err
	}
	var pair journalPair
	if local.fresh {
		if err := errors.Join(local.validate(), local.requireUnusedRoots(true)); err != nil {
			return Result{}, err
		}
		claim, err := authority.ClaimWorkerBootstrap(ctx, p.request)
		if err != nil {
			return Result{}, fmt.Errorf("claim Worker bootstrap: %w", err)
		}
		if !p.matchesClaim(claim) || !claim.Fresh {
			return Result{}, fmt.Errorf("%w: missing fresh matching claim", ErrIncomplete)
		}
		if err := checkpoint("claim-committed"); err != nil {
			return Result{}, err
		}
		if err := local.validate(); err != nil {
			return Result{}, err
		}
		p.worker.Initialize = true
		worker, err := stageworkeragent.PrepareAssignmentJournal(ctx, p.worker)
		if err != nil {
			return Result{}, fmt.Errorf("prepare Worker journal: %w", err)
		}
		if err := checkpoint("worker-prepared"); err != nil {
			return Result{}, err
		}
		if err := local.validate(); err != nil {
			return Result{}, err
		}
		runtime, err := modelruntime.PrepareExecutionJournal(ctx, p.launch, config.Validator,
			modelruntime.ExecutionFloorStateConfig{Directory: local.paths[runtimeRoot], Initialize: true})
		if err != nil {
			return Result{}, fmt.Errorf("prepare Runtime journal: %w", err)
		}
		if err := checkpoint("runtime-prepared"); err != nil {
			return Result{}, err
		}
		pair = journalPair{RequestID: p.request.RequestID, WorkerID: worker.JournalID, WorkerScope: worker.Scope,
			RuntimeID: runtime.JournalID, RuntimeScope: runtime.Scope}
		if err := local.writePair(pair); err != nil {
			return Result{}, err
		}
	}
	if err := checkpoint("pair-durable"); err != nil {
		return Result{}, err
	}
	pair, err = local.readPair()
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrIncomplete, err)
	}
	if err := local.validate(); err != nil {
		return Result{}, err
	}
	p.worker.Initialize = false
	worker, err := stageworkeragent.PrepareAssignmentJournal(ctx, p.worker)
	if err != nil {
		return Result{}, fmt.Errorf("recover recorded Worker journal: %w", err)
	}
	runtime, err := modelruntime.PrepareExecutionJournal(ctx, p.launch, config.Validator,
		modelruntime.ExecutionFloorStateConfig{Directory: local.paths[runtimeRoot]})
	if err != nil {
		return Result{}, fmt.Errorf("recover recorded Runtime journal: %w", err)
	}
	if worker.JournalID != pair.WorkerID || worker.Scope != pair.WorkerScope ||
		runtime.JournalID != pair.RuntimeID || runtime.Scope != pair.RuntimeScope {
		return Result{}, errors.New("worker bootstrap journal pair was replaced")
	}
	if err := checkpoint("pair-recovered"); err != nil {
		return Result{}, err
	}
	if err := local.validate(); err != nil {
		return Result{}, err
	}
	recordedAt, err := authority.RecordWorkerBootstrapReceipt(ctx, fleet.WorkerBootstrapReceipt{
		RequestID: pair.RequestID, WorkerJournalID: pair.WorkerID, WorkerScope: bytes.Clone(pair.WorkerScope[:]),
		RuntimeJournalID: pair.RuntimeID, RuntimeScope: bytes.Clone(pair.RuntimeScope[:]), ActorIdentity: config.ActorIdentity,
	})
	if err != nil {
		return Result{}, fmt.Errorf("record Worker bootstrap pair: %w", err)
	}
	if recordedAt.IsZero() {
		return Result{}, errors.New("worker bootstrap receipt has no committed timestamp")
	}
	if err := checkpoint("receipt-committed"); err != nil {
		return Result{}, err
	}
	if err := local.validate(); err != nil {
		return Result{}, err
	}
	return Result{RequestID: pair.RequestID, Worker: worker, Runtime: runtime, RecordedAt: recordedAt}, nil
}

func bind(config Config) (preparation, error) {
	if config.Validator == nil || config.MaxRecords < 1 || config.MaxRecords > 64 ||
		strings.TrimSpace(config.ActorIdentity) != config.ActorIdentity || config.ActorIdentity == "" ||
		len(config.ActorIdentity) > 500 || !utf8.ValidString(config.ActorIdentity) || strings.ContainsAny(config.ActorIdentity, "\x00\r\n") ||
		!filepath.IsAbs(config.ScratchDirectory) || filepath.Clean(config.ScratchDirectory) != config.ScratchDirectory {
		return preparation{}, errors.New("worker bootstrap configuration is invalid")
	}
	workerID, workerErr := uuid.Parse(config.Launch.WorkerInstanceID)
	memberID, memberErr := uuid.Parse(config.Launch.WorkerMemberID)
	if workerErr != nil || memberErr != nil {
		return preparation{}, errors.New("worker bootstrap launch identity is invalid")
	}
	approved, err := fleetcontroller.WorkerMemberLaunchManifest(config.Bundle, workerID, memberID)
	if err != nil {
		return preparation{}, err
	}
	expected, err := modelruntime.EncodeLaunchManifest(approved)
	if err != nil {
		return preparation{}, err
	}
	actual, err := modelruntime.EncodeLaunchManifest(config.Launch)
	if err != nil || !bytes.Equal(expected, actual) {
		return preparation{}, errors.New("worker bootstrap launch differs from approved bundle")
	}
	node := ""
	for _, worker := range config.Bundle.WorkerInstances {
		for _, member := range worker.Members {
			if member.ID == memberID {
				node = member.NodeIdentity
			}
		}
	}
	if node == "" || node != config.NodeIdentity {
		return preparation{}, errors.New("worker bootstrap target node differs from approved bundle")
	}
	wire, err := fleetcontroller.WorkerBundleActuationManifest(config.Bundle)
	if err != nil || len(wire) > fleet.MaximumWorkerBootstrapManifestBytes {
		return preparation{}, errors.New("worker bootstrap bundle exceeds its validated manifest bound")
	}
	// Only filesystem locations are mapped to the node mount. The journal scope
	// remains the approved member topology, also used in the serving namespace.
	for i := range approved.Runtimes {
		approved.Runtimes[i].ScratchRoot = config.ScratchDirectory
		approved.Runtimes[i].InputRoot = filepath.Join(config.ScratchDirectory, "inputs")
		approved.Runtimes[i].OutputRoot = filepath.Join(config.ScratchDirectory, "outputs")
	}
	worker, err := workerjournal.AssignmentConfig(approved, stageworkeragent.AssignmentAdmissionConfig{
		Directory: filepath.Join(config.ScratchDirectory, "worker-admission"), Validator: config.Validator,
		MaxRecords: config.MaxRecords, MaxClockSkew: authoritypolicy.ProductionMaxClockSkew,
	})
	if err != nil {
		return preparation{}, err
	}
	return preparation{config: config, launch: approved, worker: worker, digest: sha256.Sum256(wire), launchID: sha256.Sum256(expected),
		request: fleet.WorkerBootstrapRequest{WorkerInstanceID: workerID, WorkerInstanceEpoch: approved.WorkerInstanceEpoch,
			WorkerMemberID: memberID, BundleManifest: wire, ActorIdentity: config.ActorIdentity}}, nil
}

func (p preparation) matchesClaim(claim fleet.WorkerBootstrapClaim) bool {
	return claim.RequestID == p.request.RequestID && claim.WorkerInstanceID == p.request.WorkerInstanceID &&
		claim.WorkerInstanceEpoch == p.request.WorkerInstanceEpoch && claim.WorkerMemberID == p.request.WorkerMemberID &&
		claim.WorkerMemberEpoch == p.launch.WorkerMemberEpoch && claim.NodeIdentity == p.config.NodeIdentity &&
		bytes.Equal(claim.BundleDigest, p.digest[:]) && !claim.ClaimedAt.IsZero()
}
