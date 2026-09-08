package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// RuntimeStartupEpoch is the complete member-local epoch vector. Fleet derives
// the same vector from its retained approved bundle; callers cannot allocate an
// arbitrary epoch or omit an AUX runtime.
type RuntimeStartupEpoch struct {
	ModelResidencyID       uuid.UUID `json:"model_residency_id"`
	ModelRuntimeIdentity   string    `json:"model_runtime_identity"`
	StageProfileRevisionID uuid.UUID `json:"stage_profile_revision_id"`
	ModelRuntimeEpoch      int64     `json:"model_runtime_epoch"`
}

// RuntimeStartupRequest is supplied by trusted Node/Fleet assembly, never by an
// unauthenticated workload. Digests bind that assembly's exact launch and live
// owner observation; PostgreSQL cannot attest either preimage or retain a pidfd.
type RuntimeStartupRequest struct {
	RequestID              uuid.UUID
	BootstrapRequestID     uuid.UUID
	NodeIdentity           string
	ActorIdentity          string
	RuntimeJournalID       uuid.UUID
	RuntimeScope           []byte
	IncarnationID          uuid.UUID
	LaunchDigest           []byte
	OwnerObservationDigest []byte
	Epochs                 []RuntimeStartupEpoch
}

// RuntimeStartupReservation is a first-use database reservation, not backend
// permission, readiness, termination proof or a replacement epoch allocator.
// Only a committed fresh insert may feed a Node grant transaction. Replays and
// history lookups always have Fresh=false, including after a lost response.
type RuntimeStartupReservation struct {
	RuntimeStartupRequest
	Fresh      bool
	ReservedAt time.Time
}

func (service *Service) ReserveRuntimeStartup(ctx context.Context, request RuntimeStartupRequest) (RuntimeStartupReservation, error) {
	if service == nil || service.registryPool == nil {
		return RuntimeStartupReservation{}, errors.New("fleet service is not configured")
	}
	if request.RequestID == uuid.Nil || request.BootstrapRequestID == uuid.Nil || request.RuntimeJournalID == uuid.Nil ||
		request.IncarnationID.Version() != 4 || request.IncarnationID.Variant() != uuid.RFC4122 ||
		!validText(request.NodeIdentity, 253) || !validText(request.ActorIdentity, 500) ||
		!startupDigest(request.RuntimeScope) || !startupDigest(request.LaunchDigest) || !startupDigest(request.OwnerObservationDigest) ||
		len(request.Epochs) == 0 || len(request.Epochs) > 64 {
		return RuntimeStartupReservation{}, &Failure{Code: FailureInvalid, Message: "Runtime startup reservation is invalid"}
	}
	request.Epochs = slices.Clone(request.Epochs)
	slices.SortFunc(request.Epochs, func(a, b RuntimeStartupEpoch) int {
		return strings.Compare(a.ModelResidencyID.String(), b.ModelResidencyID.String())
	})
	for i, epoch := range request.Epochs {
		if epoch.ModelResidencyID == uuid.Nil || epoch.StageProfileRevisionID == uuid.Nil || epoch.ModelRuntimeEpoch <= 0 ||
			!validText(epoch.ModelRuntimeIdentity, 300) || (i > 0 && epoch.ModelResidencyID == request.Epochs[i-1].ModelResidencyID) {
			return RuntimeStartupReservation{}, &Failure{Code: FailureInvalid, Message: "Runtime startup epoch vector is invalid"}
		}
	}
	epochs, err := json.Marshal(request.Epochs)
	if err != nil {
		return RuntimeStartupReservation{}, err
	}
	var fresh bool
	var reservedAt time.Time
	err = service.registryPool.QueryRow(ctx, `SELECT fresh, reserved_at FROM vela_reserve_runtime_startup(
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, request.RequestID, request.BootstrapRequestID,
		request.NodeIdentity, request.ActorIdentity, request.RuntimeJournalID, request.RuntimeScope,
		request.IncarnationID, request.LaunchDigest, request.OwnerObservationDigest, epochs).Scan(&fresh, &reservedAt)
	if err != nil {
		// In particular, a deferred commit/quorum error must discard returned rows.
		return RuntimeStartupReservation{}, mapDatabaseError("reserve Runtime startup", err)
	}
	request.RuntimeScope = slices.Clone(request.RuntimeScope)
	request.LaunchDigest = slices.Clone(request.LaunchDigest)
	request.OwnerObservationDigest = slices.Clone(request.OwnerObservationDigest)
	return RuntimeStartupReservation{RuntimeStartupRequest: request, Fresh: fresh, ReservedAt: reservedAt}, nil
}

func (service *Service) LookupRuntimeStartup(ctx context.Context, request WorkerBootstrapLookup) (RuntimeStartupReservation, error) {
	if service == nil || service.registryPool == nil {
		return RuntimeStartupReservation{}, errors.New("fleet service is not configured")
	}
	if request.RequestID == uuid.Nil || !validText(request.NodeIdentity, 253) || !validText(request.ActorIdentity, 500) {
		return RuntimeStartupReservation{}, &Failure{Code: FailureInvalid, Message: "Runtime startup lookup is invalid"}
	}
	var result RuntimeStartupReservation
	err := service.registryPool.QueryRow(ctx, `SELECT request_id, bootstrap_request_id, node_identity, actor_identity,
		runtime_journal_id, runtime_scope, incarnation_id, launch_digest, owner_observation_digest, epochs, reserved_at
		FROM vela_lookup_runtime_startup($1,$2,$3)`, request.RequestID, request.NodeIdentity, request.ActorIdentity).Scan(
		&result.RequestID, &result.BootstrapRequestID, &result.NodeIdentity, &result.ActorIdentity,
		&result.RuntimeJournalID, &result.RuntimeScope, &result.IncarnationID, &result.LaunchDigest,
		&result.OwnerObservationDigest, &result.Epochs, &result.ReservedAt)
	if err != nil {
		return RuntimeStartupReservation{}, mapDatabaseError("lookup Runtime startup", err)
	}
	return result, nil
}

func startupDigest(value []byte) bool {
	return len(value) == 32 && [32]byte(value) != ([32]byte{})
}
