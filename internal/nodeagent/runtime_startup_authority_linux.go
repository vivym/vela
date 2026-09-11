package nodeagent

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
)

var ErrRuntimeStartupAuthority = errors.New("runtime startup authority source is incomplete or untrusted")

// RuntimeStartupAuthorizationEvidence is issued by an independent startup
// policy after Fleet reservation. The digest is usable only with the exact
// operation and request it names, and only during its bounded validity window.
type RuntimeStartupAuthorizationEvidence struct {
	OperationID    uuid.UUID
	RequestDigest  [sha256.Size]byte
	EvidenceDigest [sha256.Size]byte
	IssuedAt       time.Time
	ExpiresAt      time.Time
}

// RuntimeStartupAuthorizationPolicy is the production policy/evidence seam.
// Implementations must evaluate the reserved operation and return structured,
// operation-bound evidence; a caller cannot supply an arbitrary digest.
type RuntimeStartupAuthorizationPolicy interface {
	IssueRuntimeStartupAuthorization(context.Context, RuntimeStartupReservationRecord) (RuntimeStartupAuthorizationEvidence, error)
}

func validateRuntimeStartupAuthorizationEvidence(evidence RuntimeStartupAuthorizationEvidence, record RuntimeStartupReservationRecord, now time.Time) error {
	if evidence.OperationID == uuid.Nil || evidence.OperationID != record.OperationID || evidence.RequestDigest != record.RequestDigest || evidence.EvidenceDigest == ([sha256.Size]byte{}) || evidence.IssuedAt.IsZero() || evidence.ExpiresAt.IsZero() || evidence.IssuedAt.Location() != time.UTC || evidence.ExpiresAt.Location() != time.UTC || !evidence.ExpiresAt.After(now) || evidence.ExpiresAt.Sub(now) > 5*time.Minute || evidence.IssuedAt.After(now.Add(time.Second)) {
		return ErrRuntimeStartupAuthority
	}
	return nil
}

// RuntimeStartupAuthority is the Node-owned composition seam for one startup
// authority. Every field is an independently established source; this type has
// no defaults, history fallback or mock Permit path. A value is single-use in
// practice because Prepare consumes the ledger's reservation opportunity.
type RuntimeStartupAuthority struct {
	Ledger              *RuntimeStartupLedger
	Plan                *RuntimeLaunchPlan
	Pods                RuntimeLaunchPodReader
	Observer            *RuntimeContainerObserver
	Custody             *RuntimeObserverCustody
	Journal             *modelruntime.ExecutionJournalOwner
	WorkerOwner         *RuntimeNamespaceOwner
	Registry            RuntimeStartupRegistry
	AuthorizationPolicy RuntimeStartupAuthorizationPolicy
	Credentials         []RuntimeCallerCredentials
	ObserverInterval    time.Duration
	ObserverTimeout     time.Duration
	ExchangeTimeout     time.Duration
}

type RuntimeStartupAuthorityConfig struct {
	Ledger              *RuntimeStartupLedger
	Plan                *RuntimeLaunchPlan
	Pods                RuntimeLaunchPodReader
	Observer            *RuntimeContainerObserver
	Custody             *RuntimeObserverCustody
	Journal             *modelruntime.ExecutionJournalOwner
	WorkerOwner         *RuntimeNamespaceOwner
	Registry            RuntimeStartupRegistry
	AuthorizationPolicy RuntimeStartupAuthorizationPolicy
	Credentials         []RuntimeCallerCredentials
	ObserverInterval    time.Duration
	ObserverTimeout     time.Duration
	ExchangeTimeout     time.Duration
}

// NewRuntimeStartupAuthority constructs a complete static authority. It does
// not contact Fleet, inspect Kubernetes/CRI, create listeners or grant
// startup. Those actions remain owned by Prepare and the caller lifecycle.
func NewRuntimeStartupAuthority(config RuntimeStartupAuthorityConfig) (RuntimeStartupAuthority, error) {
	authority := RuntimeStartupAuthority{
		Ledger: config.Ledger, Plan: config.Plan, Pods: config.Pods, Observer: config.Observer,
		Custody: config.Custody, Journal: config.Journal, WorkerOwner: config.WorkerOwner,
		Registry: config.Registry, AuthorizationPolicy: config.AuthorizationPolicy,
		Credentials:      append([]RuntimeCallerCredentials(nil), config.Credentials...),
		ObserverInterval: config.ObserverInterval, ObserverTimeout: config.ObserverTimeout,
		ExchangeTimeout: config.ExchangeTimeout,
	}
	if err := authority.validateSources(); err != nil {
		return RuntimeStartupAuthority{}, err
	}
	return authority, nil
}

func (authority RuntimeStartupAuthority) validate(caller *RuntimeCaller) error {
	if err := authority.validateSources(); err != nil {
		return err
	}
	if caller == nil {
		return ErrRuntimeStartupAuthority
	}
	return nil
}

func (authority RuntimeStartupAuthority) validateSources() error {
	if authority.Ledger == nil || authority.Plan == nil || authority.Pods == nil || authority.Observer == nil || authority.Journal == nil || authority.WorkerOwner == nil || authority.Registry == nil || authority.Custody == nil || authority.AuthorizationPolicy == nil {
		return ErrRuntimeStartupAuthority
	}
	if len(authority.Credentials) == 0 || authority.ExchangeTimeout <= 0 || authority.ObserverInterval <= 0 || authority.ObserverInterval > time.Second || authority.ObserverTimeout <= 0 || authority.ObserverTimeout > 5*time.Second {
		return ErrRuntimeStartupAuthority
	}
	// Credentials are an identity binding, not merely a set of allowed IDs.
	// They must match the identities authenticated into the verified Pod plan;
	// accepting a different UID/GID would let Node assembly authorize a caller
	// that the signed launch topology did not create.
	if !authority.credentialsMatchPlan() {
		return ErrRuntimeStartupAuthority
	}
	for i, credential := range authority.Credentials {
		if credential.UID == 0 || credential.GID == 0 || credential.UID == ^uint32(0) || credential.GID == ^uint32(0) || slices.Contains(authority.Credentials[:i], credential) {
			return ErrRuntimeStartupAuthority
		}
	}
	return nil
}

func (authority RuntimeStartupAuthority) credentialsMatchPlan() bool {
	return authority.Plan != nil && authority.Plan.uid != 0 && authority.Plan.gid != 0 &&
		len(authority.Credentials) == 1 && authority.Credentials[0].UID == authority.Plan.uid &&
		authority.Credentials[0].GID == authority.Plan.gid
}

// Prepare authenticates the request already held by caller through the exact
// reservation/observation path and returns an orchestration whose ServeCaller
// consumes that same caller. It never parses a caller-supplied plan, creates a
// reservation from history, or fabricates authorization evidence.
func (authority RuntimeStartupAuthority) Prepare(ctx context.Context, caller *RuntimeCaller) (*RuntimeStartupOrchestration, RuntimeStartupReservationRecord, error) {
	if err := contextError(ctx); err != nil {
		return nil, RuntimeStartupReservationRecord{}, err
	}
	if err := authority.validate(caller); err != nil {
		return nil, RuntimeStartupReservationRecord{}, err
	}
	orchestration, record, err := authority.Ledger.PrepareRemoteStartupOrchestration(ctx, RemoteStartupOrchestrationConfig{
		Reservation: RuntimeStartupReservationConfig{
			Plan: authority.Plan, Pods: authority.Pods, Observer: authority.Observer,
			Caller: caller, Journal: authority.Journal, Registry: authority.Registry,
		},
		WorkerOwner: authority.WorkerOwner, Observer: authority.Custody,
		AuthorizationPolicy: authority.AuthorizationPolicy, Credentials: authority.Credentials,
		ObserverInterval: authority.ObserverInterval, ObserverTimeout: authority.ObserverTimeout,
		ExchangeTimeout: authority.ExchangeTimeout,
	})
	if err != nil {
		return nil, record, err
	}
	return orchestration, record, nil
}
