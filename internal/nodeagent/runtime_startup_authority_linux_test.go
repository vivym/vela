package nodeagent

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/runtimelaunch"
	"github.com/vivym/vela/internal/runtimepolicy"
)

type startupAuthorizationPolicyFixture struct{}

func (startupAuthorizationPolicyFixture) IssueRuntimeStartupAuthorization(_ context.Context, record RuntimeStartupReservationRecord) (RuntimeStartupAuthorizationEvidence, error) {
	reservationDigest, _ := runtimepolicy.ReservationBindingDigest(record.OperationID, record.JournalID, record.RequestDigest, record.ReservedAt)
	return RuntimeStartupAuthorizationEvidence{
		OperationID: record.OperationID, JournalID: record.JournalID, RequestDigest: record.RequestDigest,
		ReservationDigest: reservationDigest,
		EvidenceDigest:    sha256.Sum256([]byte("fixture policy evidence")),
		IssuedAt:          time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Minute),
	}, nil
}

var _ RuntimeStartupAuthorizationPolicy = startupAuthorizationPolicyFixture{}

func TestRuntimeStartupAuthorizationEvidenceIsOperationBound(t *testing.T) {
	record := RuntimeStartupReservationRecord{OperationID: uuid.New(), JournalID: uuid.New(), RequestDigest: sha256.Sum256([]byte("request")), ReservedAt: time.Now().UTC()}
	evidence, err := (startupAuthorizationPolicyFixture{}).IssueRuntimeStartupAuthorization(t.Context(), record)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRuntimeStartupAuthorizationEvidence(evidence, record, time.Now().UTC()); err != nil {
		t.Fatalf("valid evidence rejected: %v", err)
	}
	evidence.OperationID = uuid.New()
	if err := validateRuntimeStartupAuthorizationEvidence(evidence, record, time.Now().UTC()); err == nil {
		t.Fatal("evidence for another operation accepted")
	}
	evidence.OperationID = record.OperationID
	evidence.ExpiresAt = time.Now().UTC().Add(6 * time.Minute)
	if err := validateRuntimeStartupAuthorizationEvidence(evidence, record, time.Now().UTC()); err == nil {
		t.Fatal("authorization evidence outside bounded lifetime accepted")
	}
}

func TestRuntimeStartupAuthorityRejectsIncompleteSources(t *testing.T) {
	var authority RuntimeStartupAuthority
	if _, err := NewRuntimeStartupAuthority(RuntimeStartupAuthorityConfig{}); err == nil {
		t.Fatal("constructor accepted incomplete authority source")
	}
	if _, _, err := authority.Prepare(t.Context(), nil); err == nil {
		t.Fatal("incomplete authority source accepted")
	}
	complete := RuntimeStartupAuthority{
		Credentials:      []RuntimeCallerCredentials{{UID: 10001, GID: 10001}},
		ObserverInterval: time.Millisecond, ObserverTimeout: time.Second, ExchangeTimeout: time.Second,
		AuthorizationPolicy:          startupAuthorizationPolicyFixture{},
		PolicyAuthorizationPublisher: func(context.Context, []byte) error { return nil },
	}
	complete.PolicyAuthorizationPublisher = nil
	if err := complete.validateSources(); err == nil {
		t.Fatal("authority without policy authorization publisher accepted")
	}
	complete.PolicyAuthorizationPublisher = func(context.Context, []byte) error { return nil }
	if _, _, err := complete.Prepare(context.Background(), &RuntimeCaller{}); err == nil {
		t.Fatal("authority with missing sources accepted")
	}
	complete.Credentials = []RuntimeCallerCredentials{{UID: 10001, GID: 10001}, {UID: 10001, GID: 10001}}
	if _, _, err := complete.Prepare(context.Background(), &RuntimeCaller{}); err == nil {
		t.Fatal("authority with duplicate credentials accepted")
	}
}

func TestRuntimeStartupAuthorityRejectsCredentialsOutsideVerifiedPlan(t *testing.T) {
	authority := RuntimeStartupAuthority{
		Ledger:           &RuntimeStartupLedger{},
		Plan:             &RuntimeLaunchPlan{uid: 10001, gid: 10002},
		Pods:             &KubernetesRuntimeLaunchPodReader{},
		Observer:         &RuntimeContainerObserver{},
		Custody:          &RuntimeObserverCustody{},
		Journal:          &modelruntime.ExecutionJournalOwner{},
		WorkerOwner:      &RuntimeNamespaceOwner{},
		Registry:         &startupReservationRegistryFixture{},
		Credentials:      []RuntimeCallerCredentials{{UID: 10002, GID: 10002}},
		ObserverInterval: time.Millisecond, ObserverTimeout: time.Second, ExchangeTimeout: time.Second,
		AuthorizationPolicy:          startupAuthorizationPolicyFixture{},
		PolicyAuthorizationPublisher: func(context.Context, []byte) error { return nil },
	}
	if err := authority.validateSources(); err == nil {
		t.Fatal("credentials unrelated to verified launch plan accepted")
	}
	authority.Credentials = []RuntimeCallerCredentials{{UID: authority.Plan.uid, GID: authority.Plan.gid}}
	if err := authority.validateSources(); err != nil {
		t.Fatalf("credentials matching verified launch plan rejected: %v", err)
	}
	authority.Plan.pod.Annotations = map[string]string{runtimelaunch.ProtocolAnnotation: runtimelaunch.Protocol}
	if err := authority.validateSources(); err == nil {
		t.Fatal("Kubernetes startup accepted no image verification source")
	}
	authority.Image = &RuntimeStartupImageConfig{Images: &RuntimeImageObserver{}}
	if err := authority.validateSources(); err == nil {
		t.Fatal("Kubernetes startup accepted no bootstrap publication")
	}
	authority.RuntimePublication = &RuntimeStartupPublicationConfig{}
	if err := authority.validateSources(); err != nil {
		t.Fatal(err)
	}
	authority.AuthorizationHash = sha256.Sum256([]byte("legacy digest"))
	if err := authority.validateSources(); err == nil {
		t.Fatal("legacy authorization hash bypassed policy requirement")
	}
}
