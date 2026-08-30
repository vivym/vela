package stageartifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
)

func TestObjectStorePullConnectorReadsOnlyResolvedExactVersion(t *testing.T) {
	now := time.Date(2026, time.August, 30, 14, 0, 0, 0, time.UTC)
	payload := []byte("committed conditioning tensor")
	digest := sha256.Sum256(payload)
	store := artifactstore.NewLocal()
	version, err := store.PutIfAbsent(
		context.Background(),
		"artifacts/stage/org/project/attempt/encoder/output.bin",
		"application/octet-stream",
		bytes.NewReader(payload),
		int64(len(payload)),
		digest,
	)
	if err != nil {
		t.Fatalf("seed exact StageArtifact: %v", err)
	}
	destination := testTransferDestination()
	authority := &recordingTransferAuthority{descriptor: TransferDescriptor{
		TicketID:      uuid.MustParse("49600000-0000-0000-0000-000000000101"),
		ArtifactID:    uuid.MustParse("49600000-0000-0000-0000-000000000102"),
		ObjectKey:     version.ObjectKey,
		ObjectVersion: version.VersionID,
		SHA256:        digest,
		SizeBytes:     int64(len(payload)),
		ContentType:   version.ContentType,
	}}
	signer, err := NewTransferTicketSigner(
		"stage-transfer-key-v1", []byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewTransferTicketSigner: %v", err)
	}
	ticket, err := signer.Sign(TransferTicketClaims{
		TicketID:    authority.descriptor.TicketID,
		Destination: destination,
		IssuedAt:    now.Add(-time.Minute),
		ExpiresAt:   now.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("sign TransferTicket: %v", err)
	}
	connector, err := NewObjectStorePullConnector(
		store, authority, signer, func() time.Time { return now },
	)
	if err != nil {
		t.Fatalf("NewObjectStorePullConnector: %v", err)
	}

	target := NewMemoryTransferTarget()
	receipt, err := connector.Pull(
		context.Background(), ticket, destination, target,
	)
	if err != nil {
		t.Fatalf("Pull exact StageArtifact: %v", err)
	}
	if !bytes.Equal(target.Bytes(), payload) || receipt.SHA256 != digest ||
		receipt.SizeBytes != int64(len(payload)) || authority.resolveCalls != 1 ||
		authority.consumeCalls != 1 {
		t.Fatalf(
			"pull output=%q receipt=%#v resolve=%d consume=%d",
			target.Bytes(), receipt, authority.resolveCalls, authority.consumeCalls,
		)
	}
	if authority.lastTokenDigest != sha256.Sum256(ticket.Token) {
		t.Fatal("Connector did not bind PostgreSQL authority to the signed token digest")
	}
}

func TestObjectStorePullConnectorRejectsAnotherDestinationBeforeResolve(t *testing.T) {
	now := time.Date(2026, time.August, 30, 15, 0, 0, 0, time.UTC)
	authority := &recordingTransferAuthority{}
	signer, err := NewTransferTicketSigner(
		"stage-transfer-key-v1", []byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewTransferTicketSigner: %v", err)
	}
	destination := testTransferDestination()
	ticket, err := signer.Sign(TransferTicketClaims{
		TicketID:    uuid.MustParse("49600000-0000-0000-0000-000000000111"),
		Destination: destination,
		IssuedAt:    now.Add(-time.Minute),
		ExpiresAt:   now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("sign TransferTicket: %v", err)
	}
	connector, err := NewObjectStorePullConnector(
		artifactstore.NewLocal(), authority, signer, func() time.Time { return now },
	)
	if err != nil {
		t.Fatalf("NewObjectStorePullConnector: %v", err)
	}
	other := destination
	other.WorkerInstanceEpoch++

	_, err = connector.Pull(context.Background(), ticket, other, NewMemoryTransferTarget())
	if !errors.Is(err, ErrTransferTicketDestinationMismatch) || authority.resolveCalls != 0 {
		t.Fatalf("Pull other destination error=%v resolve calls=%d", err, authority.resolveCalls)
	}
}

func TestTransferTicketContainsNoObjectStoreCredential(t *testing.T) {
	now := time.Date(2026, time.August, 30, 16, 0, 0, 0, time.UTC)
	signer, err := NewTransferTicketSigner(
		"stage-transfer-key-v1", []byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewTransferTicketSigner: %v", err)
	}
	ticket, err := signer.Sign(TransferTicketClaims{
		TicketID:    uuid.MustParse("49600000-0000-0000-0000-000000000121"),
		Destination: testTransferDestination(),
		IssuedAt:    now,
		ExpiresAt:   now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("sign TransferTicket: %v", err)
	}
	for _, forbidden := range [][]byte{
		[]byte("access_key"), []byte("secret"), []byte("s3://"), []byte("https://"),
	} {
		if bytes.Contains(bytes.ToLower(ticket.Token), forbidden) {
			t.Fatalf("signed TransferTicket contains forbidden credential material %q", forbidden)
		}
	}
}

func TestTransferTicketReplayKeepsOriginalSigningKeyAcrossRotation(t *testing.T) {
	now := time.Date(2026, time.August, 30, 17, 0, 0, 0, time.UTC)
	keys := map[string][]byte{
		"stage-key-n-1": bytes.Repeat([]byte{0x71}, 32),
		"stage-key-n":   bytes.Repeat([]byte{0x72}, 32),
	}
	beforeRotation, err := NewTransferTicketKeyringSigner("stage-key-n-1", keys)
	if err != nil {
		t.Fatalf("construct N-1 TransferTicket signer: %v", err)
	}
	afterRotation, err := NewTransferTicketKeyringSigner("stage-key-n", keys)
	if err != nil {
		t.Fatalf("construct N TransferTicket signer: %v", err)
	}
	claims := TransferTicketClaims{
		TicketID:    uuid.MustParse("49600000-0000-0000-0000-000000000141"),
		Destination: testTransferDestination(),
		IssuedAt:    now,
		ExpiresAt:   now.Add(time.Minute),
	}
	original, err := beforeRotation.SignWithKeyID("stage-key-n-1", claims)
	if err != nil {
		t.Fatalf("sign original TransferTicket: %v", err)
	}
	replayed, err := afterRotation.SignWithKeyID("stage-key-n-1", claims)
	if err != nil {
		t.Fatalf("replay N-1 TransferTicket after rotation: %v", err)
	}
	if !bytes.Equal(original.Token, replayed.Token) {
		t.Fatal("TransferTicket replay changed token after active-key rotation")
	}
	if _, err := afterRotation.Verify(replayed, now.Add(time.Second)); err != nil {
		t.Fatalf("verify N-1 TransferTicket from rotated keyring: %v", err)
	}
}

func testTransferDestination() TransferDestination {
	return TransferDestination{
		WorkerInstanceID:    uuid.MustParse("49600000-0000-0000-0000-000000000131"),
		WorkerInstanceEpoch: 7,
		ModelResidencyID:    uuid.MustParse("49600000-0000-0000-0000-000000000132"),
		ModelRuntimeEpoch:   11,
		ConnectorRevisionID: uuid.MustParse("49600000-0000-0000-0000-000000000133"),
	}
}

type recordingTransferAuthority struct {
	descriptor      TransferDescriptor
	resolveCalls    int
	consumeCalls    int
	lastTokenDigest [sha256.Size]byte
}

func (authority *recordingTransferAuthority) Resolve(
	_ context.Context,
	command ResolveTransferCommand,
) (TransferDescriptor, error) {
	authority.resolveCalls++
	authority.lastTokenDigest = command.TokenDigest
	return authority.descriptor, nil
}

func (authority *recordingTransferAuthority) Consume(
	_ context.Context,
	command ConsumeTransferCommand,
) error {
	authority.consumeCalls++
	authority.lastTokenDigest = command.TokenDigest
	return nil
}
