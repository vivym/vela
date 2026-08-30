package stageartifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
)

var (
	ErrConditionalPublicationConflict = errors.New("StageArtifact conditional publication conflict")
	ErrMaterializationLeaseExpired    = errors.New("StageMaterializationLease expired")
)

type MaterializationLease struct {
	ID          uuid.UUID
	ArtifactID  uuid.UUID
	ObjectKey   string
	ContentType string
	SHA256      [sha256.Size]byte
	TokenDigest [sha256.Size]byte
	SizeBytes   int64
	IssuedAt    time.Time
	ExpiresAt   time.Time
}

type CommitCommand struct {
	CommandID              uuid.UUID
	ProgressReceiptID      uuid.UUID
	MaterializationLeaseID uuid.UUID
	ArtifactID             uuid.UUID
	ObjectKey              string
	ObjectVersion          string
	SHA256                 [sha256.Size]byte
	SizeBytes              int64
	TokenDigest            [sha256.Size]byte
	CommittedAt            time.Time
}

type Artifact struct {
	ID            uuid.UUID
	ObjectKey     string
	ObjectVersion string
	SHA256        [sha256.Size]byte
	SizeBytes     int64
	CommittedAt   time.Time
}

type Committer interface {
	Commit(context.Context, CommitCommand) (Artifact, error)
}

type Materializer struct {
	store     artifactstore.VersionedStore
	committer Committer
	now       func() time.Time
}

func NewMaterializer(
	store artifactstore.VersionedStore,
	committer Committer,
	now func() time.Time,
) (*Materializer, error) {
	if store == nil || committer == nil || now == nil {
		return nil, errors.New("StageArtifact Materializer configuration is incomplete")
	}
	return &Materializer{store: store, committer: committer, now: now}, nil
}

func (materializer *Materializer) Materialize(
	ctx context.Context,
	lease MaterializationLease,
	source io.Reader,
) (Artifact, error) {
	if materializer == nil || materializer.store == nil || materializer.committer == nil ||
		materializer.now == nil {
		return Artifact{}, errors.New("StageArtifact Materializer is not configured")
	}
	if err := validateMaterializationLease(lease); err != nil {
		return Artifact{}, err
	}
	if ctx == nil || source == nil {
		return Artifact{}, errors.New("StageArtifact materialization source is required")
	}
	now := materializer.now().UTC()
	if !now.Before(lease.ExpiresAt) {
		return Artifact{}, ErrMaterializationLeaseExpired
	}

	object, err := materializer.store.PutIfAbsent(
		ctx,
		lease.ObjectKey,
		lease.ContentType,
		source,
		lease.SizeBytes,
		lease.SHA256,
	)
	if errors.Is(err, artifactstore.ErrObjectAlreadyExists) {
		object, err = materializer.reconcileExisting(ctx, lease)
	}
	if err != nil {
		return Artifact{}, fmt.Errorf("publish sealed StageArtifact: %w", err)
	}
	if err := materializer.verifyExact(ctx, lease, object); err != nil {
		return Artifact{}, err
	}
	artifact, err := materializer.committer.Commit(ctx, CommitCommand{
		CommandID:              deterministicCommandID("commit", lease.ID),
		ProgressReceiptID:      deterministicCommandID("progress", lease.ID),
		MaterializationLeaseID: lease.ID,
		ArtifactID:             lease.ArtifactID,
		ObjectKey:              object.ObjectKey,
		ObjectVersion:          object.VersionID,
		SHA256:                 lease.SHA256,
		SizeBytes:              lease.SizeBytes,
		TokenDigest:            lease.TokenDigest,
		CommittedAt:            now,
	})
	if err != nil {
		return Artifact{}, fmt.Errorf("commit durable StageArtifact metadata: %w", err)
	}
	if artifact.ID != lease.ArtifactID || artifact.ObjectKey != object.ObjectKey ||
		artifact.ObjectVersion != object.VersionID || artifact.SHA256 != lease.SHA256 ||
		artifact.SizeBytes != lease.SizeBytes {
		return Artifact{}, errors.New("StageArtifact commit returned mismatched immutable identity")
	}
	return artifact, nil
}

func deterministicCommandID(kind string, leaseID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("vela/stage-artifact/"+kind+"/"+leaseID.String()))
}

func (materializer *Materializer) reconcileExisting(
	ctx context.Context,
	lease MaterializationLease,
) (artifactstore.ObjectVersion, error) {
	object, exists, err := materializer.store.ResolveCurrentVersion(ctx, lease.ObjectKey)
	if err != nil {
		return artifactstore.ObjectVersion{}, err
	}
	if !exists || !objectMatchesLease(object, lease) {
		return artifactstore.ObjectVersion{}, ErrConditionalPublicationConflict
	}
	return object, nil
}

func (materializer *Materializer) verifyExact(
	ctx context.Context,
	lease MaterializationLease,
	object artifactstore.ObjectVersion,
) error {
	if !objectMatchesLease(object, lease) {
		return ErrConditionalPublicationConflict
	}
	reader, err := materializer.store.ReadExactVersion(ctx, object.ObjectKey, object.VersionID)
	if err != nil {
		return fmt.Errorf("read published StageArtifact exact version: %w", err)
	}
	defer func() { _ = reader.Close() }()
	digest := sha256.New()
	written, err := io.Copy(digest, io.LimitReader(reader, lease.SizeBytes+1))
	if err != nil {
		return fmt.Errorf("verify published StageArtifact exact version: %w", err)
	}
	if written != lease.SizeBytes || !bytes.Equal(digest.Sum(nil), lease.SHA256[:]) {
		return errors.New("published StageArtifact exact version failed integrity verification")
	}
	return nil
}

func objectMatchesLease(
	object artifactstore.ObjectVersion,
	lease MaterializationLease,
) bool {
	checksum, err := base64.StdEncoding.DecodeString(object.ChecksumSHA256)
	return err == nil && object.ObjectKey == lease.ObjectKey && object.VersionID != "" &&
		object.SizeBytes == lease.SizeBytes && object.ContentType == lease.ContentType &&
		bytes.Equal(checksum, lease.SHA256[:])
}

func validateMaterializationLease(lease MaterializationLease) error {
	if lease.ID == uuid.Nil || lease.ArtifactID == uuid.Nil || lease.ObjectKey == "" ||
		lease.ContentType == "" || lease.SHA256 == [sha256.Size]byte{} ||
		lease.TokenDigest == [sha256.Size]byte{} ||
		lease.SizeBytes <= 0 || lease.IssuedAt.IsZero() ||
		!lease.ExpiresAt.After(lease.IssuedAt) {
		return errors.New("StageMaterializationLease is incomplete")
	}
	return nil
}
