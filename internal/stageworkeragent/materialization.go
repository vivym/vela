package stageworkeragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrMaterializationJournalFull = errors.New("StageArtifact materialization journal is full")
var ErrMaterializationSourceLostReported = errors.New("local StageArtifact source loss reported")

type MaterializationConfig struct {
	Validator          *materializationauthority.Validator
	Source             stageartifact.LocalOutputSource
	Publisher          stageartifact.Publisher
	Journal            MaterializationJournal
	SourceLossEvidence MaterializationSourceLossEvidenceProvider
	MaxClockSkew       time.Duration
}

type MaterializationSourceLossEvidence struct {
	FailureFingerprint    [sha256.Size]byte
	ConsumedResourceUnits int64
	LostAt                time.Time
	RetryAt               time.Time
}

type MaterializationSourceLossEvidenceProvider interface {
	Evidence(context.Context, PendingMaterialization) (MaterializationSourceLossEvidence, error)
}

type MaterializationSourceLossEvidenceFunc func(
	context.Context,
	PendingMaterialization,
) (MaterializationSourceLossEvidence, error)

func (function MaterializationSourceLossEvidenceFunc) Evidence(
	ctx context.Context,
	record PendingMaterialization,
) (MaterializationSourceLossEvidence, error) {
	return function(ctx, record)
}

type PendingMaterialization struct {
	ID                       string
	StageAuthority           *velav1.StageAuthority
	LocalReceipt             *velav1.LocalMaterializationReceipt
	MaterializationAuthority *velav1.MaterializationAuthority
	ObjectVersion            string
	CommittedAt              time.Time
	SourceLoss               *MaterializationSourceLossEvidence
}

type MaterializationJournal interface {
	EnsureCapacity(context.Context) error
	Put(context.Context, PendingMaterialization) error
	List(context.Context) ([]PendingMaterialization, error)
	Delete(context.Context, string) error
}

type MemoryMaterializationJournal struct {
	mu      sync.Mutex
	limit   int
	order   []string
	records map[string]PendingMaterialization
}

func NewMemoryMaterializationJournal(limit int) (*MemoryMaterializationJournal, error) {
	if limit <= 0 || limit > 10000 {
		return nil, errors.New("materialization journal limit is invalid")
	}
	return &MemoryMaterializationJournal{
		limit: limit, records: make(map[string]PendingMaterialization, limit),
	}, nil
}

func (journal *MemoryMaterializationJournal) EnsureCapacity(ctx context.Context) error {
	if journal == nil || journal.records == nil || ctx == nil {
		return errors.New("materialization journal is not configured")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if len(journal.records) >= journal.limit {
		return ErrMaterializationJournalFull
	}
	return nil
}

func (journal *MemoryMaterializationJournal) Put(
	ctx context.Context,
	record PendingMaterialization,
) error {
	if journal == nil || journal.records == nil || ctx == nil {
		return errors.New("materialization journal is not configured")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validatePendingMaterialization(record); err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if _, exists := journal.records[record.ID]; !exists {
		if len(journal.records) >= journal.limit {
			return ErrMaterializationJournalFull
		}
		journal.order = append(journal.order, record.ID)
	}
	journal.records[record.ID] = clonePendingMaterialization(record)
	return nil
}

func (journal *MemoryMaterializationJournal) List(
	ctx context.Context,
) ([]PendingMaterialization, error) {
	if journal == nil || journal.records == nil || ctx == nil {
		return nil, errors.New("materialization journal is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	records := make([]PendingMaterialization, 0, len(journal.order))
	for _, id := range journal.order {
		record, exists := journal.records[id]
		if exists {
			records = append(records, clonePendingMaterialization(record))
		}
	}
	return records, nil
}

func (journal *MemoryMaterializationJournal) Delete(ctx context.Context, id string) error {
	if journal == nil || journal.records == nil || ctx == nil || id == "" {
		return errors.New("materialization journal delete is invalid")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if _, exists := journal.records[id]; !exists {
		return nil
	}
	delete(journal.records, id)
	index := slices.Index(journal.order, id)
	if index >= 0 {
		journal.order = slices.Delete(journal.order, index, index+1)
	}
	return nil
}

type MaterializationResult struct {
	PendingID          string
	LocalSealed        bool
	GPUReleased        bool
	L2Published        bool
	Committed          bool
	SourceLostReported bool
}

func (agent *StreamAgent) SealAndMaterialize(
	ctx context.Context,
) (MaterializationResult, error) {
	result := MaterializationResult{}
	if agent == nil || agent.materialization == nil || ctx == nil {
		return result, errors.New("stage worker materialization is not configured")
	}
	agent.materializationMu.Lock()
	defer agent.materializationMu.Unlock()
	authority := agent.activeAuthority()
	if authority == nil {
		return result, errors.New("missing active StageAuthority to seal")
	}
	receipt, err := agent.runtime.SealOutput(ctx, authority)
	if err != nil {
		return result, err
	}
	result.PendingID = receipt.GetReceiptId()
	result.LocalSealed = true
	record := PendingMaterialization{
		ID: receipt.GetReceiptId(), StageAuthority: authority, LocalReceipt: receipt,
	}
	if err := agent.materialization.journal.Put(ctx, record); err != nil {
		return result, fmt.Errorf("persist sealed local output before releasing StageAuthority: %w", err)
	}
	agent.clearActive(authority)
	result.GPUReleased = true
	advanced, err := agent.advancePendingMaterialization(ctx, record)
	advanced.LocalSealed = true
	advanced.GPUReleased = true
	return advanced, err
}

func (agent *StreamAgent) ResumeMaterializations(
	ctx context.Context,
) (MaterializationResult, error) {
	result := MaterializationResult{}
	if agent == nil || agent.materialization == nil || ctx == nil {
		return result, errors.New("stage worker materialization is not configured")
	}
	agent.materializationMu.Lock()
	defer agent.materializationMu.Unlock()
	records, err := agent.materialization.journal.List(ctx)
	if err != nil {
		return result, err
	}
	for _, record := range records {
		result, err = agent.advancePendingMaterialization(ctx, record)
		result.LocalSealed = true
		result.GPUReleased = true
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

type streamMaterialization struct {
	validator          *materializationauthority.Validator
	source             stageartifact.LocalOutputSource
	publisher          stageartifact.Publisher
	journal            MaterializationJournal
	sourceLossEvidence MaterializationSourceLossEvidenceProvider
	maxClockSkew       time.Duration
}

func (agent *StreamAgent) advancePendingMaterialization(
	ctx context.Context,
	record PendingMaterialization,
) (MaterializationResult, error) {
	result := MaterializationResult{PendingID: record.ID}
	if record.MaterializationAuthority == nil {
		response, err := agent.control.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
			Operation: &velav1.StageWorkerControlServiceConnectRequest_SealStageOutput{
				SealStageOutput: &velav1.SealStageOutputRequest{
					Authority: record.StageAuthority, LocalReceipt: record.LocalReceipt,
				},
			},
		})
		if err != nil {
			return result, fmt.Errorf("seal local StageArtifact with control authority: %w", err)
		}
		if command := response.GetStageCommandResult(); command != nil {
			return result, fmt.Errorf(
				"control rejected StageArtifact seal: operation=%s decision=%s detail=%s",
				command.GetOperation(),
				command.GetDecision(),
				command.GetDetail(),
			)
		}
		authority := response.GetMaterializationAuthority()
		verified, err := agent.materialization.validator.ValidateWithClockSkew(
			authority,
			agent.materialization.maxClockSkew,
		)
		if err != nil {
			return result, fmt.Errorf("validate issued MaterializationAuthority: %w", err)
		}
		if err := materializationMatchesPending(verified, record); err != nil {
			return result, err
		}
		record.MaterializationAuthority = proto.Clone(authority).(*velav1.MaterializationAuthority)
		if err := agent.materialization.journal.Put(ctx, record); err != nil {
			return result, fmt.Errorf("persist issued MaterializationAuthority: %w", err)
		}
	}
	if record.ObjectVersion == "" {
		manifest, err := stageartifact.ParseLocalOutputManifestV1(
			record.LocalReceipt.GetOutputManifestJson(),
		)
		if err != nil {
			return result, err
		}
		lease, err := materializationLease(record.MaterializationAuthority)
		if err != nil {
			return result, err
		}
		source, err := agent.materialization.source.Open(ctx, manifest)
		if err != nil {
			if errors.Is(err, stageartifact.ErrLocalOutputSourceLost) {
				return agent.reportMaterializationSourceLost(ctx, record, err)
			}
			return result, err
		}
		published, publishErr := agent.materialization.publisher.Publish(ctx, lease, source)
		closeErr := source.Close()
		if publishErr != nil || closeErr != nil {
			return result, errors.Join(publishErr, closeErr)
		}
		if published.ObjectKey != lease.ObjectKey || published.ObjectVersion == "" {
			return result, errors.New("StageArtifact Publisher returned mismatched immutable identity")
		}
		record.ObjectVersion = published.ObjectVersion
		if err := agent.materialization.journal.Put(ctx, record); err != nil {
			return result, fmt.Errorf("persist exact published object version: %w", err)
		}
		result.L2Published = true
	} else {
		result.L2Published = true
	}
	if record.CommittedAt.IsZero() {
		record.CommittedAt = time.Now().UTC()
		if err := agent.materialization.journal.Put(ctx, record); err != nil {
			return result, fmt.Errorf("persist StageArtifact commit time: %w", err)
		}
	}
	response, err := agent.control.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_CommitStageMaterialization{
			CommitStageMaterialization: &velav1.CommitStageMaterializationRequest{
				MaterializationAuthority: record.MaterializationAuthority,
				ObjectVersion:            record.ObjectVersion,
				CommittedAt:              timestamppb.New(record.CommittedAt),
			},
		},
	})
	if err != nil {
		return result, fmt.Errorf("commit durable StageArtifact: %w", err)
	}
	command := response.GetStageCommandResult()
	if command == nil || command.GetOperation() !=
		velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_COMMIT_STAGE_MATERIALIZATION ||
		(command.GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED &&
			command.GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED) {
		return result, errors.New("control rejected StageArtifact materialization commit")
	}
	if err := agent.materialization.journal.Delete(ctx, record.ID); err != nil {
		return result, fmt.Errorf("clear committed materialization journal record: %w", err)
	}
	result.Committed = true
	return result, nil
}

func (agent *StreamAgent) reportMaterializationSourceLost(
	ctx context.Context,
	record PendingMaterialization,
	sourceErr error,
) (MaterializationResult, error) {
	result := MaterializationResult{PendingID: record.ID}
	if record.SourceLoss == nil {
		if agent.materialization.sourceLossEvidence == nil {
			return result, errors.Join(
				sourceErr,
				errors.New("source-loss evidence provider is not configured"),
			)
		}
		evidence, err := agent.materialization.sourceLossEvidence.Evidence(
			ctx,
			clonePendingMaterialization(record),
		)
		if err != nil {
			return result, errors.Join(sourceErr, err)
		}
		if err := validateSourceLossEvidence(evidence); err != nil {
			return result, errors.Join(sourceErr, err)
		}
		record.SourceLoss = &evidence
		if err := agent.materialization.journal.Put(ctx, record); err != nil {
			return result, errors.Join(
				sourceErr,
				fmt.Errorf("persist source-loss evidence: %w", err),
			)
		}
	}
	evidence := record.SourceLoss
	response, err := agent.control.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_ReportMaterializationSourceLost{
			ReportMaterializationSourceLost: &velav1.ReportMaterializationSourceLostRequest{
				MaterializationAuthority: record.MaterializationAuthority,
				FailureFingerprint:       append([]byte(nil), evidence.FailureFingerprint[:]...),
				ConsumedResourceUnits:    evidence.ConsumedResourceUnits,
				LostAt:                   timestamppb.New(evidence.LostAt),
				RetryAt:                  timestamppb.New(evidence.RetryAt),
			},
		},
	})
	if err != nil {
		return result, errors.Join(
			sourceErr,
			fmt.Errorf("report materialization source loss: %w", err),
		)
	}
	command := response.GetStageCommandResult()
	if command == nil || command.GetOperation() !=
		velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REPORT_MATERIALIZATION_SOURCE_LOST ||
		(command.GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED &&
			command.GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED) {
		return result, errors.Join(
			sourceErr,
			errors.New("control rejected materialization source loss"),
		)
	}
	if err := agent.materialization.journal.Delete(ctx, record.ID); err != nil {
		return result, fmt.Errorf("clear source-loss journal record: %w", err)
	}
	result.SourceLostReported = true
	return result, fmt.Errorf("%w: %v", ErrMaterializationSourceLostReported, sourceErr)
}

func materializationMatchesPending(
	verified materializationauthority.Verified,
	record PendingMaterialization,
) error {
	stageDigest, err := stageauthority.Digest(record.StageAuthority)
	if err != nil {
		return err
	}
	authority := verified.Authority
	receipt := record.LocalReceipt
	manifest, err := stageartifact.ParseLocalOutputManifestV1(receipt.GetOutputManifestJson())
	if err != nil {
		return err
	}
	if !bytes.Equal(authority.GetStageAuthorityDigest(), stageDigest[:]) ||
		authority.GetLocalReceiptId() != receipt.GetReceiptId() ||
		!bytes.Equal(authority.GetLocalReceiptDigest(), receipt.GetManifestSha256()) ||
		!bytes.Equal(authority.GetSha256(), manifest.PayloadSHA256[:]) ||
		authority.GetSizeBytes() != manifest.SizeBytes ||
		authority.GetContentType() != manifest.ContentType ||
		authority.GetSourceWorkerInstanceId() != record.StageAuthority.GetWorkerInstanceId() ||
		authority.GetSourceWorkerInstanceEpoch() != record.StageAuthority.GetWorkerInstanceEpoch() ||
		len(record.StageAuthority.GetMembers()) != 1 ||
		authority.GetSourceWorkerMemberId() != record.StageAuthority.GetMembers()[0].GetWorkerMemberId() ||
		authority.GetSourceWorkerMemberEpoch() != record.StageAuthority.GetMembers()[0].GetMemberEpoch() {
		return errors.New("issued MaterializationAuthority does not match sealed local output")
	}
	return nil
}

func materializationLease(
	authority *velav1.MaterializationAuthority,
) (stageartifact.MaterializationLease, error) {
	verifiedDigest, err := materializationauthority.Digest(authority)
	if err != nil {
		return stageartifact.MaterializationLease{}, err
	}
	leaseID, err := uuid.Parse(authority.GetStageMaterializationLeaseId())
	if err != nil {
		return stageartifact.MaterializationLease{}, err
	}
	artifactID, err := uuid.Parse(authority.GetStageArtifactId())
	if err != nil {
		return stageartifact.MaterializationLease{}, err
	}
	lease := stageartifact.MaterializationLease{
		ID: leaseID, ArtifactID: artifactID, ObjectKey: authority.GetObjectKey(),
		ContentType: authority.GetContentType(), SizeBytes: authority.GetSizeBytes(),
		TokenDigest: verifiedDigest,
		IssuedAt:    authority.GetIssuedAt().AsTime().UTC(),
		ExpiresAt:   authority.GetExpiresAt().AsTime().UTC(),
	}
	copy(lease.SHA256[:], authority.GetSha256())
	return lease, nil
}

func validatePendingMaterialization(record PendingMaterialization) error {
	if record.ID == "" || record.StageAuthority == nil || record.LocalReceipt == nil ||
		record.ID != record.LocalReceipt.GetReceiptId() ||
		len(record.LocalReceipt.GetManifestSha256()) != sha256.Size ||
		len(record.LocalReceipt.GetOutputManifestJson()) == 0 ||
		record.LocalReceipt.GetTotalSizeBytes() <= 0 || record.LocalReceipt.GetSealedAt() == nil ||
		record.LocalReceipt.GetSealedAt().CheckValid() != nil {
		return errors.New("pending StageArtifact materialization is incomplete")
	}
	if _, err := stageauthority.Digest(record.StageAuthority); err != nil {
		return err
	}
	manifestDigest := sha256.Sum256(record.LocalReceipt.GetOutputManifestJson())
	if !bytes.Equal(manifestDigest[:], record.LocalReceipt.GetManifestSha256()) {
		return errors.New("pending StageArtifact manifest digest is mismatched")
	}
	if record.ObjectVersion != "" && record.MaterializationAuthority == nil {
		return errors.New("published StageArtifact version lacks MaterializationAuthority")
	}
	if !record.CommittedAt.IsZero() && record.ObjectVersion == "" {
		return errors.New("StageArtifact commit time lacks published object version")
	}
	if record.SourceLoss != nil {
		if record.MaterializationAuthority == nil {
			return errors.New("source-loss evidence lacks MaterializationAuthority")
		}
		if err := validateSourceLossEvidence(*record.SourceLoss); err != nil {
			return err
		}
	}
	return nil
}

func validateSourceLossEvidence(evidence MaterializationSourceLossEvidence) error {
	if evidence.FailureFingerprint == [sha256.Size]byte{} ||
		evidence.ConsumedResourceUnits <= 0 || evidence.LostAt.IsZero() ||
		!evidence.RetryAt.After(evidence.LostAt) {
		return errors.New("materialization source-loss evidence is invalid")
	}
	return nil
}

func clonePendingMaterialization(record PendingMaterialization) PendingMaterialization {
	cloned := record
	if record.StageAuthority != nil {
		cloned.StageAuthority = proto.Clone(record.StageAuthority).(*velav1.StageAuthority)
	}
	if record.LocalReceipt != nil {
		cloned.LocalReceipt = proto.Clone(record.LocalReceipt).(*velav1.LocalMaterializationReceipt)
	}
	if record.MaterializationAuthority != nil {
		cloned.MaterializationAuthority = proto.Clone(record.MaterializationAuthority).(*velav1.MaterializationAuthority)
	}
	if record.SourceLoss != nil {
		evidence := *record.SourceLoss
		cloned.SourceLoss = &evidence
	}
	return cloned
}
