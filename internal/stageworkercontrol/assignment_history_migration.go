package stageworkercontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const maxAssignmentHistoryWireBytes = 4 << 20

type assignmentHistoryCandidate struct {
	commandID uuid.UUID
	wire      []byte
}

type AssignmentHistoryMigrationOptions struct {
	RetireUnverifiable bool
}

// BackfillAssignmentHistory records verified authority from a bounded batch of
// original assignments. It never rewrites their delivery bytes or signs history.
// The returned count includes candidates concurrently recorded by another caller.
func BackfillAssignmentHistory(
	ctx context.Context, pool *pgxpool.Pool, validator *stageauthority.Validator, limit int,
) (int, error) {
	return BackfillAssignmentHistoryWithOptions(ctx, pool, validator, limit, AssignmentHistoryMigrationOptions{})
}

// BackfillAssignmentHistoryWithOptions can explicitly retire unverifiable
// delivery payloads, preserving a tombstone instead of inventing signed history.
func BackfillAssignmentHistoryWithOptions(
	ctx context.Context, pool *pgxpool.Pool, validator *stageauthority.Validator, limit int,
	options AssignmentHistoryMigrationOptions,
) (int, error) {
	if ctx == nil || pool == nil || validator == nil || limit < 1 || limit > 100 {
		return 0, errors.New("assignment history backfill requires a database, validator, and limit from 1 to 100")
	}
	rows, err := pool.Query(ctx, `
		SELECT command_id, assignment_wire
		FROM vela_read_stage_assignment_history_backfill($1)
	`, limit)
	if err != nil {
		return 0, redactedAssignmentHistoryError("read assignment history candidates", err)
	}
	defer rows.Close()
	candidates := make([]assignmentHistoryCandidate, 0, limit)
	defer func() {
		for _, candidate := range candidates {
			clear(candidate.wire)
		}
	}()
	for rows.Next() {
		var candidate assignmentHistoryCandidate
		if err := rows.Scan(&candidate.commandID, &candidate.wire); err != nil {
			clear(candidate.wire)
			return 0, redactedAssignmentHistoryError("decode assignment history candidate", err)
		}
		if candidate.commandID == uuid.Nil || len(candidate.wire) == 0 ||
			len(candidate.wire) > maxAssignmentHistoryWireBytes || len(candidates) >= limit {
			clear(candidate.wire)
			return 0, errors.New("assignment history candidates exceed their bounded format")
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return 0, redactedAssignmentHistoryError("read assignment history candidates", err)
	}
	// Release the reader connection before recording, including for one-connection pools.
	rows.Close()
	for index, candidate := range candidates {
		if err := backfillAssignmentHistoryCandidate(ctx, pool, validator, candidate, options); err != nil {
			return index, fmt.Errorf("backfill StageAssignment %s: %w", candidate.commandID, err)
		}
		clear(candidate.wire)
	}
	return len(candidates), nil
}

func backfillAssignmentHistoryCandidate(
	ctx context.Context, pool *pgxpool.Pool, validator *stageauthority.Validator,
	candidate assignmentHistoryCandidate, options AssignmentHistoryMigrationOptions,
) error {
	assignment := new(velav1.StageAssignment)
	defer proto.Reset(assignment)
	if err := proto.Unmarshal(candidate.wire, assignment); err != nil {
		return retireUnverifiableAssignmentHistory(ctx, pool, candidate, options, "INVALID_ASSIGNMENT_PROTOBUF")
	}
	if authority := assignment.GetAuthority(); authority != nil &&
		assignmentHistoryHasUnknownFields(authority.ProtoReflect()) {
		return retireUnverifiableAssignmentHistory(ctx, pool, candidate, options, "UNKNOWN_AUTHORITY_FIELDS")
	}
	verified, err := validator.ValidateEnvelopeForReplay(assignment.GetAuthority(), 0)
	if err != nil {
		reason := "INVALID_AUTHORITY"
		switch {
		case errors.Is(err, stageauthority.ErrUnknownKey):
			reason = "UNKNOWN_SIGNING_KEY"
		case errors.Is(err, stageauthority.ErrInvalidSignature):
			reason = "INVALID_AUTHORITY_SIGNATURE"
		case errors.Is(err, stageauthority.ErrStale):
			reason = "FUTURE_ISSUED_AUTHORITY"
		case !errors.Is(err, stageauthority.ErrInvalid):
			return errors.New("stored signed authority verification failed")
		}
		return retireUnverifiableAssignmentHistory(ctx, pool, candidate, options, reason)
	}
	evidence, err := stageAssignmentAuthorityEvidence(verified.Authority, candidate.wire)
	if err != nil {
		return err
	}
	evidence["command_id"] = candidate.commandID
	payload, err := json.Marshal(evidence)
	if err != nil {
		return errors.New("encode assignment authority evidence failed")
	}
	defer clear(payload)
	var recorded bool
	if err := pool.QueryRow(ctx, `
		SELECT vela_record_stage_assignment_authority($1::jsonb)
	`, payload).Scan(&recorded); err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "55000" &&
			postgresError.ConstraintName == "stage_assignment_authority_mapping_mismatch" {
			return retireUnverifiableAssignmentHistory(ctx, pool, candidate, options, "AUTHORITY_MAPPING_MISMATCH")
		}
		return redactedAssignmentHistoryError("record assignment authority evidence", err)
	}
	return nil
}

func retireUnverifiableAssignmentHistory(
	ctx context.Context, pool *pgxpool.Pool, candidate assignmentHistoryCandidate,
	options AssignmentHistoryMigrationOptions, reason string,
) error {
	if !options.RetireUnverifiable {
		return fmt.Errorf("stored assignment cannot be verified (%s)", reason)
	}
	digest := sha256.Sum256(candidate.wire)
	payload, err := json.Marshal(map[string]any{
		"schema_version":    1,
		"command_id":        candidate.commandID,
		"assignment_digest": hex.EncodeToString(digest[:]),
		"reason":            reason,
	})
	if err != nil {
		return errors.New("encode unverifiable assignment retirement failed")
	}
	var retired bool
	if err := pool.QueryRow(ctx, `
		SELECT vela_retire_unverifiable_stage_assignment($1::jsonb)
	`, payload).Scan(&retired); err != nil {
		return redactedAssignmentHistoryError("retire unverifiable assignment delivery", err)
	}
	return nil
}

// Callers pass the canonical authority returned by Sign or signature validation.
// The original assignment wire is hashed as received, without reserialization.
func stageAssignmentAuthorityEvidence(authority *velav1.StageAuthority, wire []byte) (map[string]any, error) {
	if len(wire) == 0 || len(wire) > maxAssignmentHistoryWireBytes {
		return nil, errors.New("assignment history wire exceeds its bounded format")
	}
	if authority != nil && assignmentHistoryHasUnknownFields(authority.ProtoReflect()) {
		return nil, errors.New("assignment history authority contains unknown protobuf fields")
	}
	authorityDigest, err := stageauthority.Digest(authority)
	if err != nil {
		return nil, errors.New("assignment history authority is invalid")
	}
	authorityWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(authority)
	if err != nil {
		return nil, errors.New("encode assignment history authority failed")
	}
	defer clear(authorityWire)
	if sha256.Sum256(authorityWire) != authorityDigest {
		return nil, errors.New("assignment history authority is not canonical")
	}
	assignmentDigest := sha256.Sum256(wire)
	tokenDigest := sha256.Sum256(authority.GetLeaseToken())
	return map[string]any{
		"schema_version":        1,
		"assignment_digest":     hex.EncodeToString(assignmentDigest[:]),
		"authority_wire":        hex.EncodeToString(authorityWire),
		"authority_digest":      hex.EncodeToString(authorityDigest[:]),
		"job_id":                authority.GetJobId(),
		"attempt_id":            authority.GetAttemptId(),
		"stage_run_id":          authority.GetStageRunId(),
		"stage_attempt_id":      authority.GetStageAttemptId(),
		"stage_allocation_id":   authority.GetStageAllocationId(),
		"stage_lease_id":        authority.GetStageLeaseId(),
		"worker_instance_id":    authority.GetWorkerInstanceId(),
		"worker_instance_epoch": authority.GetWorkerInstanceEpoch(),
		"token_digest":          hex.EncodeToString(tokenDigest[:]),
		"execution_nonce":       hex.EncodeToString(authority.GetExecutionNonce()),
	}, nil
}

func assignmentHistoryHasUnknownFields(message protoreflect.Message) bool {
	if !message.IsValid() {
		return false
	}
	if len(message.GetUnknown()) != 0 {
		return true
	}
	unknown := false
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsMap():
			if field.MapValue().Message() != nil {
				value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
					unknown = assignmentHistoryHasUnknownFields(item.Message())
					return !unknown
				})
			}
		case field.IsList():
			if field.Message() != nil {
				list := value.List()
				for index := 0; index < list.Len() && !unknown; index++ {
					unknown = assignmentHistoryHasUnknownFields(list.Get(index).Message())
				}
			}
		case field.Message() != nil:
			unknown = assignmentHistoryHasUnknownFields(value.Message())
		}
		return !unknown
	})
	return unknown
}

func redactedAssignmentHistoryError(operation string, err error) error {
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("%s: %w", operation, context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w", operation, context.DeadlineExceeded)
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		return fmt.Errorf("%s failed (SQLSTATE %s)", operation, postgresError.Code)
	}
	return fmt.Errorf("%s failed", operation)
}
