package stageworkeragent

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/vivym/vela/internal/modelruntimetransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// ExecutionExclusionProof contains exactly one validated member result. Drain and
// never-admitted retain distinct meanings; neither proves Worker input exclusion.
type ExecutionExclusionProof struct {
	Drain         *velav1.ModelRuntimeExecutionDrainResult
	NeverAdmitted *velav1.ModelRuntimeExecutionNonAdmissionResult
}

// TerminalExecutionExclusionResult is not a durable Worker retirement receipt or
// filesystem deletion authority. Every signed allocation/member pair is required.
type TerminalExecutionExclusionResult struct {
	DispositionDigest   [sha256.Size]byte
	Cutoff              int64
	RequiredAllocations int
	RequiredMembers     int
	Allocations         map[string]map[string]ExecutionExclusionProof
	AllExcluded         bool
}

func (agent *Agent) InspectTerminalExecutionExclusions(ctx context.Context, disposition *velav1.StageTerminalDisposition, authorities map[string]*velav1.StageAuthority, targets map[string]*velav1.ModelRuntimeIdentity) (TerminalExecutionExclusionResult, error) {
	return agent.collectTerminalExecutionExclusions(ctx, disposition, authorities, targets, false)
}

// CheckpointTerminalExecutionExclusions may persist current-epoch non-admission
// proofs after independently installed Runtime floors. It never installs a floor,
// cancels/drains a backend, or infers history from a missing Runtime record.
func (agent *Agent) CheckpointTerminalExecutionExclusions(ctx context.Context, disposition *velav1.StageTerminalDisposition, authorities map[string]*velav1.StageAuthority, targets map[string]*velav1.ModelRuntimeIdentity) (TerminalExecutionExclusionResult, error) {
	return agent.collectTerminalExecutionExclusions(ctx, disposition, authorities, targets, true)
}

func (agent *Agent) collectTerminalExecutionExclusions(ctx context.Context, disposition *velav1.StageTerminalDisposition, authorities map[string]*velav1.StageAuthority, targets map[string]*velav1.ModelRuntimeIdentity, checkpoint bool) (TerminalExecutionExclusionResult, error) {
	result := TerminalExecutionExclusionResult{}
	if agent == nil || agent.floor == nil || ctx == nil {
		return result, errors.New("terminal execution exclusion is not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, agent.floor.timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	history, err := agent.terminalExecutionQueries(ctx, disposition, authorities, targets)
	if err != nil {
		return result, err
	}
	result.DispositionDigest, result.Cutoff = history.verified.Digest, history.verified.Disposition.GetCutoff()
	result.RequiredAllocations, result.RequiredMembers = len(history.authorities), len(agent.ids)
	result.Allocations = make(map[string]map[string]ExecutionExclusionProof, result.RequiredAllocations)
	var joined error
	for _, allocation := range history.verified.Disposition.GetAllocations() {
		if err := ctx.Err(); err != nil {
			return result, errors.Join(joined, err)
		}
		id := allocation.GetStageAllocationId()
		scopes, err := agent.executionDrainScopes(history.authorities[id], history.readers, true)
		if err != nil {
			return result, errors.Join(joined, err)
		}
		type memberResult struct {
			id    string
			proof ExecutionExclusionProof
			err   error
		}
		completed := make(chan memberResult, len(agent.ids))
		for _, memberID := range agent.ids {
			go func() {
				proof, err := agent.collectMemberExclusion(ctx, memberID, scopes[memberID], checkpoint)
				completed <- memberResult{id: memberID, proof: proof, err: err}
			}()
		}
		members := make(map[string]ExecutionExclusionProof)
		result.Allocations[id] = members
		failures := make(map[string]error)
		for range agent.ids {
			select {
			case <-ctx.Done():
				return result, errors.Join(joined, ctx.Err())
			case member := <-completed:
				if err := ctx.Err(); err != nil {
					return result, errors.Join(joined, err)
				}
				if member.err != nil {
					failures[member.id] = member.err
				} else {
					members[member.id] = member.proof
				}
			}
		}
		for _, memberID := range agent.ids {
			if err := failures[memberID]; err != nil {
				joined = errors.Join(joined, fmt.Errorf("allocation %s member %s exclusion: %w", id, memberID, err))
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return result, errors.Join(joined, err)
	}
	result.AllExcluded = joined == nil && len(result.Allocations) == result.RequiredAllocations
	for _, members := range result.Allocations {
		result.AllExcluded = result.AllExcluded && len(members) == result.RequiredMembers
	}
	return result, joined
}

func (agent *Agent) collectMemberExclusion(ctx context.Context, memberID string, scope *velav1.ModelRuntimeExecutionDrainScope, checkpoint bool) (ExecutionExclusionProof, error) {
	proof := ExecutionExclusionProof{}
	if err := ctx.Err(); err != nil {
		return proof, err
	}
	verified, err := modelruntimetransport.ValidateExecutionDrainScope(agent.floor.validator, scope, 0, true)
	if err != nil {
		return proof, err
	}
	client := agent.members[memberID]
	drain, err := client.InspectStageAllocationDrain(ctx, &velav1.ModelRuntimeServiceInspectStageAllocationDrainRequest{Scope: proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope)})
	if err != nil {
		return proof, err
	}
	if err := ctx.Err(); err != nil {
		return proof, err
	}
	if drain == nil || len(drain.ProtoReflect().GetUnknown()) != 0 {
		return proof, errors.New("invalid allocation drain wrapper")
	}
	if err := modelruntimetransport.ValidateAllocationDrainResult(agent.floor.validator, scope, verified.Digest, drain.GetResult()); err != nil {
		return proof, err
	}
	if drain.GetResult().GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		return proof, errors.New("allocation drain inspection rejected")
	}
	if drain.GetResult().GetCheckpoint() != nil {
		proof.Drain = proto.Clone(drain.GetResult()).(*velav1.ModelRuntimeExecutionDrainResult)
		return proof, nil
	}
	read, err := client.InspectStageNonAdmission(ctx, &velav1.ModelRuntimeServiceInspectStageNonAdmissionRequest{Scope: proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope)})
	if err != nil {
		return proof, err
	}
	if err := ctx.Err(); err != nil {
		return proof, err
	}
	if read == nil || len(read.ProtoReflect().GetUnknown()) != 0 {
		return proof, errors.New("invalid non-admission inspection wrapper")
	}
	result := read.GetResult()
	if err := modelruntimetransport.ValidateExecutionNonAdmissionResult(agent.floor.validator, scope, verified.Digest, result); err != nil {
		return proof, err
	}
	if result.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		return proof, errors.New("non-admission inspection rejected")
	}
	if result.GetCheckpoint() == nil && checkpoint {
		// Other members may already have historical proof after retiring their
		// original profile. Only this new checkpoint requires original residency.
		checkpointScope, err := agent.executionDrainMemberScope(verified.Authority, memberID, nil, false)
		if err != nil {
			return proof, err
		}
		response, err := client.CheckpointStageNonAdmission(ctx, &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{Scope: proto.Clone(checkpointScope).(*velav1.ModelRuntimeExecutionDrainScope)})
		if err != nil {
			return proof, err
		}
		if err := ctx.Err(); err != nil {
			return proof, err
		}
		if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 {
			return proof, errors.New("invalid non-admission checkpoint wrapper")
		}
		result = response.GetResult()
		if err := modelruntimetransport.ValidateExecutionNonAdmissionResult(agent.floor.validator, checkpointScope, verified.Digest, result); err != nil {
			return proof, err
		}
	}
	if result.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || result.GetCheckpoint() == nil {
		return proof, errors.New("member execution exclusion is unproven")
	}
	proof.NeverAdmitted = proto.Clone(result).(*velav1.ModelRuntimeExecutionNonAdmissionResult)
	return proof, nil
}
