package stageworkeragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// TerminalExecutionDrainResult covers the complete allocation history signed for
// this Worker. It is an observation, not a persisted retirement receipt, installed
// input/Runtime floor or permission to delete files.
type TerminalExecutionDrainResult struct {
	DispositionDigest   [sha256.Size]byte
	Cutoff              int64
	RequiredAllocations int
	Allocations         map[string]ExecutionDrainResult
	AllDrained          bool
}

// InspectTerminalExecutionDrains requires one authentic execution envelope per
// signed allocation and one trusted current journal reader per member. Different
// members may have drained different renewals of the same immutable execution.
// All requests are validated before any RPC; missing history stays unproven.
func (agent *Agent) InspectTerminalExecutionDrains(ctx context.Context, disposition *velav1.StageTerminalDisposition, authorities map[string]*velav1.StageAuthority, targets map[string]*velav1.ModelRuntimeIdentity) (TerminalExecutionDrainResult, error) {
	result := TerminalExecutionDrainResult{}
	if agent == nil || agent.floor == nil || ctx == nil {
		return result, errors.New("terminal execution drain inspection is not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, agent.floor.timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	verified, err := agent.floor.validator.ValidateTerminalDispositionEnvelope(disposition)
	if err != nil {
		return result, err
	}
	value := verified.Disposition
	if len(authorities) != len(value.GetAllocations()) || len(targets) != len(agent.ids) {
		return result, errors.New("terminal execution drain history or readers are incomplete")
	}
	readers := make(map[string]*velav1.ModelRuntimeIdentity, len(targets))
	for id, identity := range targets {
		if identity == nil {
			return result, errors.New("terminal execution drain reader is missing")
		}
		readers[id] = proto.Clone(identity).(*velav1.ModelRuntimeIdentity)
	}
	queries := make(map[string]*velav1.StageAuthority, len(authorities))
	for _, allocation := range value.GetAllocations() {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		authority := authorities[allocation.GetStageAllocationId()]
		if authority == nil || proto.Size(authority) > 64<<10 {
			return result, errors.New("terminal execution drain original is missing or oversized")
		}
		original, err := agent.floor.validator.ValidateEnvelopeForReplay(authority, 0)
		if err != nil {
			return result, err
		}
		if !proto.Equal(original.Authority, authority) || stageauthority.ValidateTerminalAllocation(value, allocation, original.Authority) != nil ||
			(allocation.GetStageAllocationId() == value.GetStageAllocationId() && !bytes.Equal(original.Digest[:], value.GetOriginalAuthorityDigest())) {
			return result, errors.New("terminal execution drain original does not match signed history")
		}
		if _, err := agent.executionDrainScopes(original.Authority, readers, true); err != nil {
			return result, err
		}
		for _, member := range allocation.GetMembers() {
			matches := 0
			for _, binding := range agent.floor.bindings {
				if binding.matchesTopology(value, allocation, member) && proto.Equal(executionDrainIdentity(binding.Runtime), readers[member.GetWorkerMemberId()]) {
					matches++
				}
			}
			if matches != 1 {
				return result, errors.New("terminal execution drain topology is missing or ambiguous")
			}
		}
		queries[allocation.GetStageAllocationId()] = original.Authority
	}
	result.DispositionDigest, result.Cutoff = verified.Digest, value.GetCutoff()
	result.RequiredAllocations = len(value.GetAllocations())
	result.Allocations = make(map[string]ExecutionDrainResult, result.RequiredAllocations)
	var joined error
	// Bound concurrency to the existing member collector; one deadline covers the
	// complete history, including validation and every allocation's member calls.
	for _, allocation := range value.GetAllocations() {
		if err := ctx.Err(); err != nil {
			return result, errors.Join(joined, err)
		}
		id := allocation.GetStageAllocationId()
		drain, err := agent.collectExecutionDrain(ctx, queries[id], readers, true, true)
		result.Allocations[id] = drain
		if err != nil || !drain.AllDrained {
			if err == nil {
				err = errors.New("incomplete membership")
			}
			joined = errors.Join(joined, fmt.Errorf("terminal allocation %s drain is unproven: %w", id, err))
		}
	}
	if err := ctx.Err(); err != nil {
		return result, errors.Join(joined, err)
	}
	result.AllDrained = joined == nil && len(result.Allocations) == result.RequiredAllocations
	return result, joined
}
