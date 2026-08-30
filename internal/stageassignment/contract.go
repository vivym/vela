package stageassignment

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const (
	maxExecutionSpecBytes = 64 * 1024
	maxExecutionInputs    = 64
	maxMemberStartTimeout = 10 * time.Minute
	MaxRequiredMembers    = 64
)

type Contract struct {
	RequiredMemberIDs  []string
	MemberStartTimeout time.Duration
}

func Validate(assignment *velav1.StageAssignment) (Contract, error) {
	if assignment == nil || assignment.GetAuthority() == nil ||
		assignment.GetExecutionSpec() == nil || assignment.GetMemberStartTimeout() == nil {
		return Contract{}, errors.New("StageAssignment contract is incomplete")
	}
	if err := ValidateExecutionSpec(assignment.GetExecutionSpec()); err != nil {
		return Contract{}, err
	}
	if _, err := stageauthority.Digest(assignment.GetAuthority()); err != nil {
		return Contract{}, fmt.Errorf("StageAssignment authority: %w", err)
	}
	if err := assignment.GetMemberStartTimeout().CheckValid(); err != nil {
		return Contract{}, fmt.Errorf("StageAssignment member start timeout: %w", err)
	}
	timeout := assignment.GetMemberStartTimeout().AsDuration()
	if timeout <= 0 || timeout > maxMemberStartTimeout {
		return Contract{}, errors.New("StageAssignment member start timeout is invalid")
	}
	required := slices.Clone(assignment.GetRequiredWorkerMemberIds())
	if len(required) == 0 || len(required) > MaxRequiredMembers {
		return Contract{}, errors.New("StageAssignment required member set is invalid")
	}
	slices.Sort(required)
	for index, memberID := range required {
		if _, err := uuid.Parse(memberID); err != nil ||
			(index > 0 && required[index-1] == memberID) {
			return Contract{}, errors.New("StageAssignment required member identity is invalid or duplicated")
		}
	}
	authorityMembers := make([]string, 0, len(assignment.GetAuthority().GetMembers()))
	for _, member := range assignment.GetAuthority().GetMembers() {
		if member == nil {
			return Contract{}, errors.New("StageAssignment authority member set is invalid")
		}
		authorityMembers = append(authorityMembers, member.GetWorkerMemberId())
	}
	slices.Sort(authorityMembers)
	if !slices.Equal(required, authorityMembers) {
		return Contract{}, errors.New("StageAssignment member barrier does not match signed authority")
	}
	executionSpecDigest, err := stageauthority.ExecutionSpecDigest(assignment.GetExecutionSpec())
	if err != nil || !bytes.Equal(
		executionSpecDigest[:], assignment.GetAuthority().GetExecutionSpecDigest(),
	) {
		return Contract{}, errors.New("StageAssignment execution spec does not match signed authority")
	}
	if err := validateInputTransferTickets(assignment); err != nil {
		return Contract{}, err
	}
	return Contract{RequiredMemberIDs: required, MemberStartTimeout: timeout}, nil
}

func ValidateExecutionSpec(spec *velav1.StageExecutionSpec) error {
	if spec == nil {
		return nil
	}
	encoded, err := proto.Marshal(spec)
	if err != nil {
		return fmt.Errorf("encode StageExecutionSpec: %w", err)
	}
	if len(encoded) > maxExecutionSpecBytes || len(spec.GetInputs()) > maxExecutionInputs {
		return errors.New("StageExecutionSpec exceeds runtime bound")
	}
	for _, input := range spec.GetInputs() {
		if input == nil || input.GetStageArtifactId() == "" || input.GetObjectVersion() == "" ||
			len(input.GetSha256()) != sha256.Size || input.GetSizeBytes() <= 0 {
			return errors.New("StageExecutionSpec input Artifact is incomplete")
		}
		if _, err := uuid.Parse(input.GetStageInterfaceRevisionId()); err != nil {
			return errors.New("StageExecutionSpec input Artifact is incomplete")
		}
	}
	return nil
}

func validateInputTransferTickets(assignment *velav1.StageAssignment) error {
	inputs := assignment.GetExecutionSpec().GetInputs()
	tickets := assignment.GetInputTransferTickets()
	if len(inputs) != len(tickets) {
		return errors.New("StageAssignment input TransferTicket set is incomplete")
	}
	type artifactVersion struct {
		artifactID    string
		objectVersion string
	}
	expected := make(map[artifactVersion]struct{}, len(inputs))
	for _, input := range inputs {
		if input == nil {
			return errors.New("StageAssignment input Artifact is invalid")
		}
		if _, err := uuid.Parse(input.GetStageArtifactId()); err != nil || input.GetObjectVersion() == "" {
			return errors.New("StageAssignment input Artifact version identity is invalid")
		}
		key := artifactVersion{
			artifactID: input.GetStageArtifactId(), objectVersion: input.GetObjectVersion(),
		}
		if _, duplicated := expected[key]; duplicated {
			return errors.New("StageAssignment input Artifact version is duplicated")
		}
		expected[key] = struct{}{}
	}
	for _, ticket := range tickets {
		if ticket == nil || len(ticket.GetTransferTicket()) == 0 {
			return errors.New("StageAssignment input TransferTicket is invalid")
		}
		if _, err := uuid.Parse(ticket.GetStageArtifactId()); err != nil || ticket.GetObjectVersion() == "" {
			return errors.New("StageAssignment input TransferTicket version identity is invalid")
		}
		key := artifactVersion{
			artifactID: ticket.GetStageArtifactId(), objectVersion: ticket.GetObjectVersion(),
		}
		if _, ok := expected[key]; !ok {
			return errors.New("StageAssignment input TransferTicket does not match an exact input version")
		}
		delete(expected, key)
	}
	if len(expected) != 0 {
		return errors.New("StageAssignment input TransferTicket set is incomplete")
	}
	return nil
}
