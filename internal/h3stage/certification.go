package h3stage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
)

var ErrOutputNotEquivalent = errors.New("H3 placement output is not exactly equivalent")

type CorpusCase struct {
	ID          string
	InputDigest [sha256.Size]byte
}

type CorpusOutput struct {
	ExactBytes []byte
	Digest     [sha256.Size]byte
}

type CorpusRunner interface {
	Execute(context.Context, CorpusCase) (CorpusOutput, error)
}

type CertificationRequest struct {
	CorpusRevision           string
	ExecutionGraphRevisionID string
	StageProfileRevisionIDs  []string
	ConnectorRevisionIDs     []string
	ReferencePlacement       string
	CandidatePlacement       string
	Cases                    []CorpusCase
}

type CertificationReceipt struct {
	CorpusRevision           string
	ExecutionGraphRevisionID string
	StageProfileRevisionIDs  []string
	ConnectorRevisionIDs     []string
	ReferencePlacement       string
	CandidatePlacement       string
	CaseCount                int
	Matched                  bool
	EvidenceDigest           [sha256.Size]byte
}

type caseEvidence struct {
	ID              string `json:"id"`
	InputDigest     string `json:"input_digest"`
	ReferenceDigest string `json:"reference_digest"`
	CandidateDigest string `json:"candidate_digest"`
}

func VerifyPlacementEquivalence(
	ctx context.Context,
	request CertificationRequest,
	reference CorpusRunner,
	candidate CorpusRunner,
) (CertificationReceipt, error) {
	canonical, err := canonicalCertificationRequest(request)
	if err != nil {
		return CertificationReceipt{}, err
	}
	if ctx == nil || reference == nil || candidate == nil {
		return CertificationReceipt{}, errors.New("H3 certification runners and context are required")
	}
	receipt := CertificationReceipt{
		CorpusRevision:           canonical.CorpusRevision,
		ExecutionGraphRevisionID: canonical.ExecutionGraphRevisionID,
		StageProfileRevisionIDs:  slices.Clone(canonical.StageProfileRevisionIDs),
		ConnectorRevisionIDs:     slices.Clone(canonical.ConnectorRevisionIDs),
		ReferencePlacement:       canonical.ReferencePlacement,
		CandidatePlacement:       canonical.CandidatePlacement,
		CaseCount:                len(canonical.Cases),
	}
	evidence := make([]caseEvidence, 0, len(canonical.Cases))
	for _, testCase := range canonical.Cases {
		referenceOutput, runErr := reference.Execute(ctx, testCase)
		if runErr != nil {
			return receipt, fmt.Errorf("execute reference corpus case %s: %w", testCase.ID, runErr)
		}
		candidateOutput, runErr := candidate.Execute(ctx, testCase)
		if runErr != nil {
			return receipt, fmt.Errorf("execute candidate corpus case %s: %w", testCase.ID, runErr)
		}
		if err := validateCorpusOutput(referenceOutput); err != nil {
			return receipt, fmt.Errorf("reference corpus case %s: %w", testCase.ID, err)
		}
		if err := validateCorpusOutput(candidateOutput); err != nil {
			return receipt, fmt.Errorf("candidate corpus case %s: %w", testCase.ID, err)
		}
		evidence = append(evidence, caseEvidence{
			ID: testCase.ID, InputDigest: hex.EncodeToString(testCase.InputDigest[:]),
			ReferenceDigest: hex.EncodeToString(referenceOutput.Digest[:]),
			CandidateDigest: hex.EncodeToString(candidateOutput.Digest[:]),
		})
		if referenceOutput.Digest != candidateOutput.Digest ||
			!bytes.Equal(referenceOutput.ExactBytes, candidateOutput.ExactBytes) {
			receipt.EvidenceDigest = certificationEvidenceDigest(canonical, evidence, false)
			return receipt, fmt.Errorf("%w: corpus case %s", ErrOutputNotEquivalent, testCase.ID)
		}
	}
	receipt.Matched = true
	receipt.EvidenceDigest = certificationEvidenceDigest(canonical, evidence, true)
	return receipt, nil
}

func canonicalCertificationRequest(request CertificationRequest) (CertificationRequest, error) {
	request.CorpusRevision = strings.TrimSpace(request.CorpusRevision)
	request.ReferencePlacement = strings.TrimSpace(request.ReferencePlacement)
	request.CandidatePlacement = strings.TrimSpace(request.CandidatePlacement)
	if request.CorpusRevision == "" || request.ReferencePlacement == "" ||
		request.CandidatePlacement == "" || len(request.Cases) == 0 {
		return CertificationRequest{}, errors.New("H3 certification request is incomplete")
	}
	if _, err := uuid.Parse(request.ExecutionGraphRevisionID); err != nil {
		return CertificationRequest{}, errors.New("H3 certification graph identity is invalid")
	}
	profiles, err := canonicalUUIDs(request.StageProfileRevisionIDs)
	if err != nil || len(profiles) != 3 {
		return CertificationRequest{}, errors.New("H3 certification requires three exact StageProfile revisions")
	}
	connectors, err := canonicalUUIDs(request.ConnectorRevisionIDs)
	if err != nil || len(connectors) == 0 {
		return CertificationRequest{}, errors.New("H3 certification connector revisions are invalid")
	}
	cases := slices.Clone(request.Cases)
	seenCases := make(map[string]struct{}, len(cases))
	for _, testCase := range cases {
		if strings.TrimSpace(testCase.ID) == "" || testCase.InputDigest == ([sha256.Size]byte{}) ||
			duplicate(seenCases, testCase.ID) {
			return CertificationRequest{}, errors.New("H3 certification corpus case is invalid")
		}
	}
	slices.SortFunc(cases, func(left, right CorpusCase) int {
		return strings.Compare(left.ID, right.ID)
	})
	request.StageProfileRevisionIDs = profiles
	request.ConnectorRevisionIDs = connectors
	request.Cases = cases
	return request, nil
}

func canonicalUUIDs(values []string) ([]string, error) {
	canonical := slices.Clone(values)
	seen := make(map[string]struct{}, len(canonical))
	for index, value := range canonical {
		parsed, err := uuid.Parse(value)
		if err != nil {
			return nil, err
		}
		canonical[index] = parsed.String()
		if duplicate(seen, canonical[index]) {
			return nil, errors.New("duplicated revision identity")
		}
	}
	slices.Sort(canonical)
	return canonical, nil
}

func validateCorpusOutput(output CorpusOutput) error {
	if len(output.ExactBytes) == 0 || output.Digest == ([sha256.Size]byte{}) {
		return errors.New("certified corpus output is empty")
	}
	digest := sha256.Sum256(output.ExactBytes)
	if digest != output.Digest {
		return errors.New("certified corpus output digest is inconsistent")
	}
	return nil
}

func certificationEvidenceDigest(
	request CertificationRequest,
	cases []caseEvidence,
	matched bool,
) [sha256.Size]byte {
	payload, err := json.Marshal(struct {
		SchemaVersion            int            `json:"schema_version"`
		CorpusRevision           string         `json:"corpus_revision"`
		ExecutionGraphRevisionID string         `json:"execution_graph_revision_id"`
		StageProfileRevisionIDs  []string       `json:"stage_profile_revision_ids"`
		ConnectorRevisionIDs     []string       `json:"connector_revision_ids"`
		ReferencePlacement       string         `json:"reference_placement"`
		CandidatePlacement       string         `json:"candidate_placement"`
		Matched                  bool           `json:"matched"`
		Cases                    []caseEvidence `json:"cases"`
	}{
		SchemaVersion: 1, CorpusRevision: request.CorpusRevision,
		ExecutionGraphRevisionID: request.ExecutionGraphRevisionID,
		StageProfileRevisionIDs:  request.StageProfileRevisionIDs,
		ConnectorRevisionIDs:     request.ConnectorRevisionIDs,
		ReferencePlacement:       request.ReferencePlacement,
		CandidatePlacement:       request.CandidatePlacement,
		Matched:                  matched, Cases: cases,
	})
	if err != nil {
		return [sha256.Size]byte{}
	}
	return sha256.Sum256(payload)
}
