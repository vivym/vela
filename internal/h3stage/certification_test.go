package h3stage_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/vivym/vela/internal/h3stage"
)

func TestVerifyPlacementEquivalenceBindsCertifiedCorpusAndExactOutputs(t *testing.T) {
	cases := []h3stage.CorpusCase{
		{ID: "prompt-001", InputDigest: sha256.Sum256([]byte("input-001"))},
		{ID: "prompt-002", InputDigest: sha256.Sum256([]byte("input-002"))},
	}
	reference := corpusRunner{outputs: map[string][]byte{
		"prompt-001": []byte("video-output-001"),
		"prompt-002": []byte("video-output-002"),
	}}
	candidate := corpusRunner{outputs: map[string][]byte{
		"prompt-001": []byte("video-output-001"),
		"prompt-002": []byte("video-output-002"),
	}}

	receipt, err := h3stage.VerifyPlacementEquivalence(
		context.Background(),
		h3stage.CertificationRequest{
			CorpusRevision:           "h3-cert-corpus@sha256:001",
			ExecutionGraphRevisionID: "49000000-0000-0000-0000-000000000001",
			StageProfileRevisionIDs: []string{
				"49000000-0000-0000-0000-000000000040",
				"49000000-0000-0000-0000-000000000041",
				"49000000-0000-0000-0000-000000000042",
			},
			ConnectorRevisionIDs: []string{
				"49000000-0000-0000-0000-000000000050",
				"49000000-0000-0000-0000-000000000051",
			},
			ReferencePlacement: "same-node/h3-node-01",
			CandidatePlacement: "cross-node/encoder-01/dit-09/vae-03",
			Cases:              cases,
		},
		reference,
		candidate,
	)
	if err != nil {
		t.Fatalf("VerifyPlacementEquivalence: %v", err)
	}
	if !receipt.Matched || receipt.CaseCount != 2 || receipt.EvidenceDigest == ([32]byte{}) ||
		receipt.ReferencePlacement != "same-node/h3-node-01" ||
		receipt.CandidatePlacement != "cross-node/encoder-01/dit-09/vae-03" {
		t.Fatalf("certification receipt = %#v", receipt)
	}
}

func TestVerifyPlacementEquivalenceFailsClosedOnOneByteDifference(t *testing.T) {
	request := h3stage.CertificationRequest{
		CorpusRevision:           "h3-cert-corpus@sha256:001",
		ExecutionGraphRevisionID: "49000000-0000-0000-0000-000000000001",
		StageProfileRevisionIDs: []string{
			"49000000-0000-0000-0000-000000000040",
			"49000000-0000-0000-0000-000000000041",
			"49000000-0000-0000-0000-000000000042",
		},
		ConnectorRevisionIDs: []string{"49000000-0000-0000-0000-000000000050"},
		ReferencePlacement:   "same-node/h3-node-01",
		CandidatePlacement:   "cross-node/encoder-01/dit-09/vae-03",
		Cases: []h3stage.CorpusCase{{
			ID: "prompt-001", InputDigest: sha256.Sum256([]byte("input-001")),
		}},
	}
	_, err := h3stage.VerifyPlacementEquivalence(
		context.Background(), request,
		corpusRunner{outputs: map[string][]byte{"prompt-001": []byte("video-output-001")}},
		corpusRunner{outputs: map[string][]byte{"prompt-001": []byte("video-output-002")}},
	)
	if !errors.Is(err, h3stage.ErrOutputNotEquivalent) {
		t.Fatalf("one-byte mismatch error = %v", err)
	}
}

type corpusRunner struct {
	outputs map[string][]byte
}

func (runner corpusRunner) Execute(
	_ context.Context,
	testCase h3stage.CorpusCase,
) (h3stage.CorpusOutput, error) {
	payload, ok := runner.outputs[testCase.ID]
	if !ok {
		return h3stage.CorpusOutput{}, errors.New("missing corpus output")
	}
	return h3stage.CorpusOutput{
		ExactBytes: append([]byte(nil), payload...),
		Digest:     sha256.Sum256(payload),
	}, nil
}
