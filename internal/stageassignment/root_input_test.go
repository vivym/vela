package stageassignment

import (
	"bytes"
	"testing"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestValidateExecutionSpecRequiresCanonicalRootInputOrder(t *testing.T) {
	spec := &velav1.StageExecutionSpec{RootInputs: []*velav1.StageRootInputMaterial{
		{ConditionIndex: 1, Uri: "vela://input/one", Sha256: bytes.Repeat([]byte{1}, 32), SizeBytes: 1},
		{ConditionIndex: 0, Uri: "vela://input/zero", Sha256: bytes.Repeat([]byte{2}, 32), SizeBytes: 2},
	}}
	if err := ValidateExecutionSpec(spec); err == nil {
		t.Fatal("ValidateExecutionSpec accepted out-of-order root input indexes")
	}
	spec.RootInputs[0].ConditionIndex = 0
	spec.RootInputs[1].ConditionIndex = 1
	if err := ValidateExecutionSpec(spec); err != nil {
		t.Fatalf("ValidateExecutionSpec: %v", err)
	}
}

func TestRootInputFetchesMustMatchExactMaterialWithoutUserInfo(t *testing.T) {
	digest := bytes.Repeat([]byte{3}, 32)
	assignment := &velav1.StageAssignment{ExecutionSpec: &velav1.StageExecutionSpec{
		RootInputs: []*velav1.StageRootInputMaterial{{
			ConditionIndex: 0, Uri: "vela://input/reference", Sha256: digest, SizeBytes: 4096,
		}},
	}}
	if err := validateRootInputFetches(assignment); err == nil {
		t.Fatal("root input material without fetch URL was accepted")
	}
	assignment.RootInputFetches = []*velav1.StageRootInputFetch{{
		ConditionIndex: 0, Sha256: digest,
		DownloadUrl: "https://objects.example.test/reference?signature=secret",
	}}
	if err := validateRootInputFetches(assignment); err != nil {
		t.Fatalf("validateRootInputFetches: %v", err)
	}
	assignment.RootInputFetches[0].DownloadUrl =
		"https://user:password@objects.example.test/reference"
	if err := validateRootInputFetches(assignment); err == nil {
		t.Fatal("root input fetch URL with userinfo was accepted")
	}
}
