package executiongraph_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/rand"
	"slices"
	"testing"
	"testing/quick"

	"github.com/vivym/vela/internal/executiongraph"
)

func TestValidateExecutionGraphAcceptsH3AndReturnsCanonicalIdentity(t *testing.T) {
	t.Parallel()

	graph := h3Graph()
	validated, err := executiongraph.ValidateExecutionGraph(graph)
	if err != nil {
		t.Fatalf("validate H3 execution graph: %v", err)
	}
	wantOrder := []string{"encoder", "dit", "vae"}
	if !slices.Equal(validated.TopologicalOrder, wantOrder) {
		t.Fatalf("topological order = %v, want %v", validated.TopologicalOrder, wantOrder)
	}
	if validated.ContentDigest == "" {
		t.Fatal("content digest is empty")
	}

	reordered := h3Graph()
	slices.Reverse(reordered.Interfaces)
	slices.Reverse(reordered.Stages)
	slices.Reverse(reordered.Edges)
	slices.Reverse(reordered.Inputs)
	slices.Reverse(reordered.Outputs)
	reordered.Stages[0].Profiles = slices.Clone(reordered.Stages[0].Profiles)
	slices.Reverse(reordered.Stages[0].Profiles)
	second, err := executiongraph.ValidateExecutionGraph(reordered)
	if err != nil {
		t.Fatalf("validate reordered H3 execution graph: %v", err)
	}
	if second.ContentDigest != validated.ContentDigest {
		t.Fatalf("reordered content digest = %q, want %q", second.ContentDigest, validated.ContentDigest)
	}
	if !slices.Equal(second.TopologicalOrder, wantOrder) {
		t.Fatalf("reordered topological order = %v, want %v", second.TopologicalOrder, wantOrder)
	}
}

func TestValidateExecutionGraphCanonicalIdentityProperty(t *testing.T) {
	t.Parallel()

	baseline, err := executiongraph.ValidateExecutionGraph(h3Graph())
	if err != nil {
		t.Fatalf("validate baseline H3 graph: %v", err)
	}
	property := func(seed uint64) bool {
		graph := h3Graph()
		random := rand.New(rand.NewSource(int64(seed)))
		random.Shuffle(len(graph.Interfaces), func(i, j int) {
			graph.Interfaces[i], graph.Interfaces[j] = graph.Interfaces[j], graph.Interfaces[i]
		})
		random.Shuffle(len(graph.Stages), func(i, j int) {
			graph.Stages[i], graph.Stages[j] = graph.Stages[j], graph.Stages[i]
		})
		for index := range graph.Stages {
			random.Shuffle(len(graph.Stages[index].Inputs), func(i, j int) {
				graph.Stages[index].Inputs[i], graph.Stages[index].Inputs[j] = graph.Stages[index].Inputs[j], graph.Stages[index].Inputs[i]
			})
			random.Shuffle(len(graph.Stages[index].Outputs), func(i, j int) {
				graph.Stages[index].Outputs[i], graph.Stages[index].Outputs[j] = graph.Stages[index].Outputs[j], graph.Stages[index].Outputs[i]
			})
			random.Shuffle(len(graph.Stages[index].Profiles), func(i, j int) {
				graph.Stages[index].Profiles[i], graph.Stages[index].Profiles[j] = graph.Stages[index].Profiles[j], graph.Stages[index].Profiles[i]
			})
		}
		random.Shuffle(len(graph.Edges), func(i, j int) {
			graph.Edges[i], graph.Edges[j] = graph.Edges[j], graph.Edges[i]
		})
		random.Shuffle(len(graph.Inputs), func(i, j int) {
			graph.Inputs[i], graph.Inputs[j] = graph.Inputs[j], graph.Inputs[i]
		})
		random.Shuffle(len(graph.Outputs), func(i, j int) {
			graph.Outputs[i], graph.Outputs[j] = graph.Outputs[j], graph.Outputs[i]
		})
		validated, validationErr := executiongraph.ValidateExecutionGraph(graph)
		return validationErr == nil &&
			validated.ContentDigest == baseline.ContentDigest &&
			slices.Equal(validated.TopologicalOrder, baseline.TopologicalOrder)
	}
	if err := quick.Check(property, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatalf("canonical identity property: %v", err)
	}
}

func TestValidateExecutionGraphDoesNotMutateInput(t *testing.T) {
	t.Parallel()

	graph := h3Graph()
	graph.Stages[0].Profiles = append(graph.Stages[0].Profiles,
		executiongraph.ProfileOption{ID: "h3-encoder-canary-v1", Certified: true, DeviceCount: 1},
	)
	before, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("encode graph before validation: %v", err)
	}
	if _, err := executiongraph.ValidateExecutionGraph(graph); err != nil {
		t.Fatalf("validate H3 execution graph: %v", err)
	}
	after, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("encode graph after validation: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("ValidateExecutionGraph mutated input\nbefore: %s\nafter:  %s", before, after)
	}
}

func TestValidateExecutionGraphRejectsCycleWithBoundedReason(t *testing.T) {
	t.Parallel()

	graph := h3Graph()
	graph.Stages[0].Inputs[0].InterfaceID = "video-v1"
	graph.Inputs[0].InterfaceID = "video-v1"
	graph.Edges = append(graph.Edges, executiongraph.Edge{
		SourceStage: "vae", SourcePort: "video", DestinationStage: "encoder", DestinationPort: "request",
		Connectors: []executiongraph.ConnectorOption{{ID: "invalid-cycle-v1", Certified: true, DurableFallback: true}},
	})
	_, err := executiongraph.ValidateExecutionGraph(graph)
	var validationError *executiongraph.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("cycle error = %v, want ValidationError", err)
	}
	if validationError.Reason != executiongraph.ReasonCycle {
		t.Fatalf("cycle reason = %q, want %q", validationError.Reason, executiongraph.ReasonCycle)
	}
}

func TestValidateExecutionGraphRejectsSelfEdgeBeforeCycleDetection(t *testing.T) {
	t.Parallel()

	graph := h3Graph()
	graph.Edges[0].DestinationStage = "encoder"
	graph.Edges[0].DestinationPort = "request"
	_, err := executiongraph.ValidateExecutionGraph(graph)
	var validationError *executiongraph.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("self-edge error = %v, want ValidationError", err)
	}
	if validationError.Reason != executiongraph.ReasonSelfEdge {
		t.Fatalf("self-edge reason = %q, want %q", validationError.Reason, executiongraph.ReasonSelfEdge)
	}
}

func TestValidateExecutionGraphRejectsDuplicateStageKey(t *testing.T) {
	t.Parallel()

	graph := h3Graph()
	graph.Stages = append(graph.Stages, graph.Stages[0])
	_, err := executiongraph.ValidateExecutionGraph(graph)
	var validationError *executiongraph.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("duplicate-stage error = %v, want ValidationError", err)
	}
	if validationError.Reason != executiongraph.ReasonDuplicateStageKey {
		t.Fatalf("duplicate-stage reason = %q, want %q", validationError.Reason, executiongraph.ReasonDuplicateStageKey)
	}
}

func TestValidateExecutionGraphRejectsDuplicateLogicalIdentity(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		mutate func(*executiongraph.GraphRevision)
	}{
		{
			name: "interface",
			mutate: func(graph *executiongraph.GraphRevision) {
				graph.Interfaces = append(graph.Interfaces, graph.Interfaces[0])
			},
		},
		{
			name: "input_port",
			mutate: func(graph *executiongraph.GraphRevision) {
				graph.Stages[0].Inputs = append(graph.Stages[0].Inputs, graph.Stages[0].Inputs[0])
			},
		},
		{
			name: "output_port",
			mutate: func(graph *executiongraph.GraphRevision) {
				graph.Stages[0].Outputs = append(graph.Stages[0].Outputs, graph.Stages[0].Outputs[0])
			},
		},
		{
			name: "profile_option",
			mutate: func(graph *executiongraph.GraphRevision) {
				graph.Stages[0].Profiles = append(graph.Stages[0].Profiles, graph.Stages[0].Profiles[0])
			},
		},
		{
			name: "edge",
			mutate: func(graph *executiongraph.GraphRevision) {
				graph.Edges = append(graph.Edges, graph.Edges[0])
			},
		},
		{
			name: "connector_option",
			mutate: func(graph *executiongraph.GraphRevision) {
				graph.Edges[0].Connectors = append(graph.Edges[0].Connectors, graph.Edges[0].Connectors[0])
			},
		},
		{
			name: "graph_input_key",
			mutate: func(graph *executiongraph.GraphRevision) {
				graph.Inputs = append(graph.Inputs, graph.Inputs[0])
			},
		},
		{
			name: "graph_input_destination",
			mutate: func(graph *executiongraph.GraphRevision) {
				duplicate := graph.Inputs[0]
				duplicate.Key = "second-request"
				graph.Inputs = append(graph.Inputs, duplicate)
			},
		},
		{
			name: "graph_output_key",
			mutate: func(graph *executiongraph.GraphRevision) {
				graph.Outputs = append(graph.Outputs, graph.Outputs[0])
			},
		},
		{
			name: "graph_output_source",
			mutate: func(graph *executiongraph.GraphRevision) {
				duplicate := graph.Outputs[0]
				duplicate.Key = "second-video"
				graph.Outputs = append(graph.Outputs, duplicate)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			graph := h3Graph()
			test.mutate(&graph)
			_, err := executiongraph.ValidateExecutionGraph(graph)
			var validationError *executiongraph.ValidationError
			if !errors.As(err, &validationError) {
				t.Fatalf("duplicate identity error = %v, want ValidationError", err)
			}
			if validationError.Reason != executiongraph.ReasonDuplicateLogicalIdentity {
				t.Fatalf("duplicate identity reason = %q, want %q", validationError.Reason, executiongraph.ReasonDuplicateLogicalIdentity)
			}
		})
	}
}

func TestValidateExecutionGraphReturnsBoundedReasonForMalformedInput(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		mutate func(*executiongraph.GraphRevision)
	}{
		{name: "unsupported_schema", mutate: func(graph *executiongraph.GraphRevision) { graph.SchemaVersion++ }},
		{name: "missing_graph_identity", mutate: func(graph *executiongraph.GraphRevision) { graph.StableID = "" }},
		{name: "missing_stages", mutate: func(graph *executiongraph.GraphRevision) { graph.Stages = nil }},
		{name: "incomplete_interface", mutate: func(graph *executiongraph.GraphRevision) { graph.Interfaces[0].MaxBytes = 0 }},
		{name: "missing_stage_key", mutate: func(graph *executiongraph.GraphRevision) { graph.Stages[0].Key = "" }},
		{name: "input_unknown_stage", mutate: func(graph *executiongraph.GraphRevision) { graph.Inputs[0].DestinationStage = "missing" }},
		{name: "input_unknown_port", mutate: func(graph *executiongraph.GraphRevision) { graph.Inputs[0].DestinationPort = "missing" }},
		{name: "output_unknown_stage", mutate: func(graph *executiongraph.GraphRevision) { graph.Outputs[0].SourceStage = "missing" }},
		{name: "output_unknown_port", mutate: func(graph *executiongraph.GraphRevision) { graph.Outputs[0].SourcePort = "missing" }},
		{name: "edge_unknown_source_stage", mutate: func(graph *executiongraph.GraphRevision) { graph.Edges[0].SourceStage = "missing" }},
		{name: "edge_unknown_destination_stage", mutate: func(graph *executiongraph.GraphRevision) { graph.Edges[0].DestinationStage = "missing" }},
		{name: "edge_unknown_port", mutate: func(graph *executiongraph.GraphRevision) { graph.Edges[0].SourcePort = "missing" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			graph := h3Graph()
			test.mutate(&graph)
			_, err := executiongraph.ValidateExecutionGraph(graph)
			var validationError *executiongraph.ValidationError
			if !errors.As(err, &validationError) {
				t.Fatalf("malformed input error = %v, want ValidationError", err)
			}
			if validationError.Reason != executiongraph.ReasonInvalidGraph {
				t.Fatalf("malformed input reason = %q, want %q", validationError.Reason, executiongraph.ReasonInvalidGraph)
			}
		})
	}
}

func TestValidateExecutionGraphRejectsMissingRequiredOutputPath(t *testing.T) {
	t.Parallel()

	graph := h3Graph()
	graph.Outputs = nil
	_, err := executiongraph.ValidateExecutionGraph(graph)
	var validationError *executiongraph.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("missing-output error = %v, want ValidationError", err)
	}
	if validationError.Reason != executiongraph.ReasonMissingRequiredOutput {
		t.Fatalf("missing-output reason = %q, want %q", validationError.Reason, executiongraph.ReasonMissingRequiredOutput)
	}
}

func TestValidateExecutionGraphRejectsIncompatibleEdgeInterfaces(t *testing.T) {
	t.Parallel()

	graph := h3Graph()
	graph.Stages[1].Inputs[0].InterfaceID = "latent-v1"
	_, err := executiongraph.ValidateExecutionGraph(graph)
	var validationError *executiongraph.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("incompatible-interface error = %v, want ValidationError", err)
	}
	if validationError.Reason != executiongraph.ReasonIncompatibleInterface {
		t.Fatalf("incompatible-interface reason = %q, want %q", validationError.Reason, executiongraph.ReasonIncompatibleInterface)
	}
}

func TestValidateExecutionGraphRejectsUnboundedFanOut(t *testing.T) {
	t.Parallel()

	graph := h3Graph()
	graph.Stages[0].MaxFanOut = 0
	_, err := executiongraph.ValidateExecutionGraph(graph)
	var validationError *executiongraph.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("unbounded-fan-out error = %v, want ValidationError", err)
	}
	if validationError.Reason != executiongraph.ReasonUnboundedFanOut {
		t.Fatalf("unbounded-fan-out reason = %q, want %q", validationError.Reason, executiongraph.ReasonUnboundedFanOut)
	}
}

func TestValidateExecutionGraphRejectsStageWithoutCertifiedProfile(t *testing.T) {
	t.Parallel()

	graph := h3Graph()
	graph.Stages[1].Profiles[0].Certified = false
	_, err := executiongraph.ValidateExecutionGraph(graph)
	var validationError *executiongraph.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("incomplete-certification error = %v, want ValidationError", err)
	}
	if validationError.Reason != executiongraph.ReasonIncompleteCertification {
		t.Fatalf("incomplete-certification reason = %q, want %q", validationError.Reason, executiongraph.ReasonIncompleteCertification)
	}
}

func TestValidateExecutionGraphRejectsEdgeWithoutCertifiedDurableFallback(t *testing.T) {
	t.Parallel()

	graph := h3Graph()
	graph.Edges[0].Connectors[0].DurableFallback = false
	_, err := executiongraph.ValidateExecutionGraph(graph)
	var validationError *executiongraph.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("connector-fallback error = %v, want ValidationError", err)
	}
	if validationError.Reason != executiongraph.ReasonConnectorFallbackMissing {
		t.Fatalf("connector-fallback reason = %q, want %q", validationError.Reason, executiongraph.ReasonConnectorFallbackMissing)
	}
}

func TestValidateExecutionGraphRejectsMissingRequiredInputPath(t *testing.T) {
	t.Parallel()

	graph := h3Graph()
	graph.Inputs = nil
	_, err := executiongraph.ValidateExecutionGraph(graph)
	var validationError *executiongraph.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("missing-input error = %v, want ValidationError", err)
	}
	if validationError.Reason != executiongraph.ReasonMissingRequiredInput {
		t.Fatalf("missing-input reason = %q, want %q", validationError.Reason, executiongraph.ReasonMissingRequiredInput)
	}
}

func TestValidateExecutionGraphRejectsFanOutAboveDeclaredBound(t *testing.T) {
	t.Parallel()

	graph := h3Graph()
	secondConsumer := graph.Stages[1]
	secondConsumer.Key = "dit-shadow"
	secondConsumer.Outputs[0].Required = false
	graph.Stages = append(graph.Stages, secondConsumer)
	graph.Edges = append(graph.Edges, executiongraph.Edge{
		SourceStage: "encoder", SourcePort: "conditioning", DestinationStage: "dit-shadow", DestinationPort: "conditioning",
		Connectors: []executiongraph.ConnectorOption{{ID: "l2-conditioning-shadow-v1", Certified: true, DurableFallback: true}},
	})
	_, err := executiongraph.ValidateExecutionGraph(graph)
	var validationError *executiongraph.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("fan-out error = %v, want ValidationError", err)
	}
	if validationError.Reason != executiongraph.ReasonFanOutExceeded {
		t.Fatalf("fan-out reason = %q, want %q", validationError.Reason, executiongraph.ReasonFanOutExceeded)
	}
}

func TestValidateExecutionGraphRejectsUnknownInterfaceRevision(t *testing.T) {
	t.Parallel()

	graph := h3Graph()
	graph.Interfaces = slices.DeleteFunc(graph.Interfaces, func(contract executiongraph.InterfaceRevision) bool {
		return contract.ID == "conditioning-v1"
	})
	_, err := executiongraph.ValidateExecutionGraph(graph)
	var validationError *executiongraph.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("unknown-interface error = %v, want ValidationError", err)
	}
	if validationError.Reason != executiongraph.ReasonUnknownInterface {
		t.Fatalf("unknown-interface reason = %q, want %q", validationError.Reason, executiongraph.ReasonUnknownInterface)
	}
}

func TestValidateExecutionGraphRejectsIncompatibleGraphBoundaryInterface(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		mutate func(*executiongraph.GraphRevision)
	}{
		{
			name: "input",
			mutate: func(graph *executiongraph.GraphRevision) {
				graph.Inputs[0].InterfaceID = "video-v1"
			},
		},
		{
			name: "output",
			mutate: func(graph *executiongraph.GraphRevision) {
				graph.Outputs[0].InterfaceID = "latent-v1"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			graph := h3Graph()
			test.mutate(&graph)
			_, err := executiongraph.ValidateExecutionGraph(graph)
			var validationError *executiongraph.ValidationError
			if !errors.As(err, &validationError) {
				t.Fatalf("boundary-interface error = %v, want ValidationError", err)
			}
			if validationError.Reason != executiongraph.ReasonIncompatibleInterface {
				t.Fatalf("boundary-interface reason = %q, want %q", validationError.Reason, executiongraph.ReasonIncompatibleInterface)
			}
		})
	}
}

func h3Graph() executiongraph.GraphRevision {
	return executiongraph.GraphRevision{
		SchemaVersion: 1,
		StableID:      "minimax-h3",
		Revision:      1,
		Interfaces: []executiongraph.InterfaceRevision{
			{ID: "request-v1", PayloadKind: "request", Serialization: "json", MaxBytes: 1 << 20},
			{ID: "conditioning-v1", PayloadKind: "tensor", DType: "bf16", Layout: "h3-conditioning", Serialization: "safetensors", MaxBytes: 64 << 20},
			{ID: "latent-v1", PayloadKind: "tensor", DType: "bf16", Layout: "h3-latent", Serialization: "safetensors", MaxBytes: 1 << 30},
			{ID: "video-v1", PayloadKind: "video", Layout: "frames-rgb", Serialization: "frame-bundle", MaxBytes: 8 << 30},
		},
		Stages: []executiongraph.Stage{
			{
				Key: "encoder", DefinitionID: "h3-encoder-v1", Required: true, MaxFanOut: 1,
				Inputs:   []executiongraph.Port{{Name: "request", InterfaceID: "request-v1", Required: true}},
				Outputs:  []executiongraph.Port{{Name: "conditioning", InterfaceID: "conditioning-v1", Required: true}},
				Profiles: []executiongraph.ProfileOption{{ID: "h3-encoder-single-gpu-v1", Certified: true, DeviceCount: 1}},
			},
			{
				Key: "dit", DefinitionID: "h3-dit-v1", Required: true, MaxFanOut: 1,
				Inputs:   []executiongraph.Port{{Name: "conditioning", InterfaceID: "conditioning-v1", Required: true}},
				Outputs:  []executiongraph.Port{{Name: "latent", InterfaceID: "latent-v1", Required: true}},
				Profiles: []executiongraph.ProfileOption{{ID: "h3-dit-single-gpu-v1", Certified: true, DeviceCount: 1}},
			},
			{
				Key: "vae", DefinitionID: "h3-vae-v1", Required: true, MaxFanOut: 1,
				Inputs:   []executiongraph.Port{{Name: "latent", InterfaceID: "latent-v1", Required: true}},
				Outputs:  []executiongraph.Port{{Name: "video", InterfaceID: "video-v1", Required: true}},
				Profiles: []executiongraph.ProfileOption{{ID: "h3-vae-single-gpu-v1", Certified: true, DeviceCount: 1}},
			},
		},
		Edges: []executiongraph.Edge{
			{SourceStage: "encoder", SourcePort: "conditioning", DestinationStage: "dit", DestinationPort: "conditioning", Connectors: []executiongraph.ConnectorOption{{ID: "l2-conditioning-v1", Certified: true, DurableFallback: true}}},
			{SourceStage: "dit", SourcePort: "latent", DestinationStage: "vae", DestinationPort: "latent", Connectors: []executiongraph.ConnectorOption{{ID: "l2-latent-v1", Certified: true, DurableFallback: true}}},
		},
		Inputs: []executiongraph.GraphInput{
			{Key: "request", InterfaceID: "request-v1", DestinationStage: "encoder", DestinationPort: "request"},
		},
		Outputs: []executiongraph.GraphOutput{
			{Key: "video", InterfaceID: "video-v1", SourceStage: "vae", SourcePort: "video", Required: true},
		},
	}
}
