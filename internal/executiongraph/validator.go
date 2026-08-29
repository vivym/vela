package executiongraph

import (
	"container/heap"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
)

const (
	SchemaVersionV1 = 1
	MaxFanOutV1     = 64
)

type ReasonCode string

const (
	ReasonCycle                    ReasonCode = "CYCLE"
	ReasonSelfEdge                 ReasonCode = "SELF_EDGE"
	ReasonDuplicateStageKey        ReasonCode = "DUPLICATE_STAGE_KEY"
	ReasonMissingRequiredInput     ReasonCode = "MISSING_REQUIRED_INPUT"
	ReasonMissingRequiredOutput    ReasonCode = "MISSING_REQUIRED_OUTPUT"
	ReasonIncompatibleInterface    ReasonCode = "INCOMPATIBLE_INTERFACE"
	ReasonUnknownInterface         ReasonCode = "UNKNOWN_INTERFACE"
	ReasonUnboundedFanOut          ReasonCode = "UNBOUNDED_FAN_OUT"
	ReasonFanOutExceeded           ReasonCode = "FAN_OUT_EXCEEDED"
	ReasonIncompleteCertification  ReasonCode = "INCOMPLETE_CERTIFICATION"
	ReasonConnectorFallbackMissing ReasonCode = "CONNECTOR_FALLBACK_MISSING"
)

type ValidationError struct {
	Reason ReasonCode
	Detail string
}

func (err *ValidationError) Error() string {
	return fmt.Sprintf("execution graph validation failed [%s]: %s", err.Reason, err.Detail)
}

type GraphRevision struct {
	SchemaVersion int
	StableID      string
	Revision      int
	Interfaces    []InterfaceRevision
	Stages        []Stage
	Edges         []Edge
	Inputs        []GraphInput
	Outputs       []GraphOutput
}

type InterfaceRevision struct {
	ID            string
	PayloadKind   string
	DType         string
	Layout        string
	Serialization string
	MaxBytes      int64
}

type Stage struct {
	Key          string
	DefinitionID string
	Required     bool
	MaxFanOut    int
	Inputs       []Port
	Outputs      []Port
	Profiles     []ProfileOption
}

type Port struct {
	Name        string
	InterfaceID string
	Required    bool
}

type ProfileOption struct {
	ID          string
	Certified   bool
	DeviceCount int
}

type Edge struct {
	SourceStage      string
	SourcePort       string
	DestinationStage string
	DestinationPort  string
	Connectors       []ConnectorOption
}

type ConnectorOption struct {
	ID              string
	Certified       bool
	DurableFallback bool
}

type GraphInput struct {
	Key              string
	InterfaceID      string
	DestinationStage string
	DestinationPort  string
}

type GraphOutput struct {
	Key         string
	InterfaceID string
	SourceStage string
	SourcePort  string
	Required    bool
}

type ValidatedGraph struct {
	TopologicalOrder []string
	ContentDigest    string
}

func ValidateExecutionGraph(graph GraphRevision) (ValidatedGraph, error) {
	if graph.SchemaVersion != SchemaVersionV1 {
		return ValidatedGraph{}, fmt.Errorf("unsupported execution graph schema version %d", graph.SchemaVersion)
	}
	if graph.StableID == "" || graph.Revision <= 0 || len(graph.Stages) == 0 {
		return ValidatedGraph{}, fmt.Errorf("execution graph identity and stages are required")
	}

	interfaces := make(map[string]struct{}, len(graph.Interfaces))
	for _, contract := range graph.Interfaces {
		if contract.ID == "" || contract.PayloadKind == "" || contract.Serialization == "" || contract.MaxBytes <= 0 {
			return ValidatedGraph{}, fmt.Errorf("execution graph interface contract is incomplete")
		}
		if _, exists := interfaces[contract.ID]; exists {
			return ValidatedGraph{}, fmt.Errorf("duplicate interface revision %q", contract.ID)
		}
		interfaces[contract.ID] = struct{}{}
	}

	stages := make(map[string]Stage, len(graph.Stages))
	indegree := make(map[string]int, len(graph.Stages))
	adjacency := make(map[string][]string, len(graph.Stages))
	outgoingCount := make(map[string]int, len(graph.Stages))
	for _, stage := range graph.Stages {
		if stage.Key == "" {
			return ValidatedGraph{}, fmt.Errorf("execution graph stage key is required")
		}
		if stage.MaxFanOut <= 0 || stage.MaxFanOut > MaxFanOutV1 {
			return ValidatedGraph{}, &ValidationError{
				Reason: ReasonUnboundedFanOut,
				Detail: fmt.Sprintf(
					"stage %q fan-out bound %d is outside [1,%d]",
					stage.Key,
					stage.MaxFanOut,
					MaxFanOutV1,
				),
			}
		}
		certifiedProfile := false
		for _, profile := range stage.Profiles {
			if profile.Certified && profile.ID != "" && profile.DeviceCount > 0 {
				certifiedProfile = true
				break
			}
		}
		if !certifiedProfile {
			return ValidatedGraph{}, &ValidationError{
				Reason: ReasonIncompleteCertification,
				Detail: fmt.Sprintf("stage %q has no complete certified profile option", stage.Key),
			}
		}
		for _, port := range append(slices.Clone(stage.Inputs), stage.Outputs...) {
			if _, exists := interfaces[port.InterfaceID]; !exists {
				return ValidatedGraph{}, &ValidationError{
					Reason: ReasonUnknownInterface,
					Detail: fmt.Sprintf(
						"port %q on stage %q references unknown interface %q",
						port.Name,
						stage.Key,
						port.InterfaceID,
					),
				}
			}
		}
		if _, exists := stages[stage.Key]; exists {
			return ValidatedGraph{}, &ValidationError{
				Reason: ReasonDuplicateStageKey,
				Detail: fmt.Sprintf("stage key %q appears more than once", stage.Key),
			}
		}
		stages[stage.Key] = stage
		indegree[stage.Key] = 0
	}
	for _, input := range graph.Inputs {
		stage, exists := stages[input.DestinationStage]
		if !exists {
			return ValidatedGraph{}, fmt.Errorf("graph input %q references unknown stage %q", input.Key, input.DestinationStage)
		}
		port, exists := findPort(stage.Inputs, input.DestinationPort)
		if !exists {
			return ValidatedGraph{}, fmt.Errorf("graph input %q references unknown port %q", input.Key, input.DestinationPort)
		}
		if port.InterfaceID != input.InterfaceID {
			return ValidatedGraph{}, &ValidationError{
				Reason: ReasonIncompatibleInterface,
				Detail: fmt.Sprintf(
					"graph input %q (%s) is incompatible with %s.%s (%s)",
					input.Key,
					input.InterfaceID,
					input.DestinationStage,
					input.DestinationPort,
					port.InterfaceID,
				),
			}
		}
	}
	for _, output := range graph.Outputs {
		stage, exists := stages[output.SourceStage]
		if !exists {
			return ValidatedGraph{}, fmt.Errorf("graph output %q references unknown stage %q", output.Key, output.SourceStage)
		}
		port, exists := findPort(stage.Outputs, output.SourcePort)
		if !exists {
			return ValidatedGraph{}, fmt.Errorf("graph output %q references unknown port %q", output.Key, output.SourcePort)
		}
		if port.InterfaceID != output.InterfaceID {
			return ValidatedGraph{}, &ValidationError{
				Reason: ReasonIncompatibleInterface,
				Detail: fmt.Sprintf(
					"graph output %q (%s) is incompatible with %s.%s (%s)",
					output.Key,
					output.InterfaceID,
					output.SourceStage,
					output.SourcePort,
					port.InterfaceID,
				),
			}
		}
	}
	for _, edge := range graph.Edges {
		if edge.SourceStage == edge.DestinationStage {
			return ValidatedGraph{}, &ValidationError{
				Reason: ReasonSelfEdge,
				Detail: fmt.Sprintf("stage %q cannot depend on itself", edge.SourceStage),
			}
		}
		sourceStage, exists := stages[edge.SourceStage]
		if !exists {
			return ValidatedGraph{}, fmt.Errorf("edge source stage %q does not exist", edge.SourceStage)
		}
		destinationStage, exists := stages[edge.DestinationStage]
		if !exists {
			return ValidatedGraph{}, fmt.Errorf("edge destination stage %q does not exist", edge.DestinationStage)
		}
		sourcePort, sourceExists := findPort(sourceStage.Outputs, edge.SourcePort)
		destinationPort, destinationExists := findPort(destinationStage.Inputs, edge.DestinationPort)
		if !sourceExists || !destinationExists {
			return ValidatedGraph{}, fmt.Errorf(
				"edge %s.%s -> %s.%s references an unknown port",
				edge.SourceStage,
				edge.SourcePort,
				edge.DestinationStage,
				edge.DestinationPort,
			)
		}
		if sourcePort.InterfaceID != destinationPort.InterfaceID {
			return ValidatedGraph{}, &ValidationError{
				Reason: ReasonIncompatibleInterface,
				Detail: fmt.Sprintf(
					"edge %s.%s (%s) -> %s.%s (%s) has incompatible interfaces",
					edge.SourceStage,
					edge.SourcePort,
					sourcePort.InterfaceID,
					edge.DestinationStage,
					edge.DestinationPort,
					destinationPort.InterfaceID,
				),
			}
		}
		certifiedFallback := false
		for _, connector := range edge.Connectors {
			if connector.ID != "" && connector.Certified && connector.DurableFallback {
				certifiedFallback = true
				break
			}
		}
		if !certifiedFallback {
			return ValidatedGraph{}, &ValidationError{
				Reason: ReasonConnectorFallbackMissing,
				Detail: fmt.Sprintf(
					"edge %s.%s -> %s.%s has no certified durable fallback connector",
					edge.SourceStage,
					edge.SourcePort,
					edge.DestinationStage,
					edge.DestinationPort,
				),
			}
		}
		adjacency[edge.SourceStage] = append(adjacency[edge.SourceStage], edge.DestinationStage)
		outgoingCount[edge.SourceStage]++
		indegree[edge.DestinationStage]++
	}
	for _, stage := range graph.Stages {
		if outgoingCount[stage.Key] > stage.MaxFanOut {
			return ValidatedGraph{}, &ValidationError{
				Reason: ReasonFanOutExceeded,
				Detail: fmt.Sprintf(
					"stage %q fan-out %d exceeds declared bound %d",
					stage.Key,
					outgoingCount[stage.Key],
					stage.MaxFanOut,
				),
			}
		}
	}
	consumedOutputs := make(map[string]struct{}, len(graph.Edges)+len(graph.Outputs))
	for _, edge := range graph.Edges {
		consumedOutputs[portIdentity(edge.SourceStage, edge.SourcePort)] = struct{}{}
	}
	for _, output := range graph.Outputs {
		consumedOutputs[portIdentity(output.SourceStage, output.SourcePort)] = struct{}{}
	}
	for _, stage := range graph.Stages {
		providedInputs := make(map[string]struct{}, len(graph.Edges)+len(graph.Inputs))
		for _, edge := range graph.Edges {
			providedInputs[portIdentity(edge.DestinationStage, edge.DestinationPort)] = struct{}{}
		}
		for _, input := range graph.Inputs {
			providedInputs[portIdentity(input.DestinationStage, input.DestinationPort)] = struct{}{}
		}
		for _, input := range stage.Inputs {
			if !input.Required {
				continue
			}
			if _, provided := providedInputs[portIdentity(stage.Key, input.Name)]; !provided {
				return ValidatedGraph{}, &ValidationError{
					Reason: ReasonMissingRequiredInput,
					Detail: fmt.Sprintf("required input %q on stage %q has no graph path", input.Name, stage.Key),
				}
			}
		}
		for _, output := range stage.Outputs {
			if !output.Required {
				continue
			}
			if _, consumed := consumedOutputs[portIdentity(stage.Key, output.Name)]; !consumed {
				return ValidatedGraph{}, &ValidationError{
					Reason: ReasonMissingRequiredOutput,
					Detail: fmt.Sprintf("required output %q on stage %q has no graph path", output.Name, stage.Key),
				}
			}
		}
	}

	ready := &stringHeap{}
	heap.Init(ready)
	for stageKey, degree := range indegree {
		if degree == 0 {
			heap.Push(ready, stageKey)
		}
	}
	order := make([]string, 0, len(graph.Stages))
	for ready.Len() > 0 {
		stageKey := heap.Pop(ready).(string)
		order = append(order, stageKey)
		slices.Sort(adjacency[stageKey])
		for _, destination := range adjacency[stageKey] {
			indegree[destination]--
			if indegree[destination] == 0 {
				heap.Push(ready, destination)
			}
		}
	}
	if len(order) != len(graph.Stages) {
		return ValidatedGraph{}, &ValidationError{
			Reason: ReasonCycle,
			Detail: "graph contains a cycle",
		}
	}

	canonical := canonicalGraph(graph)
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return ValidatedGraph{}, fmt.Errorf("encode canonical execution graph: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return ValidatedGraph{
		TopologicalOrder: order,
		ContentDigest:    hex.EncodeToString(digest[:]),
	}, nil
}

func canonicalGraph(graph GraphRevision) GraphRevision {
	canonical := graph
	canonical.Interfaces = slices.Clone(graph.Interfaces)
	canonical.Stages = slices.Clone(graph.Stages)
	canonical.Edges = slices.Clone(graph.Edges)
	canonical.Inputs = slices.Clone(graph.Inputs)
	canonical.Outputs = slices.Clone(graph.Outputs)
	slices.SortFunc(canonical.Interfaces, func(left, right InterfaceRevision) int {
		return compareStrings(left.ID, right.ID)
	})
	for index := range canonical.Stages {
		canonical.Stages[index].Inputs = slices.Clone(canonical.Stages[index].Inputs)
		canonical.Stages[index].Outputs = slices.Clone(canonical.Stages[index].Outputs)
		canonical.Stages[index].Profiles = slices.Clone(canonical.Stages[index].Profiles)
		slices.SortFunc(canonical.Stages[index].Inputs, comparePort)
		slices.SortFunc(canonical.Stages[index].Outputs, comparePort)
		slices.SortFunc(canonical.Stages[index].Profiles, func(left, right ProfileOption) int {
			return compareStrings(left.ID, right.ID)
		})
	}
	slices.SortFunc(canonical.Stages, func(left, right Stage) int {
		return compareStrings(left.Key, right.Key)
	})
	for index := range canonical.Edges {
		canonical.Edges[index].Connectors = slices.Clone(canonical.Edges[index].Connectors)
		slices.SortFunc(canonical.Edges[index].Connectors, func(left, right ConnectorOption) int {
			return compareStrings(left.ID, right.ID)
		})
	}
	slices.SortFunc(canonical.Edges, compareEdge)
	slices.SortFunc(canonical.Inputs, func(left, right GraphInput) int {
		return compareStrings(left.Key, right.Key)
	})
	slices.SortFunc(canonical.Outputs, func(left, right GraphOutput) int {
		return compareStrings(left.Key, right.Key)
	})
	return canonical
}

func comparePort(left, right Port) int {
	return compareStrings(left.Name, right.Name)
}

func portIdentity(stageKey, portName string) string {
	return stageKey + "\x00" + portName
}

func findPort(ports []Port, name string) (Port, bool) {
	for _, port := range ports {
		if port.Name == name {
			return port, true
		}
	}
	return Port{}, false
}

func compareEdge(left, right Edge) int {
	for _, pair := range [][2]string{
		{left.SourceStage, right.SourceStage},
		{left.SourcePort, right.SourcePort},
		{left.DestinationStage, right.DestinationStage},
		{left.DestinationPort, right.DestinationPort},
	} {
		if comparison := compareStrings(pair[0], pair[1]); comparison != 0 {
			return comparison
		}
	}
	return 0
}

func compareStrings(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

type stringHeap []string

func (values stringHeap) Len() int           { return len(values) }
func (values stringHeap) Less(i, j int) bool { return values[i] < values[j] }
func (values stringHeap) Swap(i, j int)      { values[i], values[j] = values[j], values[i] }

func (values *stringHeap) Push(value any) {
	*values = append(*values, value.(string))
}

func (values *stringHeap) Pop() any {
	old := *values
	last := old[len(old)-1]
	*values = old[:len(old)-1]
	return last
}
