package stageprotocol_test

import (
	"slices"
	"strings"
	"testing"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func TestStageWorkerControlProtocolIsIndependentAndClosed(t *testing.T) {
	// Keep the generated vela.v1 file descriptors linked into the test binary.
	_ = (&velav1.ConnectRequest{}).ProtoReflect()

	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(
		"vela.v1.StageWorkerControlService",
	)
	if err != nil {
		t.Fatalf("StageWorkerControlService descriptor: %v", err)
	}
	service, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		t.Fatalf("StageWorkerControlService descriptor type = %T", descriptor)
	}
	connect := service.Methods().ByName("Connect")
	if connect == nil || !connect.IsStreamingClient() || !connect.IsStreamingServer() {
		t.Fatalf("StageWorkerControlService.Connect = %#v", connect)
	}
	assertOneofFields(t, connect.Input(), "operation", []protoreflect.Name{
		"register_worker_evidence",
		"report_capacity_observation",
		"acquire_stage",
		"start_stage",
		"heartbeat_stage",
		"seal_stage_output",
		"commit_stage_materialization",
		"fail_stage",
		"reattach_stage",
		"report_materialization_source_lost",
		"resolve_input_transfer",
		"consume_input_transfer",
	})
	assertOneofFields(t, connect.Output(), "result", []protoreflect.Name{
		"worker_readiness_decision",
		"stage_assignment",
		"no_work",
		"stage_command_result",
		"stop_stage",
		"materialization_authority",
		"resolved_input_transfer",
	})
}

func TestModelRuntimeProtocolHasOnlyLongLivedRuntimeOperations(t *testing.T) {
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(
		"vela.v1.ModelRuntimeService",
	)
	if err != nil {
		t.Fatalf("ModelRuntimeService descriptor: %v", err)
	}
	service, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		t.Fatalf("ModelRuntimeService descriptor type = %T", descriptor)
	}
	got := make([]string, 0, service.Methods().Len())
	for index := 0; index < service.Methods().Len(); index++ {
		got = append(got, string(service.Methods().Get(index).Name()))
	}
	want := []string{
		"ProbeReadiness",
		"PrepareStage",
		"StartStage",
		"CancelStage",
		"Status",
		"SealOutput",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("ModelRuntimeService methods = %v, want %v", got, want)
	}
	for _, forbidden := range []protoreflect.Name{"LoadModel", "UnloadModel", "ReplaceModel"} {
		if service.Methods().ByName(forbidden) != nil {
			t.Fatalf("ModelRuntimeService unexpectedly exposes %s", forbidden)
		}
	}
}

func TestTransferTicketAuthorityStopsAtWorkerAgentBoundary(t *testing.T) {
	runtimeInput, err := protoregistry.GlobalFiles.FindDescriptorByName(
		"vela.v1.StageInputArtifact",
	)
	if err != nil {
		t.Fatalf("StageInputArtifact descriptor: %v", err)
	}
	runtimeMessage := runtimeInput.(protoreflect.MessageDescriptor)
	if runtimeMessage.Fields().ByName("transfer_ticket") != nil {
		t.Fatal("ModelRuntime StageInputArtifact exposes TransferTicket authority")
	}
	for _, messageName := range []protoreflect.FullName{
		"vela.v1.StageInputArtifact",
		"vela.v1.StageExecutionSpec",
		"vela.v1.ModelRuntimeServicePrepareStageRequest",
	} {
		descriptor, descriptorErr := protoregistry.GlobalFiles.FindDescriptorByName(messageName)
		if descriptorErr != nil {
			t.Fatalf("%s descriptor: %v", messageName, descriptorErr)
		}
		message := descriptor.(protoreflect.MessageDescriptor)
		for fieldIndex := 0; fieldIndex < message.Fields().Len(); fieldIndex++ {
			fieldName := strings.ToLower(string(message.Fields().Get(fieldIndex).Name()))
			for _, forbidden := range []string{
				"transfer_ticket", "presigned", "url", "credential", "access_key",
				"secret", "object_store_token",
			} {
				if strings.Contains(fieldName, forbidden) {
					t.Errorf("ModelRuntime %s exposes forbidden field %s", messageName, fieldName)
				}
			}
		}
	}
	assignmentDescriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(
		"vela.v1.StageAssignment",
	)
	if err != nil {
		t.Fatalf("StageAssignment descriptor: %v", err)
	}
	assignment := assignmentDescriptor.(protoreflect.MessageDescriptor)
	tickets := assignment.Fields().ByName("input_transfer_tickets")
	if tickets == nil || !tickets.IsList() || tickets.Message() == nil ||
		tickets.Message().FullName() != "vela.v1.StageInputTransferTicket" {
		t.Fatalf("StageAssignment input_transfer_tickets = %#v", tickets)
	}
	runtimeServiceDescriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(
		"vela.v1.ModelRuntimeService",
	)
	if err != nil {
		t.Fatalf("ModelRuntimeService descriptor: %v", err)
	}
	runtimeService := runtimeServiceDescriptor.(protoreflect.ServiceDescriptor)
	seen := make(map[protoreflect.FullName]struct{})
	for methodIndex := 0; methodIndex < runtimeService.Methods().Len(); methodIndex++ {
		assertNoObjectStoreAuthorityFields(t, runtimeService.Methods().Get(methodIndex).Input(), seen)
	}
}

func TestModelRuntimeResponsesCarryIndependentMemberIdentity(t *testing.T) {
	identityDescriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(
		"vela.v1.ModelRuntimeIdentity",
	)
	if err != nil {
		t.Fatalf("ModelRuntimeIdentity descriptor: %v", err)
	}
	identity := identityDescriptor.(protoreflect.MessageDescriptor)
	for _, field := range []protoreflect.Name{"worker_member_id", "worker_member_epoch"} {
		if identity.Fields().ByName(field) == nil {
			t.Errorf("ModelRuntimeIdentity field %s is missing", field)
		}
	}
	for _, name := range []protoreflect.FullName{
		"vela.v1.ModelRuntimeServicePrepareStageResponse",
		"vela.v1.ModelRuntimeServiceStartStageResponse",
		"vela.v1.ModelRuntimeServiceCancelStageResponse",
		"vela.v1.ModelRuntimeServiceStatusResponse",
		"vela.v1.ModelRuntimeServiceSealOutputResponse",
	} {
		descriptor, descriptorErr := protoregistry.GlobalFiles.FindDescriptorByName(name)
		if descriptorErr != nil {
			t.Errorf("%s descriptor: %v", name, descriptorErr)
			continue
		}
		message := descriptor.(protoreflect.MessageDescriptor)
		if message.Fields().ByName("runtime_identity") == nil {
			t.Errorf("%s.runtime_identity is missing", name)
		}
	}
}

func TestModelRuntimeFailedStatusCarriesStructuredFailureEvidence(t *testing.T) {
	statusDescriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(
		"vela.v1.ModelRuntimeServiceStatusResponse",
	)
	if err != nil {
		t.Fatalf("ModelRuntimeServiceStatusResponse descriptor: %v", err)
	}
	status := statusDescriptor.(protoreflect.MessageDescriptor)
	failureField := status.Fields().ByName("failure_evidence")
	if failureField == nil || failureField.Message() == nil ||
		failureField.Message().FullName() != "vela.v1.ModelRuntimeFailureEvidence" {
		t.Fatalf("ModelRuntimeServiceStatusResponse.failure_evidence = %#v", failureField)
	}
	failure := failureField.Message()
	for _, name := range []protoreflect.Name{
		"failure_class",
		"failure_fingerprint",
		"detail",
		"worker_reusable",
		"consumed_resource_units",
		"failed_at",
		"retry_at",
	} {
		if failure.Fields().ByName(name) == nil {
			t.Errorf("ModelRuntimeFailureEvidence.%s is missing", name)
		}
	}
}

func TestStageAuthorityBindsEveryExecutionFenceAndRuntimeEpoch(t *testing.T) {
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName("vela.v1.StageAuthority")
	if err != nil {
		t.Fatalf("StageAuthority descriptor: %v", err)
	}
	message, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		t.Fatalf("StageAuthority descriptor type = %T", descriptor)
	}
	for _, name := range []protoreflect.Name{
		"schema_version",
		"job_id",
		"attempt_id",
		"stage_run_id",
		"stage_attempt_id",
		"stage_allocation_id",
		"stage_lease_id",
		"attempt_fence",
		"stage_fence",
		"stage_version",
		"worker_instance_id",
		"worker_instance_epoch",
		"device_set_digest",
		"devices",
		"membership_digest",
		"members",
		"model_residency_id",
		"model_runtime_identity",
		"stage_profile_revision_id",
		"capacity_observation_sequence",
		"capacity_vector",
		"lease_token",
		"execution_nonce",
		"signing_key_id",
		"issued_at",
		"expires_at",
		"monotonic_valid_for",
		"signature",
		"execution_spec_digest",
	} {
		if message.Fields().ByName(name) == nil {
			t.Errorf("StageAuthority field %s is missing", name)
		}
	}
	memberDescriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(
		"vela.v1.StageAuthorityMemberEpoch",
	)
	if err != nil {
		t.Fatalf("StageAuthorityMemberEpoch descriptor: %v", err)
	}
	member := memberDescriptor.(protoreflect.MessageDescriptor)
	if member.Fields().ByName("model_runtime_epoch") == nil {
		t.Error("StageAuthorityMemberEpoch.model_runtime_epoch is missing")
	}
}

func assertOneofFields(
	t *testing.T,
	message protoreflect.MessageDescriptor,
	oneofName protoreflect.Name,
	want []protoreflect.Name,
) {
	t.Helper()
	oneof := message.Oneofs().ByName(oneofName)
	if oneof == nil {
		t.Fatalf("%s.%s oneof is missing", message.FullName(), oneofName)
	}
	got := make([]protoreflect.Name, 0, oneof.Fields().Len())
	for index := 0; index < oneof.Fields().Len(); index++ {
		got = append(got, oneof.Fields().Get(index).Name())
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s.%s fields = %v, want %v", message.FullName(), oneofName, got, want)
	}
}

func assertNoObjectStoreAuthorityFields(
	t *testing.T,
	message protoreflect.MessageDescriptor,
	seen map[protoreflect.FullName]struct{},
) {
	t.Helper()
	if _, exists := seen[message.FullName()]; exists {
		return
	}
	seen[message.FullName()] = struct{}{}
	for fieldIndex := 0; fieldIndex < message.Fields().Len(); fieldIndex++ {
		field := message.Fields().Get(fieldIndex)
		fieldName := strings.ToLower(string(field.Name()))
		for _, forbidden := range []string{
			"transfer_ticket", "presigned", "object_key", "object_store", "bucket",
			"endpoint", "credential", "access_key", "secret_key",
		} {
			if strings.Contains(fieldName, forbidden) {
				t.Errorf("ModelRuntime %s exposes object-store authority field %s", message.FullName(), fieldName)
			}
		}
		if field.Message() != nil && !field.IsMap() {
			assertNoObjectStoreAuthorityFields(t, field.Message(), seen)
		}
	}
}
