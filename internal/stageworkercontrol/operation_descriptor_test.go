package stageworkercontrol

import (
	"testing"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestOperationDescriptorsCoverEveryProtocolOperation(t *testing.T) {
	for number, name := range velav1.StageWorkerOperation_name {
		operation := Operation(number)
		if operation == velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_UNSPECIFIED {
			continue
		}
		descriptor, ok := descriptorForOperation(operation)
		if !ok || descriptor.validate == nil {
			t.Fatalf("operation %s has no complete descriptor", name)
		}
		switch descriptor.authority {
		case operationAuthorityNone:
			if descriptor.stageAuthority != nil || descriptor.materializationAuthority != nil ||
				descriptor.activeState != nil {
				t.Fatalf("authority-free operation %s has authority metadata", name)
			}
		case operationAuthorityStage:
			if descriptor.stageAuthority == nil || descriptor.materializationAuthority != nil ||
				descriptor.activeState == nil {
				t.Fatalf("stage-authorized operation %s has inconsistent metadata", name)
			}
		case operationAuthorityMaterialization:
			if descriptor.stageAuthority != nil || descriptor.materializationAuthority == nil ||
				descriptor.activeState != nil {
				t.Fatalf("materialization-authorized operation %s has inconsistent metadata", name)
			}
		default:
			t.Fatalf("operation %s has unknown authority kind %d", name, descriptor.authority)
		}
	}
	if got, want := len(operationDescriptors), len(velav1.StageWorkerOperation_name)-1; got != want {
		t.Fatalf("operation descriptor count = %d, want %d", got, want)
	}
}
