package labv2contract

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestDescriptorsFormOneIsolatedStageWorkerContract(t *testing.T) {
	stages := StageDescriptors()
	workers := WorkerDescriptors()
	if len(stages) != 4 || len(workers) != 3 {
		t.Fatalf("descriptor counts stages=%d workers=%d", len(stages), len(workers))
	}

	stageByKey := make(map[string]StageDescriptor, len(stages))
	for _, stage := range stages {
		if _, duplicate := stageByKey[stage.Key]; duplicate {
			t.Fatalf("duplicate Stage key %q", stage.Key)
		}
		stageByKey[stage.Key] = stage
	}
	seenDevices := make(map[string]struct{}, len(workers))
	seenResidencies := make(map[string]struct{}, len(stages))
	for _, worker := range workers {
		if _, duplicate := seenDevices[worker.DeviceID]; duplicate {
			t.Fatalf("shared Device ID %q", worker.DeviceID)
		}
		seenDevices[worker.DeviceID] = struct{}{}
		for _, stageKey := range worker.StageKeys {
			stage, ok := stageByKey[stageKey]
			if !ok {
				t.Fatalf("Worker %s references unknown Stage %q", worker.Name, stageKey)
			}
			if stage.WorkerProfileID != worker.WorkerProfileID {
				t.Fatalf("Stage %s profile %s does not match Worker %s profile %s",
					stage.Key, stage.WorkerProfileID, worker.Name, worker.WorkerProfileID)
			}
			if _, duplicate := seenResidencies[stage.ResidencyID]; duplicate {
				t.Fatalf("shared ModelResidency ID %q", stage.ResidencyID)
			}
			seenResidencies[stage.ResidencyID] = struct{}{}
		}
		if worker.ResourceClass == ThumbnailResourceClass {
			if worker.GPUUUID != "" || worker.PCIBDF != "" {
				t.Fatalf("CPU Worker has GPU identity: %#v", worker)
			}
			var want map[string]int64
			if err := json.Unmarshal([]byte(ThumbnailCapacityVector), &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(worker.CapacityVector, want) {
				t.Fatalf("thumbnail capacity = %#v, want %#v", worker.CapacityVector, want)
			}
		} else if worker.GPUUUID == "" || worker.PCIBDF == "" ||
			!reflect.DeepEqual(worker.CapacityVector, map[string]int64{"concurrency": 1}) {
			t.Fatalf("GPU Worker identity/capacity is incomplete: %#v", worker)
		}
	}
	if len(seenResidencies) != len(stages) {
		t.Fatalf("assigned residencies=%d stages=%d", len(seenResidencies), len(stages))
	}
	if workers[0].SharedSlot != AuxSharedSlot ||
		!reflect.DeepEqual(workers[0].StageKeys, []string{"encoder", "vae"}) {
		t.Fatalf("AUX shared-slot contract = %#v", workers[0])
	}
}
