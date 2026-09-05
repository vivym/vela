package stageauthority_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSameExecutionAllowsObservationAcrossRenewalsWithoutGrantingRenewal(t *testing.T) {
	now := time.Date(2026, 9, 6, 1, 0, 0, 0, time.UTC)
	signer, err := stageauthority.NewSigner(map[string][]byte{"stage-key-7": bytes.Repeat([]byte{0x28}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	a := validAuthority(now)
	a.SchemaVersion, a.ExecutionSequence = stageauthority.SchemaVersionV2, 17
	a, err = signer.Sign(a)
	if err != nil {
		t.Fatal(err)
	}
	b := proto.Clone(a).(*velav1.StageAuthority)
	b.StageVersion++
	b.IssuedAt = timestamppb.New(a.IssuedAt.AsTime().Add(time.Second))
	b.ExpiresAt = timestamppb.New(a.ExpiresAt.AsTime().Add(time.Second))
	b, err = signer.Sign(b)
	if err != nil {
		t.Fatal(err)
	}
	if stageauthority.ValidateSameExecution(a, b) != nil || stageauthority.ValidateSameExecution(b, a) != nil ||
		stageauthority.ValidateSameExecution(a, a) != nil || stageauthority.ValidateRenewal(b, a) == nil {
		t.Fatal("historical identity comparison changed the monotonic renewal contract")
	}
	for name, mutate := range map[string]func(*velav1.StageAuthority){
		"sequence":    func(v *velav1.StageAuthority) { v.ExecutionSequence++ },
		"nonce":       func(v *velav1.StageAuthority) { v.ExecutionNonce[0] ^= 1 },
		"lease token": func(v *velav1.StageAuthority) { v.LeaseToken[0] ^= 1 },
		"spec":        func(v *velav1.StageAuthority) { v.ExecutionSpecDigest[0] ^= 1 },
		"barrier":     func(v *velav1.StageAuthority) { v.ModelRuntimeBarrierGeneration++ },
		"fence":       func(v *velav1.StageAuthority) { v.StageFence++ },
		"member":      func(v *velav1.StageAuthority) { v.Members[0].ModelRuntimeEpoch++ },
		"device":      func(v *velav1.StageAuthority) { v.Devices[0].DeviceEpoch++ },
		"worker":      func(v *velav1.StageAuthority) { v.WorkerInstanceEpoch++ },
		"profile":     func(v *velav1.StageAuthority) { v.StageProfileRevisionId = "10000000-0000-0000-0000-000000000099" },
		"capacity":    func(v *velav1.StageAuthority) { v.CapacityObservationSequence++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(b).(*velav1.StageAuthority)
			mutate(changed)
			if stageauthority.ValidateSameExecution(a, changed) == nil || stageauthority.ValidateSameExecution(changed, a) == nil {
				t.Fatal("changed immutable execution identity matched")
			}
		})
	}
}
