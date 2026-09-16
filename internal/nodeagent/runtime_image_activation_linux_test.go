package nodeagent

import (
	"testing"
	"time"

	"github.com/containerd/containerd/api/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRuntimeImageActivationPersistedRepresentations(t *testing.T) {
	initial := &types.ActivationInfo{Name: "test-view", Active: []*types.ActiveMount{{Mount: &types.Mount{Type: "bind", Source: "/native/snapshot", Options: []string{"ro", "rbind"}}, MountPoint: "/mounts/test-view", MountedAt: timestamppb.New(time.Unix(100, 0))}}, System: []*types.Mount{{Type: "bind", Source: "/mounts/test-view", Options: []string{"rbind"}}}}
	for _, omitSystem := range []bool{false, true} {
		current := runtimeImagePersistedActivation(initial)
		if omitSystem {
			current.System = nil
		}
		if !sameRuntimeImageActivation(initial, current) {
			t.Fatal("unchanged persisted activation rejected")
		}
		if _, err := runtimeImageActivationPath(initial.Name, current); err != nil {
			t.Fatal(err)
		}
		for _, change := range []string{"mountpoint", "timestamp", "type", "data", "system"} {
			replaced := proto.CloneOf(current)
			switch change {
			case "mountpoint":
				replaced.Active[0].MountPoint = "/mounts/replaced"
			case "timestamp":
				replaced.Active[0].MountedAt.Seconds++
			case "type":
				replaced.Active[0].Mount.Type = "overlay"
			case "data":
				replaced.Active[0].Data = map[string]string{"foreign": "data"}
			case "system":
				replaced.System = []*types.Mount{{Type: "bind", Source: "/foreign", Options: []string{"rbind"}}}
			}
			if sameRuntimeImageActivation(initial, replaced) {
				t.Fatalf("accepted replacement %s", change)
			}
		}
	}
	if initial.Active[0].Mount.Source != "/native/snapshot" || len(initial.System) != 1 {
		t.Fatal("mutated original activation")
	}
}
