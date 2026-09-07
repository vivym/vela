package nodeagent

import (
	"strings"
	"testing"
)

func TestRuntimeContainerSocketPinRequiresLinux(t *testing.T) {
	observer, err := DialRuntimeContainerObserver(t.Context(), RuntimeContainerObserverConfig{
		SocketPath: "/run/containerd/containerd.sock", NodeIdentity: "node-1",
	})
	if observer != nil || err == nil || !strings.Contains(err.Error(), "requires Linux") {
		t.Fatalf("unsupported kernel produced an observer: %v %v", observer, err)
	}
}
