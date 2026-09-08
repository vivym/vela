package nodeagent

import (
	"strings"
	"testing"
)

func TestRuntimeDaemonStateDirectory(t *testing.T) {
	for _, configuration := range []string{
		"", "null", "[]", `{}`, `{"StateDir":"/run/containerd/io.containerd.grpc.v1.cri"}`,
		`{"stateDir":null}`, `{"stateDir":1}`, `{"stateDir":"relative/io.containerd.grpc.v1.cri"}`,
		`{"stateDir":"/run/containerd/../io.containerd.grpc.v1.cri"}`, `{"stateDir":"/run/containerd/io.containerd.grpc.v1.cri/"}`,
		`{"stateDir":"/io.containerd.grpc.v1.cri"}`, `{"stateDir":"/run/containerd/io.containerd.runtime.v2.task"}`,
		`{"stateDir":"/run/\u0000/io.containerd.grpc.v1.cri"}`,
		`{"stateDir":"/run/containerd/io.containerd.grpc.v1.cri","stateDir":"/other/io.containerd.grpc.v1.cri"}`,
		`{"stateDir":"/run/containerd/io.containerd.grpc.v1.cri","nested":{"a":1,"a":2}}`,
		`{"stateDir":"/run/containerd/io.containerd.grpc.v1.cri"} {}`,
		strings.Repeat(" ", (1<<20)+1),
	} {
		if state, err := runtimeDaemonStateDirectory(configuration); err == nil || state != "" {
			t.Fatalf("ambiguous, missing or unsupported daemon configuration produced a state path: %q", configuration[:min(200, len(configuration))])
		}
	}
	state, err := runtimeDaemonStateDirectory(`{"stateDir":"/run/containerd/io.containerd.grpc.v1.cri","containerdRootDir":"/var/lib/containerd","other":{"ignored":true}}`)
	if err != nil || state != "/run/containerd" {
		t.Fatalf("valid daemon configuration rejected: %q %v", state, err)
	}
}
