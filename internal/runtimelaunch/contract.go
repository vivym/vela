// Package runtimelaunch defines the image entrypoint and Kubernetes handoff
// paths shared by Fleet, Node and the container's pre-exec process.
package runtimelaunch

// AssignmentMaxRecords is shared by bootstrap and the rendered Worker journal.
const AssignmentMaxRecords = 32

const (
	Entrypoint         = "/usr/local/bin/vela-runtime-entrypoint"
	Runtime            = "/usr/local/bin/vela-model-runtime"
	Worker             = "/usr/local/bin/vela-stage-worker-agent"
	Bootstrap          = "/run/vela-model-runtime-bootstrap/bootstrap.json"
	OfferRoot          = "/run/vela-launch"
	Gate               = "vela.ai/runtime-startup"
	ProtocolAnnotation = "vela.ai/runtime-launch-protocol"
	Protocol           = "kubernetes-pidfd-v1"
	BrokerRoot         = "/run/vela-pidfd-broker"
	BrokerSocket       = BrokerRoot + "/broker.sock"
)

func MemberRoot(memberID string) string { return "/run/vela/w/" + memberID }

func RuntimeArguments() []string {
	return []string{Runtime, "serve-remote", "--bootstrap-file", Bootstrap}
}

// DisabledServiceEnvironment overrides kubelet's always-injected Kubernetes
// API Service links, even when enableServiceLinks is false. These exact empty
// values are signed into the Pod; no live endpoint becomes approved input.
func DisabledServiceEnvironment() []string {
	return []string{
		"KUBERNETES_SERVICE_HOST=", "KUBERNETES_SERVICE_PORT=", "KUBERNETES_SERVICE_PORT_HTTPS=",
		"KUBERNETES_PORT=", "KUBERNETES_PORT_443_TCP=", "KUBERNETES_PORT_443_TCP_PROTO=",
		"KUBERNETES_PORT_443_TCP_PORT=", "KUBERNETES_PORT_443_TCP_ADDR=",
	}
}

// RuntimeEntrypoint recognizes only the fixed Runtime pre-exec image command.
// Arbitrary wrappers, flags and caller-selected executable paths are rejected.
func RuntimeEntrypoint(argv []string) bool {
	return len(argv) == 2 && argv[0] == Entrypoint && argv[1] == "runtime"
}
