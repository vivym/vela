package stageworkeragent

import (
	"context"
	"crypto/sha256"

	"github.com/vivym/vela/internal/tracing"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"go.opentelemetry.io/otel/trace"
)

type assignmentTrace struct {
	identity [sha256.Size]byte
	parent   string
}

func (agent *StreamAgent) rememberAssignmentTrace(assignment *velav1.StageAssignment) {
	identity, err := assignmentExecutionIdentity(assignment.GetAuthority())
	if err != nil {
		return
	}
	agent.mu.Lock()
	defer agent.mu.Unlock()
	if agent.assignmentTrace == nil || agent.assignmentTrace.identity != identity {
		agent.assignmentTrace = &assignmentTrace{identity: identity, parent: tracing.CanonicalParent(assignment.GetOriginTraceParent())}
	}
}

// The durable journal is the correlation source after renewal, reattach and
// materialization recovery. Lookup cannot authorize or reopen an execution.
func (agent *StreamAgent) startStageTrace(ctx context.Context, operation string, authority *velav1.StageAuthority) (context.Context, trace.Span) {
	parent := ""
	identity, err := assignmentExecutionIdentity(authority)
	if err == nil {
		if agent.admission != nil {
			parent = agent.admission.traceParent(identity)
		} else {
			agent.mu.Lock()
			if agent.assignmentTrace != nil && agent.assignmentTrace.identity == identity {
				parent = agent.assignmentTrace.parent
			}
			agent.mu.Unlock()
		}
	}
	return tracing.StartStage(tracing.WithDurableParent(ctx, &parent), operation, authority)
}

func (gate *FileAssignmentAdmission) traceParent(identity [sha256.Size]byte) string {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.state.Latest != nil && gate.state.Latest.Identity == identity {
		return tracing.CanonicalParent(gate.state.Latest.OriginTraceParent)
	}
	for _, entry := range gate.state.Pending {
		if entry.Identity == identity {
			return tracing.CanonicalParent(entry.OriginTraceParent)
		}
	}
	return ""
}
