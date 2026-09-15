package tracing

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// GRPCHandler traces unary and streaming RPC lifetimes without touching message
// payloads, authorization metadata, cancellation, retries or TLS credentials.
// A long-lived control stream is one transport span, not one span per StageRun.
type GRPCHandler struct{ Client bool }

func (h GRPCHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	kind := trace.SpanKindServer
	if h.Client {
		kind = trace.SpanKindClient
	} else if md, ok := metadata.FromIncomingContext(ctx); ok {
		ctx = propagation.TraceContext{}.Extract(ctx, parentCarrier{metadataCarrier(md)})
	}
	service, method := describedMethod(info.FullMethodName)
	ctx, _ = otel.Tracer("vela/grpc").Start(ctx, service+"/"+method,
		trace.WithSpanKind(kind), trace.WithAttributes(attribute.String("rpc.system", "grpc"),
			attribute.String("rpc.service", service), attribute.String("rpc.method", method)))
	if h.Client {
		md, _ := metadata.FromOutgoingContext(ctx)
		md = md.Copy()
		// Replace only trace fields; preserve independently authenticated metadata.
		md.Delete("traceparent")
		md.Delete("tracestate")
		propagation.TraceContext{}.Inject(ctx, parentCarrier{metadataCarrier(md)})
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

func (GRPCHandler) HandleRPC(ctx context.Context, event stats.RPCStats) {
	if end, ok := event.(*stats.End); ok {
		span := trace.SpanFromContext(ctx)
		code := status.Code(end.Error)
		span.SetAttributes(attribute.Int("rpc.grpc.status_code", int(code)))
		if code != grpccodes.OK {
			// Status messages can contain input data or credentials.
			span.SetStatus(codes.Error, code.String())
		}
		span.End()
	}
}

func (GRPCHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (GRPCHandler) HandleConn(context.Context, stats.ConnStats)                       {}

type metadataCarrier metadata.MD

func (m metadataCarrier) Get(key string) string {
	values := metadata.MD(m).Get(key)
	if len(values) == 1 {
		return values[0]
	}
	return ""
}
func (m metadataCarrier) Set(key, value string) { metadata.MD(m).Set(key, value) }
func (m metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func describedMethod(full string) (string, string) {
	parts := strings.Split(strings.TrimPrefix(full, "/"), "/")
	if len(parts) == 2 {
		descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(parts[0]))
		if service, ok := descriptor.(protoreflect.ServiceDescriptor); err == nil && ok &&
			service.Methods().ByName(protoreflect.Name(parts[1])) != nil {
			return parts[0], parts[1]
		}
	}
	// Unknown method strings come from untrusted network peers.
	return "unknown", "unknown"
}
