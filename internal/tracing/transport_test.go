package tracing_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/vivym/vela/internal/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const parentHeader = "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01"

func recording(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	old := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(old)
	})
	return recorder
}

func awaitSpans(t *testing.T, recorder *tracetest.SpanRecorder, count int) []sdktrace.ReadOnlySpan {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if spans := recorder.Ended(); len(spans) >= count {
			return spans
		}
		time.Sleep(time.Millisecond)
	}
	names := make([]string, 0)
	for _, span := range recorder.Ended() {
		names = append(names, span.Name())
	}
	t.Fatalf("got %d completed spans, want %d: %v", len(recorder.Ended()), count, names)
	return nil
}

type probeHealth struct {
	healthpb.UnimplementedHealthServer
	metadata chan metadata.MD
}

func (h *probeHealth) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	h.metadata <- md
	return nil, status.Error(grpccodes.Unavailable, "private-error-detail")
}

func (*probeHealth) Watch(_ *healthpb.HealthCheckRequest, stream healthpb.Health_WatchServer) error {
	if err := stream.Send(&healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return status.FromContextError(stream.Context().Err()).Err()
}

func healthConnection(t *testing.T, service *probeHealth) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.StatsHandler(tracing.GRPCHandler{}))
	healthpb.RegisterHealthServer(server, service)
	go func() { _ = server.Serve(listener) }()
	connection, err := grpc.NewClient("passthrough:///tracing-test",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(tracing.GRPCHandler{Client: true}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close(); server.Stop(); _ = listener.Close() })
	return connection
}

func TestHTTPToGRPCContinuityAndPrivacy(t *testing.T) {
	recorder := recording(t)
	service := &probeHealth{metadata: make(chan metadata.MD, 1)}
	connection := healthConnection(t, service)
	client := healthpb.NewHealthClient(connection)
	router := chi.NewRouter()
	router.Use(tracing.HTTPServer)
	router.Post("/jobs/{job}", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		ctx := metadata.NewOutgoingContext(r.Context(), metadata.Pairs(
			"authorization", "Bearer private-rpc-credential", "x-runtime-authority", "private-authority"))
		_, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: "private-request-payload"})
		if status.Code(err) != grpccodes.Unavailable {
			t.Errorf("RPC status changed: %v", err)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	request := httptest.NewRequest(http.MethodPost, "/jobs/private-job-id?token=private-query", strings.NewReader("private-body"))
	request.Header.Set("Traceparent", parentHeader)
	request.Header.Set("Tracestate", "customer=private-tracestate")
	request.Header.Set("Authorization", "Bearer private-http-credential")
	request.Header.Set("Baggage", "customer=private-baggage")
	router.ServeHTTP(httptest.NewRecorder(), request)
	spans := awaitSpans(t, recorder, 3)
	if len(spans) != 3 {
		t.Fatalf("want one HTTP and two RPC spans, got %d", len(spans))
	}
	var httpSpan, clientSpan, serverSpan sdktrace.ReadOnlySpan
	for _, span := range spans {
		if span.SpanContext().TraceID().String() != "1234567890abcdef1234567890abcdef" || span.Status().Code != codes.Error {
			t.Errorf("incorrect trace or error status: %s", span.Name())
		}
		if span.SpanContext().TraceState().Len() != 0 || span.Parent().TraceState().Len() != 0 {
			t.Fatal("untrusted vendor state reached span")
		}
		switch {
		case span.Name() == "POST /jobs/{job}":
			httpSpan = span
		case span.SpanKind() == trace.SpanKindClient:
			clientSpan = span
		case span.Name() == "grpc.health.v1.Health/Check":
			serverSpan = span
		}
		encoded, err := json.Marshal(tracetest.SpanStubFromReadOnlySpan(span))
		if err != nil || strings.Contains(string(encoded), "private-") {
			t.Fatalf("export contained private request data or failed encoding: %v", err)
		}
	}
	if httpSpan == nil || clientSpan == nil || serverSpan == nil {
		t.Fatal("missing HTTP/client/server span")
	}
	if httpSpan.Parent().SpanID().String() != "1234567890abcdef" || !httpSpan.Parent().IsRemote() ||
		clientSpan.Parent().SpanID() != httpSpan.SpanContext().SpanID() ||
		serverSpan.Parent().SpanID() != clientSpan.SpanContext().SpanID() || !serverSpan.Parent().IsRemote() {
		t.Fatal("HTTP -> RPC client -> RPC server parent chain broken")
	}
	md := <-service.metadata
	if md.Get("authorization")[0] != "Bearer private-rpc-credential" || md.Get("x-runtime-authority")[0] != "private-authority" {
		t.Fatal("tracing changed independently authenticated metadata")
	}
	if len(md.Get("baggage")) != 0 || len(md.Get("tracestate")) != 0 || len(md.Get("traceparent")) != 1 {
		t.Fatal("unexpected trace propagation metadata")
	}
}

func TestStreamingRPCRemainsOpenUntilCancellation(t *testing.T) {
	recorder := recording(t)
	connection := healthConnection(t, &probeHealth{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := healthpb.NewHealthClient(connection).Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response, err := stream.Recv(); err != nil || response.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("first stream message: %v, %v", response, err)
	}
	if len(recorder.Started()) != 2 || len(recorder.Ended()) != 0 {
		t.Fatal("stream spans ended while transport was still open")
	}
	cancel()
	if _, err := stream.Recv(); status.Code(err) != grpccodes.Canceled {
		t.Fatalf("cancellation changed: %v", err)
	}
	for _, span := range awaitSpans(t, recorder, 2) {
		if span.Status().Description != "Canceled" || span.EndTime().Before(span.StartTime()) {
			t.Fatalf("incorrect stream end: %s %+v", span.Name(), span.Status())
		}
	}
}

func TestUnknownRPCAndUnmatchedHTTPDoNotRecordInput(t *testing.T) {
	recorder := recording(t)
	connection := healthConnection(t, &probeHealth{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := connection.Invoke(ctx, "/private-service/private-method", &healthpb.HealthCheckRequest{}, &healthpb.HealthCheckResponse{})
	if status.Code(err) != grpccodes.Unimplemented {
		t.Fatalf("unknown method status: %v", err)
	}
	router := chi.NewRouter()
	router.Use(tracing.HTTPServer)
	router.Get("/known", func(http.ResponseWriter, *http.Request) {})
	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("PRIVATE-VERB", "/private-path?secret=private-query", nil))
	// gRPC rejects unknown methods before invoking server stats handlers. The
	// client still records the bounded failed call alongside the HTTP 405.
	for _, span := range awaitSpans(t, recorder, 2) {
		if span.Name() != "OTHER unmatched" && span.Name() != "unknown/unknown" {
			t.Errorf("unbounded span name: %q", span.Name())
		}
	}
}

func TestHTTPFlushAndPanic(t *testing.T) {
	recorder := recording(t)
	finished := make(chan struct{})
	router := chi.NewRouter()
	router.Use(tracing.HTTPServer)
	router.Get("/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: ready\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(finished)
	})
	server := httptest.NewServer(router)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/events", nil)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	line, err := bufio.NewReader(response.Body).ReadString('\n')
	if err != nil || line != "data: ready\n" || len(recorder.Ended()) != 0 {
		t.Fatalf("stream buffered or ended early: %q %v", line, err)
	}
	cancel()
	<-finished
	awaitSpans(t, recorder, 1)
	func() {
		defer func() {
			if recover() != "private-panic" {
				t.Error("middleware changed panic semantics")
			}
		}()
		tracing.HTTPServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("private-panic") })).
			ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/private-path", nil))
	}()
	span := awaitSpans(t, recorder, 2)[1]
	if span.Status().Description != "handler panic" || len(span.Events()) != 0 {
		t.Fatal("panic detail leaked or panic span unfinished")
	}
}
