// Command trace-canary exercises the application's production tracing package
// in two isolated processes. Both listeners are loopback-only, use synthetic
// data, and stop on SIGTERM. It performs no Vela job or model execution.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/vivym/vela/internal/tracing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const fakeCredential = "Bearer private-canary-credential"

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	if err := run(ctx); err != nil {
		// Keep errors out of the evidence and never print input or OTLP details.
		_, _ = io.WriteString(os.Stderr, "trace canary failed\n")
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	mode := os.Getenv("VELA_TRACE_CANARY_MODE")
	if mode != "frontend" && mode != "backend" {
		return errors.New("frontend or backend mode required")
	}
	stop, err := tracing.Start(ctx, "vela-trace-canary-"+mode)
	if err != nil {
		return err
	}
	defer stop()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	if mode == "backend" {
		server := grpc.NewServer(grpc.StatsHandler(tracing.GRPCHandler{}))
		healthpb.RegisterHealthServer(server, canaryHealth{})
		go func() { _ = server.Serve(listener) }()
		if err := announce(listener); err != nil {
			server.Stop()
			return err
		}
		<-ctx.Done()
		server.GracefulStop()
		return nil
	}
	backend := os.Getenv("VELA_TRACE_CANARY_BACKEND")
	host, _, err := net.SplitHostPort(backend)
	if err != nil || host != "127.0.0.1" {
		return errors.New("backend must be numeric loopback address")
	}
	connection, err := grpc.NewClient(backend,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(tracing.GRPCHandler{Client: true}))
	if err != nil {
		return err
	}
	defer connection.Close()
	client := healthpb.NewHealthClient(connection)
	router := chi.NewRouter()
	router.Use(tracing.HTTPServer)
	router.Post("/probe/{case}", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != fakeCredential {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if _, err := io.Copy(io.Discard, http.MaxBytesReader(w, r.Body, 4096)); err != nil {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		callCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		callCtx = metadata.AppendToOutgoingContext(callCtx, "authorization", fakeCredential)
		payload := "private-canary-payload"
		if strings.HasPrefix(chi.URLParam(r, "case"), "fail") {
			payload = "fail-private-canary-payload"
		}
		_, err := client.Check(callCtx, &healthpb.HealthCheckRequest{Service: payload})
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	server := &http.Server{Handler: router, ReadHeaderTimeout: 3 * time.Second}
	go func() { _ = server.Serve(listener) }()
	if err := announce(listener); err != nil {
		_ = server.Close()
		return err
	}
	<-ctx.Done()
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(stopCtx)
}

func announce(listener net.Listener) error {
	return json.NewEncoder(os.Stdout).Encode(map[string]string{"address": listener.Addr().String()})
}

type canaryHealth struct {
	healthpb.UnimplementedHealthServer
}

func (canaryHealth) Check(ctx context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if values := md.Get("authorization"); len(values) != 1 || values[0] != fakeCredential {
		return nil, status.Error(codes.Unauthenticated, "private-canary-credential-rejected")
	}
	if strings.HasPrefix(request.GetService(), "fail") {
		return nil, status.Error(codes.Unavailable, "private-canary-backend-error")
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}
