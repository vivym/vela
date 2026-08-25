package workeragent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPArtifactStoreProbeReportsReachabilityTransitions(t *testing.T) {
	var healthy atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodHead {
			t.Errorf("Artifact-store probe method = %s", request.Method)
		}
		if healthy.Load() {
			response.WriteHeader(http.StatusNoContent)
			return
		}
		response.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	probe, err := NewHTTPArtifactStoreProbe(HTTPArtifactStoreProbeConfig{
		URL: server.URL, Timeout: time.Second, AllowHTTP: true,
	})
	if err != nil {
		t.Fatalf("NewHTTPArtifactStoreProbe: %v", err)
	}

	if probe.Reachable(context.Background()) {
		t.Fatal("unhealthy Artifact store reported reachable")
	}
	healthy.Store(true)
	if !probe.Reachable(context.Background()) {
		t.Fatal("healthy Artifact store reported unreachable")
	}
}

func TestHTTPArtifactStoreProbeStopsAtTheOperationDeadline(t *testing.T) {
	requestCanceled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
		close(requestCanceled)
	}))
	t.Cleanup(server.Close)
	probe, err := NewHTTPArtifactStoreProbe(HTTPArtifactStoreProbeConfig{
		URL: server.URL, Timeout: time.Second, AllowHTTP: true,
	})
	if err != nil {
		t.Fatalf("NewHTTPArtifactStoreProbe: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	if probe.Reachable(ctx) {
		t.Fatal("canceled Artifact-store probe reported reachable")
	}
	select {
	case <-requestCanceled:
	case <-time.After(time.Second):
		t.Fatal("Artifact-store request did not inherit the operation deadline")
	}
}
