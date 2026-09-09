package nodeagent

import (
	"context"
	"testing"
	"time"
)

func TestRuntimeStartupServerRejectsInvalidConfiguration(t *testing.T) {
	if server, err := NewRuntimeStartupServer(nil, []RuntimeCallerCredentials{{UID: 10001, GID: 10001}}, time.Second); server != nil || err == nil {
		t.Fatal("nil coordinator accepted")
	}
	if err := (&RuntimeStartupServer{}).HandleConnection(t.Context(), nil); err == nil {
		t.Fatal("nil connection accepted")
	}
	if err := (&RuntimeStartupServer{}).Serve(context.Background(), nil); err == nil {
		t.Fatal("nil listener accepted")
	}
}
