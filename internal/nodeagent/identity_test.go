package nodeagent

import (
	"context"
	"testing"
)

func TestStaticControllerIdentityResolverRequiresExplicitControllerSPIFFE(t *testing.T) {
	if _, err := NewStaticControllerIdentityResolver(map[string]string{
		"spiffe://vela.internal/node/node-1": "controller/control-1",
	}); err == nil {
		t.Fatal("node SPIFFE identity was accepted as a controller")
	}
	resolver, err := NewStaticControllerIdentityResolver(map[string]string{
		"spiffe://vela.internal/controller/control-1": "controller/control-1",
	})
	if err != nil {
		t.Fatalf("NewStaticControllerIdentityResolver: %v", err)
	}
	identity, err := resolver.ResolveController(context.Background(), "spiffe://vela.internal/controller/control-1")
	if err != nil || identity.ActorIdentity != "controller/control-1" {
		t.Fatalf("ResolveController = %#v error=%v", identity, err)
	}
	if _, err := resolver.ResolveController(context.Background(), "spiffe://vela.internal/controller/other"); err == nil {
		t.Fatal("unregistered controller identity was accepted")
	}
}
