package authoritypolicy

import (
	"testing"
	"time"
)

func TestProductionMaxClockSkew(t *testing.T) {
	if ProductionMaxClockSkew != 30*time.Second {
		t.Fatalf("production authority clock skew = %s, want 30s", ProductionMaxClockSkew)
	}
}
