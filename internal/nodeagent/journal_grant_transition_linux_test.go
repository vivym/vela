package nodeagent

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
)

func TestJournalReadOnlyGrantTransition(t *testing.T) {
	f := newJournalEndpointConfiguredFixture(t, 0, nil, true)
	grant := &JournalWriteGrant{endpoint: f.endpoint, expires: time.Now().Add(time.Minute)}
	if _, err := rand.Read(grant.nonce[:]); err != nil {
		t.Fatal(err)
	}
	if err := f.endpoint.ActivateJournalWriteGrant(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	if err := f.endpoint.ActivateJournalWriteGrant(t.Context(), grant); err == nil {
		t.Fatal("single-use grant activated twice")
	}
	if _, err := f.endpoint.Handle(t.Context(), nil); err == nil {
		t.Fatal("nil caller bypassed writable route")
	}
	// A fresh endpoint remains read-only until its own grant is activated.
	readonly := f.endpoint
	readonly.readOnly = true
	readonly.grantUsed = false
	wrong := &JournalWriteGrant{endpoint: &JournalEndpoint{}, expires: time.Now().Add(time.Minute)}
	if _, err := rand.Read(wrong.nonce[:]); err != nil {
		t.Fatal(err)
	}
	if err := readonly.ActivateJournalWriteGrant(t.Context(), wrong); err == nil {
		t.Fatal("grant for a different endpoint activated")
	}
	_ = modelruntime.JournalRuntimeRole // compile-time role boundary remains explicit
}
