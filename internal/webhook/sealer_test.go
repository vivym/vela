package webhook

import (
	"bytes"
	"testing"
)

func TestAESGCMSealerBindsCiphertextToSubscriptionIdentity(t *testing.T) {
	sealer, err := NewAESGCMSealer("webhook-key-v1", map[string][]byte{
		"webhook-key-v1": []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatalf("configure secret sealer: %v", err)
	}

	plaintext := []byte("vwhsec_example-signing-secret")
	identity := []byte("organization/project/subscription/secret/1")
	sealed, err := sealer.Seal(plaintext, identity)
	if err != nil {
		t.Fatalf("seal secret: %v", err)
	}
	if sealed.KeyID != "webhook-key-v1" || len(sealed.Nonce) != 12 ||
		bytes.Contains(sealed.Ciphertext, plaintext) {
		t.Fatalf("sealed secret = %#v", sealed)
	}

	opened, err := sealer.Open(sealed, identity)
	if err != nil {
		t.Fatalf("open secret: %v", err)
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatalf("opened secret = %q, want %q", opened, plaintext)
	}
	clear(opened)

	if _, err := sealer.Open(sealed, []byte("other/project/subscription/secret/1")); err == nil {
		t.Fatal("open with substituted Subscription identity succeeded")
	}
}
