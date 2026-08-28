package imageref

import (
	"strings"
	"testing"
)

func TestValidPinned(t *testing.T) {
	digest := strings.Repeat("a", 64)
	tests := []struct {
		value string
		valid bool
	}{
		{value: "ghcr.io/vivym/vela@sha256:" + digest, valid: true},
		{value: "registry.example.com:5000/team/vela@sha256:" + digest, valid: true},
		{value: "docker.io/library/busybox@sha256:" + digest, valid: true},
		{value: "https://ghcr.io/vivym/vela@sha256:" + digest},
		{value: "ghcr.io//vivym/vela@sha256:" + digest},
		{value: "ghcr.io/vivym/../vela@sha256:" + digest},
		{value: "ghcr.io/Vivym/vela@sha256:" + digest},
		{value: "ghcr.io/vivym/vela:r1@sha256:" + digest},
		{value: "ghcr.io/vivym/vela@sha256:" + strings.Repeat("0", 64)},
		{value: "ghcr.io/vivym/vela@sha512:" + strings.Repeat("a", 128)},
		{value: "ghcr.io/vivym/vela@sha256:" + digest + "@sha256:" + digest},
		{value: "ghcr.io:bad:5000/vivym/vela@sha256:" + digest},
	}
	for _, test := range tests {
		if got := ValidPinned(test.value); got != test.valid {
			t.Errorf("ValidPinned(%q) = %t, want %t", test.value, got, test.valid)
		}
	}
}
