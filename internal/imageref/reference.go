package imageref

import (
	"strings"

	distributionreference "github.com/distribution/reference"
)

func ValidPinned(value string) bool {
	if value == "" || strings.TrimSpace(value) != value {
		return false
	}
	named, err := distributionreference.ParseNormalizedNamed(value)
	if err != nil || named.String() != value {
		return false
	}
	if _, tagged := named.(distributionreference.Tagged); tagged {
		return false
	}
	digested, ok := named.(distributionreference.Digested)
	if !ok || digested.Digest().Algorithm() != "sha256" {
		return false
	}
	encoded := digested.Digest().Encoded()
	return len(encoded) == 64 && encoded != strings.Repeat("0", 64)
}
