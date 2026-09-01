package fleetcontroller

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/vivym/vela/internal/fleetcontract"
	"github.com/vivym/vela/internal/imageref"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	protectedLabel      = fleetcontract.ProtectedLabel
	workerIDLabel       = fleetcontract.WorkerIDLabel
	workerEpochLabel    = fleetcontract.WorkerEpochLabel
	protectionFinalizer = fleetcontract.ProtectionFinalizer
)

var (
	ErrResourceNotFound       = errors.New("fleet resource not found")
	ErrProtectedResourceDrift = errors.New("protected Fleet resource differs from desired revision")
)

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func validBoundedText(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, '\x00')
}

func runtimeResourceName(namespace, name string) string {
	return namespace + "\x00" + name
}

func validResourceName(value string) bool {
	return len(validation.IsDNS1123Subdomain(value)) == 0 && !containsTemplateMarker(value)
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value ||
		value == strings.Repeat("0", sha256.Size*2) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validPinnedImage(value string) bool {
	return !containsTemplateMarker(value) && imageref.ValidPinned(value)
}

func containsTemplateMarker(value string) bool {
	lower := strings.ToLower(value)
	return strings.Contains(lower, "placeholder") || strings.Contains(lower, "replace-with") ||
		strings.Contains(lower, "changeme") || strings.Contains(lower, "todo") ||
		strings.Contains(lower, ".invalid")
}
