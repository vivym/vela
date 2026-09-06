package modelruntime

import (
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/vivym/vela/internal/driverdrain"
	"github.com/vivym/vela/internal/driverinspection"
)

const (
	maxDriverEnvironmentEntries = 128
	maxDriverEnvironmentBytes   = 4096
)

// ValidateDriverEnvironment checks the complete declared driver environment.
// Runtime adds only its fixed protocol/channel entries; there is no inherited
// environment. Fleet and direct Runtime callers share this contract.
func ValidateDriverEnvironment(environment []string) error {
	if len(environment) > maxDriverEnvironmentEntries {
		return errors.New("ModelRuntime driver environment is too large")
	}
	seen := make(map[string]struct{}, len(environment))
	for _, entry := range environment {
		name, _, found := strings.Cut(entry, "=")
		if !found || name == "" || len(entry) > maxDriverEnvironmentBytes ||
			!utf8.ValidString(entry) || strings.ContainsRune(entry, '\x00') {
			return errors.New("ModelRuntime driver environment is invalid")
		}
		if _, duplicate := seen[name]; duplicate || name == "VELA_MODEL_DRIVER_PROTOCOL" ||
			name == driverinspection.Environment || name == driverdrain.Environment {
			return errors.New("ModelRuntime driver environment is duplicated or reserved")
		}
		seen[name] = struct{}{}
	}
	return nil
}
