package main

import (
	"fmt"
	"strings"

	"github.com/vivym/vela/internal/securefile"
)

const maxSecretTextBytes = 16 << 10

func readSecretText(path string, description string) (string, error) {
	content, err := securefile.Read(path, maxSecretTextBytes, true)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", description, err)
	}
	defer clear(content)
	value := strings.TrimSpace(string(content))
	if value == "" || len(value) > maxSecretTextBytes || strings.ContainsRune(value, '\x00') {
		return "", fmt.Errorf("%s is empty or invalid", description)
	}
	return value, nil
}
