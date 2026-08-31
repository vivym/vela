package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/vivym/vela/internal/securefile"
)

const (
	maxAuthorityKeyringBytes = 64 << 10
	maxSecretTextBytes       = 16 << 10
)

func readAuthorityKeyring(path string) (map[string][]byte, error) {
	document, err := securefile.Read(path, maxAuthorityKeyringBytes, true)
	if err != nil {
		return nil, fmt.Errorf("read Stage Worker authority keyring: %w", err)
	}
	defer clear(document)
	decoder := json.NewDecoder(bytes.NewReader(document))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return nil, errors.New("Stage Worker authority keyring must be one JSON object")
	}
	keyring := make(map[string][]byte)
	for decoder.More() {
		keyToken, tokenErr := decoder.Token()
		keyID, ok := keyToken.(string)
		if tokenErr != nil || !ok || keyID == "" || len(keyID) > 100 ||
			strings.TrimSpace(keyID) != keyID || strings.ContainsAny(keyID, "\x00\r\n\t ") {
			clearAuthorityKeyring(keyring)
			return nil, errors.New("Stage Worker authority keyring contains an invalid key id")
		}
		if _, duplicate := keyring[keyID]; duplicate {
			clearAuthorityKeyring(keyring)
			return nil, errors.New("Stage Worker authority keyring contains a duplicate key id")
		}
		var encoded string
		if err := decoder.Decode(&encoded); err != nil {
			clearAuthorityKeyring(keyring)
			return nil, errors.New("Stage Worker authority keyring values must be base64 strings")
		}
		key, decodeErr := base64.StdEncoding.Strict().DecodeString(encoded)
		if decodeErr != nil || len(key) < 32 || len(key) > 4096 {
			clear(key)
			clearAuthorityKeyring(keyring)
			return nil, errors.New("Stage Worker authority keys must encode 32 to 4096 bytes")
		}
		keyring[keyID] = key
		if len(keyring) > 32 {
			clearAuthorityKeyring(keyring)
			return nil, errors.New("Stage Worker authority keyring contains too many keys")
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		clearAuthorityKeyring(keyring)
		return nil, errors.New("Stage Worker authority keyring JSON object is incomplete")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		clearAuthorityKeyring(keyring)
		return nil, errors.New("Stage Worker authority keyring must contain one JSON document")
	}
	if len(keyring) == 0 {
		return nil, errors.New("Stage Worker authority keyring contains no keys")
	}
	return keyring, nil
}

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

func clearAuthorityKeyring(keyring map[string][]byte) {
	for keyID, key := range keyring {
		clear(key)
		delete(keyring, keyID)
	}
}
