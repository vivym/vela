package stageauthority

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/vivym/vela/internal/securefile"
)

const (
	maxKeyringFileBytes = 64 << 10
	maxKeyringKeys      = 32
	maxSigningKeyBytes  = 4096
)

func ReadKeyringFile(path string) (map[string][]byte, error) {
	return readKeyringFile(path, "StageAuthority", minSigningKeyBytes, maxSigningKeyBytes)
}

func ReadVerifierKeyringFile(path string) (map[string][]byte, error) {
	return readKeyringFile(path, "StageAuthority verifier", ed25519.PublicKeySize, ed25519.PublicKeySize)
}

func readKeyringFile(path, description string, minimumKeyBytes, maximumKeyBytes int) (map[string][]byte, error) {
	document, err := securefile.Read(path, maxKeyringFileBytes, true)
	if err != nil {
		return nil, fmt.Errorf("read %s keyring: %w", description, err)
	}
	defer clear(document)
	decoder := json.NewDecoder(bytes.NewReader(document))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return nil, fmt.Errorf("%s keyring must be one JSON object", description)
	}
	keyring := make(map[string][]byte)
	for decoder.More() {
		keyToken, tokenErr := decoder.Token()
		keyID, ok := keyToken.(string)
		if tokenErr != nil || !ok || keyID == "" || len(keyID) > 100 ||
			strings.TrimSpace(keyID) != keyID || strings.ContainsAny(keyID, "\x00\r\n\t ") {
			ClearKeyring(keyring)
			return nil, fmt.Errorf("%s keyring contains an invalid key id", description)
		}
		if _, duplicate := keyring[keyID]; duplicate {
			ClearKeyring(keyring)
			return nil, fmt.Errorf("%s keyring contains a duplicate key id", description)
		}
		var encoded string
		if err := decoder.Decode(&encoded); err != nil {
			ClearKeyring(keyring)
			return nil, fmt.Errorf("%s keyring values must be base64 strings", description)
		}
		key, decodeErr := base64.StdEncoding.Strict().DecodeString(encoded)
		if decodeErr != nil || len(key) < minimumKeyBytes || len(key) > maximumKeyBytes {
			clear(key)
			ClearKeyring(keyring)
			return nil, fmt.Errorf(
				"%s keys must encode %d to %d bytes", description, minimumKeyBytes, maximumKeyBytes,
			)
		}
		keyring[keyID] = key
		if len(keyring) > maxKeyringKeys {
			ClearKeyring(keyring)
			return nil, fmt.Errorf("%s keyring contains too many keys", description)
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		ClearKeyring(keyring)
		return nil, fmt.Errorf("%s keyring JSON object is incomplete", description)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		ClearKeyring(keyring)
		return nil, fmt.Errorf("%s keyring must contain one JSON document", description)
	}
	if len(keyring) == 0 {
		return nil, fmt.Errorf("%s keyring contains no keys", description)
	}
	return keyring, nil
}

func ClearKeyring(keyring map[string][]byte) {
	for keyID, key := range keyring {
		clear(key)
		delete(keyring, keyID)
	}
}
