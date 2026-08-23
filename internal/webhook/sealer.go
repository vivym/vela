package webhook

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

const maxWebhookEncryptionKeys = 32

type SealedSecret struct {
	KeyID      string
	Nonce      []byte
	Ciphertext []byte
}

type SecretSealer interface {
	Seal(plaintext, associatedData []byte) (SealedSecret, error)
	Open(sealed SealedSecret, associatedData []byte) ([]byte, error)
}

type AESGCMSealer struct {
	activeKeyID string
	keys        map[string][]byte
}

func NewAESGCMSealer(activeKeyID string, keys map[string][]byte) (*AESGCMSealer, error) {
	if activeKeyID == "" {
		return nil, errors.New("active webhook encryption key id is required")
	}
	if len(keys) == 0 || len(keys) > maxWebhookEncryptionKeys {
		return nil, fmt.Errorf("webhook encryption keyring must contain between 1 and %d keys", maxWebhookEncryptionKeys)
	}
	copied := make(map[string][]byte, len(keys))
	for keyID, key := range keys {
		if keyID == "" || len(keyID) > 200 {
			return nil, errors.New("webhook encryption keyring contains an invalid key id")
		}
		if len(key) != 32 {
			return nil, fmt.Errorf("webhook encryption key %q must contain exactly 32 bytes", keyID)
		}
		copied[keyID] = append([]byte(nil), key...)
	}
	if _, ok := copied[activeKeyID]; !ok {
		return nil, fmt.Errorf("active webhook encryption key %q is absent from the keyring", activeKeyID)
	}
	return &AESGCMSealer{activeKeyID: activeKeyID, keys: copied}, nil
}

func (s *AESGCMSealer) Seal(plaintext, associatedData []byte) (SealedSecret, error) {
	if s == nil {
		return SealedSecret{}, errors.New("webhook secret sealer is not configured")
	}
	if len(plaintext) == 0 || len(associatedData) == 0 {
		return SealedSecret{}, errors.New("webhook secret and associated identity are required")
	}
	aead, err := s.aead(s.activeKeyID)
	if err != nil {
		return SealedSecret{}, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return SealedSecret{}, fmt.Errorf("generate webhook secret nonce: %w", err)
	}
	return SealedSecret{
		KeyID:      s.activeKeyID,
		Nonce:      nonce,
		Ciphertext: aead.Seal(nil, nonce, plaintext, associatedData),
	}, nil
}

func (s *AESGCMSealer) Open(sealed SealedSecret, associatedData []byte) ([]byte, error) {
	if s == nil {
		return nil, errors.New("webhook secret sealer is not configured")
	}
	if len(associatedData) == 0 {
		return nil, errors.New("webhook secret associated identity is required")
	}
	aead, err := s.aead(sealed.KeyID)
	if err != nil {
		return nil, err
	}
	if len(sealed.Nonce) != aead.NonceSize() || len(sealed.Ciphertext) < aead.Overhead() {
		return nil, errors.New("webhook secret ciphertext is malformed")
	}
	plaintext, err := aead.Open(nil, sealed.Nonce, sealed.Ciphertext, associatedData)
	if err != nil {
		return nil, errors.New("webhook secret ciphertext authentication failed")
	}
	return plaintext, nil
}

func (s *AESGCMSealer) aead(keyID string) (cipher.AEAD, error) {
	key, ok := s.keys[keyID]
	if !ok {
		return nil, fmt.Errorf("webhook encryption key %q is unavailable", keyID)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("configure webhook secret cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("configure webhook secret authentication: %w", err)
	}
	return aead, nil
}
