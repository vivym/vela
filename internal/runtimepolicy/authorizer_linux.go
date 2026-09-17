//go:build linux

package runtimepolicy

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
)

const maxAuthorizationBytes = 64 << 10

// FileAuthorizer consumes Fleet attestations from a root-owned directory.
// Fleet is responsible for producing and signing each Authorization file;
// this process never creates an attestation and never signs a Reply without
// one. Consumption is durable and one-shot: a crash after consumption fails
// closed instead of allowing a replay.
type FileAuthorizer struct {
	directory string
	publicKey []byte
	mu        sync.Mutex
}

func NewFileAuthorizer(directory string, publicKey []byte) (*FileAuthorizer, error) {
	if !canonical(directory) || directory == "/" || len(publicKey) != ed25519.PublicKeySize {
		return nil, errors.New("runtime policy authorization directory or key is invalid")
	}
	if _, err := securefile.ResolveTrustedDirectory(directory); err != nil {
		return nil, err
	}
	return &FileAuthorizer{directory: directory, publicKey: append([]byte(nil), publicKey...)}, nil
}

func (authorizer *FileAuthorizer) Authorize(ctx context.Context, request Request) (time.Time, error) {
	if ctx == nil || authorizer == nil || !request.Valid() {
		return time.Time{}, errors.New("runtime policy authorization request is invalid")
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	authorizer.mu.Lock()
	defer authorizer.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	operationPath := filepath.Join(authorizer.directory, request.OperationID.String()+".json")
	wire, err := securefile.Read(operationPath, maxAuthorizationBytes, true)
	if err != nil {
		return time.Time{}, fmt.Errorf("read runtime policy authorization: %w", err)
	}
	if err := strictjson.RejectDuplicateKeys(wire); err != nil {
		return time.Time{}, err
	}
	var authorization Authorization
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&authorization); err != nil {
		return time.Time{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return time.Time{}, errors.New("runtime policy authorization has trailing data")
	}
	if err := VerifyAuthorization(authorization, request, ed25519.PublicKey(authorizer.publicKey), nowUTC()); err != nil {
		return time.Time{}, err
	}
	consumedPath := filepath.Join(authorizer.directory, request.OperationID.String()+".consumed")
	marker, err := os.OpenFile(consumedPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return time.Time{}, errors.New("runtime policy authorization was already consumed")
		}
		return time.Time{}, fmt.Errorf("consume runtime policy authorization: %w", err)
	}
	if _, err := marker.WriteString(request.OperationID.String() + "\n"); err != nil {
		_ = marker.Close()
		_ = os.Remove(consumedPath)
		return time.Time{}, err
	}
	if err := marker.Sync(); err != nil {
		_ = marker.Close()
		_ = os.Remove(consumedPath)
		return time.Time{}, err
	}
	if err := marker.Close(); err != nil {
		return time.Time{}, err
	}
	directoryFile, err := os.Open(authorizer.directory)
	if err != nil {
		return time.Time{}, err
	}
	defer func(cleanup func() error) { _ = cleanup() }(directoryFile.Close)
	if err := directoryFile.Sync(); err != nil {
		return time.Time{}, err
	}
	return authorization.ExpiresAt, nil
}

var nowUTC = func() time.Time { return time.Now().UTC() }

func AuthorizationPath(directory string, operationID uuid.UUID) string {
	if operationID == uuid.Nil {
		return ""
	}
	return filepath.Join(directory, operationID.String()+".json")
}
