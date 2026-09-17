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
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
)

func (signer *AuthorizationSigner) Publish(ctx context.Context, directory string, authorization Authorization) error {
	if signer == nil {
		return errors.New("runtime policy authorization signer is unavailable")
	}
	if err := VerifyAuthorization(authorization, authorization.Request(), signer.privateKey.Public().(ed25519.PublicKey), signer.now().UTC()); err != nil {
		return err
	}
	return PublishAuthorization(ctx, directory, authorization)
}

// PublishAuthorization atomically installs one Fleet-signed authorization in
// the issuer's root-owned inbox. Existing identical bytes are idempotent;
// replacing an operation with different bytes is rejected.
func PublishAuthorization(ctx context.Context, directory string, authorization Authorization) error {
	if ctx == nil || !canonical(directory) || directory == "/" || authorization.OperationID == uuid.Nil ||
		len(authorization.Signature) != ed25519.SignatureSize {
		return errors.New("runtime policy authorization publication is invalid")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := securefile.ResolveTrustedDirectory(directory); err != nil {
		return err
	}
	wire, err := json.Marshal(authorization)
	if err != nil || len(wire) > maxAuthorizationBytes {
		return errors.New("runtime policy authorization publication is too large")
	}
	target := AuthorizationPath(directory, authorization.OperationID)
	if target == "" {
		return errors.New("runtime policy authorization operation is invalid")
	}
	if existing, readErr := os.ReadFile(target); readErr == nil {
		if string(existing) == string(wire) {
			return nil
		}
		return errors.New("runtime policy authorization operation already has different bytes")
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	temporary, err := os.CreateTemp(directory, ".authorization-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	cleanup := func() { _ = temporary.Close(); _ = os.Remove(temporaryName) }
	defer cleanup()
	if err := temporary.Chmod(0o600); err != nil {
		return err
	}
	if _, err := temporary.Write(wire); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Link(temporaryName, target); err != nil {
		if errors.Is(err, os.ErrExist) {
			if existing, readErr := os.ReadFile(target); readErr == nil && string(existing) == string(wire) {
				return nil
			}
		}
		return fmt.Errorf("publish runtime policy authorization: %w", err)
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer func(cleanup func() error) { _ = cleanup() }(directoryFile.Close)
	if err := directoryFile.Sync(); err != nil {
		return err
	}
	return nil
}

func PublishAuthorizationWire(ctx context.Context, directory string, wire []byte, publicKey ed25519.PublicKey, now time.Time) error {
	if len(publicKey) != ed25519.PublicKeySize || len(wire) == 0 || len(wire) > maxAuthorizationBytes {
		return errors.New("runtime policy authorization wire is invalid")
	}
	if err := strictjson.RejectDuplicateKeys(wire); err != nil {
		return err
	}
	var authorization Authorization
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&authorization); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("runtime policy authorization wire has trailing data")
	}
	request := authorization.Request()
	if err := VerifyAuthorization(authorization, request, publicKey, now.UTC()); err != nil {
		return err
	}
	return PublishAuthorization(ctx, directory, authorization)
}
