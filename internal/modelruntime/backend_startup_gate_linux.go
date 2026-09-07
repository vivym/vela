package modelruntime

import (
	"context"
	"crypto/sha256"

	"github.com/vivym/vela/internal/runtimechannel"
)

func nodeBackendStartupGate(socket string) RuntimeBackendStartupGate {
	return func(ctx context.Context, request BackendStartupRequest) error {
		document, err := EncodeBackendStartupRequest(request)
		if err != nil {
			return err
		}
		response, err := runtimechannel.Exchange(ctx, socket, document)
		if err != nil {
			return err
		}
		var decision BackendStartupDecision
		if err := decodeBackendStartup(response, &decision); err != nil {
			return err
		}
		if decision.SchemaVersion != 1 || decision.RequestDigest != sha256.Sum256(document) || !decision.Permit {
			return ErrBackendStartupDenied
		}
		return nil
	}
}
