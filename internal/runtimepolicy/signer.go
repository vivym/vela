package runtimepolicy

import (
	"context"
	"crypto/ed25519"
	"errors"
	"time"
)

// AuthorizationSigner is the Fleet-side producer. It signs only a request
// whose reservation binding has already been computed by the authoritative
// reservation service. It does not look up history or allocate a new
// operation.
type AuthorizationSigner struct {
	privateKey ed25519.PrivateKey
	now        func() time.Time
}

func NewAuthorizationSigner(privateKey ed25519.PrivateKey, now func() time.Time) (*AuthorizationSigner, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, errors.New("runtime policy authorization signer key is invalid")
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &AuthorizationSigner{privateKey: append(ed25519.PrivateKey(nil), privateKey...), now: now}, nil
}

// Sign signs the exact request after the reservation has committed. The reply
// validity window is deliberately short and never extends past five minutes.
func (signer *AuthorizationSigner) Sign(ctx context.Context, request Request) (Authorization, error) {
	if signer == nil {
		return Authorization{}, errors.New("runtime policy authorization signer is unavailable")
	}
	return signer.SignAt(ctx, request, signer.now().UTC())
}

// SignAt is used by Fleet recovery so the first authorization and a replay
// after a process restart are byte-identical. The reservation timestamp is
// part of the signed binding and therefore supplies the stable issuance time.
func (signer *AuthorizationSigner) SignAt(ctx context.Context, request Request, issued time.Time) (Authorization, error) {
	if ctx == nil || signer == nil || !request.Valid() {
		return Authorization{}, errors.New("runtime policy authorization request is invalid")
	}
	if err := ctx.Err(); err != nil {
		return Authorization{}, err
	}
	issued = issued.UTC()
	if issued.IsZero() {
		return Authorization{}, errors.New("runtime policy authorization clock is invalid")
	}
	authorization := Authorization{
		Version: ProtocolVersion, OperationID: request.OperationID, JournalID: request.JournalID,
		RequestDigest: request.RequestDigest, ReservationDigest: request.ReservationDigest,
		IssuedAt: issued, ExpiresAt: issued.Add(2 * time.Minute),
	}
	wire, err := AuthorizationSigningBytes(authorization)
	if err != nil {
		return Authorization{}, err
	}
	authorization.Signature = ed25519.Sign(signer.privateKey, wire)
	return authorization, nil
}
