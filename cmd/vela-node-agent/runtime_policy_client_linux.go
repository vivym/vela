//go:build linux

package main

import (
	"context"

	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/runtimepolicy"
)

// externalRuntimeStartupPolicy is the Node adapter for the independently
// supervised policy issuer. The launcher control channel is deliberately not
// used as an authorization source when this adapter is configured.
type externalRuntimeStartupPolicy struct {
	client *runtimepolicy.Client
}

func newExternalRuntimeStartupPolicy(socketPath, publicKeyPath string) (*externalRuntimeStartupPolicy, error) {
	client, err := runtimepolicy.NewClient(socketPath, publicKeyPath)
	if err != nil {
		return nil, err
	}
	return &externalRuntimeStartupPolicy{client: client}, nil
}

func (policy *externalRuntimeStartupPolicy) IssueRuntimeStartupAuthorization(ctx context.Context, record nodeagent.RuntimeStartupReservationRecord) (nodeagent.RuntimeStartupAuthorizationEvidence, error) {
	if policy == nil || policy.client == nil {
		return nodeagent.RuntimeStartupAuthorizationEvidence{}, nodeagent.ErrRuntimeStartupAuthority
	}
	reservationDigest, err := runtimepolicy.ReservationBindingDigest(record.OperationID, record.JournalID, record.RequestDigest, record.ReservedAt.UTC())
	if err != nil {
		return nodeagent.RuntimeStartupAuthorizationEvidence{}, err
	}
	request := runtimepolicy.Request{
		Version:           runtimepolicy.ProtocolVersion,
		OperationID:       record.OperationID,
		JournalID:         record.JournalID,
		RequestDigest:     record.RequestDigest,
		ReservationDigest: reservationDigest,
	}
	reply, err := policy.client.Issue(ctx, request)
	if err != nil {
		return nodeagent.RuntimeStartupAuthorizationEvidence{}, err
	}
	return nodeagent.RuntimeStartupAuthorizationEvidence{
		OperationID:       reply.OperationID,
		JournalID:         reply.JournalID,
		RequestDigest:     reply.RequestDigest,
		ReservationDigest: reply.ReservationDigest,
		EvidenceDigest:    reply.EvidenceDigest,
		IssuedAt:          reply.IssuedAt,
		ExpiresAt:         reply.ExpiresAt,
	}, nil
}

var _ nodeagent.RuntimeStartupAuthorizationPolicy = (*externalRuntimeStartupPolicy)(nil)
