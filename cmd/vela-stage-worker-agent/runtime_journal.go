package main

import (
	"crypto/sha256"
	"errors"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

// Only mutation acknowledgements use this identity. Original storage ownership
// remains with Node; UUID/scope alone must never authorize snapshot adoption.
func runtimeJournalIdentity(binding *velav1.WorkerBootstrapBinding) (modelruntime.ExecutionJournalIdentity, error) {
	var identity modelruntime.ExecutionJournalIdentity
	id, err := uuid.Parse(binding.GetPair().GetRuntimeJournalId())
	if err != nil || id == uuid.Nil || len(binding.GetPair().GetRuntimeScope()) != sha256.Size {
		return identity, errors.New("verified Runtime journal binding identity is invalid")
	}
	identity.JournalID = id
	copy(identity.Scope[:], binding.GetPair().GetRuntimeScope())
	if identity.Scope == ([sha256.Size]byte{}) {
		return modelruntime.ExecutionJournalIdentity{}, errors.New("verified Runtime journal binding scope is empty")
	}
	return identity, nil
}
