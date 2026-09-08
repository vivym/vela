package modelruntime

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/vivym/vela/internal/strictjson"
)

// Two maximum-size protobuf envelopes plus JSON base64 framing fit this bound.
// Startup permission and full journal snapshots are not commands in this API.
const MaximumJournalCommandBytes = 192 << 10

var ErrJournalCommand = errors.New("invalid typed execution journal command")

type JournalAuthorityCommand struct {
	Authority []byte `json:"authority"`
}

type JournalCandidatesCommand struct {
	Authority []byte `json:"authority"`
	Confirmed []byte `json:"confirmed,omitempty"`
}

type JournalSealCommand struct {
	Authority []byte `json:"authority"`
	Receipt   []byte `json:"receipt"`
}

type JournalDrainCommand struct {
	Authority []byte       `json:"authority"`
	Drain     BackendDrain `json:"drain"`
}

type JournalHealthCommand struct {
	Authority []byte           `json:"authority"`
	Evidence  *FailureEvidence `json:"evidence"`
}

type JournalFloorCommand struct {
	Disposition []byte `json:"disposition"`
}

type JournalTerminalNonAdmissionCommand struct {
	Disposition []byte `json:"disposition"`
	Allocation  string `json:"allocation"`
}

// JournalReadCommand selects a bounded page of the current owner document.
// A nonzero digest pins subsequent pages; a changed document is refused.
type JournalReadCommand struct {
	StateDigest [32]byte `json:"state_digest"`
	Offset      int      `json:"offset"`
}

// Exactly one operation is required. Role, route, clock, storage, initialization
// and replacement state are resolved by the owner, never accepted on the wire.
type JournalCommand struct {
	SchemaVersion        int                                 `json:"schema_version"`
	Admit                *JournalAuthorityCommand            `json:"admit,omitempty"`
	Candidates           *JournalCandidatesCommand           `json:"candidates,omitempty"`
	Seal                 *JournalSealCommand                 `json:"seal,omitempty"`
	Drain                *JournalDrainCommand                `json:"drain,omitempty"`
	Health               *JournalHealthCommand               `json:"health,omitempty"`
	Floor                *JournalFloorCommand                `json:"floor,omitempty"`
	NonAdmission         *JournalAuthorityCommand            `json:"non_admission,omitempty"`
	TerminalNonAdmission *JournalTerminalNonAdmissionCommand `json:"terminal_non_admission,omitempty"`
	Read                 *JournalReadCommand                 `json:"read,omitempty"`
}

func EncodeJournalCommand(command JournalCommand) ([]byte, error) {
	if err := command.validate(); err != nil {
		return nil, err
	}
	wire, err := json.Marshal(command)
	if err != nil || len(wire) > MaximumJournalCommandBytes {
		return nil, ErrJournalCommand
	}
	return wire, nil
}

func ParseJournalCommand(wire []byte) (JournalCommand, error) {
	var command JournalCommand
	if len(wire) == 0 || len(wire) > MaximumJournalCommandBytes || strictjson.RejectDuplicateKeys(wire) != nil {
		return command, ErrJournalCommand
	}
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&command) != nil || !errors.Is(decoder.Decode(&struct{}{}), io.EOF) {
		return JournalCommand{}, ErrJournalCommand
	}
	canonical, err := EncodeJournalCommand(command)
	if err != nil || !bytes.Equal(canonical, wire) {
		return JournalCommand{}, ErrJournalCommand
	}
	return command, nil
}

func (command JournalCommand) validate() error {
	if command.SchemaVersion != 1 {
		return ErrJournalCommand
	}
	count := 0
	for _, selected := range []bool{command.Admit != nil, command.Candidates != nil, command.Seal != nil, command.Drain != nil,
		command.Health != nil, command.Floor != nil, command.NonAdmission != nil, command.TerminalNonAdmission != nil, command.Read != nil} {
		if selected {
			count++
		}
	}
	if count != 1 {
		return ErrJournalCommand
	}
	return nil
}
