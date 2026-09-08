package modelruntime

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestJournalResponseBindsDurableAcknowledgement(t *testing.T) {
	request := sha256.Sum256([]byte("command"))
	identity := ExecutionJournalIdentity{JournalID: uuid.New(), Scope: sha256.Sum256([]byte("scope"))}
	for _, fault := range []string{"none", "request", "journal", "scope", "schema", "state", "floor", "ambiguous", "uncertain", "rejected", "unknown", "duplicate", "trailing"} {
		t.Run(fault, func(t *testing.T) {
			receipt := &JournalMutationReceipt{SchemaVersion: 1, RequestDigest: request, JournalID: identity.JournalID,
				JournalScope: identity.Scope, StateDigest: sha256.Sum256([]byte("state")), Highest: 1, Floor: 2}
			response := JournalEndpointResponse{SchemaVersion: 1, RequestDigest: request, Receipt: receipt}
			switch fault {
			case "request":
				receipt.RequestDigest[0]++
			case "journal":
				receipt.JournalID = uuid.New()
			case "scope":
				receipt.JournalScope[0]++
			case "schema":
				receipt.SchemaVersion++
			case "state":
				receipt.StateDigest = [32]byte{}
			case "floor":
				receipt.Floor = -1
			case "ambiguous":
				response.Error = "REJECTED"
			case "uncertain":
				response.Receipt, response.Error = nil, "UNCERTAIN"
			case "rejected":
				response.Receipt, response.Error = nil, "REJECTED"
			}
			wire, err := json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "unknown":
				wire = append(wire[:len(wire)-1], []byte(`,"startup_permission":true}`)...)
			case "duplicate":
				wire = append(wire[:len(wire)-1], []byte(`,"schema_version":1}`)...)
			case "trailing":
				wire = append(wire, ' ')
			}
			got, err := parseJournalResponse(wire, request, identity)
			if fault == "none" {
				if err != nil || got != *receipt {
					t.Fatalf("legal acknowledgement: %+v %v", got, err)
				}
			} else if err == nil || got != (JournalMutationReceipt{}) || fault == "uncertain" && !errors.Is(err, ErrExecutionStateRecovery) {
				t.Fatalf("invalid/uncertain reply produced acknowledgement: %+v %v", got, err)
			}
		})
	}
}

func TestJournalResponseReadFailuresAreNotTransportUnavailable(t *testing.T) {
	request := sha256.Sum256([]byte("read"))
	for _, fault := range []string{"uncertain", "rejected", "unknown", "changed", "wrong-request", "ambiguous", "mutation", "trailing", "duplicate"} {
		t.Run(fault, func(t *testing.T) {
			response := JournalEndpointResponse{SchemaVersion: 1, RequestDigest: request, Error: "REJECTED"}
			want := ErrJournalCommand
			switch fault {
			case "uncertain":
				response.Error, want = "UNCERTAIN", ErrExecutionStateRecovery
			case "changed":
				response.Error, want = "CHANGED", ErrJournalChanged
			case "unknown":
				response.Error = "BUSY"
			case "wrong-request":
				response.RequestDigest[0]++
			case "ambiguous":
				response.Page = &JournalPage{}
			case "mutation":
				response.Error, response.Receipt = "", &JournalMutationReceipt{}
			}
			wire, err := json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			if fault == "trailing" {
				wire = append(wire, ' ')
			}
			if fault == "duplicate" {
				wire = append(wire[:len(wire)-1], []byte(`,"schema_version":1}`)...)
			}
			page, err := parseJournalReadResponse(wire, request)
			if !errors.Is(err, want) || errors.Is(err, ErrJournalReadUnavailable) || page.Document != nil || page.LockDocument != nil {
				t.Fatalf("owner/protocol failure was downgraded or exposed data: %+v %v", page, err)
			}
		})
	}
}
