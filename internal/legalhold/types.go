package legalhold

import (
	"context"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
)

type Kind string

const (
	KindHoldPlaced   Kind = "HOLD_PLACED"
	KindHoldReleased Kind = "HOLD_RELEASED"
)

type Scope string

const (
	ScopeOrganization Scope = "ORGANIZATION"
	ScopeProject      Scope = "PROJECT"
	ScopeJob          Scope = "JOB"
)

type RecordClass string

const (
	RecordClassMetadata  RecordClass = "METADATA"
	RecordClassFinancial RecordClass = "FINANCIAL"
)

type State string

const (
	StateActive   State = "ACTIVE"
	StateReleased State = "RELEASED"
)

type Identity struct {
	PrincipalID    uuid.UUID
	StableID       string
	TLSURIIdentity string
}

type Request struct {
	IdempotencyKey    string
	SourceSequence    int64
	HoldID            uuid.UUID
	Kind              Kind
	Scope             Scope
	OrganizationID    uuid.UUID
	ProjectID         *uuid.UUID
	JobID             *uuid.UUID
	RecordClasses     []RecordClass
	ReasonCode        string
	ExternalReference string
	EffectiveAt       time.Time
}

type Result struct {
	EventID       uuid.UUID
	Replayed      bool
	HoldID        uuid.UUID
	State         State
	Scope         Scope
	RecordClasses []RecordClass
	RecordedAt    time.Time
	ReleasedAt    *time.Time
}

type Applier interface {
	Identity() Identity
	Apply(context.Context, Request) (Result, error)
}

func validBoundedText(value string, maximum int) bool {
	if value == "" || len(value) > maximum || strings.TrimSpace(value) != value {
		return false
	}
	return strings.IndexFunc(value, unicode.IsControl) < 0
}

func validURIIdentity(value string) bool {
	if !validBoundedText(value, 500) || strings.IndexFunc(value, unicode.IsSpace) >= 0 {
		return false
	}
	identity, err := url.Parse(value)
	return err == nil && identity.IsAbs() && identity.String() == value
}

func canonicalRecordClasses(classes []RecordClass) ([]RecordClass, bool) {
	var metadata, financial bool
	for _, class := range classes {
		switch class {
		case RecordClassMetadata:
			if metadata {
				return nil, false
			}
			metadata = true
		case RecordClassFinancial:
			if financial {
				return nil, false
			}
			financial = true
		default:
			return nil, false
		}
	}
	canonical := make([]RecordClass, 0, 2)
	if metadata {
		canonical = append(canonical, RecordClassMetadata)
	}
	if financial {
		canonical = append(canonical, RecordClassFinancial)
	}
	return canonical, len(canonical) > 0
}
