package nodeagent

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"
)

func TestRuntimeStartupAuthorityRejectsIncompleteSources(t *testing.T) {
	var authority RuntimeStartupAuthority
	if _, err := NewRuntimeStartupAuthority(RuntimeStartupAuthorityConfig{}); err == nil {
		t.Fatal("constructor accepted incomplete authority source")
	}
	if _, _, err := authority.Prepare(t.Context(), nil); err == nil {
		t.Fatal("incomplete authority source accepted")
	}
	complete := RuntimeStartupAuthority{
		Credentials:      []RuntimeCallerCredentials{{UID: 10001, GID: 10001}},
		ObserverInterval: time.Millisecond, ObserverTimeout: time.Second, ExchangeTimeout: time.Second,
		AuthorizationHash: sha256.Sum256([]byte("independent policy evidence")),
	}
	if _, _, err := complete.Prepare(context.Background(), &RuntimeCaller{}); err == nil {
		t.Fatal("authority with missing sources accepted")
	}
	complete.Credentials = []RuntimeCallerCredentials{{UID: 10001, GID: 10001}, {UID: 10001, GID: 10001}}
	if _, _, err := complete.Prepare(context.Background(), &RuntimeCaller{}); err == nil {
		t.Fatal("authority with duplicate credentials accepted")
	}
}

func TestRuntimeStartupAuthorityRejectsCredentialsOutsideVerifiedPlan(t *testing.T) {
	authority := RuntimeStartupAuthority{
		Plan:             &RuntimeLaunchPlan{uid: 10001, gid: 10002},
		Credentials:      []RuntimeCallerCredentials{{UID: 10002, GID: 10002}},
		ObserverInterval: time.Millisecond, ObserverTimeout: time.Second, ExchangeTimeout: time.Second,
		AuthorizationHash: sha256.Sum256([]byte("independent policy evidence")),
	}
	if authority.credentialsMatchPlan() {
		t.Fatal("credentials unrelated to verified launch plan accepted")
	}
	authority.Credentials = []RuntimeCallerCredentials{{UID: authority.Plan.uid, GID: authority.Plan.gid}}
	if !authority.credentialsMatchPlan() {
		t.Fatal("credentials matching verified launch plan rejected")
	}
}
