package identity

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRemoteOIDCTokenVerifierReturnsVerifiedIdentity(t *testing.T) {
	key := oidcTestRSAKey(t)
	issuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/jwks" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(oidcTestJWKS(&key.PublicKey))
	}))
	t.Cleanup(issuer.Close)

	verifier, err := NewRemoteOIDCTokenVerifier(OIDCVerifierConfig{
		Issuer:     issuer.URL,
		Audience:   "vela-control",
		JWKSURL:    issuer.URL + "/jwks",
		HTTPClient: issuer.Client(),
	})
	if err != nil {
		t.Fatalf("NewRemoteOIDCTokenVerifier: %v", err)
	}
	expiresAt := time.Now().UTC().Add(10 * time.Minute).Truncate(time.Second)
	token := oidcTestToken(t, key, map[string]any{
		"iss": issuer.URL,
		"sub": "engineering@example.com",
		"aud": "vela-control",
		"exp": expiresAt.Unix(),
		"iat": time.Now().UTC().Add(-time.Minute).Unix(),
	})

	identity, err := verifier.Verify(context.Background(), token)
	if err != nil {
		t.Fatalf("Verify valid OIDC token: %v", err)
	}
	if identity.Issuer != issuer.URL || identity.Subject != "engineering@example.com" ||
		!identity.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("verified OIDC identity = %#v", identity)
	}
}

func TestRemoteOIDCTokenVerifierRejectsInvalidIdentityClaimsAndSignatures(t *testing.T) {
	key := oidcTestRSAKey(t)
	issuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/jwks" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(oidcTestJWKS(&key.PublicKey))
	}))
	t.Cleanup(issuer.Close)
	verifier, err := NewRemoteOIDCTokenVerifier(OIDCVerifierConfig{
		Issuer:     issuer.URL,
		Audience:   "vela-control",
		JWKSURL:    issuer.URL + "/jwks",
		HTTPClient: issuer.Client(),
	})
	if err != nil {
		t.Fatalf("NewRemoteOIDCTokenVerifier: %v", err)
	}
	now := time.Now().UTC()
	validClaims := func() map[string]any {
		return map[string]any{
			"iss": issuer.URL,
			"sub": "engineering@example.com",
			"aud": "vela-control",
			"exp": now.Add(10 * time.Minute).Unix(),
			"iat": now.Add(-time.Minute).Unix(),
		}
	}
	wrongSignatureKey := oidcTestRSAKey(t)
	tests := []struct {
		name  string
		token func() string
	}{
		{
			name: "wrong issuer",
			token: func() string {
				claims := validClaims()
				claims["iss"] = "https://other-issuer.example.com"
				return oidcTestToken(t, key, claims)
			},
		},
		{
			name: "wrong audience",
			token: func() string {
				claims := validClaims()
				claims["aud"] = "other-service"
				return oidcTestToken(t, key, claims)
			},
		},
		{
			name:  "wrong signature",
			token: func() string { return oidcTestToken(t, wrongSignatureKey, validClaims()) },
		},
		{
			name: "expired",
			token: func() string {
				claims := validClaims()
				claims["exp"] = now.Add(-time.Minute).Unix()
				return oidcTestToken(t, key, claims)
			},
		},
		{
			name: "missing subject",
			token: func() string {
				claims := validClaims()
				delete(claims, "sub")
				return oidcTestToken(t, key, claims)
			},
		},
		{
			name: "unknown key",
			token: func() string {
				return oidcTestTokenWithHeader(t, key, validClaims(), map[string]any{
					"alg": "RS256", "kid": "unknown-key", "typ": "JWT",
				})
			},
		},
		{
			name: "unsigned token",
			token: func() string {
				header, marshalErr := json.Marshal(map[string]any{"alg": "none", "typ": "JWT"})
				if marshalErr != nil {
					t.Fatalf("marshal unsigned token header: %v", marshalErr)
				}
				payload, marshalErr := json.Marshal(validClaims())
				if marshalErr != nil {
					t.Fatalf("marshal unsigned token claims: %v", marshalErr)
				}
				return base64.RawURLEncoding.EncodeToString(header) + "." +
					base64.RawURLEncoding.EncodeToString(payload) + "."
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, verifyErr := verifier.Verify(context.Background(), test.token()); !errors.Is(verifyErr, ErrInvalidOIDCToken) {
				t.Fatalf("Verify invalid OIDC token error = %v, want ErrInvalidOIDCToken", verifyErr)
			}
		})
	}
}

func TestRemoteOIDCTokenVerifierRejectsInsecureConfiguration(t *testing.T) {
	for _, test := range []struct {
		name   string
		config OIDCVerifierConfig
	}{
		{
			name: "HTTP issuer",
			config: OIDCVerifierConfig{
				Issuer: "http://identity.example.com", Audience: "vela-control",
				JWKSURL: "https://identity.example.com/jwks",
			},
		},
		{
			name: "HTTP JWKS",
			config: OIDCVerifierConfig{
				Issuer: "https://identity.example.com", Audience: "vela-control",
				JWKSURL: "http://identity.example.com/jwks",
			},
		},
		{
			name: "missing audience",
			config: OIDCVerifierConfig{
				Issuer: "https://identity.example.com", JWKSURL: "https://identity.example.com/jwks",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewRemoteOIDCTokenVerifier(test.config); err == nil {
				t.Fatal("insecure OIDC verifier configuration was accepted")
			}
		})
	}
}

func TestRemoteOIDCTokenVerifierAcceptsHTTPSJWKSURLWithQuery(t *testing.T) {
	if _, err := NewRemoteOIDCTokenVerifier(OIDCVerifierConfig{
		Issuer:   "https://identity.example.com",
		Audience: "vela-control",
		JWKSURL:  "https://identity.example.com/jwks?tenant=vela",
	}); err != nil {
		t.Fatalf("configure HTTPS JWKS URL with query: %v", err)
	}
}

func TestRemoteOIDCTokenVerifierRejectsHTTPSJWKSRedirectToHTTP(t *testing.T) {
	key := oidcTestRSAKey(t)
	var insecureRequests atomic.Int32
	insecureJWKS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		insecureRequests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(oidcTestJWKS(&key.PublicKey))
	}))
	t.Cleanup(insecureJWKS.Close)

	issuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/jwks" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, insecureJWKS.URL+"/keys", http.StatusFound)
	}))
	t.Cleanup(issuer.Close)
	verifier, err := NewRemoteOIDCTokenVerifier(OIDCVerifierConfig{
		Issuer: issuer.URL, Audience: "vela-control", JWKSURL: issuer.URL + "/jwks",
		HTTPClient: issuer.Client(),
	})
	if err != nil {
		t.Fatalf("NewRemoteOIDCTokenVerifier: %v", err)
	}
	token := oidcTestToken(t, key, map[string]any{
		"iss": issuer.URL,
		"sub": "engineering@example.com",
		"aud": "vela-control",
		"exp": time.Now().UTC().Add(10 * time.Minute).Unix(),
		"iat": time.Now().UTC().Add(-time.Minute).Unix(),
	})

	if _, err := verifier.Verify(context.Background(), token); !errors.Is(err, ErrInvalidOIDCToken) {
		t.Fatalf("Verify JWKS HTTPS-to-HTTP redirect error = %v, want ErrInvalidOIDCToken", err)
	}
	if requests := insecureRequests.Load(); requests != 0 {
		t.Fatalf("insecure JWKS endpoint received %d requests, want 0", requests)
	}
}

func oidcTestRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate OIDC test key: %v", err)
	}
	return key
}

func oidcTestJWKS(key *rsa.PublicKey) map[string]any {
	exponent := big.NewInt(int64(key.E)).Bytes()
	return map[string]any{"keys": []map[string]any{{
		"kty": "RSA",
		"use": "sig",
		"alg": "RS256",
		"kid": "vela-oidc-test-key",
		"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(exponent),
	}}}
}

func oidcTestToken(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	return oidcTestTokenWithHeader(t, key, claims, map[string]any{
		"alg": "RS256", "kid": "vela-oidc-test-key", "typ": "JWT",
	})
}

func oidcTestTokenWithHeader(
	t *testing.T,
	key *rsa.PrivateKey,
	claims map[string]any,
	headerClaims map[string]any,
) string {
	t.Helper()
	header, err := json.Marshal(headerClaims)
	if err != nil {
		t.Fatalf("marshal OIDC test header: %v", err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal OIDC test claims: %v", err)
	}
	encodedHeader := base64.RawURLEncoding.EncodeToString(header)
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := encodedHeader + "." + encodedPayload
	digest := sha256.Sum256([]byte(signingInput))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatalf("sign OIDC test token: %v", err)
	}
	return fmt.Sprintf(
		"%s.%s",
		signingInput,
		base64.RawURLEncoding.EncodeToString(signature),
	)
}
