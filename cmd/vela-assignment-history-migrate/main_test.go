package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunRejectsInvalidArgumentsBeforeOpeningFiles(t *testing.T) {
	base := []string{"--database-url-file", "/missing/database-url", "--verifier-keyring-file", "/missing/keyring.json"}
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "missing flags"},
		{name: "missing verifier", args: base[:2]},
		{name: "missing database", args: base[2:]},
		{name: "unknown flag", args: appendArguments(base, "--unknown")},
		{name: "positional argument", args: appendArguments(base, "extra")},
		{name: "zero batch", args: appendArguments(base, "--batch-size", "0")},
		{name: "negative batch", args: appendArguments(base, "--batch-size", "-1")},
		{name: "oversized batch", args: appendArguments(base, "--batch-size", "101")},
		{name: "invalid batch", args: appendArguments(base, "--batch-size", "one")},
		{name: "zero timeout", args: appendArguments(base, "--timeout", "0s")},
		{name: "negative timeout", args: appendArguments(base, "--timeout", "-1s")},
		{name: "invalid timeout", args: appendArguments(base, "--timeout", "later")},
		{name: "invalid retirement mode", args: appendArguments(base, "--retire-unverifiable=maybe")},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(test.args, &stdout, &stderr); code != 2 || stdout.Len() != 0 || stderr.Len() == 0 {
				t.Fatalf("run = %d, stdout = %q, stderr = %q", code, stdout.String(), stderr.String())
			}
			if strings.Contains(stderr.String(), "database_url_file_invalid") {
				t.Fatal("invalid arguments reached database file loading")
			}
		})
	}
}

func TestRunAcceptsBatchBoundsAndExplicitRetirementFlags(t *testing.T) {
	for _, args := range [][]string{
		{"--batch-size", "1"},
		{"--batch-size", "100"},
		{"--retire-unverifiable"},
		{"--retire-unverifiable=false"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			args = appendArguments([]string{"--database-url-file", "/missing/database-url",
				"--verifier-keyring-file", "/missing/keyring.json"}, args...)
			if code := run(args, &stdout, &stderr); code != 1 || stdout.Len() != 0 {
				t.Fatalf("run = %d, stdout = %q, stderr = %q", code, stdout.String(), stderr.String())
			}
			assertFailureCode(t, stderr.Bytes(), "database_url_file_invalid")
		})
	}
}

func TestRunHelpNeedsNoCredentials(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--help"}, &stdout, &stderr); code != 0 || stdout.Len() != 0 ||
		!strings.Contains(stderr.String(), "retire-unverifiable") {
		t.Fatalf("help = %d, stdout = %q, stderr = %q", code, stdout.String(), stderr.String())
	}
}

func TestRunRedactsDatabaseAndVerifierConfigurationErrors(t *testing.T) {
	const secret = "assignment-migration-test-secret"
	validKeyring, err := json.Marshal(map[string]string{
		"test-public-key": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x31}, 32)),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name       string
		database   string
		keyring    []byte
		permission os.FileMode
		code       string
	}{
		{name: "database parse", database: "postgres://operator:" + secret + "@localhost:invalid-port/vela",
			keyring: validKeyring, permission: 0o600, code: "database_open_failed"},
		{name: "verifier parse", database: "postgres://operator:" + secret + "@localhost/vela",
			keyring: []byte(`{"public-key":"` + secret + `"}`), permission: 0o600, code: "verifier_keyring_invalid"},
		{name: "database permissions", database: "postgres://operator:" + secret + "@localhost/vela",
			keyring: validKeyring, permission: 0o644, code: "database_url_file_invalid"},
		{name: "empty database", database: " \n", keyring: validKeyring,
			permission: 0o600, code: "database_url_file_invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			databasePath := filepath.Join(directory, secret+"-database-url")
			keyringPath := filepath.Join(directory, secret+"-keyring.json")
			if err := os.WriteFile(databasePath, []byte(test.database), test.permission); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(databasePath, test.permission); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(keyringPath, test.keyring, 0o600); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			code := run([]string{"--database-url-file", databasePath,
				"--verifier-keyring-file", keyringPath}, &stdout, &stderr)
			if code != 1 || stdout.Len() != 0 {
				t.Fatalf("run = %d, stdout = %q, stderr = %q", code, stdout.String(), stderr.String())
			}
			assertFailureCode(t, stderr.Bytes(), test.code)
			if strings.Contains(stderr.String(), secret) || strings.Contains(stderr.String(), "postgres://") {
				t.Fatal("operation error exposed a credential, keyring value, or private file path")
			}
		})
	}
}

func appendArguments(base []string, additional ...string) []string {
	return append(append([]string(nil), base...), additional...)
}

func assertFailureCode(t *testing.T, output []byte, expected string) {
	t.Helper()
	var failure struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(output, &failure); err != nil {
		t.Fatalf("decode failure: %v", err)
	}
	if failure.Code != expected || failure.Message == "" {
		t.Fatalf("failure = %#v, want code %s", failure, expected)
	}
}
