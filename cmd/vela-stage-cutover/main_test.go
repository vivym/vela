package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivym/vela/internal/stagecutover"
)

func TestRunRequiresCommandRequestAndDatabaseURL(t *testing.T) {
	requestPath := filepath.Join(t.TempDir(), "request.json")
	if err := os.WriteFile(requestPath, []byte(`{
		"snapshot_id":"52000000-0000-0000-0000-000000000001",
		"observed_by":"operator-test"
	}`), 0o600); err != nil {
		t.Fatalf("write request: %v", err)
	}
	for _, test := range []struct {
		name      string
		arguments []string
		message   string
	}{
		{name: "command", message: "usage:"},
		{
			name:      "request",
			arguments: []string{"capture-inventory"},
			message:   "usage:",
		},
		{
			name:      "database URL",
			arguments: []string{"capture-inventory", "--request", requestPath},
			message:   "VELA_STAGE_CUTOVER_DATABASE_URL is required",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(test.arguments, "", &stdout, &stderr)
			if code != 2 || stdout.Len() != 0 ||
				!strings.Contains(stderr.String(), test.message) {
				t.Fatalf(
					"run = code %d stdout %q stderr %q",
					code, stdout.String(), stderr.String(),
				)
			}
		})
	}
}

func TestLoadRequestRejectsAmbiguousJSON(t *testing.T) {
	valid := `{
		"snapshot_id":"52000000-0000-0000-0000-000000000001",
		"observed_by":"operator-test"
	}`
	for _, test := range []struct {
		name    string
		content string
		match   string
	}{
		{
			name:    "unknown field",
			content: strings.Replace(valid, `"observed_by"`, `"unexpected":true,"observed_by"`, 1),
			match:   "unknown field",
		},
		{
			name:    "duplicate field",
			content: strings.Replace(valid, `"observed_by"`, `"observed_by":"first","observed_by"`, 1),
			match:   "duplicate JSON key",
		},
		{name: "trailing document", content: valid + `{}`, match: "trailing"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "request.json")
			if err := os.WriteFile(path, []byte(test.content), 0o600); err != nil {
				t.Fatalf("write request: %v", err)
			}
			var request stagecutover.CaptureInventoryRequest
			if err := loadRequest(path, &request); err == nil ||
				!strings.Contains(err.Error(), test.match) {
				t.Fatalf("load request error = %v, want %q", err, test.match)
			}
		})
	}
}

func TestWriteOperationErrorEmitsPostgresConstraintAsJSON(t *testing.T) {
	var stderr bytes.Buffer
	writeOperationError(
		&stderr,
		"seal_zero_backlog_failed",
		fmt.Errorf("seal Stage cutover zero backlog: %w", &pgconn.PgError{
			Code:           "55000",
			ConstraintName: "stage_cutover_zero_backlog_window_too_short",
			Message:        "Zero-backlog observation window is shorter than the cutover policy",
		}),
	)
	var result operationError
	decoder := json.NewDecoder(&stderr)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		t.Fatalf("decode operation error %q: %v", stderr.String(), err)
	}
	if result.Code != "stage_cutover_zero_backlog_window_too_short" ||
		result.SQLState != "55000" ||
		!strings.Contains(result.Message, "observation window") {
		t.Fatalf("operation error = %#v", result)
	}
}
