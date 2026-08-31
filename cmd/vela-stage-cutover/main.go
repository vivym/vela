package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/stagecutover"
	"github.com/vivym/vela/internal/strictjson"
)

const databaseURLEnvironment = "VELA_STAGE_CUTOVER_DATABASE_URL"

func main() {
	os.Exit(run(
		os.Args[1:],
		os.Getenv(databaseURLEnvironment),
		os.Stdout,
		os.Stderr,
	))
}

func run(arguments []string, databaseURL string, stdout, stderr io.Writer) int {
	command, requestPath, valid := parseCommand(arguments)
	if !valid {
		writeUsage(stderr)
		return 2
	}
	if databaseURL == "" {
		_, _ = fmt.Fprintf(stderr, "%s is required\n", databaseURLEnvironment)
		return 2
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		writeOperationError(stderr, "database_open_failed", err)
		return 1
	}
	defer pool.Close()
	service, err := stagecutover.New(ctx, pool)
	if err != nil {
		writeOperationError(stderr, "operator_configuration_failed", err)
		return 1
	}
	result, err := execute(ctx, service, command, requestPath)
	if err != nil {
		writeOperationError(
			stderr,
			strings.ReplaceAll(command, "-", "_")+"_failed",
			err,
		)
		return 1
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(result); err != nil {
		writeOperationError(stderr, "result_encoding_failed", err)
		return 1
	}
	return 0
}

type operationError struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	SQLState string `json:"sqlstate,omitempty"`
}

func writeOperationError(writer io.Writer, fallbackCode string, err error) {
	result := operationError{Code: fallbackCode, Message: err.Error()}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		if postgresError.ConstraintName != "" {
			result.Code = postgresError.ConstraintName
		}
		result.Message = postgresError.Message
		result.SQLState = postgresError.Code
	}
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(result)
}

func parseCommand(arguments []string) (string, string, bool) {
	if len(arguments) == 0 {
		return "", "", false
	}
	command := arguments[0]
	switch command {
	case "capture-inventory", "record-external-evidence", "seal-zero-backlog",
		"prepare-legacy-h3-contraction":
	default:
		return "", "", false
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	requestPath := flags.String("request", "", "Stage cutover request JSON")
	if flags.Parse(arguments[1:]) != nil || flags.NArg() != 0 || *requestPath == "" {
		return "", "", false
	}
	return command, *requestPath, true
}

func execute(
	ctx context.Context,
	service *stagecutover.Service,
	command,
	requestPath string,
) (any, error) {
	switch command {
	case "capture-inventory":
		var request stagecutover.CaptureInventoryRequest
		if err := loadRequest(requestPath, &request); err != nil {
			return nil, err
		}
		return service.CaptureInventory(ctx, request)
	case "record-external-evidence":
		var request stagecutover.RecordExternalDrainEvidenceRequest
		if err := loadRequest(requestPath, &request); err != nil {
			return nil, err
		}
		return service.RecordExternalDrainEvidence(ctx, request)
	case "seal-zero-backlog":
		var request stagecutover.SealZeroBacklogRequest
		if err := loadRequest(requestPath, &request); err != nil {
			return nil, err
		}
		return service.SealZeroBacklog(ctx, request)
	case "prepare-legacy-h3-contraction":
		var request stagecutover.PrepareLegacyH3ContractionRequest
		if err := loadRequest(requestPath, &request); err != nil {
			return nil, err
		}
		return service.PrepareLegacyH3Contraction(ctx, request)
	default:
		return nil, errors.New("unsupported Stage cutover command")
	}
}

func loadRequest(path string, target any) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read request: %w", err)
	}
	if err := strictjson.RejectDuplicateKeys(content); err != nil {
		return fmt.Errorf("validate request JSON: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode request JSON: %w", err)
	}
	if decoder.More() {
		return errors.New("request JSON contains trailing data")
	}
	return nil
}

func writeUsage(writer io.Writer) {
	_, _ = fmt.Fprintln(
		writer,
		"usage: vela-stage-cutover <capture-inventory|record-external-evidence|seal-zero-backlog|prepare-legacy-h3-contraction> --request <request.json>",
	)
}
