package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(arguments []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("vela-assignment-history-migrate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	databaseURLFile := flags.String("database-url-file", "", "absolute path to the private migration-role database URL file")
	verifierKeyringFile := flags.String("verifier-keyring-file", "", "absolute path to the public StageAuthority verifier keyring JSON")
	batchSize := flags.Int("batch-size", 20, "assignment history candidates per batch, from 1 to 100")
	timeout := flags.Duration("timeout", 5*time.Minute, "deadline for the complete backfill operation")
	retireUnverifiable := flags.Bool("retire-unverifiable", false, "retire unverifiable legacy delivery payloads with a durable tombstone")
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || *databaseURLFile == "" || *verifierKeyringFile == "" ||
		*batchSize < 1 || *batchSize > 100 || *timeout <= 0 {
		flags.Usage()
		return 2
	}
	databaseURL, err := securefile.Read(*databaseURLFile, 64<<10, true)
	if err != nil {
		writeFailure(stderr, "database_url_file_invalid", "Cannot read the private database URL file")
		return 1
	}
	defer clear(databaseURL)
	if strings.TrimSpace(string(databaseURL)) == "" {
		writeFailure(stderr, "database_url_file_invalid", "Database URL file is empty")
		return 1
	}
	keyring, err := stageauthority.ReadVerifierKeyringFile(*verifierKeyringFile)
	if err != nil {
		writeFailure(stderr, "verifier_keyring_invalid", "Cannot read the public verifier keyring")
		return 1
	}
	defer stageauthority.ClearKeyring(keyring)
	validator, err := stageauthority.NewVerifier(keyring, nil)
	if err != nil {
		writeFailure(stderr, "verifier_keyring_invalid", "Cannot configure the public authority verifier")
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, strings.TrimSpace(string(databaseURL)))
	if err != nil {
		writeFailure(stderr, "database_open_failed", "Cannot configure the migration database connection")
		return 1
	}
	defer pool.Close()
	if err := veladb.VerifyRole(ctx, pool, veladb.RoleAssignmentHistoryMigration); err != nil {
		writeFailure(stderr, "database_role_invalid", "Assignment history migration role verification failed")
		return 1
	}
	var processed int64
	for {
		count, err := stageworkercontrol.BackfillAssignmentHistoryWithOptions(ctx, pool, validator, *batchSize,
			stageworkercontrol.AssignmentHistoryMigrationOptions{RetireUnverifiable: *retireUnverifiable})
		processed += int64(count)
		if err != nil {
			writeFailure(stderr, "assignment_history_backfill_failed", err.Error())
			return 1
		}
		if count == 0 {
			break
		}
	}
	if err := json.NewEncoder(stdout).Encode(struct {
		Processed int64 `json:"processed"`
	}{Processed: processed}); err != nil {
		writeFailure(stderr, "result_encoding_failed", "Cannot write the migration result")
		return 1
	}
	return 0
}

func writeFailure(writer io.Writer, code, message string) {
	_ = json.NewEncoder(writer).Encode(struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{Code: code, Message: message})
}
