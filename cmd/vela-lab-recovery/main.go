package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/vivym/vela/internal/recovery"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "vela-lab-recovery:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, arguments []string) error {
	if len(arguments) == 0 {
		return errors.New("command required: status, quiesce, capture, restore-drill, or reopen")
	}
	flags := flag.NewFlagSet(arguments[0], flag.ContinueOnError)
	idValue := flags.String("operation", "", "existing recovery operation UUID; required for every source mutation")
	generation := flags.Int64("generation", 0, "exact closed gate generation for reopening")
	directory := flags.String("output", "", "absolute snapshot evidence directory")
	roles := flags.String("roles", "db/bootstrap/roles.sql", "release-owned recovery role bootstrap file")
	dumpBinary := flags.String("pg-dump", "pg_dump", "PostgreSQL 17 pg_dump executable")
	image := flags.String("postgres-image", "postgres:17-alpine", "disposable PostgreSQL 17 image")
	timeout := flags.Duration("timeout", 5*time.Minute, "maximum operation duration")
	if err := flags.Parse(arguments[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *timeout <= 0 {
		return errors.New("unexpected arguments or invalid timeout")
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	if arguments[0] == "restore-drill" {
		receipt, err := recovery.RestoreDrill(ctx, *directory, *roles, *image)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(receipt)
	}
	id, err := uuid.Parse(*idValue)
	if arguments[0] != "status" && (err != nil || id == uuid.Nil) {
		return errors.New("--operation must be an explicit UUID retained for retries")
	}
	variable := "VELA_RECOVERY_DATABASE_URL"
	if arguments[0] == "capture" {
		variable = "VELA_RECOVERY_BACKUP_DATABASE_URL"
	}
	dsn := os.Getenv(variable)
	if dsn == "" {
		return fmt.Errorf("%s is required", variable)
	}
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return errors.New("cannot connect to the source recovery database")
	}
	defer func() { _ = connection.Close(context.Background()) }()
	switch arguments[0] {
	case "status":
		status, err := recovery.Status(ctx, connection)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(status)
	case "quiesce":
		receipt, err := recovery.Quiesce(ctx, connection, id, time.Second)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(receipt)
	case "capture":
		manifest, err := recovery.Capture(ctx, connection, id, *directory, *roles, recovery.PGDump(*dumpBinary, dsn))
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(manifest)
	case "reopen":
		return recovery.Reopen(ctx, connection, id, *generation)
	default:
		return errors.New("unsupported recovery command")
	}
}
