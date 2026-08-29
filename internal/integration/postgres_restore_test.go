//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"testing"
)

func capturePostgresDatabaseSnapshot(t *testing.T, database testDatabase, path string) string {
	t.Helper()
	runPostgresContainerCommand(t, database, []string{
		"pg_dump",
		"--username=postgres",
		"--format=custom",
		"--file=" + path,
		"vela",
	})
	return path
}

func restorePostgresDatabaseSnapshot(
	t *testing.T,
	source testDatabase,
	snapshotPath string,
	databaseName string,
) testDatabase {
	t.Helper()
	runPostgresContainerCommand(t, source, []string{
		"createdb",
		"--username=postgres",
		databaseName,
	})
	runPostgresContainerCommand(t, source, []string{
		"pg_restore",
		"--username=postgres",
		"--exit-on-error",
		"--dbname=" + databaseName,
		snapshotPath,
	})

	dsn, err := url.Parse(source.DSN)
	if err != nil {
		t.Fatalf("parse source PostgreSQL DSN: %v", err)
	}
	dsn.Path = "/" + databaseName
	dsn.RawPath = ""
	restored, err := sql.Open("pgx", dsn.String())
	if err != nil {
		t.Fatalf("open restored PostgreSQL database: %v", err)
	}
	t.Cleanup(func() {
		if err := restored.Close(); err != nil {
			t.Errorf("close restored PostgreSQL database: %v", err)
		}
	})
	if err := restored.PingContext(context.Background()); err != nil {
		t.Fatalf("ping restored PostgreSQL database: %v", err)
	}
	return testDatabase{Admin: restored, DSN: dsn.String(), Container: source.Container}
}

func runPostgresContainerCommand(t *testing.T, database testDatabase, command []string) {
	t.Helper()
	if database.Container == nil {
		t.Fatal("PostgreSQL test container is unavailable")
	}
	exitCode, output, err := database.Container.Exec(context.Background(), command)
	if err != nil {
		t.Fatalf("execute PostgreSQL container command %q: %v", command[0], err)
	}
	contents, readErr := io.ReadAll(output)
	if readErr != nil {
		t.Fatalf("read PostgreSQL container command %q output: %v", command[0], readErr)
	}
	if exitCode != 0 {
		t.Fatalf(
			"PostgreSQL container command %q exited %d: %s",
			command[0],
			exitCode,
			fmt.Sprintf("%s", contents),
		)
	}
}
