// Command schema-reference materializes repository migrations in an isolated
// PostgreSQL instance reached only through its /work Unix socket. It is an
// audit tool, not a production migration command.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	veladb "github.com/vivym/vela/internal/database"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf("usage: schema-reference <database-source-directory>; isolated /work PostgreSQL socket only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	db, err := sql.Open("pgx", "host=/work user=postgres dbname=postgres sslmode=disable")
	if err != nil {
		return err
	}
	defer db.Close()
	var tables int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'").Scan(&tables); err != nil {
		return err
	}
	if tables != 0 {
		return fmt.Errorf("reference database is not empty: %d public tables", tables)
	}
	roles, err := os.ReadFile(filepath.Join(os.Args[1], "bootstrap", "roles.sql"))
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, string(roles)); err != nil {
		return fmt.Errorf("reference roles: %w", err)
	}
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	if err := goose.UpContext(ctx, db, filepath.Join(os.Args[1], "migrations")); err != nil {
		return fmt.Errorf("reference migrations: %w", err)
	}
	version, err := goose.GetDBVersionContext(ctx, db)
	if err != nil {
		return err
	}
	fmt.Printf("reference schema migrated to %d\n", version)
	if _, err := db.ExecContext(ctx, "CREATE ROLE vela_schema_fleet_login LOGIN IN ROLE vela_fleet"); err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, "host=/work user=vela_schema_fleet_login dbname=postgres sslmode=disable")
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := veladb.VerifyRole(ctx, pool, veladb.RoleFleet); err != nil {
		return fmt.Errorf("Fleet boundary does not accept the reference migrations: %w", err)
	}
	for _, probe := range []struct{ change, restore string }{
		{"GRANT SELECT ON public.jobs TO vela_fleet", "REVOKE SELECT ON public.jobs FROM vela_fleet"},
		{"REVOKE EXECUTE ON FUNCTION public.vela_get_runtime_startup_authorization(uuid) FROM vela_fleet", "GRANT EXECUTE ON FUNCTION public.vela_get_runtime_startup_authorization(uuid) TO vela_fleet"},
	} {
		if _, err := db.ExecContext(ctx, probe.change); err != nil {
			return err
		}
		if err := veladb.VerifyRole(ctx, pool, veladb.RoleFleet); err == nil {
			return fmt.Errorf("Fleet accepted a missing or expanded privilege")
		}
		if _, err := db.ExecContext(ctx, probe.restore); err != nil {
			return err
		}
		if err := veladb.VerifyRole(ctx, pool, veladb.RoleFleet); err != nil {
			return err
		}
	}
	fmt.Println("Fleet schema-96 boundary: required privileges accepted; excess and missing privileges rejected; restoration passed")
	return nil
}
