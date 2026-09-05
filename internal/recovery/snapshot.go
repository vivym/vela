package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type DumpFunc func(context.Context, string, io.Writer) error

type TableFingerprint struct {
	Rows   int64  `json:"rows"`
	SHA256 string `json:"sha256"`
}

type Manifest struct {
	Schema         string                      `json:"schema"`
	Scope          string                      `json:"scope"`
	ProductionGate bool                        `json:"production_gate"`
	Receipt        Receipt                     `json:"quiescence"`
	SnapshotID     string                      `json:"snapshot_id"`
	CapturedAt     time.Time                   `json:"captured_at"`
	DumpSHA256     string                      `json:"dump_sha256"`
	RolesSHA256    string                      `json:"roles_sha256"`
	OperatorSHA256 string                      `json:"operator_sha256"`
	Tables         []string                    `json:"tables"`
	Fingerprints   map[string]TableFingerprint `json:"fingerprints"`
	CatalogSHA256  string                      `json:"catalog_sha256"`
	CatalogParts   map[string]string           `json:"catalog_parts"`
}

const tablesSQL = `SELECT relname FROM pg_catalog.pg_class
WHERE relnamespace = 'public'::regnamespace AND relkind = 'r' ORDER BY relname`

// Bind the schema, owners, grants, RLS policies, and authority function bodies,
// as well as data. PostgreSQL object OIDs differ in an independent restore.
const catalogObjectSQL = `jsonb_build_object(
    'tables', (SELECT jsonb_agg(jsonb_build_array(c.relname, pg_get_userbyid(c.relowner), c.relrowsecurity, c.relforcerowsecurity,
        (SELECT jsonb_agg(a::text ORDER BY a::text) FROM unnest(COALESCE(c.relacl,acldefault(CASE WHEN c.relkind='S' THEN 's'::"char" ELSE 'r'::"char" END,c.relowner))) a)) ORDER BY c.relname)
        FROM pg_class c WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r','S','v')),
    'columns', (SELECT jsonb_agg(jsonb_build_array(c.relname,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull)
        ORDER BY c.relname,a.attnum) FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
        WHERE c.relnamespace='public'::regnamespace AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped),
    'policies', (SELECT jsonb_agg(jsonb_build_array(tablename,policyname,permissive,roles,cmd,qual,with_check) ORDER BY tablename,policyname)
        FROM pg_policies WHERE schemaname='public'),
    'constraints', (SELECT jsonb_agg(jsonb_build_array(c.relname,k.conname,COALESCE(n.definition,pg_get_constraintdef(k.oid))) ORDER BY c.relname,k.conname)
        FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
        LEFT JOIN pg_temp.vela_recovery_normalized_checks n ON n.table_name=c.relname AND n.constraint_name=k.conname
        WHERE c.relnamespace='public'::regnamespace),
    'functions', (SELECT jsonb_agg(jsonb_build_array(p.proname,pg_get_function_identity_arguments(p.oid),pg_get_functiondef(p.oid),
        pg_get_userbyid(p.proowner),(SELECT jsonb_agg(a::text ORDER BY a::text) FROM unnest(COALESCE(p.proacl,acldefault('f',p.proowner))) a))
        ORDER BY p.proname,pg_get_function_identity_arguments(p.oid)) FROM pg_proc p
        WHERE p.pronamespace='public'::regnamespace AND p.prokind='f')
)`

const catalogSQL = `SELECT encode(sha256(convert_to(` + catalogObjectSQL + `::text,'UTF8')),'hex')`

// Dump/restore reparses CHECK expressions, flattening equivalent Boolean
// groups. Use PostgreSQL's parser to normalize them on both sides, never
// remove expression text or approximate SQL with string substitutions.
const normalizeCatalogSQL = `DO $normalize$
DECLARE constraint_record record; normalized text;
BEGIN
    CREATE TEMP TABLE vela_recovery_normalized_checks(table_name text,constraint_name text,definition text) ON COMMIT DROP;
    FOR constraint_record IN SELECT c.relname,k.conname,pg_get_constraintdef(k.oid) AS definition
        FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
        WHERE c.relnamespace='public'::regnamespace AND k.contype='c'
    LOOP
        EXECUTE format('CREATE TEMP TABLE vela_recovery_check_parse (LIKE public.%I)',constraint_record.relname);
        EXECUTE format('ALTER TABLE pg_temp.vela_recovery_check_parse ADD CONSTRAINT parsed %s',constraint_record.definition);
        SELECT pg_get_constraintdef(oid) INTO STRICT normalized FROM pg_constraint
            WHERE conrelid='pg_temp.vela_recovery_check_parse'::regclass AND conname='parsed';
        INSERT INTO vela_recovery_normalized_checks VALUES(constraint_record.relname,constraint_record.conname,normalized);
        DROP TABLE pg_temp.vela_recovery_check_parse;
    END LOOP;
END $normalize$;`

func Capture(ctx context.Context, connection *pgx.Conn, operation uuid.UUID, directory, rolesPath string, dump DumpFunc) (Manifest, error) {
	if connection == nil || operation == uuid.Nil || dump == nil || !filepath.IsAbs(directory) {
		return Manifest{}, errors.New("source connection, operation, dump executor, and absolute new output directory are required")
	}
	rolesDigest, err := hashFile(rolesPath)
	if err != nil {
		return Manifest{}, fmt.Errorf("hash recovery role definitions: %w", err)
	}
	executable, err := os.Executable()
	if err != nil {
		return Manifest{}, err
	}
	operatorDigest, err := hashFile(executable)
	if err != nil {
		return Manifest{}, err
	}
	transaction, err := connection.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return Manifest{}, err
	}
	defer func() { _ = transaction.Rollback(context.Background()) }()
	var receiptBytes []byte
	var systemID string
	var databaseName string
	var databaseOID uint32
	var version int
	// This shared gate lock prevents reopening while pg_dump imports our exact
	// snapshot. No application table writes or locks are performed here.
	err = transaction.QueryRow(ctx, `SELECT receipt.receipt,
		(SELECT system_identifier::text FROM pg_control_system()), current_database(),
		(SELECT oid FROM pg_database WHERE datname=current_database()), current_setting('server_version_num')::integer
		FROM recovery_admission_control gate JOIN recovery_quiescence_receipts receipt ON receipt.operation_id=gate.operation_id
		WHERE gate.singleton AND NOT gate.admission_open AND gate.operation_id=$1 FOR SHARE OF gate`, operation).
		Scan(&receiptBytes, &systemID, &databaseName, &databaseOID, &version)
	if err != nil {
		return Manifest{}, fmt.Errorf("read closed source gate and receipt: %w", err)
	}
	manifest := Manifest{Schema: "vela-db-snapshot-v1", Scope: "DATABASE_ONLY", RolesSHA256: rolesDigest, OperatorSHA256: operatorDigest}
	if err := json.Unmarshal(receiptBytes, &manifest.Receipt); err != nil {
		return Manifest{}, err
	}
	if err := manifest.Receipt.Validate(); err != nil {
		return Manifest{}, err
	}
	if version/10000 != 17 || systemID != manifest.Receipt.SystemIdentifier || databaseName != manifest.Receipt.DatabaseName || databaseOID != manifest.Receipt.DatabaseOID {
		return Manifest{}, errors.New("snapshot source differs from the PostgreSQL 17 quiescence authority")
	}
	var inventory map[string]int64
	if err := transaction.QueryRow(ctx, `SELECT vela_recovery_inventory()`).Scan(&inventory); err != nil {
		return Manifest{}, err
	}
	for _, count := range inventory {
		if count != 0 {
			return Manifest{}, errors.New("source authority became nonquiescent")
		}
	}
	rows, err := transaction.Query(ctx, tablesSQL)
	if err != nil {
		return Manifest{}, err
	}
	manifest.Tables, err = pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return Manifest{}, err
	}
	if err := transaction.QueryRow(ctx, fingerprintSQL(manifest.Tables)).Scan(&manifest.Fingerprints); err != nil {
		return Manifest{}, fmt.Errorf("fingerprint source data: %w", err)
	}
	if _, err := transaction.Exec(ctx, normalizeCatalogSQL); err != nil {
		return Manifest{}, fmt.Errorf("normalize source CHECK constraints: %w", err)
	}
	if err := transaction.QueryRow(ctx, catalogSQL).Scan(&manifest.CatalogSHA256); err != nil {
		return Manifest{}, fmt.Errorf("fingerprint source catalog: %w", err)
	}
	if err := transaction.QueryRow(ctx, catalogPartsSQL()).Scan(&manifest.CatalogParts); err != nil {
		return Manifest{}, fmt.Errorf("fingerprint source catalog parts: %w", err)
	}
	if err := transaction.QueryRow(ctx, `SELECT pg_export_snapshot(),clock_timestamp()`).Scan(&manifest.SnapshotID, &manifest.CapturedAt); err != nil {
		return Manifest{}, err
	}
	if err := os.Mkdir(directory, 0o700); err != nil {
		return Manifest{}, fmt.Errorf("create private nonexisting evidence directory: %w", err)
	}
	file, err := os.OpenFile(filepath.Join(directory, "database.dump"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return Manifest{}, err
	}
	defer func() { _ = file.Close() }()
	digest := sha256.New()
	if err := dump(ctx, manifest.SnapshotID, io.MultiWriter(file, digest)); err != nil {
		return Manifest{}, fmt.Errorf("export source database; incomplete evidence retained: %w", err)
	}
	if err := file.Sync(); err != nil {
		return Manifest{}, err
	}
	if err := file.Close(); err != nil {
		return Manifest{}, err
	}
	manifest.DumpSHA256 = hex.EncodeToString(digest.Sum(nil))
	if err := transaction.Commit(ctx); err != nil {
		return Manifest{}, err
	}
	if err := writeJSONExclusive(filepath.Join(directory, "snapshot.json"), manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func PGDump(binary, dsn string) DumpFunc {
	return func(ctx context.Context, snapshot string, destination io.Writer) error {
		command := exec.CommandContext(ctx, binary, "--format=custom", "--snapshot="+snapshot)
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, "PG") {
				command.Env = append(command.Env, entry)
			}
		}
		command.Env = append(command.Env, "PGDATABASE="+dsn, "PGCONNECT_TIMEOUT=10")
		command.Stdout = destination
		if err := command.Run(); err != nil {
			return fmt.Errorf("pg_dump did not complete: %w", err)
		}
		return nil
	}
}

func fingerprintSQL(tables []string) string {
	parts := make([]string, 0, len(tables))
	for _, table := range tables {
		name := pgx.Identifier{"public", table}.Sanitize()
		literal := "'" + strings.ReplaceAll(table, "'", "''") + "'"
		parts = append(parts, `SELECT `+literal+` AS name,jsonb_build_object('rows',count(*),'sha256',
		encode(sha256(convert_to(COALESCE(string_agg(encode(sha256(convert_to(to_jsonb(record)::text,'UTF8')),'hex'),''
		ORDER BY encode(sha256(convert_to(to_jsonb(record)::text,'UTF8')),'hex')),''),'UTF8')),'hex')) AS fingerprint FROM `+name+` record`)
	}
	return `SELECT jsonb_object_agg(name,fingerprint) FROM (` + strings.Join(parts, " UNION ALL ") + `) tables`
}

func catalogPartsSQL() string {
	return `SELECT jsonb_object_agg(category.key || '/' || (entry.value->>0) || '/' || (entry.value->>1),
		encode(sha256(convert_to(entry.value::text,'UTF8')),'hex')) FROM jsonb_each(` + catalogObjectSQL + `) category,
		LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(category.value)='array' THEN category.value ELSE '[]'::jsonb END) entry`
}

func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func writeJSONExclusive(path string, value any) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = parent.Close() }()
	return parent.Sync()
}
