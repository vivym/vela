package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

type RestoreReceipt struct {
	Schema                  string    `json:"schema"`
	Scope                   string    `json:"scope"`
	ProductionGate          bool      `json:"production_gate"`
	OperationID             uuid.UUID `json:"operation_id"`
	DumpSHA256              string    `json:"dump_sha256"`
	SourceSystemIdentifier  string    `json:"source_system_identifier"`
	TargetSystemIdentifier  string    `json:"target_system_identifier"`
	PostgresImageID         string    `json:"postgres_image_id"`
	TablesVerified          int       `json:"tables_verified"`
	CatalogVerified         bool      `json:"catalog_verified"`
	AdmissionClosed         bool      `json:"admission_closed"`
	UnboundTenantReadDenied bool      `json:"unbound_tenant_read_denied"`
	StartedAt               time.Time `json:"started_at"`
	CompletedAt             time.Time `json:"completed_at"`
}

// RestoreDrill owns the entire target lifecycle. It accepts no target DSN,
// volume, name, or network, so it cannot overwrite an existing database.
func RestoreDrill(ctx context.Context, directory, rolesPath, image string) (result RestoreReceipt, returnedErr error) {
	manifest, err := loadManifest(directory)
	if err != nil {
		return result, err
	}
	rolesDigest, err := hashFile(rolesPath)
	if err != nil || rolesDigest != manifest.RolesSHA256 {
		return result, errors.New("recovery role definitions differ from the captured artifact")
	}
	if image == "" || strings.HasPrefix(image, "-") || strings.ContainsAny(image, " \t\r\n") {
		return result, errors.New("PostgreSQL image reference is required")
	}
	receiptPath := filepath.Join(directory, "restore-receipt.json")
	if _, err := os.Lstat(receiptPath); !os.IsNotExist(err) {
		return result, errors.New("restore receipt path already exists or cannot be inspected")
	}
	result = RestoreReceipt{Schema: "vela-db-restore-drill-v1", Scope: "DATABASE_ONLY",
		OperationID: manifest.Receipt.ID, DumpSHA256: manifest.DumpSHA256,
		SourceSystemIdentifier: manifest.Receipt.SystemIdentifier, StartedAt: time.Now().UTC()}
	name := "vela-restore-" + uuid.NewString()
	containerOutput, err := docker(ctx, nil, "run", "--detach", "--network=none", "--name="+name,
		"--label=vela.ai/recovery-drill="+name, "--env=POSTGRES_HOST_AUTH_METHOD=trust", image)
	if err != nil {
		return result, err
	}
	containerID := strings.TrimSpace(string(containerOutput))
	if !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(containerID) {
		return result, errors.New("docker did not return an exact disposable container identity")
	}
	cleaned := false
	cleanup := func() error {
		if cleaned {
			return nil
		}
		cleanupContext, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, err := docker(cleanupContext, nil, "rm", "--force", "--volumes", containerID)
		if err != nil {
			return fmt.Errorf("remove isolated recovery container %s: %w", containerID, err)
		}
		cleaned = true
		return nil
	}
	defer func() {
		returnedErr = errors.Join(returnedErr, cleanup())
	}()
	for {
		if _, err := docker(ctx, nil, "exec", containerID, "pg_isready", "--host=127.0.0.1", "--username=postgres"); err == nil {
			break
		}
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, ctx.Err()
		case <-timer.C:
		}
	}
	imageOutput, err := docker(ctx, nil, "inspect", "--format={{.Image}}", containerID)
	if err != nil {
		return result, err
	}
	result.PostgresImageID = strings.TrimSpace(string(imageOutput))
	identityOutput, err := targetQuery(ctx, containerID, "postgres", `SELECT system_identifier::text || ' ' || current_setting('server_version_num') FROM pg_control_system()`)
	if err != nil {
		return result, err
	}
	identity := strings.Fields(string(identityOutput))
	if len(identity) != 2 || !strings.HasPrefix(identity[1], "17") || len(identity[1]) != 6 || identity[0] == result.SourceSystemIdentifier {
		return result, errors.New("restore target is not an independent PostgreSQL 17 instance")
	}
	result.TargetSystemIdentifier = identity[0]
	roles, err := os.Open(rolesPath)
	if err != nil {
		return result, err
	}
	_, bootstrapErr := docker(ctx, roles, "exec", "--interactive", containerID,
		"psql", "--username=postgres", "--dbname=postgres", "--no-psqlrc", "--set=ON_ERROR_STOP=1", "--file=-")
	_ = roles.Close()
	if bootstrapErr != nil {
		return result, bootstrapErr
	}
	if _, err := docker(ctx, nil, "exec", containerID, "createdb", "--username=postgres", "--template=template0", "vela_restore"); err != nil {
		return result, err
	}
	dump, err := os.Open(filepath.Join(directory, "database.dump"))
	if err != nil {
		return result, err
	}
	_, restoreErr := docker(ctx, dump, "exec", "--interactive", containerID,
		"pg_restore", "--username=postgres", "--dbname=vela_restore", "--exit-on-error")
	_ = dump.Close()
	if restoreErr != nil {
		return result, restoreErr
	}
	tablesOutput, err := targetQuery(ctx, containerID, "vela_restore", tablesSQL)
	if err != nil {
		return result, err
	}
	if !reflect.DeepEqual(strings.Fields(string(tablesOutput)), manifest.Tables) {
		return result, errors.New("restored table inventory differs from the source snapshot")
	}
	fingerprintOutput, err := targetQuery(ctx, containerID, "vela_restore", fingerprintSQL(manifest.Tables))
	if err != nil {
		return result, err
	}
	var fingerprints map[string]TableFingerprint
	if err := json.Unmarshal(fingerprintOutput, &fingerprints); err != nil {
		return result, err
	}
	if !reflect.DeepEqual(fingerprints, manifest.Fingerprints) {
		return result, errors.New("restored row identities or content differ from the source snapshot")
	}
	result.TablesVerified = len(fingerprints)
	catalogOutput, err := targetQuery(ctx, containerID, "vela_restore", "BEGIN;"+normalizeCatalogSQL+catalogSQL+";COMMIT;")
	if err != nil {
		return result, err
	}
	if strings.TrimSpace(string(catalogOutput)) != manifest.CatalogSHA256 {
		partsOutput, err := targetQuery(ctx, containerID, "vela_restore", "BEGIN;"+normalizeCatalogSQL+catalogPartsSQL()+";COMMIT;")
		if err != nil {
			return result, err
		}
		var parts map[string]string
		if err := json.Unmarshal(partsOutput, &parts); err != nil {
			return result, err
		}
		var changed []string
		for key, expected := range manifest.CatalogParts {
			if parts[key] != expected {
				changed = append(changed, key)
			}
		}
		for key := range parts {
			if _, expected := manifest.CatalogParts[key]; !expected {
				changed = append(changed, key)
			}
		}
		sort.Strings(changed)
		return result, fmt.Errorf("restored catalog differs: %s", strings.Join(changed, ", "))
	}
	result.CatalogVerified = true
	gateOutput, err := targetQuery(ctx, containerID, "vela_restore", `SELECT NOT admission_open FROM recovery_admission_control WHERE singleton`)
	if err != nil || strings.TrimSpace(string(gateOutput)) != "t" {
		return result, errors.New("restored Admission is not closed")
	}
	result.AdmissionClosed = true
	// An unbound request identity must never read any customer Job. This is a
	// read-only RLS probe, followed by an authority write probe rolled back below.
	isolationOutput, err := targetQuery(ctx, containerID, "vela_restore", `SET ROLE vela_request; SELECT count(*) FROM jobs; RESET ROLE`)
	if err != nil || strings.TrimSpace(string(isolationOutput)) != "0" {
		return result, errors.New("unbound request role can read restored Customer Jobs")
	}
	result.UnboundTenantReadDenied = true
	_, err = targetQuery(ctx, containerID, "vela_restore", `SET ROLE vela_recovery;
		DO $probe$ DECLARE rejected_constraint text; BEGIN
		  BEGIN
		    PERFORM vela_reopen_recovery_admission('`+manifest.Receipt.ID.String()+`',`+fmt.Sprint(manifest.Receipt.Generation)+`);
		  EXCEPTION WHEN SQLSTATE '55000' THEN
		    GET STACKED DIAGNOSTICS rejected_constraint = CONSTRAINT_NAME;
		    IF rejected_constraint = 'recovery_operation_stale' THEN RETURN; END IF;
		    RAISE;
		  END;
		  RAISE EXCEPTION 'Restored source operation unexpectedly reopened Admission';
		END $probe$;`)
	if err != nil {
		return result, errors.New("restored source-operation fencing probe failed")
	}
	if err := cleanup(); err != nil {
		return result, err
	}
	result.CompletedAt = time.Now().UTC()
	if err := writeJSONExclusive(receiptPath, result); err != nil {
		return result, err
	}
	return result, nil
}

func loadManifest(directory string) (Manifest, error) {
	file, err := os.Open(filepath.Join(directory, "snapshot.json"))
	if err != nil {
		return Manifest{}, err
	}
	defer func() { _ = file.Close() }()
	var manifest Manifest
	decoder := json.NewDecoder(io.LimitReader(file, 4<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return Manifest{}, errors.New("snapshot manifest has trailing or oversized content")
	}
	if manifest.Schema != "vela-db-snapshot-v1" || manifest.Scope != "DATABASE_ONLY" || manifest.ProductionGate ||
		len(manifest.Tables) == 0 || len(manifest.Tables) != len(manifest.Fingerprints) || manifest.SnapshotID == "" {
		return Manifest{}, errors.New("snapshot manifest scope or inventory is invalid")
	}
	if err := manifest.Receipt.Validate(); err != nil {
		return Manifest{}, err
	}
	digest, err := hashFile(filepath.Join(directory, "database.dump"))
	if err != nil || digest != manifest.DumpSHA256 {
		return Manifest{}, errors.New("database dump does not match the captured SHA256")
	}
	return manifest, nil
}

func targetQuery(ctx context.Context, container, database, query string) ([]byte, error) {
	return docker(ctx, strings.NewReader(query), "exec", "--interactive", container,
		"psql", "--username=postgres", "--dbname="+database, "--no-psqlrc", "--quiet", "--tuples-only", "--no-align", "--set=ON_ERROR_STOP=1", "--file=-")
}

func docker(ctx context.Context, input io.Reader, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, "docker", arguments...)
	command.Stdin = input
	var output bytes.Buffer
	command.Stdout = &output
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("disposable recovery Docker operation %s failed: %w", arguments[0], err)
	}
	return output.Bytes(), nil
}
