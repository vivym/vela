package legacyh3reachability

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/vivym/vela/internal/releasebundle"
	"github.com/vivym/vela/internal/strictjson"
)

const (
	SchemaVersion                  = 1
	ContractedReleaseSchemaVersion = 2
	MaxEvidenceSize                = 1 << 20
	ResultPass                     = "PASS"
	ResultFail                     = "FAIL"
	expectedAbsent                 = "ABSENT"
	expectedPresent                = "PRESENT"
)

var (
	digestPattern         = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	sourceRevisionPattern = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)
)

type Evidence struct {
	SchemaVersion         int       `json:"schema_version"`
	ReleaseDigest         string    `json:"release_digest"`
	ConfigurationRevision string    `json:"configuration_revision"`
	SourceRevision        string    `json:"source_revision"`
	ObservedAt            time.Time `json:"observed_at"`
	ObservedBy            string    `json:"observed_by"`
	Result                string    `json:"result"`
	Checks                []Check   `json:"checks"`
}

type Check struct {
	ID       string   `json:"id"`
	Expected string   `json:"expected"`
	Passed   bool     `json:"passed"`
	Matches  []string `json:"matches"`
}

type contentProbe struct {
	path  string
	token string
}

type checkContract struct {
	id       string
	expected string
	paths    []string
	content  []contentProbe
	bundle   func(releasebundle.Bundle) []string
}

var contract = [...]checkContract{
	{
		id: "legacy-worker-assignment-protocol", expected: expectedAbsent,
		paths: []string{
			"proto/vela/v1/runner.proto",
			"proto/vela/v1/worker_host.proto",
			"proto/gen/vela/v1/runner.pb.go",
			"proto/gen/vela/v1/runner_grpc.pb.go",
			"proto/gen/vela/v1/worker_host.pb.go",
			"proto/gen/vela/v1/worker_host_grpc.pb.go",
		},
		content: []contentProbe{
			{path: "proto/vela/v1/worker_control.proto", token: "message WorkerAssignment"},
			{path: "proto/vela/v1/worker_control.proto", token: "WorkerAssignment assignment"},
			{path: "proto/gen/vela/v1/worker_control.pb.go", token: "type WorkerAssignment struct"},
			{path: "proto/gen/vela/v1/worker_control.pb.go", token: "type ConnectResponse_Assignment struct"},
			{path: "proto/vela/v1/fleet_maintenance.proto", token: "worker_assignment_allowed"},
			{path: "proto/gen/vela/v1/fleet_maintenance.pb.go", token: "WorkerAssignmentAllowed"},
			{path: "internal/store/sqlc/querier.go", token: "GetActiveWorkerAssignment"},
			{path: "internal/store/sqlc/models.go", token: "ExecutionAuthorityKindLEGACYWORKER"},
		},
	},
	{
		id: "legacy-worker-runtime", expected: expectedAbsent,
		paths: []string{
			"cmd/vela-worker-agent",
			"internal/runnertransport",
			"internal/workeragent",
			"internal/workercontrol",
			"internal/workerhost",
			"internal/workerrecovery",
			"internal/workertransport",
			"runner",
		},
		content: []contentProbe{
			{path: "cmd/vela-control/main.go", token: `"github.com/vivym/vela/internal/workercontrol"`},
			{path: "cmd/vela-control/main.go", token: `"github.com/vivym/vela/internal/workertransport"`},
			{path: "cmd/vela-node-agent/main.go", token: `"github.com/vivym/vela/internal/workerhost"`},
			{path: "cmd/vela-node-agent/main.go", token: `"github.com/vivym/vela/internal/workerrecovery"`},
			{path: "internal/artifactvalidator/inspector.go", token: `"github.com/vivym/vela/internal/workercontrol"`},
			{path: "internal/cancellation/service.go", token: `"github.com/vivym/vela/internal/workercontrol"`},
		},
	},
	{
		id: "legacy-worker-orchestration", expected: expectedAbsent,
		paths: []string{
			"internal/scheduler",
			"internal/finalizationreconciler",
		},
		content: []contentProbe{
			{path: "internal/fleetcontroller/h3_daemonset.go", token: "func h3WorkerAgentContainer"},
			{path: "internal/fleetcontroller/h3_daemonset.go", token: "func h3RunnerContainer"},
			{path: "internal/fleet/service.go", token: "WorkerAssignmentAllowed"},
			{path: "internal/fleettransport/server.go", token: "WorkerAssignmentAllowed"},
		},
	},
	{
		id: "legacy-assignment-sql", expected: expectedAbsent,
		paths: []string{
			"db/queries/assignment.sql",
			"db/queries/scheduler.sql",
			"internal/store/sqlc/assignment.sql.go",
			"internal/store/sqlc/scheduler.sql.go",
		},
		content: []contentProbe{
			{path: "db/queries/finalization.sql", token: "LEGACY_WORKER"},
			{path: "internal/store/sqlc/finalization.sql.go", token: "LEGACY_WORKER"},
			{path: "db/queries/failure.sql", token: "LEGACY_WORKER"},
			{path: "internal/store/sqlc/failure.sql.go", token: "LEGACY_WORKER"},
		},
	},
	{
		id: "legacy-worker-deployment", expected: expectedAbsent,
		paths: []string{"deploy/worker-agent"},
		content: []contentProbe{
			{path: "deploy/fleet-controller/desired-revisions.yaml", token: "workerAgentImage:"},
			{path: "deploy/fleet-controller/desired-revisions.yaml", token: "runnerImage:"},
		},
	},
	{
		id: "legacy-release-surface", expected: expectedAbsent,
		content: []contentProbe{
			{path: "Dockerfile", token: "AS vela-worker-agent"},
			{path: "docker-bake.hcl", token: `target "vela-worker-agent"`},
			{path: "internal/releaseartifacts/oci_images.go", token: `{name: "vela-h3-runner"`},
			{path: "internal/releaseartifacts/oci_images.go", token: `{name: "vela-worker-agent"`},
			{path: "internal/releaseartifacts/host_packages.go", token: `Name: "h3-runner"`},
			{path: "internal/releasebundle/build.go", token: `"worker-agent"`},
			{path: "internal/releasebundle/build.go", token: `"h3-runner"`},
			{path: "internal/releasebundle/render.go", token: `"worker-agent":`},
		},
		bundle: legacyBundleMatches,
	},
	{
		id: "legacy-machine-h3-tests", expected: expectedAbsent,
		paths: []string{
			"internal/deploymentcontract/worker_agent_test.go",
			"internal/integration/assignment_test.go",
			"internal/integration/heartbeat_test.go",
			"internal/integration/start_test.go",
			"internal/integration/worker_recovery_conformance_test.go",
			"internal/integration/worker_transport_test.go",
		},
	},
	{
		id: "stage-worker-protocol", expected: expectedPresent,
		paths: []string{"proto/vela/v1/stage_worker_control.proto"},
	},
	{
		id: "stage-worker-runtime", expected: expectedPresent,
		paths: []string{
			"cmd/vela-stage-worker-agent/main.go",
			"internal/stageworkeragent/agent.go",
		},
	},
	{
		id: "stage-scheduler-runtime", expected: expectedPresent,
		paths: []string{"internal/stagescheduler/service.go"},
	},
	{
		id: "stage-worker-deployment", expected: expectedPresent,
		paths: []string{"deploy/stage-worker/kustomization.yaml"},
	},
	{
		id: "stage-release-surface", expected: expectedPresent,
		bundle: stageBundleMatches,
	},
}

func Scan(
	root string,
	bundle releasebundle.Bundle,
	sourceRevision,
	observedBy string,
	observedAt time.Time,
) (Evidence, []byte, string, error) {
	if bundle.SchemaVersion == ContractedReleaseSchemaVersion &&
		bundle.ConfigurationManifest.SourceRevision != sourceRevision {
		return Evidence{}, nil, "", invalid("source revision does not match the release bundle")
	}
	if err := VerifySourceCheckout(root, sourceRevision); err != nil {
		return Evidence{}, nil, "", err
	}
	return scanVerified(root, bundle, sourceRevision, observedBy, observedAt)
}

func scanVerified(
	root string,
	bundle releasebundle.Bundle,
	sourceRevision,
	observedBy string,
	observedAt time.Time,
) (Evidence, []byte, string, error) {
	information, err := os.Lstat(root)
	if err != nil || !information.IsDir() || information.Mode()&os.ModeSymlink != 0 {
		return Evidence{}, nil, "", invalid("source root must be an existing directory")
	}
	if !validText(sourceRevision, 300) || !validText(observedBy, 300) ||
		observedAt.IsZero() || observedAt.Location() != time.UTC ||
		!digestPattern.MatchString(bundle.ReleaseDigest) ||
		!digestPattern.MatchString(bundle.ConfigurationRevision) {
		return Evidence{}, nil, "", invalid("scan binding is invalid")
	}
	evidence := Evidence{
		SchemaVersion:         SchemaVersion,
		ReleaseDigest:         bundle.ReleaseDigest,
		ConfigurationRevision: bundle.ConfigurationRevision,
		SourceRevision:        sourceRevision,
		ObservedAt:            observedAt,
		ObservedBy:            observedBy,
		Result:                ResultPass,
		Checks:                make([]Check, 0, len(contract)),
	}
	for _, item := range contract {
		matches, err := scanCheck(root, bundle, item)
		if err != nil {
			return Evidence{}, nil, "", err
		}
		passed := len(matches) == 0
		if item.expected == expectedPresent {
			passed = len(matches) == len(item.paths)+1
			if item.bundle == nil {
				passed = len(matches) == len(item.paths)
			}
		}
		if !passed {
			evidence.Result = ResultFail
		}
		evidence.Checks = append(evidence.Checks, Check{
			ID: item.id, Expected: item.expected, Passed: passed, Matches: matches,
		})
	}
	encoded, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return Evidence{}, nil, "", invalid("encode evidence: %v", err)
	}
	encoded = append(encoded, '\n')
	digest := sha256.Sum256(encoded)
	return evidence, encoded, "sha256:" + hex.EncodeToString(digest[:]), nil
}

func Load(path string, bundle releasebundle.Bundle) (Evidence, []byte, string, error) {
	information, err := os.Lstat(path)
	if err != nil || !information.Mode().IsRegular() || information.Size() > MaxEvidenceSize {
		return Evidence{}, nil, "", invalid("evidence must be a bounded regular file")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return Evidence{}, nil, "", invalid("read evidence: %v", err)
	}
	var evidence Evidence
	if err := decodeStrict(encoded, &evidence); err != nil {
		return Evidence{}, nil, "", invalid("decode evidence: %v", err)
	}
	if err := ValidatePass(evidence, bundle); err != nil {
		return Evidence{}, nil, "", err
	}
	digest := sha256.Sum256(encoded)
	return evidence, encoded, "sha256:" + hex.EncodeToString(digest[:]), nil
}

func ValidatePass(evidence Evidence, bundle releasebundle.Bundle) error {
	if evidence.SchemaVersion != SchemaVersion || evidence.Result != ResultPass ||
		bundle.SchemaVersion != ContractedReleaseSchemaVersion ||
		bundle.ConfigurationManifest.SchemaVersion != ContractedReleaseSchemaVersion ||
		evidence.ReleaseDigest != bundle.ReleaseDigest ||
		evidence.ConfigurationRevision != bundle.ConfigurationRevision ||
		evidence.SourceRevision != bundle.ConfigurationManifest.SourceRevision ||
		!digestPattern.MatchString(evidence.ReleaseDigest) ||
		!digestPattern.MatchString(evidence.ConfigurationRevision) ||
		!sourceRevisionPattern.MatchString(evidence.SourceRevision) ||
		!validText(evidence.ObservedBy, 300) ||
		evidence.ObservedAt.IsZero() || evidence.ObservedAt.Location() != time.UTC ||
		len(evidence.Checks) != len(contract) {
		return invalid("PASS evidence binding is invalid")
	}
	if matches := legacyBundleMatches(bundle); len(matches) != 0 ||
		len(stageBundleMatches(bundle)) != 1 {
		return invalid("release bundle still exposes a legacy surface or lacks the Stage surface")
	}
	for index, item := range contract {
		check := evidence.Checks[index]
		if check.ID != item.id || check.Expected != item.expected || !check.Passed {
			return invalid("check %s is missing or failed", item.id)
		}
		expectedMatches := expectedPassMatches(bundle, item)
		if !slices.Equal(check.Matches, expectedMatches) {
			return invalid("check %s observations are invalid", item.id)
		}
	}
	return nil
}

func VerifySourceCheckout(root, expectedRevision string) error {
	if !sourceRevisionPattern.MatchString(expectedRevision) {
		return invalid("source revision must be a full Git object id")
	}
	canonicalRoot, err := canonicalDirectory(root)
	if err != nil {
		return invalid("resolve source root: %v", err)
	}
	topLevel, err := runGit(root, "rev-parse", "--show-toplevel")
	if err != nil {
		return invalid("resolve Git repository root: %v", err)
	}
	canonicalTopLevel, err := canonicalDirectory(strings.TrimSpace(topLevel))
	if err != nil || canonicalRoot != canonicalTopLevel {
		return invalid("source root must be the Git repository root")
	}
	head, err := runGit(root, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil || strings.TrimSpace(head) != expectedRevision {
		return invalid("source root HEAD does not match source revision")
	}
	status, err := runGit(root, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return invalid("inspect source worktree: %v", err)
	}
	if status != "" {
		return invalid("source worktree must be clean and include no untracked files")
	}
	return nil
}

func canonicalDirectory(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	information, err := os.Lstat(resolved)
	if err != nil || !information.IsDir() || information.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("path must resolve to an existing non-symlink directory")
	}
	return filepath.Clean(resolved), nil
}

func runGit(root string, arguments ...string) (string, error) {
	command := exec.Command("git", append([]string{"-C", root}, arguments...)...)
	command.Env = append(
		os.Environ(),
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_CONFIG_NOSYSTEM=1",
	)
	output, err := command.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(string(output), "\n"), nil
}

func Write(path string, encoded []byte) error {
	if len(encoded) == 0 || len(encoded) > MaxEvidenceSize {
		return invalid("encoded evidence is empty or oversized")
	}
	parent := filepath.Dir(path)
	information, err := os.Lstat(parent)
	if err != nil || !information.IsDir() || information.Mode()&os.ModeSymlink != 0 {
		return invalid("evidence parent must be an existing directory")
	}
	file, err := os.CreateTemp(parent, ".vela-h3-reachability-*")
	if err != nil {
		return invalid("create temporary evidence: %v", err)
	}
	temporaryPath := file.Name()
	defer func() {
		_ = file.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := file.Chmod(0o400); err != nil {
		return invalid("protect temporary evidence: %v", err)
	}
	if _, err := file.Write(encoded); err != nil {
		return invalid("write evidence: %v", err)
	}
	if err := file.Sync(); err != nil {
		return invalid("sync evidence: %v", err)
	}
	if err := file.Close(); err != nil {
		return invalid("close evidence: %v", err)
	}
	if err := os.Link(temporaryPath, path); err != nil {
		return invalid("publish evidence without replacement: %v", err)
	}
	if err := syncDirectory(parent); err != nil {
		_ = os.Remove(path)
		return invalid("sync evidence parent: %v", err)
	}
	return nil
}

func expectedPassMatches(bundle releasebundle.Bundle, item checkContract) []string {
	if item.expected == expectedAbsent {
		return nil
	}
	matches := append([]string(nil), item.paths...)
	if item.bundle != nil {
		matches = append(matches, item.bundle(bundle)...)
	}
	sort.Strings(matches)
	return matches
}

func scanCheck(root string, bundle releasebundle.Bundle, item checkContract) ([]string, error) {
	var matches []string
	for _, relative := range item.paths {
		present, err := pathPresent(root, relative)
		if err != nil {
			return nil, invalid("inspect %s: %v", relative, err)
		}
		if present {
			matches = append(matches, relative)
		}
	}
	for _, probe := range item.content {
		present, err := contentPresent(root, probe)
		if err != nil {
			return nil, invalid("inspect %s: %v", probe.path, err)
		}
		if present {
			matches = append(matches, probe.path+"#"+probe.token)
		}
	}
	if item.bundle != nil {
		matches = append(matches, item.bundle(bundle)...)
	}
	sort.Strings(matches)
	return matches, nil
}

func pathPresent(root, relative string) (bool, error) {
	information, err := inspectPath(root, relative)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if information.Mode()&os.ModeSymlink != 0 {
		return false, errors.New("symbolic links are not accepted")
	}
	return true, nil
}

func contentPresent(root string, probe contentProbe) (bool, error) {
	path := filepath.Join(root, filepath.FromSlash(probe.path))
	information, err := inspectPath(root, probe.path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !information.Mode().IsRegular() || information.Size() > MaxEvidenceSize {
		return false, errors.New("content probe target must be a bounded regular file")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	return bytes.Contains(encoded, []byte(probe.token)), nil
}

func inspectPath(root, relative string) (os.FileInfo, error) {
	if relative == "" || !filepath.IsLocal(relative) || filepath.Clean(relative) != relative {
		return nil, errors.New("probe path must be canonical and relative")
	}
	current := root
	for _, component := range strings.Split(filepath.FromSlash(relative), string(os.PathSeparator)) {
		current = filepath.Join(current, component)
		information, err := os.Lstat(current)
		if err != nil {
			return nil, err
		}
		if information.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("symbolic links are not accepted")
		}
	}
	return os.Lstat(current)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
}

func legacyBundleMatches(bundle releasebundle.Bundle) []string {
	var matches []string
	for _, render := range bundle.ConfigurationManifest.FinalRenders {
		if render.Name == "worker-agent" {
			matches = append(matches, "bundle:render/worker-agent")
		}
	}
	for _, item := range bundle.ConfigurationManifest.Packages {
		if item.Name == "h3-runner" {
			matches = append(matches, "bundle:package/h3-runner")
		}
	}
	for _, image := range bundle.OCIImages {
		if strings.Contains(image.Image, "/vela-worker-agent@") ||
			strings.Contains(image.Image, "/vela-h3-runner@") {
			matches = append(matches, "bundle:image/"+image.Image)
		}
	}
	sort.Strings(matches)
	return matches
}

func stageBundleMatches(bundle releasebundle.Bundle) []string {
	if bundle.SchemaVersion != ContractedReleaseSchemaVersion ||
		bundle.ConfigurationManifest.SchemaVersion != ContractedReleaseSchemaVersion ||
		!sourceRevisionPattern.MatchString(bundle.ConfigurationManifest.SourceRevision) {
		return nil
	}
	hasStageRender := false
	for _, render := range bundle.ConfigurationManifest.FinalRenders {
		if render.Name == "stage-worker" {
			hasStageRender = true
			break
		}
	}
	hasStageImage := false
	for _, image := range bundle.OCIImages {
		if strings.Contains(image.Image, "/vela-stage-worker-agent@") {
			hasStageImage = true
			break
		}
	}
	if !hasStageRender || !hasStageImage {
		return nil
	}
	return []string{"bundle:contracted-stage-release"}
}

func decodeStrict(encoded []byte, target any) error {
	if len(encoded) == 0 {
		return errors.New("JSON document is empty")
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("JSON document contains trailing data")
	}
	return nil
}

func validText(value string, maximum int) bool {
	if value == "" || len(value) > maximum || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range []byte(value) {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func invalid(format string, arguments ...any) error {
	return fmt.Errorf("invalid Legacy H3 reachability evidence: "+format, arguments...)
}
