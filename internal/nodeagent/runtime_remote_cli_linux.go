package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/opencontainers/runtime-spec/specs-go"
)

var ErrRuntimeRemoteCLI = errors.New("runtime remote CLI arguments or environment are not approved")

const runtimeRemoteCLIPath = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

// RuntimeRemoteCLIObservation binds the approved task vectors and their sampled
// original-process procfs values. Digests avoid storing environment values in
// the ledger. This does not attest loaded code or uninterrupted execution.
type RuntimeRemoteCLIObservation struct {
	SchemaVersion     int               `json:"schema_version"`
	ArgumentsDigest   [sha256.Size]byte `json:"arguments_digest"`
	EnvironmentDigest [sha256.Size]byte `json:"environment_digest"`
}

// ReservePublishedRemoteCLI additionally requires the image's default command
// to be the actual remote CLI with the independently configured bootstrap path.
// The narrow environment admits fixed PATH/HOME and the planned Pod hostname.
// Legacy Fleet environment injection is intentionally unsupported by this
// entry point. Production rendering, execution continuity and once-only grant
// are still required; a reservation never permits initialization by itself.
func (ledger *RuntimeStartupLedger) ReservePublishedRemoteCLI(ctx context.Context, config RuntimeStartupReservationConfig, image RuntimeStartupImageConfig, publication RuntimeStartupPublicationConfig) (RuntimeStartupReservationRecord, error) {
	if image.Images == nil || !validRuntimeBootstrapPath(publication.BootstrapPath) || publication.Directory == "" {
		return RuntimeStartupReservationRecord{}, ErrRuntimeRemoteCLI
	}
	config.publication = &publication
	return ledger.reserveRemote(ctx, config, &runtimeStartupImageCheck{config: image, remoteCLI: true})
}

func checkRemoteCLIConfiguration(image ocispec.ImageConfig, task *specs.Process, bootstrapPath, hostname string) error {
	arguments, err := runtimeImageDefaultArguments(image)
	if err != nil || task == nil || hostname == "" || task.Terminal || task.Cwd != "/" || image.WorkingDir != "" && image.WorkingDir != "/" ||
		len(arguments) != 4 || arguments[1] != "serve-remote" || arguments[2] != "--bootstrap-file" || arguments[3] != bootstrapPath || !slices.Equal(task.Args, arguments) {
		return ErrRuntimeRemoteCLI
	}
	// No ambient loader, Go runtime, inherited backend or local-journal knobs.
	// Image defaults are signed inputs, but cannot broaden this CLI contract.
	seen := make(map[string]bool)
	for _, item := range image.Env {
		if seen[item] || item != "PATH="+runtimeRemoteCLIPath && item != "HOME=/" {
			return ErrRuntimeRemoteCLI
		}
		seen[item] = true
	}
	if len(task.Env) != 3 {
		return ErrRuntimeRemoteCLI
	}
	// Set HOME explicitly in the task; runc otherwise adds a passwd-derived
	// value after writing config.json, outside the approved vector.
	expected := []string{"HOME=/", "HOSTNAME=" + hostname, "PATH=" + runtimeRemoteCLIPath}
	environment := slices.Clone(task.Env)
	slices.Sort(environment)
	if !slices.Equal(environment, expected) {
		return ErrRuntimeRemoteCLI
	}
	return nil
}

func (caller *RuntimeCaller) inspectRemoteCLIVectors(ctx context.Context, task *specs.Process) (RuntimeRemoteCLIObservation, error) {
	if err := contextError(ctx); err != nil {
		return RuntimeRemoteCLIObservation{}, err
	}
	if caller == nil || task == nil {
		return RuntimeRemoteCLIObservation{}, ErrRuntimeRemoteCLI
	}
	caller.mu.Lock()
	defer caller.mu.Unlock()
	first, err := caller.inspectLocked(ctx)
	if err != nil {
		return RuntimeRemoteCLIObservation{}, err
	}
	read := func(name string, expected []string) error {
		file, err := caller.process.Open(name)
		if err != nil {
			return err
		}
		defer func() { _ = file.Close() }()
		wire, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
		if err != nil || len(wire) > 64<<10 || !bytes.Equal(wire, []byte(strings.Join(expected, "\x00")+"\x00")) {
			return errors.Join(ErrRuntimeRemoteCLI, err)
		}
		return ctx.Err()
	}
	for range 2 {
		if err := errors.Join(read("cmdline", task.Args), read("environ", task.Env)); err != nil {
			return RuntimeRemoteCLIObservation{}, err
		}
	}
	last, err := caller.inspectLocked(ctx)
	first.ObservedAt = last.ObservedAt
	if err != nil || first != last {
		return RuntimeRemoteCLIObservation{}, errors.Join(ErrRuntimeRemoteCLI, err)
	}
	arguments, _ := json.Marshal(task.Args)
	environment, _ := json.Marshal(task.Env)
	return RuntimeRemoteCLIObservation{SchemaVersion: 1, ArgumentsDigest: sha256.Sum256(arguments), EnvironmentDigest: sha256.Sum256(environment)}, nil
}
