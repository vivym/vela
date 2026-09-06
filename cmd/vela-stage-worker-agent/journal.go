package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"io"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
)

func runCommand(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	if len(arguments) == 0 {
		return run(ctx)
	}
	if arguments[0] != "journal" {
		return errors.New("expected no arguments to serve, or journal with an explicit action")
	}
	return runJournal(ctx, arguments[1:], stdout, stderr)
}

func runJournal(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	if ctx == nil || stdout == nil || stderr == nil {
		return errors.New("worker journal context and output writers are required")
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	flags := flag.NewFlagSet("vela-stage-worker-agent journal", flag.ContinueOnError)
	flags.SetOutput(stderr)
	action := flags.String("action", "", "initialize, recover, upgrade-v2, upgrade-v3, or upgrade-v4")
	manifestPath := flags.String("launch-manifest-file", "", "trusted private launch manifest")
	verifierPath := flags.String("verifier-keyring-file", "", "public StageAuthority verifier keyring file")
	directory := flags.String("directory", "", "existing private Worker admission journal directory")
	maxRecords := flags.Int("max-records", 0, "bound on retained assignment and retirement history (1-64)")
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *manifestPath == "" || *verifierPath == "" || *directory == "" || *maxRecords < 1 || *maxRecords > 64 {
		return errors.New("journal preparation requires launch-manifest-file, verifier-keyring-file, directory, max-records and an explicit action")
	}
	config := stageworkeragent.AssignmentAdmissionConfig{Directory: *directory, MaxRecords: *maxRecords}
	switch *action {
	case "initialize":
		config.Initialize = true
	case "recover":
	case "upgrade-v2":
		config.UpgradeV2 = true
	case "upgrade-v3":
		config.UpgradeV3 = true
	case "upgrade-v4":
		config.UpgradeV4 = true
	default:
		return errors.New("journal action must be initialize, recover, upgrade-v2, upgrade-v3, or upgrade-v4")
	}
	manifest, err := modelruntime.LoadLaunchManifest(*manifestPath)
	if err != nil {
		return err
	}
	keyring, err := stageauthority.ReadVerifierKeyringFile(*verifierPath)
	if err != nil {
		return err
	}
	defer stageauthority.ClearKeyring(keyring)
	config.Validator, err = stageauthority.NewVerifier(keyring, nil)
	if err != nil {
		return err
	}
	config, err = offlineAdmissionConfig(manifest, config)
	if err != nil {
		return err
	}
	result, err := stageworkeragent.PrepareAssignmentJournal(ctx, config)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(struct {
		Action  string                                   `json:"action"`
		Journal stageworkeragent.AssignmentJournalStatus `json:"journal"`
	}{Action: *action, Journal: result})
}

// These bindings carry topology only. No local or remote Runtime epoch is
// observed, and the temporary preparation gate never exposes an execution API.
func offlineAdmissionConfig(manifest modelruntime.LaunchManifest, config stageworkeragent.AssignmentAdmissionConfig) (stageworkeragent.AssignmentAdmissionConfig, error) {
	bindings, err := manifest.RuntimeBindings()
	if err != nil {
		return stageworkeragent.AssignmentAdmissionConfig{}, err
	}
	members, err := manifest.ExecutionFloorMembers()
	if err != nil {
		return stageworkeragent.AssignmentAdmissionConfig{}, err
	}
	config.WorkerInstanceID, err = uuid.Parse(manifest.WorkerInstanceID)
	if err != nil {
		return stageworkeragent.AssignmentAdmissionConfig{}, err
	}
	config.WorkerMemberID, err = uuid.Parse(manifest.WorkerMemberID)
	if err != nil {
		return stageworkeragent.AssignmentAdmissionConfig{}, err
	}
	config.WorkerInstanceEpoch = manifest.WorkerInstanceEpoch
	config.InputRoot, config.OutputRoot = manifest.Runtimes[0].InputRoot, manifest.Runtimes[0].OutputRoot
	for _, runtime := range manifest.Runtimes {
		if runtime.InputRoot != config.InputRoot || runtime.OutputRoot != config.OutputRoot {
			return stageworkeragent.AssignmentAdmissionConfig{}, errors.New("worker journal requires one shared local input/output root pair")
		}
	}
	baseline := bindings[0]
	baseline.ModelResidencyID, baseline.ModelRuntimeIdentity, baseline.StageProfileRevisionID = "", "", ""
	baseline.ModelRuntimeEpoch = 0
	config.Bindings = nil
	for _, member := range members {
		binding := baseline
		binding.WorkerMemberID, binding.WorkerMemberEpoch = member.WorkerMemberID, member.MemberEpoch
		config.Bindings = append(config.Bindings, stageworkeragent.AdmissionRuntimeBinding{
			Runtime: binding, IdentityDigest: [sha256.Size]byte(member.IdentityDigest),
			DeviceSubsetDigest: [sha256.Size]byte(member.DeviceSubsetDigest),
		})
	}
	return config, nil
}
