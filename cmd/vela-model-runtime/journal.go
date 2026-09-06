package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
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
	if ctx == nil {
		return errors.New("ModelRuntime journal context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	flags := flag.NewFlagSet("vela-model-runtime journal", flag.ContinueOnError)
	flags.SetOutput(stderr)
	action := flags.String("action", "", "initialize, recover, upgrade-v2, or upgrade-v3")
	manifestPath := flags.String("launch-manifest-file", "", "trusted private launch manifest")
	verifierPath := flags.String("verifier-keyring-file", "", "public StageAuthority verifier keyring file")
	directory := flags.String("directory", "", "existing private execution journal directory")
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *manifestPath == "" || *verifierPath == "" || *directory == "" {
		return errors.New("journal preparation requires launch-manifest-file, verifier-keyring-file, directory and an explicit action")
	}
	state := modelruntime.ExecutionFloorStateConfig{Directory: *directory}
	switch *action {
	case "initialize":
		state.Initialize = true
	case "recover":
	case "upgrade-v2":
		state.UpgradeV2 = true
	case "upgrade-v3":
		state.UpgradeV3 = true
	default:
		return errors.New("journal action must be initialize, recover, upgrade-v2, or upgrade-v3")
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
	validator, err := stageauthority.NewVerifier(keyring, nil)
	if err != nil {
		return err
	}
	result, err := modelruntime.PrepareExecutionJournal(ctx, manifest, validator, state)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(struct {
		Action  string                              `json:"action"`
		Journal modelruntime.ExecutionJournalStatus `json:"journal"`
	}{Action: *action, Journal: result})
}
