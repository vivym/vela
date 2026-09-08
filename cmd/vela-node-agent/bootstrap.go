package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/fleettransport"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/workerbootstrap"
)

func runCommand(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	if len(arguments) == 0 {
		return run()
	}
	if arguments[0] != "bootstrap" && arguments[0] != "inspect-runtime-container" && arguments[0] != "runtime-image-maintenance" {
		return errors.New("expected no arguments to serve, bootstrap with an explicit action, inspect-runtime-container, or runtime-image-maintenance")
	}
	if ctx == nil || stdout == nil || stderr == nil {
		return errors.New("worker bootstrap context and output writers are required")
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	if arguments[0] == "runtime-image-maintenance" {
		return runRuntimeImageMaintenance(ctx, arguments[1:], stdout, stderr, dialRuntimeImageMaintenance)
	}
	if arguments[0] == "inspect-runtime-container" {
		return runRuntimeContainerInspection(ctx, arguments[1:], stdout, stderr)
	}
	return runBootstrap(ctx, arguments[1:], stdout, stderr)
}

func runBootstrap(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	flags := flag.NewFlagSet("vela-node-agent bootstrap", flag.ContinueOnError)
	flags.SetOutput(stderr)
	action := flags.String("action", "", "provision, inspect-provision, prepare, reconcile-pair, history, binding, or abandon")
	address := flags.String("fleet-address", "", "Fleet host and port")
	serverName := flags.String("fleet-server-name", "", "Fleet TLS server name")
	caPath := flags.String("fleet-ca-file", "", "Fleet server CA file")
	certificatePath := flags.String("client-cert-file", "", "registered Node Agent certificate file")
	keyPath := flags.String("client-key-file", "", "private Node Agent certificate key file")
	timeout := flags.Duration("timeout", 30*time.Second, "total operation timeout (at most 5m)")
	bundlePath := flags.String("bundle-manifest-file", "", "canonical approved Worker bundle manifest")
	launchPath := flags.String("launch-manifest-file", "", "trusted private member launch manifest")
	verifierPath := flags.String("verifier-keyring-file", "", "public StageAuthority verifier keyring file")
	bindingVerifierPath := flags.String("binding-verifier-keyring-file", "", "public Registry journal binding verifier keyring file")
	directory := flags.String("scratch-directory", "", "preprovisioned private scratch mount")
	nodeDirectory := flags.String("node-state-directory", "", "root-only Linux Node directory: empty for provision, retained for inspect-provision")
	maxRecords := flags.Int("max-records", 0, "bound on retained Worker history (1-64)")
	request := flags.String("request-id", "", "original bootstrap request UUID")
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *address == "" || *serverName == "" || *caPath == "" || *certificatePath == "" || *keyPath == "" ||
		*timeout <= 0 || *timeout > 5*time.Minute {
		return errors.New("bootstrap requires Fleet address, server name, CA, client certificate/key and a timeout in (0, 5m]")
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	var config workerbootstrap.Config
	var requestID uuid.UUID
	var bindingVerifier *journalbinding.Verifier
	protected := *action == "provision" || *action == "inspect-provision"
	if protected != (*nodeDirectory != "") {
		return errors.New("only provision/inspect-provision requires node-state-directory")
	}
	switch *action {
	case "provision", "inspect-provision", "prepare", "reconcile-pair":
		if *request != "" || *bindingVerifierPath != "" || *bundlePath == "" || *launchPath == "" || *verifierPath == "" ||
			(!protected) != (*directory != "") || *maxRecords < 1 || *maxRecords > 64 {
			return errors.New("preparation requires bundle-manifest-file, launch-manifest-file, verifier-keyring-file and max-records; provision/inspect-provision derives scratch from node-state-directory, prepare/reconcile-pair requires scratch-directory")
		}
		wire, err := securefile.Read(*bundlePath, fleet.MaximumWorkerBootstrapManifestBytes, true)
		if err != nil {
			return err
		}
		config.Bundle, err = fleetcontroller.ParseWorkerBundleActuationManifest(wire)
		if err != nil {
			return err
		}
		config.Launch, err = modelruntime.LoadLaunchManifest(*launchPath)
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
		config.ScratchDirectory, config.MaxRecords = *directory, *maxRecords
		if protected {
			config.ScratchDirectory = filepath.Join(*nodeDirectory, "scratch")
		}
	case "history", "binding", "abandon":
		var err error
		requestID, err = uuid.Parse(*request)
		if err != nil || requestID == uuid.Nil || requestID.String() != *request ||
			*bundlePath != "" || *launchPath != "" || *verifierPath != "" || *directory != "" || *maxRecords != 0 ||
			(*action == "binding") != (*bindingVerifierPath != "") {
			return errors.New("history/binding/abandon requires a canonical nonzero request-id and no local preparation settings; binding also requires binding-verifier-keyring-file")
		}
		if *action == "binding" {
			bindingVerifier, err = journalbinding.ReadVerifierFile(*bindingVerifierPath)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("bootstrap action must be provision, inspect-provision, prepare, reconcile-pair, history, binding, or abandon")
	}
	transport, identity, err := fleettransport.NewWorkerBootstrapTLSCredentials(*certificatePath, *keyPath, *caPath, *serverName)
	if err != nil {
		return err
	}
	dialContext, cancelDial := context.WithTimeout(ctx, defaultFleetDialTimeout)
	client, err := fleettransport.DialClient(dialContext, *address, transport)
	cancelDial()
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	authority, err := client.WorkerBootstrap(identity)
	if err != nil {
		return err
	}
	if *action == "history" || *action == "abandon" {
		var history fleet.WorkerBootstrapHistory
		if *action == "abandon" {
			history, err = authority.AbandonWorkerBootstrap(ctx, requestID)
		} else {
			history, err = authority.LookupWorkerBootstrap(ctx, requestID)
		}
		if err != nil {
			return err
		}
		if err := context.Cause(ctx); err != nil {
			return err
		}
		result := bootstrapHistoryOutput(history)
		result.Action = *action
		return json.NewEncoder(stdout).Encode(result)
	}
	if *action == "binding" {
		binding, err := authority.LookupWorkerBootstrapBinding(ctx, requestID, bindingVerifier)
		if err != nil {
			return err
		}
		wire, err := journalbinding.Encode(binding)
		if err != nil {
			return err
		}
		if err := context.Cause(ctx); err != nil {
			return err
		}
		_, err = stdout.Write(append(wire, '\n'))
		return err
	}
	config.NodeIdentity, config.ActorIdentity = authority.NodeIdentity(), authority.ActorIdentity()
	if *action == "provision" {
		return provisionBootstrap(ctx, config, *nodeDirectory, authority, stdout)
	}
	if *action == "inspect-provision" {
		return inspectProvisionBootstrap(ctx, config, *nodeDirectory, authority, stdout)
	}
	var result workerbootstrap.Result
	if *action == "reconcile-pair" {
		result, err = workerbootstrap.ReconcileRecordedPair(ctx, config, authority)
	} else {
		result, err = workerbootstrap.Prepare(ctx, config, authority)
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(struct {
		SchemaVersion int                                      `json:"schema_version"`
		Action        string                                   `json:"action"`
		RequestID     uuid.UUID                                `json:"request_id"`
		NodeIdentity  string                                   `json:"node_identity"`
		ActorIdentity string                                   `json:"actor_identity"`
		Worker        stageworkeragent.AssignmentJournalStatus `json:"worker"`
		Runtime       modelruntime.ExecutionJournalStatus      `json:"runtime"`
		RecordedAt    time.Time                                `json:"recorded_at"`
	}{1, *action, result.RequestID, config.NodeIdentity, config.ActorIdentity, result.Worker, result.Runtime, result.RecordedAt})
}

type bootstrapHistory struct {
	SchemaVersion       int                   `json:"schema_version"`
	Action              string                `json:"action"`
	RequestID           uuid.UUID             `json:"request_id"`
	NodeIdentity        string                `json:"node_identity"`
	ActorIdentity       string                `json:"actor_identity"`
	WorkerInstanceID    uuid.UUID             `json:"worker_instance_id"`
	WorkerInstanceEpoch int64                 `json:"worker_instance_epoch"`
	WorkerMemberID      uuid.UUID             `json:"worker_member_id"`
	WorkerMemberEpoch   int64                 `json:"worker_member_epoch"`
	BundleDigest        string                `json:"bundle_digest"`
	ClaimedAt           time.Time             `json:"claimed_at"`
	Pair                *bootstrapHistoryPair `json:"pair,omitempty"`
	Abandonment         *bootstrapAbandonment `json:"abandonment,omitempty"`
}

type bootstrapAbandonment struct {
	FencedInstanceEpoch int64     `json:"fenced_instance_epoch"`
	AbandonedAt         time.Time `json:"abandoned_at"`
}

type bootstrapHistoryPair struct {
	WorkerJournalID  uuid.UUID `json:"worker_journal_id"`
	WorkerScope      string    `json:"worker_scope"`
	RuntimeJournalID uuid.UUID `json:"runtime_journal_id"`
	RuntimeScope     string    `json:"runtime_scope"`
	RecordedAt       time.Time `json:"recorded_at"`
}

// History deliberately has no initialization, readiness or drain permission.
func bootstrapHistoryOutput(history fleet.WorkerBootstrapHistory) bootstrapHistory {
	claim := history.Claim
	result := bootstrapHistory{SchemaVersion: 1, Action: "history", RequestID: claim.RequestID,
		NodeIdentity: claim.NodeIdentity, ActorIdentity: history.ActorIdentity, WorkerInstanceID: claim.WorkerInstanceID,
		WorkerInstanceEpoch: claim.WorkerInstanceEpoch, WorkerMemberID: claim.WorkerMemberID, WorkerMemberEpoch: claim.WorkerMemberEpoch,
		BundleDigest: hex.EncodeToString(claim.BundleDigest), ClaimedAt: claim.ClaimedAt}
	if pair := history.Receipt; pair != nil {
		result.Pair = &bootstrapHistoryPair{WorkerJournalID: pair.WorkerJournalID, WorkerScope: hex.EncodeToString(pair.WorkerScope),
			RuntimeJournalID: pair.RuntimeJournalID, RuntimeScope: hex.EncodeToString(pair.RuntimeScope), RecordedAt: history.RecordedAt}
	}
	if abandonment := history.Abandonment; abandonment != nil {
		result.Abandonment = &bootstrapAbandonment{FencedInstanceEpoch: abandonment.FencedInstanceEpoch, AbandonedAt: abandonment.AbandonedAt}
	}
	return result
}
