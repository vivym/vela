//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStageTerminalDispositionThroughAuthenticatedControl(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "terminal-disposition-control")
	command := stageWorkerAcquireCommand(fixture)
	acquired, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(context.Background(), command, stageWorkerAcquireRequest(fixture))
	if err != nil || acquired.Assignment == nil {
		t.Fatalf("Acquire: %+v %v", acquired, err)
	}
	original := acquired.Assignment.GetAuthority()
	handler, validator, signer := terminalDispositionControl(t, fixture)
	dial := terminalDispositionDialer(t, handler)
	client := dial(command.Identity.SPIFFEID, command.ControlSessionEpoch)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	memberID := original.GetMembers()[0].GetWorkerMemberId()
	if result, err := client.ReadTerminalDisposition(ctx, validator, original, memberID, command.CommandID); err != nil || result != nil {
		t.Fatalf("active StageRun must RETAIN: %+v %v", result, err)
	}
	failTerminalHistoryAssignment(t, fixture, original)
	if _, err := validator.ValidateEnvelope(original); !errors.Is(err, stageauthority.ErrStale) {
		t.Fatalf("test clock must expire original authority: %v", err)
	}
	result, err := client.ReadTerminalDisposition(ctx, validator, original, memberID, command.CommandID)
	if err != nil || result == nil || result.Disposition.GetTerminalState() != velav1.StageTerminalState_STAGE_TERMINAL_STATE_FAILED ||
		result.Disposition.GetCutoff() != original.GetExecutionSequence() || len(result.Disposition.GetAllocations()) != 1 {
		t.Fatalf("terminal disposition through mTLS: %+v %v", result, err)
	}
	if again, err := client.ReadTerminalDisposition(ctx, validator, original, memberID, command.CommandID); err != nil || again == nil || again.Disposition.GetCutoff() != result.Disposition.GetCutoff() {
		t.Fatalf("repeat exact history lookup: %+v %v", again, err)
	}
	legacy := proto.Clone(original).(*velav1.StageAuthority)
	legacy.SchemaVersion, legacy.ExecutionSequence = stageauthority.SchemaVersionV1, 0
	legacy, err = signer.Sign(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if retained, err := client.ReadTerminalDisposition(ctx, validator, legacy, memberID, command.CommandID); err != nil || retained != nil {
		t.Fatalf("V1 authority must RETAIN: %+v %v", retained, err)
	}
	for _, test := range []struct {
		name, identity string
		epoch          int64
		acquire        uuid.UUID
	}{
		{"other member identity", "spiffe://vela/worker/unregistered", command.ControlSessionEpoch, command.CommandID},
		{"stale database session", command.Identity.SPIFFEID, command.ControlSessionEpoch + 1, command.CommandID},
		{"other Acquire", command.Identity.SPIFFEID, command.ControlSessionEpoch, uuid.New()},
		{"missing original Acquire", command.Identity.SPIFFEID, command.ControlSessionEpoch, uuid.Nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			other := dial(test.identity, test.epoch)
			if actual, err := other.ReadTerminalDisposition(ctx, validator, original, memberID, test.acquire); err != nil || actual != nil {
				t.Fatalf("mismatched historical caller must RETAIN: %+v %v", actual, err)
			}
		})
	}
	for _, kind := range []string{"schema", "Acquire", "signature", "future"} {
		t.Run(kind, func(t *testing.T) {
			query := &velav1.ReadStageTerminalDispositionRequest{SchemaVersion: 1, Authority: proto.Clone(original).(*velav1.StageAuthority)}
			want := velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED
			switch kind {
			case "schema":
				query.SchemaVersion++
			case "Acquire":
				query.AcquireCommandId = "invalid"
			case "signature":
				query.Authority.Signature[0] ^= 1
				want = velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE
			case "future":
				query.Authority.IssuedAt = timestamppb.New(time.Now().Add(time.Hour))
				query.Authority.ExpiresAt = timestamppb.New(time.Now().Add(2 * time.Hour))
				var err error
				query.Authority, err = signer.Sign(query.Authority)
				if err != nil {
					t.Fatal(err)
				}
				want = velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE
			}
			response, err := client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
				Operation: &velav1.StageWorkerControlServiceConnectRequest_ReadStageTerminalDisposition{ReadStageTerminalDisposition: query},
			})
			if err != nil || response.GetStageCommandResult().GetDecision() != want || response.GetStageTerminalDispositionResult() != nil {
				t.Fatalf("invalid historical query: %v %v", response, err)
			}
		})
	}
	request := &velav1.StageWorkerControlServiceConnectRequest{
		RequestId: uuid.NewString(), ControlSessionEpoch: command.ControlSessionEpoch,
		Operation: &velav1.StageWorkerControlServiceConnectRequest_ReadStageTerminalDisposition{
			ReadStageTerminalDisposition: &velav1.ReadStageTerminalDispositionRequest{SchemaVersion: 1, Authority: original, AcquireCommandId: command.CommandID.String()},
		},
	}
	response, err := client.Exchange(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*velav1.StageWorkerControlServiceConnectResponse){
		"request": func(r *velav1.StageWorkerControlServiceConnectResponse) { r.RequestId = uuid.NewString() },
		"schema": func(r *velav1.StageWorkerControlServiceConnectResponse) {
			r.GetStageTerminalDispositionResult().SchemaVersion++
		},
		"digest": func(r *velav1.StageWorkerControlServiceConnectResponse) {
			r.GetStageTerminalDispositionResult().OriginalAuthorityDigest[0] ^= 1
		},
		"decision": func(r *velav1.StageWorkerControlServiceConnectResponse) {
			r.GetStageTerminalDispositionResult().InputDisposition = velav1.StageInputDisposition_STAGE_INPUT_DISPOSITION_RETAIN
		},
		"signature": func(r *velav1.StageWorkerControlServiceConnectResponse) {
			r.GetStageTerminalDispositionResult().Disposition.Signature[0] ^= 1
		},
		"missing signed facts": func(r *velav1.StageWorkerControlServiceConnectResponse) {
			r.GetStageTerminalDispositionResult().Disposition = nil
		},
		"session": func(r *velav1.StageWorkerControlServiceConnectResponse) {
			r.GetStageTerminalDispositionResult().Disposition.ControlSessionEpoch++
		},
	} {
		t.Run("response "+name, func(t *testing.T) {
			changed := proto.Clone(response).(*velav1.StageWorkerControlServiceConnectResponse)
			mutate(changed)
			if verified, err := stageworkertransport.ValidateTerminalDispositionResponse(validator, request, changed, memberID); err == nil || verified != nil {
				t.Fatal("client accepted altered terminal response")
			}
		})
	}
	var state string
	var latestSequence int64
	if err := fixture.database.Admin.QueryRow(`SELECT state::text, (SELECT max(execution_sequence) FROM stage_allocations WHERE stage_run_id = $1) FROM stage_runs WHERE id = $1`, original.GetStageRunId()).Scan(&state, &latestSequence); err != nil {
		t.Fatal(err)
	}
	if state != "FAILED" || latestSequence != original.GetExecutionSequence() {
		t.Fatalf("historical queries changed execution state: %s %d", state, latestSequence)
	}
}

func readSignedTerminalDisposition(t *testing.T, handler *stageworkercontrol.Handler, validator *stageauthority.Validator, command stageworkercontrol.CommandContext, authority *velav1.StageAuthority) *velav1.StageTerminalDisposition {
	t.Helper()
	request := &velav1.StageWorkerControlServiceConnectRequest{
		RequestId: uuid.NewString(), ControlSessionEpoch: command.ControlSessionEpoch,
		Operation: &velav1.StageWorkerControlServiceConnectRequest_ReadStageTerminalDisposition{
			ReadStageTerminalDisposition: &velav1.ReadStageTerminalDispositionRequest{SchemaVersion: 1, Authority: authority, AcquireCommandId: command.CommandID.String()},
		},
	}
	response, err := handler.Handle(context.Background(), command.Identity, command.ControlSessionEpoch, request)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := stageworkertransport.ValidateTerminalDispositionResponse(validator, request, response, authority.GetMembers()[0].GetWorkerMemberId())
	if err != nil || verified == nil {
		t.Fatalf("authenticate signed terminal history: %v %v", response, err)
	}
	return verified.Disposition
}

func terminalDispositionControl(t *testing.T, fixture stageSchedulerFixture) (*stageworkercontrol.Handler, *stageauthority.Validator, *stageauthority.Signer) {
	t.Helper()
	return terminalDispositionControlWithAssignments(t, fixture, unusedMaterializationReplayDependencies{})
}

func terminalDispositionControlWithAssignments(t *testing.T, fixture stageSchedulerFixture, assignments stageworkercontrol.AssignmentOperations) (*stageworkercontrol.Handler, *stageauthority.Validator, *stageauthority.Signer) {
	t.Helper()
	keys := map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatal(err)
	}
	// Expire the one-minute execution envelope without expiring fresh terminal facts.
	clock := func() time.Time { return time.Now().Add(90 * time.Second) }
	public, err := stageauthority.DeriveVerifierKeyring(keys)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := stageauthority.NewVerifier(public, clock)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := stageworkercontrol.NewPostgresTerminalHistoryReader(newRolePool(t, fixture.database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password"), validator)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := stageworkercontrol.NewTerminalDispositionBackend(reader, signer, validator, "stage-authority-key-v1")
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := stageartifact.NewPostgresRepository(newRolePool(t, fixture.database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password"))
	if err != nil {
		t.Fatal(err)
	}
	unused := unusedMaterializationReplayDependencies{}
	evidence, err := stageworkercontrol.NewPostgresWorkerEvidenceBackend(newRolePool(t, fixture.database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password"))
	if err != nil {
		t.Fatal(err)
	}
	backend, err := stageworkercontrol.NewPostgresOperationBackend(stageworkercontrol.PostgresOperationConfig{
		TerminalDispositions: terminal, WorkerEvidence: evidence, Assignments: assignments, Execution: unused, MaterializationIssuer: unused,
		StageArtifacts: artifacts, StageAttempts: fixture.coordinator, Reattachments: unused, Transfers: unused,
	})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := stageworkercontrol.NewProductionExecutor(backend)
	if err != nil {
		t.Fatal(err)
	}
	materializationValidator, err := materializationauthority.NewValidator(keys, clock)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := stageworkercontrol.NewHandler(stageworkercontrol.Config{
		Validator: validator, Authorizer: terminalUnexpectedActiveAuthorizer{}, MaterializationValidator: materializationValidator,
		MaterializationAuthorizer: artifacts, Executor: executor,
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler, validator, signer
}

type terminalUnexpectedActiveAuthorizer struct{}

func (terminalUnexpectedActiveAuthorizer) IsActive(context.Context, stageworkertransport.Identity, int64, stageworkercontrol.Operation, stageauthority.Verified) (bool, error) {
	return false, errors.New("historical query reached active execution authorization")
}

type terminalQuietStops struct{}

func (terminalQuietStops) Stops(context.Context, stageworkertransport.Identity, int64) <-chan *velav1.StopStage {
	return nil
}

func terminalDispositionDialer(t *testing.T, handler *stageworkercontrol.Handler, sources ...stageworkertransport.ControlSessionEpochSource) func(string, int64) *stageworkertransport.Client {
	t.Helper()
	ca, caKey, caPEM := issueWorkerTransportTestCA(t)
	serverName := "terminal-control.internal"
	certPEM, keyPEM := issueWorkerTransportTestCertificate(t, ca, caKey, pkix.Name{CommonName: serverName}, []string{serverName}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("invalid CA")
	}
	adapter, err := stageworkertransport.NewServer(stageworkertransport.ServerConfig{Authenticator: stageworkertransport.PeerAuthenticator{}, Handler: handler, StopSource: terminalQuietStops{}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots})))
	velav1.RegisterStageWorkerControlServiceServer(server, adapter)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	return func(identity string, epoch int64) *stageworkertransport.Client {
		t.Helper()
		certPEM, keyPEM := issueWorkerTransportTestCertificate(t, ca, caKey, pkix.Name{CommonName: "vela-stage-worker"}, nil, []*url.URL{mustParseWorkerSPIFFEID(t, identity)}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
		certificate, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		config := stageworkertransport.ClientConfig{
			Address: listener.Addr().String(), InitialControlSessionEpoch: epoch,
			TransportCredentials: credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, RootCAs: roots, ServerName: serverName}),
		}
		if len(sources) == 1 {
			config.InitialControlSessionEpoch, config.ControlSessionEpochSource = 0, sources[0]
		} else if len(sources) != 0 {
			t.Fatal("test dialer accepts at most one durable session source")
		}
		client, err := stageworkertransport.DialClient(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		return client
	}
}
