package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRunWithContextRunsAndClosesProductionRuntime(t *testing.T) {
	runtime := &recordingStageWorkerRuntime{runError: errors.New("run failed")}
	err := runWithContextUsing(
		context.Background(),
		config{},
		func(context.Context, config) (stageWorkerRuntime, error) { return runtime, nil },
	)
	if !errors.Is(err, runtime.runError) || runtime.runCalls != 1 || runtime.closeCalls != 1 {
		t.Fatalf("run result error=%v runtime=%#v", err, runtime)
	}
}

func TestRunWithContextClosesPartialRuntimeWhenRunSucceeds(t *testing.T) {
	runtime := &recordingStageWorkerRuntime{closeError: errors.New("close failed")}
	err := runWithContextUsing(
		context.Background(),
		config{},
		func(context.Context, config) (stageWorkerRuntime, error) { return runtime, nil },
	)
	if !errors.Is(err, runtime.closeError) || runtime.runCalls != 1 || runtime.closeCalls != 1 {
		t.Fatalf("close result error=%v runtime=%#v", err, runtime)
	}
}

type recordingStageWorkerRuntime struct {
	runCalls   int
	closeCalls int
	runError   error
	closeError error
}

func (runtime *recordingStageWorkerRuntime) Run(context.Context) error {
	runtime.runCalls++
	return runtime.runError
}

func (runtime *recordingStageWorkerRuntime) Close() error {
	runtime.closeCalls++
	return runtime.closeError
}

func TestLoadConfigRequiresStageWorkerRuntimeBoundary(t *testing.T) {
	setValidStageWorkerEnv(t)
	t.Setenv("VELA_WORKER_INSTANCE_ID", "")

	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "VELA_WORKER_INSTANCE_ID") {
		t.Fatalf("missing WorkerInstance error = %v", err)
	}
}

func TestLoadConfigBindsSingleMemberRuntimeAndDurableMaterialization(t *testing.T) {
	setValidStageWorkerEnv(t)

	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if configuration.workerInstanceID.String() != "48000000-0000-0000-0000-000000000001" ||
		configuration.workerInstanceEpoch != 5 ||
		configuration.workerMemberID.String() != "48000000-0000-0000-0000-000000000004" ||
		configuration.workerMemberEpoch != 11 || configuration.runtimeExpectedUID != 10001 ||
		configuration.runtimeStartupTimeout != 30*time.Second ||
		configuration.capacityTTL != 2*time.Minute || configuration.heartbeatInterval != 20*time.Second ||
		configuration.materializationJournalLimit != 16 ||
		configuration.connectorRevisionID.String() != "48000000-0000-0000-0000-000000000007" ||
		configuration.authorityActiveKeyID != "stage-authority-key-v1" ||
		configuration.sourceLossConsumedResourceUnits != 37 ||
		configuration.artifactSignedGETTTL != 5*time.Minute ||
		filepath.Dir(configuration.productionStateRoot) != configuration.scratchRoot ||
		filepath.Dir(configuration.inputRoot) != configuration.scratchRoot ||
		filepath.Dir(configuration.inputTransferJournalRoot) != configuration.scratchRoot ||
		!reflect.DeepEqual(configuration.capacityVector, map[string]int64{"gpu": 1, "slots": 1}) ||
		len(configuration.devices) != 1 ||
		len(configuration.members) != 1 ||
		configuration.members[0].workerMemberID != configuration.workerMemberID ||
		configuration.members[0].memberEpoch != configuration.workerMemberEpoch ||
		configuration.devices[0].GetDeviceId() != "49000000-0000-0000-0000-000000000001" ||
		configuration.devices[0].GetDeviceEpoch() != 3 || !configuration.artifactS3PathStyle {
		t.Fatalf("configuration = %#v", configuration)
	}
}

func TestLoadConfigBindsMultiMemberRuntimeTransport(t *testing.T) {
	setValidStageWorkerEnv(t)
	root := t.TempDir()
	localDigest := sha256.Sum256([]byte("spiffe://vela.internal/stage-worker/member-1"))
	remoteDigest := sha256.Sum256([]byte("spiffe://vela.internal/stage-worker/member-0"))
	t.Setenv("VELA_STAGE_WORKER_MEMBERS_JSON", `[`+
		`{"worker_member_id":"48000000-0000-0000-0000-000000000003","member_epoch":9,`+
		`"identity_digest":"`+fmt.Sprintf("%x", remoteDigest)+`",`+
		`"address":"worker-0.vela-system.svc:7444","server_name":"worker-0.vela-system.svc"},`+
		`{"worker_member_id":"48000000-0000-0000-0000-000000000004","member_epoch":11,`+
		`"identity_digest":"`+fmt.Sprintf("%x", localDigest)+`",`+
		`"address":"worker-1.vela-system.svc:7444","server_name":"worker-1.vela-system.svc"}]`)
	for name, value := range map[string]string{
		"VELA_STAGE_WORKER_MEMBER_LISTEN_ADDRESS":       "0.0.0.0:7444",
		"VELA_STAGE_WORKER_MEMBER_CLIENT_TLS_CERT_FILE": filepath.Join(root, "client", "tls.crt"),
		"VELA_STAGE_WORKER_MEMBER_CLIENT_TLS_KEY_FILE":  filepath.Join(root, "client", "tls.key"),
		"VELA_STAGE_WORKER_MEMBER_SERVER_CA_FILE":       filepath.Join(root, "client", "ca.crt"),
		"VELA_STAGE_WORKER_MEMBER_SERVER_TLS_CERT_FILE": filepath.Join(root, "server", "tls.crt"),
		"VELA_STAGE_WORKER_MEMBER_SERVER_TLS_KEY_FILE":  filepath.Join(root, "server", "tls.key"),
		"VELA_STAGE_WORKER_MEMBER_CLIENT_CA_FILE":       filepath.Join(root, "server", "ca.crt"),
		"VELA_STAGE_WORKER_MEMBER_DIAL_TIMEOUT":         "15s",
		"VELA_STAGE_WORKER_MEMBER_SHUTDOWN_TIMEOUT":     "20s",
	} {
		t.Setenv(name, value)
	}

	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if len(configuration.members) != 2 ||
		configuration.members[0].workerMemberID.String() != "48000000-0000-0000-0000-000000000003" ||
		configuration.members[0].memberEpoch != 9 ||
		configuration.members[0].address != "worker-0.vela-system.svc:7444" ||
		configuration.members[1].workerMemberID != configuration.workerMemberID ||
		configuration.members[1].identityDigest != localDigest ||
		configuration.memberListenAddress != "0.0.0.0:7444" ||
		configuration.memberDialTimeout != 15*time.Second ||
		configuration.memberShutdownTimeout != 20*time.Second {
		t.Fatalf("multi-member configuration = %#v", configuration)
	}
}

func TestLoadConfigRejectsMaterializationJournalOutsideScratch(t *testing.T) {
	setValidStageWorkerEnv(t)
	t.Setenv("VELA_STAGE_WORKER_MATERIALIZATION_JOURNAL_ROOT", filepath.Join(t.TempDir(), "outside"))

	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "under VELA_WORKER_SCRATCH_ROOT") {
		t.Fatalf("outside materialization journal error = %v", err)
	}
}

func TestLoadConfigRejectsInputTransferStateOutsideScratch(t *testing.T) {
	setValidStageWorkerEnv(t)
	t.Setenv("VELA_STAGE_WORKER_INPUT_TRANSFER_JOURNAL_ROOT", filepath.Join(t.TempDir(), "outside"))

	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "under VELA_WORKER_SCRATCH_ROOT") {
		t.Fatalf("outside input transfer journal error = %v", err)
	}
}

func setValidStageWorkerEnv(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	values := map[string]string{
		"VELA_WORKER_INSTANCE_ID":                               "48000000-0000-0000-0000-000000000001",
		"VELA_WORKER_INSTANCE_EPOCH":                            "5",
		"VELA_WORKER_MEMBER_ID":                                 "48000000-0000-0000-0000-000000000004",
		"VELA_WORKER_MEMBER_EPOCH":                              "11",
		"VELA_STAGE_WORKER_DEVICES_JSON":                        `[{"device_id":"49000000-0000-0000-0000-000000000001","device_epoch":3}]`,
		"VELA_STAGE_WORKER_CAPACITY_VECTOR_JSON":                `{"gpu":1,"slots":1}`,
		"VELA_STAGE_WORKER_MEMBERS_JSON":                        `[{"worker_member_id":"48000000-0000-0000-0000-000000000004","member_epoch":11,"identity_digest":"cb5c0ed488b244036e02dc438bf95fd5d65c3d5c78db20dddfebf602f2474314"}]`,
		"VELA_WORKER_CONTROL_ADDRESS":                           "vela-control.vela-system.svc:9444",
		"VELA_WORKER_CONTROL_SERVER_NAME":                       "vela-control.vela-system.svc",
		"VELA_WORKER_TLS_CERT_FILE":                             filepath.Join(root, "tls.crt"),
		"VELA_WORKER_TLS_KEY_FILE":                              filepath.Join(root, "tls.key"),
		"VELA_WORKER_CONTROL_CA_FILE":                           filepath.Join(root, "ca.crt"),
		"VELA_MODEL_RUNTIME_SOCKET":                             filepath.Join(root, "runtime.sock"),
		"VELA_MODEL_RUNTIME_EXPECTED_UID":                       "10001",
		"VELA_MODEL_RUNTIME_STARTUP_TIMEOUT":                    "30s",
		"VELA_WORKER_SCRATCH_ROOT":                              filepath.Join(root, "scratch"),
		"VELA_STAGE_WORKER_PRODUCTION_STATE_ROOT":               filepath.Join(root, "scratch", "production-state"),
		"VELA_STAGE_WORKER_INPUT_ROOT":                          filepath.Join(root, "scratch", "inputs"),
		"VELA_STAGE_WORKER_INPUT_TRANSFER_JOURNAL_ROOT":         filepath.Join(root, "scratch", "input-transfer-journal"),
		"VELA_STAGE_WORKER_OUTPUT_ROOT":                         filepath.Join(root, "scratch", "outputs"),
		"VELA_STAGE_WORKER_MATERIALIZATION_JOURNAL_ROOT":        filepath.Join(root, "scratch", "materialization"),
		"VELA_STAGE_WORKER_MATERIALIZATION_JOURNAL_LIMIT":       "16",
		"VELA_STAGE_WORKER_AUTHORITY_KEYRING_FILE":              filepath.Join(root, "authority-keyring.json"),
		"VELA_STAGE_WORKER_AUTHORITY_ACTIVE_KEY_ID":             "stage-authority-key-v1",
		"VELA_STAGE_WORKER_CONNECTOR_REVISION_ID":               "48000000-0000-0000-0000-000000000007",
		"VELA_STAGE_WORKER_CAPACITY_TTL":                        "2m",
		"VELA_STAGE_WORKER_HEARTBEAT_INTERVAL":                  "20s",
		"VELA_STAGE_WORKER_RETRY_MINIMUM":                       "1s",
		"VELA_STAGE_WORKER_RETRY_MAXIMUM":                       "30s",
		"VELA_ARTIFACT_S3_ENDPOINT":                             "https://minio.vela-storage.svc:9000",
		"VELA_ARTIFACT_S3_REGION":                               "us-east-1",
		"VELA_ARTIFACT_S3_BUCKET":                               "vela-artifacts",
		"VELA_ARTIFACT_S3_ACCESS_KEY_ID_FILE":                   filepath.Join(root, "access-key-id"),
		"VELA_ARTIFACT_S3_SECRET_ACCESS_KEY_FILE":               filepath.Join(root, "secret-access-key"),
		"VELA_ARTIFACT_S3_CA_FILE":                              filepath.Join(root, "artifact-ca.crt"),
		"VELA_ARTIFACT_S3_PATH_STYLE":                           "true",
		"VELA_ARTIFACT_S3_SIGNED_GET_TTL":                       "5m",
		"VELA_STAGE_WORKER_SOURCE_LOSS_RETRY":                   "30s",
		"VELA_STAGE_WORKER_SOURCE_LOSS_CONSUMED_RESOURCE_UNITS": "37",
	}
	for name, value := range values {
		t.Setenv(name, value)
	}
}
