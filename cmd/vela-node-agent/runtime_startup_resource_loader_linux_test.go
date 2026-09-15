//go:build linux

package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"unsafe"

	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/stageauthority"
	corev1 "k8s.io/api/core/v1"
	corefake "k8s.io/client-go/kubernetes/fake"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

type resourceLoaderPodReaderFixture struct{}

func (resourceLoaderPodReaderFixture) GetWorkerInstancePod(context.Context, fleetcontroller.ResourceKey) (corev1.Pod, error) {
	return corev1.Pod{}, nil
}

type resourceLoaderRegistryFixture struct{}

func (resourceLoaderRegistryFixture) NodeIdentity() string  { return "cpu-node" }
func (resourceLoaderRegistryFixture) ActorIdentity() string { return "node/validation" }
func (resourceLoaderRegistryFixture) ReserveRuntimeStartup(context.Context, fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error) {
	return fleet.RuntimeStartupReservation{}, nil
}

// setValidationPlanCredentials is deliberately confined to this test. The
// production RuntimeLaunchPlan has no public constructor for unverified data;
// the loader test only needs to exercise ownership ordering after the plan
// loader seam has already authenticated its result.
func setValidationPlanCredentials(plan *nodeagent.RuntimeLaunchPlan, uid, gid uint32) {
	value := reflect.ValueOf(plan).Elem()
	for name, number := range map[string]uint32{"uid": uid, "gid": gid} {
		field := value.FieldByName(name)
		reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().SetUint(uint64(number))
	}
}

func TestLoadRuntimeStartupResourcesWithFactoryAssemblesAndClosesDependencies(t *testing.T) {
	setValidNodeAgentEnv(t)
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	configuration, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	configuration.runtimeStartupEnabled = true
	configuration.nodeIdentity = "cpu-node"
	configuration.runtimeStartupLedgerDir = filepath.Join(root, "ledger")
	configuration.runtimeBindingVerifierFile = filepath.Join(root, "registry-keys.json")
	configuration.runtimeStartupSocket = filepath.Join(root, "startup.sock")
	if err := os.Mkdir(configuration.runtimeStartupLedgerDir, 0o700); err != nil {
		t.Fatal(err)
	}
	key := make([]byte, ed25519.PublicKeySize)
	encoded, err := json.Marshal(map[string]string{"registry": base64.StdEncoding.EncodeToString(key)})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configuration.runtimeBindingVerifierFile, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	plan := &nodeagent.RuntimeLaunchPlan{}
	setValidationPlanCredentials(plan, 65532, 65532)
	validator, err := stageauthority.NewValidator(map[string][]byte{"authority": make([]byte, 32)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	core := corefake.NewSimpleClientset().CoreV1()
	var order []string
	appendOrder := func(name string) { order = append(order, name) }
	factory := runtimeStartupResourceFactory{
		loadPlan: func(config) (*nodeagent.RuntimeLaunchPlan, error) {
			appendOrder("plan")
			return plan, nil
		},
		loadValidator: func(string) (*stageauthority.Validator, error) {
			appendOrder("validator")
			return validator, nil
		},
		loadJournal: func(config config, _ *nodeagent.RuntimeLaunchPlan, _ *stageauthority.Validator) (*modelruntime.ExecutionJournalOwner, error) {
			appendOrder("journal")
			return &modelruntime.ExecutionJournalOwner{}, nil
		},
		openLedger: func(ctx context.Context, directory, identity string, initialize bool) (*nodeagent.RuntimeStartupLedger, error) {
			appendOrder("ledger")
			return nodeagent.OpenRuntimeStartupLedger(ctx, directory, identity, initialize)
		},
		loadKubernetesCore: func(string) (coreclient.CoreV1Interface, error) {
			appendOrder("kubernetes")
			return core, nil
		},
		newPodReader: func(coreclient.CoreV1Interface) (nodeagent.RuntimeLaunchPodReader, error) {
			appendOrder("pods")
			return resourceLoaderPodReaderFixture{}, nil
		},
		loadContainerObserver: func(context.Context, config) (*nodeagent.RuntimeContainerObserver, error) {
			appendOrder("observer")
			return &nodeagent.RuntimeContainerObserver{}, nil
		},
		loadFleetRegistry: func(context.Context, config) (nodeagent.RuntimeStartupRegistry, func() error, error) {
			appendOrder("fleet")
			return resourceLoaderRegistryFixture{}, func() error { appendOrder("fleet-close"); return nil }, nil
		},
		listenStartupSocket: func(config, uint32) (*runtimeStartupSocket, error) {
			appendOrder("socket")
			return listenRuntimeStartupSocket(configuration)
		},
	}
	resources, err := loadRuntimeStartupResourcesWithFactory(t.Context(), configuration, factory)
	if err != nil {
		t.Fatal(err)
	}
	if resources == nil || resources.plan != plan || resources.ledger == nil || resources.registry == nil || resources.socket == nil {
		t.Fatalf("resource assembly returned incomplete ownership: %+v", resources)
	}
	if err := resources.Close(); err != nil {
		t.Fatal(err)
	}
	want := []string{"plan", "validator", "journal", "ledger", "kubernetes", "pods", "observer", "fleet", "socket", "fleet-close"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("resource construction order=%v, want %v", order, want)
	}
	if _, err := os.Lstat(configuration.runtimeStartupSocket); !os.IsNotExist(err) {
		t.Fatalf("startup socket survived resource close: %v", err)
	}
	reopened, err := nodeagent.OpenRuntimeStartupLedger(t.Context(), configuration.runtimeStartupLedgerDir, configuration.nodeIdentity, false)
	if err != nil {
		t.Fatalf("resource close left ledger locked or corrupt: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened ledger: %v", err)
	}
}
