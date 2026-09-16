package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageworkeragent"
)

func TestProductionWorkerRequiresRuntimeJournalWriter(t *testing.T) {
	identity := productionSmokeIdentity("49800000-0000-0000-0000-000000000004", 11)
	configuration := productionSmokeConfig(t, identity)
	address, files, _ := serveStageWorkerControlSmoke(t, identity)
	configuration.controlAddress, configuration.tlsCertificateFile = address, files.clientCertificate
	configuration.tlsPrivateKeyFile, configuration.controlCAFile = files.clientPrivateKey, files.ca
	enableDurableSmoke(t, &configuration, identity)
	configuration.workerJournalSocket = filepath.Join(configuration.scratchRoot, "worker-journal.sock")
	configuration.workerJournalPIDFDBrokerSocket = filepath.Join(configuration.scratchRoot, "broker.sock")
	want := errors.New("runtime journal writer unavailable")
	consumers := durableSmokeConsumers(stageworkeragent.NewDurableStreamAgent)
	consumers.newRuntimeJournalWriter = func(_ config, launch *durableWorkerLaunch) (modelruntime.JournalCommandWriter, error) {
		if launch == nil || launch.admission.RegistryBinding == nil {
			t.Fatal("journal writer constructed before trusted binding")
		}
		return nil, want
	}
	runtime, err := newProductionRuntimeUsing(context.Background(), configuration, consumers)
	if runtime != nil {
		_ = runtime.Close()
	}
	if !errors.Is(err, want) {
		t.Fatalf("production startup bypassed required runtime journal writer: %v", err)
	}
}
