package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/vivym/vela/internal/fleettransport"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/nodeagent"
)

func waitRuntimeStartupWithReporting(ctx context.Context, configuration config, lifecycle *runtimeStartupLifecycle) error {
	resources := lifecycle.resources
	if resources == nil || resources.runtimeOwner == nil || resources.plan == nil {
		return nodeagent.ErrRuntimeStartupAuthority
	}
	templates, err := loadWorkerInstanceTemplates(configuration.workerInstancesFile, nodeagent.NodeAgentIdentity{NodeIdentity: configuration.nodeIdentity, AgentID: configuration.agentID, AgentEpoch: configuration.agentEpoch})
	if err != nil {
		return err
	}
	claim := resources.plan.RegistryBinding().GetClaim()
	if len(templates) != 1 || templates[0].Evidence.WorkerInstanceID.String() != claim.WorkerInstanceId {
		return errors.New("startup reporter requires exactly the current WorkerInstance template")
	}
	epochs, err := nodeagent.NewFileWorkerInstanceEpochStore(nodeagent.FileWorkerInstanceEpochStoreConfig{Directory: configuration.workerInstanceStateDirectory, NodeIdentity: configuration.nodeIdentity, BootIDPath: configuration.bootIDPath})
	if err != nil {
		return err
	}
	defer func(cleanup func() error) { _ = cleanup() }(epochs.Close)
	var probe nodeagent.WorkerInstanceDeviceProbe
	if len(templates[0].Evidence.DeviceSet.Devices) == 1 && templates[0].Evidence.DeviceSet.Devices[0].Kind == "CPU" {
		probe = &nodeagent.CPUDeviceProbe{NodeIdentity: configuration.nodeIdentity, OnlineCPUsPath: "/sys/devices/system/cpu/online", Epochs: epochs}
	} else {
		probe, err = nodeagent.NewNVIDIAGPUProbe(nodeagent.NVIDIAGPUProbeConfig{NodeIdentity: configuration.nodeIdentity, NVIDIASMIPath: configuration.nvidiaSMIPath, PCIBusDevicesRoot: configuration.pciBusDevicesRoot, SysDevicesRoot: configuration.sysDevicesRoot, DriverVersionPath: configuration.nvidiaDriverVersionPath}, nodeagent.ExecNVIDIAInventoryRunner{}, epochs)
		if err != nil {
			return err
		}
	}
	credentials, err := fleettransport.NewClientTLSCredentials(configuration.fleetClientCertificate, configuration.fleetClientPrivateKey, configuration.fleetCA, configuration.fleetServerName)
	if err != nil {
		return err
	}
	dial, cancel := context.WithTimeout(ctx, configuration.fleetDialTimeout)
	fleetClient, err := fleettransport.DialClient(dial, configuration.fleetAddress, credentials)
	cancel()
	if err != nil {
		return err
	}
	defer func(cleanup func() error) { _ = cleanup() }(fleetClient.Close)
	reporter, err := nodeagent.NewWorkerInstanceEvidenceReporter(probe, fleetClient, epochs, configuration.workerInstanceEvidenceTTL, time.Now)
	if err != nil {
		return err
	}
	verifier, err := journalbinding.NewVerifier(resources.registryVerifierKeys)
	if err != nil {
		return err
	}
	live := &nodeagent.RuntimeWorkerEvidenceReporter{Reporter: reporter, Owner: resources.runtimeOwner, Plan: resources.plan, RegistryVerifier: verifier, SocketPath: "/run/vela-model-runtime/private/runtime.sock"}
	lifetime, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- nodeagent.RunWorkerInstanceEvidenceReporting(lifetime, live, templates, nodeagent.WorkerInstanceReportingConfig{Interval: configuration.workerInstanceReportInterval, CallTimeout: configuration.workerInstanceCallTimeout, InitialBackoff: configuration.workerInstanceBackoffInitial, MaxBackoff: configuration.workerInstanceBackoffMax, ObserveResult: func(result nodeagent.WorkerInstanceReportResult) {
			if result.Err != nil {
				fmt.Fprintf(os.Stderr, "WorkerInstance %s live Runtime evidence report failed: %v\n", result.WorkerInstanceID, result.Err)
			}
		}})
	}()
	err = lifecycle.orchestration.Wait(ctx)
	stop()
	return errors.Join(err, <-done)
}
