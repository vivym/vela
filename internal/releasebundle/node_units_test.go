package releasebundle

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestBundleBindsBothDeployedNodeUnits(t *testing.T) {
	fixture := newBundleFixture(t)
	writeTestFile(t, filepath.Join(fixture.directory, fixture.plan.NodeAgentUnit.Ref),
		readTestFile(t, "../../deploy/node-agent/vela-node-agent.service"))
	fixture.writePlan(t)
	bundle, _, err := buildTestBundle(fixture.planPath)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.ConfigurationManifest.RuntimeImageMaintenanceUnit.Artifact.Ref != fixture.plan.RuntimeImageMaintenanceUnit.Ref ||
		bundle.ReleaseDescriptor.SchemaVersion != 2 || bundle.ConfigurationManifest.SchemaVersion != 3 {
		t.Fatalf("maintenance or OCI contract lost: %+v", bundle)
	}
	path := filepath.Join(fixture.directory, fixture.plan.RuntimeImageMaintenanceUnit.Ref)
	writeTestFile(t, path, append(readTestFile(t, path), []byte("# changed unit bytes\n")...))
	root, err := openRootedFS(fixture.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	if err := verify(root, bundle); !errors.Is(err, ErrInvalidBundle) {
		t.Fatalf("maintenance artifact tamper accepted: %v", err)
	}
}

func TestBundleRequiresIndependentMaintenanceContract(t *testing.T) {
	for _, fault := range []string{"schema-2", "missing", "alias", "name", "remediation-dependency", "containerd-dependency", "wrong-command", "additional-hook", "duplicate", "privileges", "limited-restarts", "stop-timeout"} {
		t.Run(fault, func(t *testing.T) {
			fixture := newBundleFixture(t)
			path := filepath.Join(fixture.directory, fixture.plan.RuntimeImageMaintenanceUnit.Ref)
			wire := string(readTestFile(t, path))
			switch fault {
			case "schema-2":
				fixture.plan.SchemaVersion = 2
			case "missing":
				fixture.plan.RuntimeImageMaintenanceUnit = ArtifactInput{}
			case "alias":
				fixture.plan.RuntimeImageMaintenanceUnit.Ref = fixture.plan.NodeAgentUnit.Ref
			case "name":
				fixture.plan.RuntimeImageMaintenanceUnit.Name = "node-agent-systemd-unit"
			case "remediation-dependency":
				wire = strings.Replace(wire, "[Unit]", "[Unit]\nRequires=vela-node-agent.service", 1)
			case "containerd-dependency":
				wire = strings.Replace(wire, "[Unit]", "[Unit]\nRequires=containerd.service", 1)
			case "wrong-command":
				wire = strings.Replace(wire, " runtime-image-maintenance", " bootstrap", 1)
			case "additional-hook":
				wire = strings.Replace(wire, "[Service]", "[Service]\nExecStartPre=/bin/true", 1)
			case "duplicate":
				wire = strings.Replace(wire, "RestartSec=5s", "RestartSec=5s\nRestartSec=1s", 1)
			case "privileges":
				wire = strings.Replace(wire, "CapabilityBoundingSet=", "CapabilityBoundingSet=CAP_SYS_ADMIN", 1)
			case "limited-restarts":
				wire = strings.Replace(wire, "StartLimitIntervalSec=0", "StartLimitIntervalSec=10s", 1)
			case "stop-timeout":
				wire = strings.Replace(wire, "TimeoutStopSec=40s", "TimeoutStopSec=1s", 1)
			}
			writeTestFile(t, path, []byte(wire))
			fixture.writePlan(t)
			if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("incomplete maintenance contract accepted: %v", err)
			}
		})
	}
}
