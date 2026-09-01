package deploymentcontract

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const disposableCampaignGoBase = "docker.io/library/golang:1.26.7-bookworm@sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2"

func TestH3DisposableCampaignImageIsPinnedAndSeparateFromReleaseImages(t *testing.T) {
	root := deploymentRepositoryRoot(t)
	dockerfile := readDisposableCampaignFile(t, root, "Dockerfile")
	for _, required := range []string{
		"# syntax=docker/dockerfile:1.20@sha256:26147acbda4f14c5add9946e2fd2ed543fc402884fd75146bd342a7f6271dc1d",
		"FROM " + disposableCampaignGoBase + " AS builder",
		"CGO_ENABLED=0",
		"go build -mod=readonly -trimpath -buildvcs=false",
		"./cmd/vela-h3-member-campaign",
		"FROM scratch",
		"ENTRYPOINT [\"/usr/local/bin/vela-h3-member-campaign\"]",
	} {
		if !strings.Contains(string(dockerfile), required) {
			t.Fatalf("disposable campaign Dockerfile omitted %q", required)
		}
	}
	bake, err := os.ReadFile(filepath.Join(root, "docker-bake.hcl"))
	if err != nil {
		t.Fatalf("read release bake contract: %v", err)
	}
	if strings.Contains(string(bake), "vela-h3-member-campaign") {
		t.Fatal("disposable campaign image leaked into the production release image graph")
	}
}

func TestH3DisposableCampaignManifestsPinCrossNodeNormalFaultAndRecoveryWorkloads(t *testing.T) {
	root := deploymentRepositoryRoot(t)
	base := loadDisposableCampaignDocuments(t, root, "campaign.yaml")
	if !containsDisposableObject(base, "Namespace", "vela-h3-disposable") ||
		!containsDisposableObject(base, "Service", "follower") ||
		!containsDisposableObject(base, "Deployment", "follower") {
		t.Fatalf("disposable campaign base object inventory = %#v", objectInventory(base))
	}
	baseText := string(readDisposableCampaignFile(t, root, "campaign.yaml"))
	for _, required := range []string{
		"vela.ai/h3-campaign-role: follower",
		"image: vela-h3-member-campaign:disposable",
		"imagePullPolicy: Never",
		"VELA_H3_MEMBER_CAMPAIGN_LISTEN_ADDRESS",
		"readOnlyRootFilesystem: true",
	} {
		if !strings.Contains(baseText, required) {
			t.Fatalf("disposable campaign base omitted %q", required)
		}
	}

	jobs := map[string]struct {
		command string
		delay   bool
	}{
		"normal-job.yaml":   {command: "run"},
		"fault-job.yaml":    {command: "run-fault", delay: true},
		"recovery-job.yaml": {command: "run"},
	}
	for name, expectation := range jobs {
		documents := loadDisposableCampaignDocuments(t, root, name)
		if len(documents) != 1 || documents[0].Kind != "Job" {
			t.Fatalf("%s inventory = %#v", name, objectInventory(documents))
		}
		text := string(readDisposableCampaignFile(t, root, name))
		for _, required := range []string{
			"vela.ai/h3-campaign-role: leader",
			"image: vela-h3-member-campaign:disposable",
			"imagePullPolicy: Never",
			"- " + expectation.command,
		} {
			if !strings.Contains(text, required) {
				t.Fatalf("%s omitted %q", name, required)
			}
		}
		hasDelay := strings.Contains(text, "VELA_H3_MEMBER_CAMPAIGN_REMOTE_START_DELAY")
		if hasDelay != expectation.delay {
			t.Fatalf("%s fault delay presence=%t want=%t", name, hasDelay, expectation.delay)
		}
	}
}

func TestH3DisposableCampaignHarnessIsSyntaxValidAndFailClosed(t *testing.T) {
	root := deploymentRepositoryRoot(t)
	script := filepath.Join(root, "hack", "run-h3-disposable-member-campaign.sh")
	if output, err := exec.Command("sh", "-n", script).CombinedOutput(); err != nil {
		t.Fatalf("validate disposable campaign harness: %v\n%s", err, output)
	}
	content, err := os.ReadFile(script)
	if err != nil {
		t.Fatalf("read disposable campaign harness: %v", err)
	}
	text := string(content)
	for _, required := range []string{
		"vela-h3-disposable-",
		"refusing to reuse existing k3d cluster",
		"--servers 1",
		"--agents 3",
		"--kubeconfig-update-default=false",
		"k3d-heimdall-staging",
		"prepared member",
		"scale deployment/follower --replicas=0",
		"FAULT_REJECTED",
		"scale deployment/follower --replicas=1",
		"recovery-job.yaml",
		"pod-inventory.json",
		"H3_DISPOSABLE_RETAIN_CLUSTER",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("disposable campaign harness omitted guard/evidence %q", required)
		}
	}
}

type disposableCampaignDocument struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
}

func loadDisposableCampaignDocuments(t *testing.T, root, name string) []disposableCampaignDocument {
	t.Helper()
	content := readDisposableCampaignFile(t, root, name)
	decoder := yaml.NewDecoder(strings.NewReader(string(content)))
	var documents []disposableCampaignDocument
	for {
		var document disposableCampaignDocument
		err := decoder.Decode(&document)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("decode disposable campaign %s: %v", name, err)
		}
		if document.Kind != "" {
			documents = append(documents, document)
		}
	}
	return documents
}

func readDisposableCampaignFile(t *testing.T, root, name string) []byte {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(root, "deploy", "h3-disposable-campaign", name))
	if err != nil {
		t.Fatalf("read disposable campaign %s: %v", name, err)
	}
	return content
}

func containsDisposableObject(documents []disposableCampaignDocument, kind, name string) bool {
	for _, document := range documents {
		if document.Kind == kind && document.Metadata.Name == name {
			return true
		}
	}
	return false
}

func objectInventory(documents []disposableCampaignDocument) []string {
	result := make([]string, 0, len(documents))
	for _, document := range documents {
		result = append(result, document.Kind+"/"+document.Metadata.Name)
	}
	return result
}
