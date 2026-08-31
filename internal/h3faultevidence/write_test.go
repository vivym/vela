package h3faultevidence

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteBundlePublishesAllEvidenceAtomicallyWithoutReplacement(t *testing.T) {
	campaign, err := Load(writeCampaignFixture(t, nil))
	if err != nil {
		t.Fatalf("load fault campaign: %v", err)
	}
	bundle, err := campaign.BuildBundle()
	if err != nil {
		t.Fatalf("build fault campaign: %v", err)
	}
	output := filepath.Join(t.TempDir(), "fault-evidence")
	artifacts, err := WriteBundle(output, bundle)
	if err != nil {
		t.Fatalf("write fault campaign bundle: %v", err)
	}
	if len(artifacts) != 3 {
		t.Fatalf("published artifacts = %d", len(artifacts))
	}
	for _, name := range []string{
		EvidenceFileName,
		"scenario-matrix.json",
		"authority-before-after.json",
		"raw-event-payloads.json",
	} {
		information, statErr := os.Stat(filepath.Join(output, name))
		if statErr != nil || !information.Mode().IsRegular() || information.Size() == 0 {
			t.Fatalf("published file %s = %#v error=%v", name, information, statErr)
		}
	}
	if _, err := WriteBundle(output, bundle); !errors.Is(err, ErrInvalidCampaign) {
		t.Fatalf("replacement WriteBundle error = %v", err)
	}
}

func TestRenameNoReplacePreservesConcurrentDestination(t *testing.T) {
	parent := t.TempDir()
	source := filepath.Join(parent, "source")
	destination := filepath.Join(parent, "destination")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatalf("create source: %v", err)
	}
	if err := os.Mkdir(destination, 0o700); err != nil {
		t.Fatalf("create destination: %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, "owner"), []byte("concurrent\n"), 0o600); err != nil {
		t.Fatalf("write destination owner: %v", err)
	}
	if err := renameNoReplace(source, destination); err == nil {
		t.Fatal("renameNoReplace replaced a concurrent destination")
	}
	if encoded, err := os.ReadFile(filepath.Join(destination, "owner")); err != nil || string(encoded) != "concurrent\n" {
		t.Fatalf("destination owner = %q error=%v", encoded, err)
	}
	if _, err := os.Stat(source); err != nil {
		t.Fatalf("source removed after rejected publication: %v", err)
	}
}
