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
