package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	api "github.com/vivym/vela/api/gen"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/h3campaignevidence"
	"github.com/vivym/vela/internal/h3campaignrunner"
	"github.com/vivym/vela/internal/h3faultevidence"
	"github.com/vivym/vela/internal/h3preflight"
	"github.com/vivym/vela/internal/productiongates"
)

func TestRunPreflightEmitsTypedReadinessResult(t *testing.T) {
	planID := uuid.MustParse("49350000-0000-0000-0000-000000000040")
	configured := func(name string) string {
		switch name {
		case databaseURLEnvironment:
			return "postgres://evidence.example/vela"
		case validationEnvironmentKey:
			return "h3-preflight-test"
		case collectorIdentityKey:
			return "spiffe://vela/test/preflight"
		case kubeconfigKey:
			return "/secure/kubeconfig"
		case kubernetesClusterUIDKey:
			return "cluster-uid-1"
		case kubernetesNamespaceUIDKey:
			return "namespace-uid-1"
		default:
			return ""
		}
	}
	for _, test := range []struct {
		name     string
		ready    bool
		wantCode int
	}{
		{name: "ready", ready: true, wantCode: 0},
		{name: "fail closed", ready: false, wantCode: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := runPreflight(
				[]string{"release-bundle.json", planID.String()},
				configured,
				&stdout,
				&stderr,
				func(
					_ context.Context,
					bundlePath string,
					actualPlanID uuid.UUID,
					config preflightConfiguration,
				) (h3preflight.Report, error) {
					if bundlePath != "release-bundle.json" || actualPlanID != planID ||
						config.databaseURL != "postgres://evidence.example/vela" ||
						config.validationEnvironment != "h3-preflight-test" ||
						config.collectorIdentity != "spiffe://vela/test/preflight" ||
						config.kubeconfig != "/secure/kubeconfig" ||
						config.kubernetesClusterUID != "cluster-uid-1" ||
						config.kubernetesNamespaceUID != "namespace-uid-1" {
						t.Fatalf("preflight inputs = %q %s %#v", bundlePath, actualPlanID, config)
					}
					return h3preflight.Report{
						SchemaVersion:           h3preflight.SchemaVersion,
						MediaType:               h3preflight.MediaType,
						ResidencyPlanRevisionID: planID,
						Ready:                   test.ready,
					}, nil
				},
			)
			if code != test.wantCode || stderr.Len() != 0 {
				t.Fatalf("runPreflight = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
			}
			var report h3preflight.Report
			decoder := json.NewDecoder(&stdout)
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&report); err != nil || report.Ready != test.ready ||
				report.ResidencyPlanRevisionID != planID {
				t.Fatalf("preflight report = %#v error=%v", report, err)
			}
		})
	}
}

func TestRunPreflightRequiresExpectedKubernetesIdentity(t *testing.T) {
	for _, missing := range []string{kubernetesClusterUIDKey, kubernetesNamespaceUIDKey} {
		t.Run(missing, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			called := false
			code := runPreflight(
				[]string{"release-bundle.json", "49350000-0000-0000-0000-000000000040"},
				func(name string) string {
					if name == missing {
						return ""
					}
					switch name {
					case databaseURLEnvironment:
						return "postgres://evidence.example/vela"
					case validationEnvironmentKey:
						return "h3-preflight-test"
					case collectorIdentityKey:
						return "spiffe://vela/test/preflight"
					case kubernetesClusterUIDKey:
						return "cluster-uid-1"
					case kubernetesNamespaceUIDKey:
						return "namespace-uid-1"
					default:
						return ""
					}
				},
				&stdout,
				&stderr,
				func(context.Context, string, uuid.UUID, preflightConfiguration) (h3preflight.Report, error) {
					called = true
					return h3preflight.Report{}, nil
				},
			)
			if code != 2 || called || stdout.Len() != 0 || !strings.Contains(stderr.String(), missing) {
				t.Fatalf("runPreflight = code %d called %t stdout %q stderr %q", code, called, stdout.String(), stderr.String())
			}
		})
	}
}

func TestRunPreflightClassifiesInvalidReleaseInput(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runPreflight(
		[]string{"missing-release-bundle.json", "49350000-0000-0000-0000-000000000040"},
		func(name string) string {
			switch name {
			case databaseURLEnvironment:
				return "postgres://evidence.example/vela"
			case validationEnvironmentKey:
				return "h3-preflight-test"
			case collectorIdentityKey:
				return "spiffe://vela/test/preflight"
			case kubernetesClusterUIDKey:
				return "cluster-uid-1"
			case kubernetesNamespaceUIDKey:
				return "namespace-uid-1"
			default:
				return ""
			}
		},
		&stdout,
		&stderr,
		func(context.Context, string, uuid.UUID, preflightConfiguration) (h3preflight.Report, error) {
			return h3preflight.Report{}, errInvalidPreflightInput
		},
	)
	if code != 2 || stdout.Len() != 0 || !strings.Contains(stderr.String(), errInvalidPreflightInput.Error()) {
		t.Fatalf("runPreflight = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestRunRequiresLiveCaptureConfiguration(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(nil, func(string) string { return "" }, &stdout, &stderr); code != 2 {
		t.Fatalf("run exit code = %d, want 2; stderr=%s", code, stderr.String())
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "usage:") ||
		strings.Contains(stderr.String(), "snapshot") {
		t.Fatalf("run output = stdout %q stderr %q", stdout.String(), stderr.String())
	}
}

func TestSelectRolloutRequiresOneExactPlanFromRelease(t *testing.T) {
	firstID := uuid.MustParse("49350000-0000-0000-0000-000000000001")
	secondID := uuid.MustParse("49350000-0000-0000-0000-000000000002")
	rollouts := []fleetcontroller.ResidencyPlanRollout{
		{ApprovedPlan: fleet.ApprovedResidencyPlan{ID: firstID}},
		{ApprovedPlan: fleet.ApprovedResidencyPlan{ID: secondID}},
	}
	selected, err := selectRollout(rollouts, secondID)
	if err != nil || selected.ApprovedPlan.ID != secondID {
		t.Fatalf("select exact rollout = %#v error=%v", selected, err)
	}
	if _, err := selectRollout(rollouts, uuid.New()); err == nil {
		t.Fatal("missing release-bound rollout was accepted")
	}
	rollouts = append(rollouts, rollouts[1])
	if _, err := selectRollout(rollouts, secondID); err == nil {
		t.Fatal("duplicate release-bound rollout was accepted")
	}
}

func TestRunCampaignRequiresStrictDistinctJobSelection(t *testing.T) {
	planID := "49350000-0000-0000-0000-000000000010"
	sameID := "49350000-0000-0000-0000-000000000011"
	crossID := "49350000-0000-0000-0000-000000000012"
	cacheID := "49350000-0000-0000-0000-000000000013"
	configured := func(name string) string {
		switch name {
		case campaignDatabaseURLEnvironment:
			return "postgres://campaign.example/vela"
		case validationEnvironmentKey:
			return "h3-campaign-test"
		case collectorIdentityKey:
			return "spiffe://vela/test/campaign-reader"
		default:
			return ""
		}
	}
	for _, test := range []struct {
		name      string
		arguments []string
	}{
		{name: "invalid plan", arguments: []string{"bundle.json", "bad", sameID, crossID, cacheID}},
		{name: "invalid same-node", arguments: []string{"bundle.json", planID, "bad", crossID, cacheID}},
		{name: "invalid cross-node", arguments: []string{"bundle.json", planID, sameID, "bad", cacheID}},
		{name: "invalid cache", arguments: []string{"bundle.json", planID, sameID, crossID, "bad"}},
		{name: "same equals cross", arguments: []string{"bundle.json", planID, sameID, sameID, cacheID}},
		{name: "same equals cache", arguments: []string{"bundle.json", planID, sameID, crossID, sameID}},
		{name: "cross equals cache", arguments: []string{"bundle.json", planID, sameID, crossID, crossID}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			called := false
			code := runCampaign(
				test.arguments, configured, &stdout, &stderr,
				func(
					context.Context,
					string,
					uuid.UUID,
					h3campaignevidence.Selection,
					campaignConfiguration,
				) (h3campaignevidence.Evidence, error) {
					called = true
					return h3campaignevidence.Evidence{}, nil
				},
			)
			if code != 2 || called || stdout.Len() != 0 {
				t.Fatalf(
					"runCampaign = code %d called %t stdout %q stderr %q",
					code, called, stdout.String(), stderr.String(),
				)
			}
		})
	}
}

func TestRunCampaignExecutionLoadsManifestAndEmitsEvidence(t *testing.T) {
	manifest := h3campaignrunner.Manifest{
		SchemaVersion: h3campaignrunner.SchemaVersion,
		ProjectID:     uuid.MustParse("49350000-0000-0000-0000-000000000101"),
		Request: api.SubmitJobRequest{
			Model: "minimax-h3", GenerationPreset: api.Quality,
			ServiceClass: api.Standard, OutputSpec: "video-1080p-5s-24fps",
			GenerationCount: 1, Prompt: "fixed certified campaign input",
		},
		SameNodeIdempotencyKey: "h3-same-v1", CrossNodeIdempotencyKey: "h3-cross-v1",
		CacheIdempotencyKey: "h3-cache-v1", PollIntervalMilliseconds: 100,
		JobTimeoutSeconds: 3600,
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("encode campaign manifest: %v", err)
	}
	manifestPath := filepath.Join(t.TempDir(), "campaign.json")
	if err := os.WriteFile(manifestPath, encoded, 0o600); err != nil {
		t.Fatalf("write campaign manifest: %v", err)
	}
	planID := uuid.MustParse("49350000-0000-0000-0000-000000000102")
	getenv := func(name string) string {
		switch name {
		case campaignDatabaseURLEnvironment:
			return "postgres://campaign.example/vela"
		case validationEnvironmentKey:
			return "h3-campaign-test"
		case collectorIdentityKey:
			return "spiffe://vela/test/campaign-runner"
		case campaignAPIURLEnvironment:
			return "https://vela.example"
		case campaignAPITokenEnvironment:
			return "secret-token"
		default:
			return ""
		}
	}
	var stdout, stderr bytes.Buffer
	code := runCampaignExecution(
		[]string{"bundle.json", planID.String(), manifestPath}, getenv, &stdout, &stderr,
		func(
			_ context.Context,
			bundlePath string,
			actualPlanID uuid.UUID,
			actualManifest h3campaignrunner.Manifest,
			config campaignExecutionConfiguration,
		) (h3campaignevidence.Evidence, error) {
			if bundlePath != "bundle.json" || actualPlanID != planID ||
				actualManifest.ProjectID != manifest.ProjectID ||
				config.databaseURL != "postgres://campaign.example/vela" ||
				config.apiURL != "https://vela.example" || config.bearerToken != "secret-token" {
				t.Fatalf("campaign execution input = %q %s %#v %#v", bundlePath, actualPlanID, actualManifest, config)
			}
			return h3campaignevidence.Evidence{
				SchemaVersion: h3campaignevidence.SchemaVersion,
				MediaType:     h3campaignevidence.MediaType,
			}, nil
		},
	)
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("runCampaignExecution = %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
	var evidence h3campaignevidence.Evidence
	if err := json.Unmarshal(stdout.Bytes(), &evidence); err != nil ||
		evidence.MediaType != h3campaignevidence.MediaType {
		t.Fatalf("campaign evidence = %#v error=%v", evidence, err)
	}
}

func TestRunCampaignExecutionRequiresAPISecretBeforeExecution(t *testing.T) {
	manifest := h3campaignrunner.Manifest{
		SchemaVersion: h3campaignrunner.SchemaVersion,
		ProjectID:     uuid.MustParse("49350000-0000-0000-0000-000000000101"),
		Request: api.SubmitJobRequest{
			Model: "minimax-h3", GenerationPreset: api.Quality,
			ServiceClass: api.Standard, OutputSpec: "video-1080p-5s-24fps",
			GenerationCount: 1, Prompt: "fixed certified campaign input",
		},
		SameNodeIdempotencyKey: "h3-same-v1", CrossNodeIdempotencyKey: "h3-cross-v1",
		CacheIdempotencyKey: "h3-cache-v1", PollIntervalMilliseconds: 100,
		JobTimeoutSeconds: 3600,
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("encode campaign manifest: %v", err)
	}
	manifestPath := filepath.Join(t.TempDir(), "campaign.json")
	if err := os.WriteFile(manifestPath, encoded, 0o600); err != nil {
		t.Fatalf("write campaign manifest: %v", err)
	}
	called := false
	var stdout, stderr bytes.Buffer
	code := runCampaignExecution(
		[]string{"bundle.json", "49350000-0000-0000-0000-000000000102", manifestPath},
		func(name string) string {
			switch name {
			case campaignDatabaseURLEnvironment:
				return "postgres://campaign.example/vela"
			case validationEnvironmentKey:
				return "h3-campaign-test"
			case collectorIdentityKey:
				return "spiffe://vela/test/campaign-runner"
			case campaignAPIURLEnvironment:
				return "https://vela.example"
			default:
				return ""
			}
		},
		&stdout,
		&stderr,
		func(context.Context, string, uuid.UUID, h3campaignrunner.Manifest, campaignExecutionConfiguration) (h3campaignevidence.Evidence, error) {
			called = true
			return h3campaignevidence.Evidence{}, nil
		},
	)
	if code != 2 || called || stdout.Len() != 0 ||
		!strings.Contains(stderr.String(), campaignAPITokenEnvironment) {
		t.Fatalf("missing API token = code %d called=%t stdout=%q stderr=%q", code, called, stdout.String(), stderr.String())
	}
}

func TestRunCampaignUsesDedicatedConfigurationAndEmitsJSON(t *testing.T) {
	planID := uuid.MustParse("49350000-0000-0000-0000-000000000020")
	selection := h3campaignevidence.Selection{
		SameNodeJobID:  uuid.MustParse("49350000-0000-0000-0000-000000000021"),
		CrossNodeJobID: uuid.MustParse("49350000-0000-0000-0000-000000000022"),
		CacheJobID:     uuid.MustParse("49350000-0000-0000-0000-000000000023"),
	}
	getenv := func(name string) string {
		switch name {
		case campaignDatabaseURLEnvironment:
			return "postgres://campaign.example/vela"
		case databaseURLEnvironment:
			return "postgres://fleet.example/vela"
		case validationEnvironmentKey:
			return "h3-campaign-test"
		case collectorIdentityKey:
			return "spiffe://vela/test/campaign-reader"
		default:
			return ""
		}
	}
	var stdout, stderr bytes.Buffer
	code := runCampaign(
		[]string{
			"bundle.json", planID.String(), selection.SameNodeJobID.String(),
			selection.CrossNodeJobID.String(), selection.CacheJobID.String(),
		},
		getenv,
		&stdout,
		&stderr,
		func(
			_ context.Context,
			bundlePath string,
			actualPlanID uuid.UUID,
			actualSelection h3campaignevidence.Selection,
			config campaignConfiguration,
		) (h3campaignevidence.Evidence, error) {
			if bundlePath != "bundle.json" || actualPlanID != planID || actualSelection != selection {
				t.Fatalf("campaign selector = %q %s %#v", bundlePath, actualPlanID, actualSelection)
			}
			if config.databaseURL != "postgres://campaign.example/vela" ||
				config.validationEnvironment != "h3-campaign-test" ||
				config.collectorIdentity != "spiffe://vela/test/campaign-reader" {
				t.Fatalf("campaign config = %#v", config)
			}
			return h3campaignevidence.Evidence{
				SchemaVersion: h3campaignevidence.SchemaVersion,
				MediaType:     h3campaignevidence.MediaType,
			}, nil
		},
	)
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("runCampaign = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
	var evidence h3campaignevidence.Evidence
	decoder := json.NewDecoder(&stdout)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&evidence); err != nil {
		t.Fatalf("decode campaign evidence JSON: %v", err)
	}
	if evidence.SchemaVersion != h3campaignevidence.SchemaVersion ||
		evidence.MediaType != h3campaignevidence.MediaType {
		t.Fatalf("campaign evidence = %#v", evidence)
	}
}

func TestRunCampaignRejectsMissingDedicatedDatabaseURL(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runCampaign(
		[]string{
			"bundle.json",
			"49350000-0000-0000-0000-000000000030",
			"49350000-0000-0000-0000-000000000031",
			"49350000-0000-0000-0000-000000000032",
			"49350000-0000-0000-0000-000000000033",
		},
		func(name string) string {
			if name == databaseURLEnvironment {
				return "postgres://fleet.example/vela"
			}
			if name == validationEnvironmentKey {
				return "h3-campaign-test"
			}
			if name == collectorIdentityKey {
				return "spiffe://vela/test/campaign-reader"
			}
			return ""
		},
		&stdout,
		&stderr,
		nil,
	)
	if code != 2 || stdout.Len() != 0 ||
		!strings.Contains(stderr.String(), campaignDatabaseURLEnvironment) {
		t.Fatalf("runCampaign = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestRunFaultCampaignPublishesNewDirectory(t *testing.T) {
	output := filepath.Join(t.TempDir(), "fault-evidence")
	bundle := h3faultevidence.Bundle{
		Evidence: productiongates.TypedEvidence{
			SchemaVersion: 1, Gate: productiongates.GateStateEventFaultInjection,
			CriteriaRevision: "vela.production-gates/state-event-fault-injection/v2",
			ReleaseDigest:    strings.Repeat("a", 71), ConfigurationRevision: "config-r1",
			ValidationEnvironment: "repository-conformance",
		},
		EvidenceBytes: []byte("{\"schema_version\":1}\n"),
		ArtifactBytes: map[h3faultevidence.ArtifactKind][]byte{
			h3faultevidence.ArtifactScenarioMatrix:       []byte("{\"kind\":\"scenario-matrix\"}\n"),
			h3faultevidence.ArtifactAuthorityBeforeAfter: []byte("{\"kind\":\"authority-before-after\"}\n"),
			h3faultevidence.ArtifactRawEventPayloads:     []byte("{\"kind\":\"raw-event-payloads\"}\n"),
		},
	}
	var stdout, stderr bytes.Buffer
	code := runFaultCampaign(
		[]string{"manifest.json", output}, &stdout, &stderr,
		func(path string) (h3faultevidence.Bundle, error) {
			if path != "manifest.json" {
				t.Fatalf("fault manifest path = %q", path)
			}
			return bundle, nil
		},
	)
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("runFaultCampaign = %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
	for _, name := range []string{
		h3faultevidence.EvidenceFileName,
		"scenario-matrix.json",
		"authority-before-after.json",
		"raw-event-payloads.json",
	} {
		if information, err := os.Stat(filepath.Join(output, name)); err != nil || information.Size() == 0 {
			t.Fatalf("fault output %s = %#v error=%v", name, information, err)
		}
	}
	var summary faultCampaignSummary
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
		t.Fatalf("decode fault campaign summary: %v", err)
	}
	if summary.EvidenceRef != h3faultevidence.EvidenceFileName || len(summary.Artifacts) != 3 {
		t.Fatalf("fault campaign summary = %#v", summary)
	}
	stdout.Reset()
	stderr.Reset()
	if code := runFaultCampaign(
		[]string{"manifest.json", output}, &stdout, &stderr,
		func(string) (h3faultevidence.Bundle, error) { return bundle, nil },
	); code != 1 || !strings.Contains(stderr.String(), "already exists") {
		t.Fatalf("replacement runFaultCampaign = %d stderr %q", code, stderr.String())
	}
}
