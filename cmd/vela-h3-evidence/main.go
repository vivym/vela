package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/h3campaignevidence"
	"github.com/vivym/vela/internal/h3faultevidence"
	"github.com/vivym/vela/internal/h3launchevidence"
	"github.com/vivym/vela/internal/releasebundle"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	resourceclient "k8s.io/client-go/kubernetes/typed/resource/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	databaseURLEnvironment         = "VELA_H3_EVIDENCE_FLEET_DATABASE_URL"
	campaignDatabaseURLEnvironment = "VELA_H3_CAMPAIGN_EVIDENCE_DATABASE_URL"
	validationEnvironmentKey       = "VELA_H3_EVIDENCE_VALIDATION_ENVIRONMENT"
	collectorIdentityKey           = "VELA_H3_EVIDENCE_COLLECTOR_IDENTITY"
	kubeconfigKey                  = "VELA_H3_EVIDENCE_KUBECONFIG"
	captureTimeout                 = 2 * time.Minute
)

type evidenceConfiguration struct {
	databaseURL           string
	validationEnvironment string
	collectorIdentity     string
}

type configuration struct {
	evidenceConfiguration
	kubeconfig string
}

type campaignConfiguration struct {
	evidenceConfiguration
}

type campaignCaptureFunc func(
	context.Context,
	string,
	uuid.UUID,
	h3campaignevidence.Selection,
	campaignConfiguration,
) (h3campaignevidence.Evidence, error)

type faultCampaignBuildFunc func(string) (h3faultevidence.Bundle, error)

type faultCampaignSummary struct {
	SchemaVersion         int                              `json:"schema_version"`
	Gate                  string                           `json:"gate"`
	CriteriaRevision      string                           `json:"criteria_revision"`
	ReleaseDigest         string                           `json:"release_digest"`
	ConfigurationRevision string                           `json:"configuration_revision"`
	ValidationEnvironment string                           `json:"validation_environment"`
	OutputDirectory       string                           `json:"output_directory"`
	EvidenceRef           string                           `json:"evidence_ref"`
	Artifacts             []h3faultevidence.OutputArtifact `json:"artifacts"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

func run(arguments []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(arguments) == 3 && arguments[0] == "build-fault-campaign" {
		return runFaultCampaign(arguments[1:], stdout, stderr, loadFaultCampaignBundle)
	}
	if len(arguments) == 6 && arguments[0] == "capture-campaign" {
		return runCampaign(arguments[1:], getenv, stdout, stderr, captureCampaign)
	}
	if len(arguments) != 3 || arguments[0] != "capture" {
		writeUsage(stderr)
		return 2
	}
	if getenv == nil || stdout == nil || stderr == nil {
		return 2
	}
	planID, err := uuid.Parse(arguments[2])
	if err != nil || planID == uuid.Nil {
		_, _ = fmt.Fprintln(stderr, "ResidencyPlan revision id is invalid")
		return 2
	}
	config, err := loadConfiguration(getenv)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}
	bundle, rollouts, err := releasebundle.LoadResidencyPlanRollouts(arguments[1])
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "verify release-bound ResidencyPlan: %v\n", err)
		return 1
	}
	rollout, err := selectRollout(rollouts, planID)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "select release-bound ResidencyPlan: %v\n", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), captureTimeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, config.databaseURL)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "configure Fleet Registry reader: %v\n", err)
		return 1
	}
	defer pool.Close()
	registry, err := h3launchevidence.NewPostgresRegistryReader(pool)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "configure Fleet Registry reader: %v\n", err)
		return 1
	}
	kubernetesConfig, err := loadKubernetesConfig(config.kubeconfig)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "configure Kubernetes reader: %v\n", err)
		return 1
	}
	core, err := coreclient.NewForConfig(kubernetesConfig)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "configure Kubernetes Core reader: %v\n", err)
		return 1
	}
	resource, err := resourceclient.NewForConfig(kubernetesConfig)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "configure Kubernetes Resource reader: %v\n", err)
		return 1
	}
	kubernetes, err := h3launchevidence.NewClientsetKubernetesReader(core, resource)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "configure Kubernetes reader: %v\n", err)
		return 1
	}
	evidence, err := h3launchevidence.Capture(ctx, kubernetes, registry, h3launchevidence.CaptureRequest{
		ReleaseDigest: bundle.ReleaseDigest, ConfigurationRevision: bundle.ConfigurationRevision,
		ValidationEnvironment: config.validationEnvironment,
		CollectorIdentity:     config.collectorIdentity, Rollout: rollout,
	})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "capture H3 launch evidence: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, evidence); err != nil {
		_, _ = fmt.Fprintf(stderr, "encode H3 launch evidence: %v\n", err)
		return 1
	}
	return 0
}

func runFaultCampaign(
	arguments []string,
	stdout io.Writer,
	stderr io.Writer,
	build faultCampaignBuildFunc,
) int {
	if len(arguments) != 2 || stdout == nil || stderr == nil || build == nil {
		writeUsage(stderr)
		return 2
	}
	bundle, err := build(arguments[0])
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "build H3 fault campaign evidence: %v\n", err)
		return 1
	}
	artifacts, err := h3faultevidence.WriteBundle(arguments[1], bundle)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "publish H3 fault campaign evidence: %v\n", err)
		return 1
	}
	summary := faultCampaignSummary{
		SchemaVersion:         bundle.Evidence.SchemaVersion,
		Gate:                  string(bundle.Evidence.Gate),
		CriteriaRevision:      bundle.Evidence.CriteriaRevision,
		ReleaseDigest:         bundle.Evidence.ReleaseDigest,
		ConfigurationRevision: bundle.Evidence.ConfigurationRevision,
		ValidationEnvironment: bundle.Evidence.ValidationEnvironment,
		OutputDirectory:       arguments[1], EvidenceRef: h3faultevidence.EvidenceFileName,
		Artifacts: artifacts,
	}
	if err := encodeJSON(stdout, summary); err != nil {
		_, _ = fmt.Fprintf(stderr, "encode H3 fault campaign summary: %v\n", err)
		return 1
	}
	return 0
}

func loadFaultCampaignBundle(path string) (h3faultevidence.Bundle, error) {
	campaign, err := h3faultevidence.Load(path)
	if err != nil {
		return h3faultevidence.Bundle{}, err
	}
	return campaign.BuildBundle()
}

func runCampaign(
	arguments []string,
	getenv func(string) string,
	stdout io.Writer,
	stderr io.Writer,
	capture campaignCaptureFunc,
) int {
	if len(arguments) != 5 || getenv == nil || stdout == nil || stderr == nil {
		writeUsage(stderr)
		return 2
	}
	planID, err := parseRequiredUUID(arguments[1], "ResidencyPlan revision id")
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}
	selection, err := parseCampaignSelection(arguments[2:])
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}
	config, err := loadCampaignConfiguration(getenv)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}
	if capture == nil {
		_, _ = fmt.Fprintln(stderr, "H3 campaign capture implementation is required")
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), captureTimeout)
	defer cancel()
	evidence, err := capture(ctx, arguments[0], planID, selection, config)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "capture H3 campaign evidence: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, evidence); err != nil {
		_, _ = fmt.Fprintf(stderr, "encode H3 campaign evidence: %v\n", err)
		return 1
	}
	return 0
}

func parseCampaignSelection(arguments []string) (h3campaignevidence.Selection, error) {
	if len(arguments) != 3 {
		return h3campaignevidence.Selection{}, errors.New("exactly three H3 campaign Job ids are required")
	}
	same, err := parseRequiredUUID(arguments[0], "same-node Job id")
	if err != nil {
		return h3campaignevidence.Selection{}, err
	}
	cross, err := parseRequiredUUID(arguments[1], "cross-node Job id")
	if err != nil {
		return h3campaignevidence.Selection{}, err
	}
	cache, err := parseRequiredUUID(arguments[2], "cache Job id")
	if err != nil {
		return h3campaignevidence.Selection{}, err
	}
	if same == cross || same == cache || cross == cache {
		return h3campaignevidence.Selection{}, errors.New("H3 campaign Job ids must be distinct")
	}
	return h3campaignevidence.Selection{
		SameNodeJobID: same, CrossNodeJobID: cross, CacheJobID: cache,
	}, nil
}

func parseRequiredUUID(value, label string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%s is invalid", label)
	}
	return id, nil
}

func loadCampaignConfiguration(getenv func(string) string) (campaignConfiguration, error) {
	config, err := loadEvidenceConfiguration(getenv, campaignDatabaseURLEnvironment)
	if err != nil {
		return campaignConfiguration{}, err
	}
	return campaignConfiguration{evidenceConfiguration: config}, nil
}

func captureCampaign(
	ctx context.Context,
	bundlePath string,
	planID uuid.UUID,
	selection h3campaignevidence.Selection,
	config campaignConfiguration,
) (h3campaignevidence.Evidence, error) {
	binding, err := h3campaignevidence.LoadEvidenceBinding(
		bundlePath, planID, config.validationEnvironment, config.collectorIdentity,
	)
	if err != nil {
		return h3campaignevidence.Evidence{}, err
	}
	pool, err := pgxpool.New(ctx, config.databaseURL)
	if err != nil {
		return h3campaignevidence.Evidence{}, fmt.Errorf("configure campaign evidence reader: %w", err)
	}
	defer pool.Close()
	if err := veladb.VerifyRole(ctx, pool, veladb.RoleH3CampaignEvidence); err != nil {
		return h3campaignevidence.Evidence{}, fmt.Errorf(
			"verify campaign evidence database role: %w", err,
		)
	}
	reader, err := h3campaignevidence.NewPostgresReader(pool)
	if err != nil {
		return h3campaignevidence.Evidence{}, fmt.Errorf("configure campaign evidence reader: %w", err)
	}
	return h3campaignevidence.Capture(ctx, reader, h3campaignevidence.CaptureRequest{
		EvidenceBinding: binding,
		Selection:       selection,
	})
}

func encodeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func loadConfiguration(getenv func(string) string) (configuration, error) {
	evidence, err := loadEvidenceConfiguration(getenv, databaseURLEnvironment)
	if err != nil {
		return configuration{}, err
	}
	config := configuration{
		evidenceConfiguration: evidence,
		kubeconfig:            getenv(kubeconfigKey),
	}
	if config.kubeconfig != "" && strings.TrimSpace(config.kubeconfig) != config.kubeconfig {
		return configuration{}, fmt.Errorf("%s must not contain surrounding whitespace", kubeconfigKey)
	}
	return config, nil
}

func loadEvidenceConfiguration(
	getenv func(string) string,
	databaseEnvironment string,
) (evidenceConfiguration, error) {
	config := evidenceConfiguration{
		databaseURL:           getenv(databaseEnvironment),
		validationEnvironment: getenv(validationEnvironmentKey),
		collectorIdentity:     getenv(collectorIdentityKey),
	}
	for _, required := range []struct {
		name  string
		value string
	}{
		{name: databaseEnvironment, value: config.databaseURL},
		{name: validationEnvironmentKey, value: config.validationEnvironment},
		{name: collectorIdentityKey, value: config.collectorIdentity},
	} {
		if required.value == "" || strings.TrimSpace(required.value) != required.value {
			return evidenceConfiguration{}, fmt.Errorf(
				"%s is required and must not contain surrounding whitespace", required.name,
			)
		}
	}
	return config, nil
}

func loadKubernetesConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		return rest.InClusterConfig()
	}
	return clientcmd.BuildConfigFromFlags("", kubeconfig)
}

func selectRollout(
	rollouts []fleetcontroller.ResidencyPlanRollout,
	planID uuid.UUID,
) (fleetcontroller.ResidencyPlanRollout, error) {
	var selected fleetcontroller.ResidencyPlanRollout
	matches := 0
	for _, rollout := range rollouts {
		if rollout.ApprovedPlan.ID == planID {
			selected = rollout
			matches++
		}
	}
	if matches != 1 {
		return fleetcontroller.ResidencyPlanRollout{}, fmt.Errorf(
			"release contains %d rollout authorities for plan %s, want exactly one",
			matches,
			planID,
		)
	}
	return selected, nil
}

func writeUsage(writer io.Writer) {
	if writer == nil {
		return
	}
	_, _ = fmt.Fprintln(
		writer,
		"usage: vela-h3-evidence capture <release-bundle.json> <residency-plan-revision-id>",
	)
	_, _ = fmt.Fprintln(
		writer,
		"       vela-h3-evidence capture-campaign <release-bundle.json> <residency-plan-revision-id> <same-node-job-id> <cross-node-job-id> <cache-job-id>",
	)
	_, _ = fmt.Fprintln(
		writer,
		"       vela-h3-evidence build-fault-campaign <fault-campaign-manifest.json> <new-output-directory>",
	)
}
