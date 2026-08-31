package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/h3launchevidence"
	"github.com/vivym/vela/internal/releasebundle"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	resourceclient "k8s.io/client-go/kubernetes/typed/resource/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	databaseURLEnvironment   = "VELA_H3_EVIDENCE_FLEET_DATABASE_URL"
	validationEnvironmentKey = "VELA_H3_EVIDENCE_VALIDATION_ENVIRONMENT"
	collectorIdentityKey     = "VELA_H3_EVIDENCE_COLLECTOR_IDENTITY"
	kubeconfigKey            = "VELA_H3_EVIDENCE_KUBECONFIG"
	captureTimeout           = 2 * time.Minute
)

type configuration struct {
	databaseURL           string
	validationEnvironment string
	collectorIdentity     string
	kubeconfig            string
}

func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

func run(arguments []string, getenv func(string) string, stdout, stderr io.Writer) int {
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
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(evidence); err != nil {
		_, _ = fmt.Fprintf(stderr, "encode H3 launch evidence: %v\n", err)
		return 1
	}
	return 0
}

func loadConfiguration(getenv func(string) string) (configuration, error) {
	config := configuration{
		databaseURL:           getenv(databaseURLEnvironment),
		validationEnvironment: getenv(validationEnvironmentKey),
		collectorIdentity:     getenv(collectorIdentityKey),
		kubeconfig:            getenv(kubeconfigKey),
	}
	for _, required := range []struct {
		name  string
		value string
	}{
		{name: databaseURLEnvironment, value: config.databaseURL},
		{name: validationEnvironmentKey, value: config.validationEnvironment},
		{name: collectorIdentityKey, value: config.collectorIdentity},
	} {
		if required.value == "" || strings.TrimSpace(required.value) != required.value {
			return configuration{}, fmt.Errorf("%s is required and must not contain surrounding whitespace", required.name)
		}
	}
	if config.kubeconfig != "" && strings.TrimSpace(config.kubeconfig) != config.kubeconfig {
		return configuration{}, fmt.Errorf("%s must not contain surrounding whitespace", kubeconfigKey)
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
}
