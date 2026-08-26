package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetadmission"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/fleettransport"
	"github.com/vivym/vela/internal/securefile"
	"gopkg.in/yaml.v3"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

const (
	maximumDesiredInputBytes = 1 << 20
	startupTimeout           = 20 * time.Second
	shutdownTimeout          = 20 * time.Second
)

type config struct {
	namespace                     string
	desiredInputFile              string
	maintenanceAddress            string
	maintenanceServerName         string
	tlsCertificateFile            string
	tlsPrivateKeyFile             string
	maintenanceCAFile             string
	admissionAddress              string
	admissionTLSCertificate       string
	admissionTLSPrivateKey        string
	admissionClientCAFile         string
	admissionClientSPIFFEIdentity string
	kubernetesUsername            string
	podControllerUsername         string
	pollInterval                  time.Duration
	desiredRevisions              []fleetcontroller.DesiredRevision
	retirementPlans               []fleetcontroller.RetirementPlan
}

type desiredInput struct {
	APIVersion  string                 `yaml:"apiVersion"`
	Kind        string                 `yaml:"kind"`
	Revisions   []desiredRevisionInput `yaml:"revisions"`
	Retirements []retirementPlanInput  `yaml:"retirements"`
}

type desiredRevisionInput struct {
	WorkerPoolID               string              `yaml:"workerPoolID"`
	Name                       string              `yaml:"name"`
	Revision                   string              `yaml:"revision"`
	WorkerProfile              string              `yaml:"workerProfile"`
	DaemonSetName              string              `yaml:"daemonSetName"`
	NodeSelector               map[string]string   `yaml:"nodeSelector"`
	InitImage                  string              `yaml:"initImage"`
	WorkerAgentImage           string              `yaml:"workerAgentImage"`
	RunnerImage                string              `yaml:"runnerImage"`
	WorkerRuntimeConfigMap     string              `yaml:"workerRuntimeConfigMap"`
	RunnerProfilesConfigMap    string              `yaml:"runnerProfilesConfigMap"`
	RunnerGPURolesConfigMap    string              `yaml:"runnerGPURolesConfigMap"`
	WorkerControlTLSSecret     string              `yaml:"workerControlTLSSecret"`
	ArtifactStoreTLSSecret     string              `yaml:"artifactStoreTLSSecret"`
	ExecutionProfileRevisionID string              `yaml:"executionProfileRevisionID"`
	InferenceBackendRevision   string              `yaml:"inferenceBackendRevision"`
	ReadinessTimeout           string              `yaml:"readinessTimeout"`
	CapacityPolicy             capacityPolicyInput `yaml:"capacityPolicy"`
}

type capacityPolicyInput struct {
	WorkerHighWatermarkBytes int64  `yaml:"workerHighWatermarkBytes"`
	WorkerLowWatermarkBytes  int64  `yaml:"workerLowWatermarkBytes"`
	WorkerCriticalFreeBytes  int64  `yaml:"workerCriticalFreeBytes"`
	PoolHighWatermarkBytes   int64  `yaml:"poolHighWatermarkBytes"`
	PoolLowWatermarkBytes    int64  `yaml:"poolLowWatermarkBytes"`
	ObservationMaxAge        string `yaml:"observationMaxAge"`
}

type retirementPlanInput struct {
	Revision                string                  `yaml:"revision"`
	WorkerPoolID            string                  `yaml:"workerPoolID"`
	WorkerPoolName          string                  `yaml:"workerPoolName"`
	WorkerPoolKubernetesUID string                  `yaml:"workerPoolKubernetesUID"`
	DaemonSetName           string                  `yaml:"daemonSetName"`
	DaemonSetKubernetesUID  string                  `yaml:"daemonSetKubernetesUID"`
	Reason                  string                  `yaml:"reason"`
	Deadline                string                  `yaml:"deadline"`
	Workers                 []workerRetirementInput `yaml:"workers"`
}

type workerRetirementInput struct {
	OperationID      string `yaml:"operationID"`
	WorkerID         string `yaml:"workerID"`
	WorkerEpoch      int64  `yaml:"workerEpoch"`
	PodName          string `yaml:"podName"`
	PodKubernetesUID string `yaml:"podKubernetesUID"`
}

type readinessSource interface {
	Ready() bool
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "vela-fleet-controller stopped: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configuration, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runWithContext(ctx, configuration)
}

func runWithContext(ctx context.Context, configuration config) error {
	if ctx == nil {
		return errors.New("fleet controller context is required")
	}
	certificate, err := tls.LoadX509KeyPair(
		configuration.admissionTLSCertificate,
		configuration.admissionTLSPrivateKey,
	)
	if err != nil {
		return fmt.Errorf("load Fleet admission TLS certificate: %w", err)
	}
	admissionTLSConfig, err := newAdmissionTLSConfig(
		certificate,
		configuration.admissionClientCAFile,
	)
	if err != nil {
		return err
	}
	kubernetesConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("load in-cluster Kubernetes configuration: %w", err)
	}
	kubernetesClient, err := dynamic.NewForConfig(kubernetesConfig)
	if err != nil {
		return fmt.Errorf("configure Kubernetes dynamic client: %w", err)
	}
	resources, err := fleetcontroller.NewKubernetesResources(
		kubernetesClient,
		configuration.namespace,
	)
	if err != nil {
		return err
	}
	transportCredentials, err := fleettransport.NewClientTLSCredentials(
		configuration.tlsCertificateFile,
		configuration.tlsPrivateKeyFile,
		configuration.maintenanceCAFile,
		configuration.maintenanceServerName,
	)
	if err != nil {
		return fmt.Errorf("configure Fleet maintenance mTLS: %w", err)
	}
	connectContext, cancelConnect := context.WithTimeout(ctx, startupTimeout)
	maintenanceClient, err := fleettransport.DialClient(
		connectContext,
		configuration.maintenanceAddress,
		transportCredentials,
	)
	cancelConnect()
	if err != nil {
		return err
	}
	defer func() { _ = maintenanceClient.Close() }()
	reconciler, err := fleetcontroller.NewReconciler(
		resources,
		maintenanceClient,
		maintenanceClient,
		maintenanceClient,
		maintenanceClient,
	)
	if err != nil {
		return err
	}
	runtimeController, err := fleetcontroller.NewRuntime(
		resources,
		reconciler,
		fleetcontroller.RuntimeConfig{
			DesiredRevisions: configuration.desiredRevisions,
			RetirementPlans:  configuration.retirementPlans,
			PollInterval:     configuration.pollInterval,
			ReportError: func(cause error) {
				slog.Error("Fleet reconciliation cycle failed", "error", cause)
			},
		},
	)
	if err != nil {
		return err
	}
	admissionHandler, err := fleetadmission.NewHandler(
		maintenanceClient,
		fleetadmission.Config{
			FleetUsername:         configuration.kubernetesUsername,
			PodControllerUsername: configuration.podControllerUsername,
			PodCreateValidator:    resources,
		},
	)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", configuration.admissionAddress)
	if err != nil {
		return fmt.Errorf("listen for Fleet admission HTTPS: %w", err)
	}
	defer func() { _ = listener.Close() }()
	httpServer := &http.Server{
		Handler: newFleetHTTPHandler(
			admissionHandler,
			runtimeController,
			configuration.admissionClientSPIFFEIdentity,
		),
		TLSConfig:         admissionTLSConfig,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    2 << 20,
	}
	runContext, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	runtimeErrors := make(chan error, 1)
	go func() {
		runtimeErrors <- runtimeController.Run(runContext)
	}()
	serverErrors := make(chan error, 1)
	go func() {
		slog.Info("vela-fleet-controller admission HTTPS server started", "address", configuration.admissionAddress)
		serverErrors <- httpServer.ServeTLS(listener, "", "")
	}()

	var serveErr error
	runtimeStopped := false
	select {
	case <-ctx.Done():
	case err := <-runtimeErrors:
		runtimeStopped = true
		if err != nil {
			serveErr = fmt.Errorf("run Fleet reconciliation: %w", err)
		} else if ctx.Err() == nil {
			serveErr = errors.New("fleet reconciliation stopped unexpectedly")
		}
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr = fmt.Errorf("serve Fleet admission HTTPS: %w", err)
		}
	}
	cancelRun()
	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	if err := httpServer.Shutdown(shutdownContext); err != nil {
		return fmt.Errorf("shut down Fleet admission HTTPS server: %w", err)
	}
	if !runtimeStopped {
		select {
		case err := <-runtimeErrors:
			if err != nil && serveErr == nil {
				serveErr = fmt.Errorf("stop Fleet reconciliation: %w", err)
			}
		case <-shutdownContext.Done():
			return errors.New("fleet reconciliation did not stop before shutdown deadline")
		}
	}
	return serveErr
}

func newFleetHTTPHandler(
	validation http.Handler,
	readiness readinessSource,
	expectedClientSPIFFEIdentity string,
) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/validate", http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !verifiedAdmissionPeer(request, expectedClientSPIFFEIdentity) {
			http.Error(writer, "verified kube-apiserver client identity is required", http.StatusUnauthorized)
			return
		}
		validation.ServeHTTP(writer, request)
	}))
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /readyz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		if readiness == nil || !readiness.Ready() {
			http.Error(writer, "Fleet reconciliation is not ready", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func newAdmissionTLSConfig(
	certificate tls.Certificate,
	clientCAFile string,
) (*tls.Config, error) {
	clientCAPEM, err := securefile.Read(clientCAFile, 4<<20, false)
	if err != nil {
		return nil, fmt.Errorf("read Fleet admission client CA: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(clientCAPEM) {
		return nil, errors.New("fleet admission client CA contains no certificates")
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.VerifyClientCertIfGiven,
		ClientCAs:    clientCAs,
	}, nil
}

func verifiedAdmissionPeer(request *http.Request, expectedIdentity string) bool {
	if request == nil || !validSPIFFEIdentity(expectedIdentity) || request.TLS == nil ||
		!request.TLS.HandshakeComplete || len(request.TLS.PeerCertificates) == 0 ||
		len(request.TLS.VerifiedChains) == 0 {
		return false
	}
	leaf := request.TLS.PeerCertificates[0]
	verifiedLeaf := false
	for _, chain := range request.TLS.VerifiedChains {
		if len(chain) != 0 && bytes.Equal(chain[0].Raw, leaf.Raw) {
			verifiedLeaf = true
			break
		}
	}
	return verifiedLeaf && len(leaf.URIs) == 1 && leaf.URIs[0] != nil &&
		leaf.URIs[0].String() == expectedIdentity
}

func validSPIFFEIdentity(value string) bool {
	identity, err := url.Parse(value)
	return err == nil && identity != nil && identity.Scheme == "spiffe" &&
		identity.Host != "" && identity.Path != "" && identity.Path != "/" &&
		identity.User == nil && identity.RawQuery == "" && identity.Fragment == "" &&
		identity.String() == value
}

func loadConfig() (config, error) {
	configuration := config{
		namespace:                     os.Getenv("VELA_FLEET_NAMESPACE"),
		desiredInputFile:              os.Getenv("VELA_FLEET_DESIRED_INPUT_FILE"),
		maintenanceAddress:            os.Getenv("VELA_FLEET_MAINTENANCE_ADDRESS"),
		maintenanceServerName:         os.Getenv("VELA_FLEET_MAINTENANCE_SERVER_NAME"),
		tlsCertificateFile:            os.Getenv("VELA_FLEET_TLS_CERT_FILE"),
		tlsPrivateKeyFile:             os.Getenv("VELA_FLEET_TLS_KEY_FILE"),
		maintenanceCAFile:             os.Getenv("VELA_FLEET_CA_FILE"),
		admissionAddress:              os.Getenv("VELA_FLEET_ADMISSION_ADDRESS"),
		admissionTLSCertificate:       os.Getenv("VELA_FLEET_ADMISSION_TLS_CERT_FILE"),
		admissionTLSPrivateKey:        os.Getenv("VELA_FLEET_ADMISSION_TLS_KEY_FILE"),
		admissionClientCAFile:         os.Getenv("VELA_FLEET_ADMISSION_CLIENT_CA_FILE"),
		admissionClientSPIFFEIdentity: os.Getenv("VELA_FLEET_ADMISSION_CLIENT_SPIFFE_ID"),
		kubernetesUsername:            os.Getenv("VELA_FLEET_KUBERNETES_USERNAME"),
		podControllerUsername:         os.Getenv("VELA_FLEET_POD_CONTROLLER_USERNAME"),
	}
	required := []struct {
		name  string
		value string
	}{
		{"VELA_FLEET_NAMESPACE", configuration.namespace},
		{"VELA_FLEET_DESIRED_INPUT_FILE", configuration.desiredInputFile},
		{"VELA_FLEET_MAINTENANCE_ADDRESS", configuration.maintenanceAddress},
		{"VELA_FLEET_MAINTENANCE_SERVER_NAME", configuration.maintenanceServerName},
		{"VELA_FLEET_TLS_CERT_FILE", configuration.tlsCertificateFile},
		{"VELA_FLEET_TLS_KEY_FILE", configuration.tlsPrivateKeyFile},
		{"VELA_FLEET_CA_FILE", configuration.maintenanceCAFile},
		{"VELA_FLEET_ADMISSION_ADDRESS", configuration.admissionAddress},
		{"VELA_FLEET_ADMISSION_TLS_CERT_FILE", configuration.admissionTLSCertificate},
		{"VELA_FLEET_ADMISSION_TLS_KEY_FILE", configuration.admissionTLSPrivateKey},
		{"VELA_FLEET_ADMISSION_CLIENT_CA_FILE", configuration.admissionClientCAFile},
		{"VELA_FLEET_ADMISSION_CLIENT_SPIFFE_ID", configuration.admissionClientSPIFFEIdentity},
		{"VELA_FLEET_KUBERNETES_USERNAME", configuration.kubernetesUsername},
		{"VELA_FLEET_POD_CONTROLLER_USERNAME", configuration.podControllerUsername},
	}
	for _, requirement := range required {
		name, value := requirement.name, requirement.value
		if value == "" || strings.TrimSpace(value) != value || strings.ContainsRune(value, '\x00') {
			return config{}, fmt.Errorf("%s is required", name)
		}
	}
	if !filepath.IsAbs(filepath.Clean(configuration.desiredInputFile)) ||
		filepath.Clean(configuration.desiredInputFile) != configuration.desiredInputFile {
		return config{}, errors.New("VELA_FLEET_DESIRED_INPUT_FILE must be an absolute clean path")
	}
	for _, path := range []string{
		configuration.tlsCertificateFile,
		configuration.tlsPrivateKeyFile,
		configuration.maintenanceCAFile,
		configuration.admissionTLSCertificate,
		configuration.admissionTLSPrivateKey,
		configuration.admissionClientCAFile,
	} {
		if !filepath.IsAbs(filepath.Clean(path)) || filepath.Clean(path) != path {
			return config{}, errors.New("fleet TLS file paths must be absolute clean paths")
		}
	}
	if !validSPIFFEIdentity(configuration.admissionClientSPIFFEIdentity) {
		return config{}, errors.New("VELA_FLEET_ADMISSION_CLIENT_SPIFFE_ID must be an exact SPIFFE URI")
	}
	if err := requireHostPort(configuration.maintenanceAddress, false); err != nil {
		return config{}, fmt.Errorf("VELA_FLEET_MAINTENANCE_ADDRESS: %w", err)
	}
	if err := requireHostPort(configuration.admissionAddress, true); err != nil {
		return config{}, fmt.Errorf("VELA_FLEET_ADMISSION_ADDRESS: %w", err)
	}
	pollInterval, err := time.ParseDuration(os.Getenv("VELA_FLEET_POLL_INTERVAL"))
	if err != nil || pollInterval <= 0 {
		return config{}, errors.New("VELA_FLEET_POLL_INTERVAL must be a positive duration")
	}
	configuration.pollInterval = pollInterval
	configuration.desiredRevisions, configuration.retirementPlans, err = loadDesiredConfiguration(
		configuration.desiredInputFile,
		configuration.namespace,
	)
	if err != nil {
		return config{}, fmt.Errorf("load Fleet desired input: %w", err)
	}
	return configuration, nil
}

func loadDesiredRevisions(path, namespace string) ([]fleetcontroller.DesiredRevision, error) {
	revisions, _, err := loadDesiredConfiguration(path, namespace)
	return revisions, err
}

func loadDesiredConfiguration(
	path string,
	namespace string,
) ([]fleetcontroller.DesiredRevision, []fleetcontroller.RetirementPlan, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximumDesiredInputBytes {
		return nil, nil, errors.New("fleet desired input file is empty, non-regular, or too large")
	}
	decoder := yaml.NewDecoder(io.LimitReader(file, maximumDesiredInputBytes+1))
	decoder.KnownFields(true)
	var input desiredInput
	if err := decoder.Decode(&input); err != nil {
		return nil, nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, nil, errors.New("fleet desired input must contain exactly one YAML document")
	}
	if input.APIVersion != "fleet.vela.ai/v1alpha1" ||
		input.Kind != "FleetDesiredRevisions" || len(input.Revisions) == 0 {
		return nil, nil, errors.New("fleet desired input apiVersion, kind, or revisions are invalid")
	}
	seenPools := map[uuid.UUID]struct{}{}
	seenNames := map[string]struct{}{}
	seenDaemonSets := map[string]struct{}{}
	revisions := make([]fleetcontroller.DesiredRevision, 0, len(input.Revisions))
	for _, encoded := range input.Revisions {
		workerPoolID, err := uuid.Parse(encoded.WorkerPoolID)
		if err != nil || workerPoolID == uuid.Nil {
			return nil, nil, errors.New("fleet desired revision WorkerPool id is invalid")
		}
		executionProfileRevisionID, err := uuid.Parse(encoded.ExecutionProfileRevisionID)
		if err != nil || executionProfileRevisionID == uuid.Nil {
			return nil, nil, errors.New("fleet desired revision ExecutionProfileRevision id is invalid")
		}
		readinessTimeout, err := time.ParseDuration(encoded.ReadinessTimeout)
		if err != nil {
			return nil, nil, errors.New("fleet desired revision readiness timeout is invalid")
		}
		observationMaxAge, err := time.ParseDuration(encoded.CapacityPolicy.ObservationMaxAge)
		if err != nil {
			return nil, nil, errors.New("fleet desired revision capacity observation age is invalid")
		}
		desired := fleetcontroller.DesiredRevision{
			WorkerPoolID: workerPoolID, Namespace: namespace, Name: encoded.Name,
			Revision: encoded.Revision, WorkerProfile: encoded.WorkerProfile,
			DaemonSetName: encoded.DaemonSetName, NodeSelector: encoded.NodeSelector,
			InitImage: encoded.InitImage, WorkerAgentImage: encoded.WorkerAgentImage,
			RunnerImage:                encoded.RunnerImage,
			WorkerRuntimeConfigMap:     encoded.WorkerRuntimeConfigMap,
			RunnerProfilesConfigMap:    encoded.RunnerProfilesConfigMap,
			RunnerGPURolesConfigMap:    encoded.RunnerGPURolesConfigMap,
			WorkerControlTLSSecret:     encoded.WorkerControlTLSSecret,
			ArtifactStoreTLSSecret:     encoded.ArtifactStoreTLSSecret,
			ExecutionProfileRevisionID: executionProfileRevisionID,
			InferenceBackendRevision:   encoded.InferenceBackendRevision,
			ReadinessTimeout:           readinessTimeout,
			CapacityPolicy: fleetcontroller.CapacityPolicySpec{
				WorkerHighWatermarkBytes: encoded.CapacityPolicy.WorkerHighWatermarkBytes,
				WorkerLowWatermarkBytes:  encoded.CapacityPolicy.WorkerLowWatermarkBytes,
				WorkerCriticalFreeBytes:  encoded.CapacityPolicy.WorkerCriticalFreeBytes,
				PoolHighWatermarkBytes:   encoded.CapacityPolicy.PoolHighWatermarkBytes,
				PoolLowWatermarkBytes:    encoded.CapacityPolicy.PoolLowWatermarkBytes,
				ObservationMaxAge:        observationMaxAge,
			},
		}
		if err := fleetcontroller.ValidateDesiredRevision(desired); err != nil {
			return nil, nil, err
		}
		if _, ok := seenPools[workerPoolID]; ok {
			return nil, nil, errors.New("fleet desired input contains a duplicate WorkerPool id")
		}
		if _, ok := seenNames[desired.Name]; ok {
			return nil, nil, errors.New("fleet desired input contains a duplicate WorkerPool name")
		}
		if _, ok := seenDaemonSets[desired.DaemonSetName]; ok {
			return nil, nil, errors.New("fleet desired input contains a duplicate DaemonSet name")
		}
		seenPools[workerPoolID] = struct{}{}
		seenNames[desired.Name] = struct{}{}
		seenDaemonSets[desired.DaemonSetName] = struct{}{}
		revisions = append(revisions, desired)
	}
	retirementPlans := make([]fleetcontroller.RetirementPlan, 0, len(input.Retirements))
	seenPlanRevisions := make(map[string]struct{}, len(input.Retirements))
	seenDrainOperations := make(map[uuid.UUID]struct{})
	for _, encoded := range input.Retirements {
		workerPoolID, err := uuid.Parse(encoded.WorkerPoolID)
		if err != nil || workerPoolID == uuid.Nil {
			return nil, nil, errors.New("fleet retirement plan WorkerPool id is invalid")
		}
		deadline, err := time.Parse(time.RFC3339, encoded.Deadline)
		if err != nil {
			return nil, nil, errors.New("fleet retirement plan deadline must be RFC3339")
		}
		plan := fleetcontroller.RetirementPlan{
			Revision: encoded.Revision, WorkerPoolID: workerPoolID, Namespace: namespace,
			WorkerPoolName:          encoded.WorkerPoolName,
			WorkerPoolKubernetesUID: encoded.WorkerPoolKubernetesUID,
			DaemonSetName:           encoded.DaemonSetName,
			DaemonSetKubernetesUID:  encoded.DaemonSetKubernetesUID,
			Reason:                  encoded.Reason, Deadline: deadline.UTC(),
			Workers: make([]fleetcontroller.WorkerRetirement, 0, len(encoded.Workers)),
		}
		for _, encodedWorker := range encoded.Workers {
			operationID, operationErr := uuid.Parse(encodedWorker.OperationID)
			workerID, workerErr := uuid.Parse(encodedWorker.WorkerID)
			if operationErr != nil || operationID == uuid.Nil ||
				workerErr != nil || workerID == uuid.Nil {
				return nil, nil, errors.New("fleet retirement Worker identity is invalid")
			}
			if _, exists := seenDrainOperations[operationID]; exists {
				return nil, nil, errors.New("fleet desired input reuses a retirement DrainOperation id")
			}
			seenDrainOperations[operationID] = struct{}{}
			plan.Workers = append(plan.Workers, fleetcontroller.WorkerRetirement{
				OperationID: operationID, WorkerID: workerID,
				WorkerEpoch: encodedWorker.WorkerEpoch, PodName: encodedWorker.PodName,
				PodKubernetesUID: encodedWorker.PodKubernetesUID,
			})
		}
		if err := fleetcontroller.ValidateRetirementPlan(plan); err != nil {
			return nil, nil, err
		}
		if _, exists := seenPlanRevisions[plan.Revision]; exists {
			return nil, nil, errors.New("fleet desired input contains a duplicate retirement plan revision")
		}
		seenPlanRevisions[plan.Revision] = struct{}{}
		retirementPlans = append(retirementPlans, plan)
	}
	if err := fleetcontroller.ValidateRuntimeConfiguration(revisions, retirementPlans); err != nil {
		return nil, nil, err
	}
	return revisions, retirementPlans, nil
}

func requireHostPort(address string, allowEmptyHost bool) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" || (!allowEmptyHost && host == "") {
		return errors.New("must contain a host and port")
	}
	return nil
}
