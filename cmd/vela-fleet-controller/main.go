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

	"github.com/vivym/vela/internal/fleetadmission"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/fleettransport"
	"github.com/vivym/vela/internal/securefile"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

const (
	startupTimeout  = 20 * time.Second
	shutdownTimeout = 20 * time.Second
)

type config struct {
	namespace                     string
	desiredInputFile              string
	residencyPlanRolloutsFile     string
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
	residencyPlanRollouts         []fleetcontroller.ResidencyPlanRollout
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
	var workerInstanceController *fleetcontroller.ResidencyPlanRolloutController
	var podCreateValidator fleetadmission.ProtectedPodCreateValidator = resources
	if len(configuration.residencyPlanRollouts) != 0 {
		actuator, err := fleetcontroller.NewWorkerInstanceActuator(resources)
		if err != nil {
			return err
		}
		workerInstanceController, err = fleetcontroller.NewResidencyPlanRolloutController(
			maintenanceClient,
			actuator,
		)
		if err != nil {
			return err
		}
		podCreateValidator, err = fleetcontroller.NewWorkerInstancePodAdmissionValidator(
			resources,
			configuration.residencyPlanRollouts,
		)
		if err != nil {
			return err
		}
	}
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
			DesiredRevisions:         configuration.desiredRevisions,
			RetirementPlans:          configuration.retirementPlans,
			ResidencyPlanRollouts:    configuration.residencyPlanRollouts,
			WorkerInstanceController: workerInstanceController,
			PollInterval:             configuration.pollInterval,
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
			PodCreateValidator:    podCreateValidator,
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
		residencyPlanRolloutsFile:     os.Getenv("VELA_FLEET_RESIDENCY_PLAN_ROLLOUTS_FILE"),
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
	if configuration.residencyPlanRolloutsFile != "" &&
		(!filepath.IsAbs(filepath.Clean(configuration.residencyPlanRolloutsFile)) ||
			filepath.Clean(configuration.residencyPlanRolloutsFile) != configuration.residencyPlanRolloutsFile) {
		return config{}, errors.New("VELA_FLEET_RESIDENCY_PLAN_ROLLOUTS_FILE must be an absolute clean path")
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
	if configuration.residencyPlanRolloutsFile != "" {
		configuration.residencyPlanRollouts, err = loadResidencyPlanRollouts(
			configuration.residencyPlanRolloutsFile,
			configuration.namespace,
		)
		if err != nil {
			return config{}, fmt.Errorf("load Fleet ResidencyPlan rollouts: %w", err)
		}
	}
	return configuration, nil
}

func loadResidencyPlanRollouts(
	path string,
	namespace string,
) ([]fleetcontroller.ResidencyPlanRollout, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 ||
		info.Size() > fleetcontroller.MaximumResidencyPlanRolloutBytes {
		return nil, errors.New("fleet ResidencyPlan rollout file is empty, non-regular, or too large")
	}
	encoded, err := io.ReadAll(io.LimitReader(
		file,
		fleetcontroller.MaximumResidencyPlanRolloutBytes+1,
	))
	if err != nil {
		return nil, err
	}
	return fleetcontroller.DecodeResidencyPlanRollouts(encoded, namespace)
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
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > fleetcontroller.MaximumDesiredConfigurationBytes {
		return nil, nil, errors.New("fleet desired input file is empty, non-regular, or too large")
	}
	encoded, err := io.ReadAll(io.LimitReader(file, fleetcontroller.MaximumDesiredConfigurationBytes+1))
	if err != nil {
		return nil, nil, err
	}
	return fleetcontroller.DecodeDesiredConfiguration(encoded, namespace)
}

func requireHostPort(address string, allowEmptyHost bool) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" || (!allowEmptyHost && host == "") {
		return errors.New("must contain a host and port")
	}
	return nil
}
