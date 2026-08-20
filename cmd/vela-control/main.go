package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/vivym/vela/internal/admission"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/httpapi"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/outbox"
)

const (
	defaultHTTPAddress    = ":8080"
	defaultPublisherBatch = 100
	defaultPublisherTick  = 500 * time.Millisecond
)

type config struct {
	httpAddress         string
	authDatabaseURL     string
	requestDatabaseURL  string
	internalDatabaseURL string
	credentialPepper    []byte
	natsURL             string
	natsCredentials     string
	natsRootCA          string
	natsClientCert      string
	natsClientKey       string
	publisherBatchSize  int32
	publisherTick       time.Duration
}

func main() {
	if err := run(); err != nil {
		slog.Error("vela-control stopped", "error", err)
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

	authPool, err := openPool(ctx, configuration.authDatabaseURL, 5, veladb.RoleAuth)
	if err != nil {
		return fmt.Errorf("open auth database pool: %w", err)
	}
	defer authPool.Close()
	requestPool, err := openPool(ctx, configuration.requestDatabaseURL, 20, veladb.RoleRequest)
	if err != nil {
		return fmt.Errorf("open request database pool: %w", err)
	}
	defer requestPool.Close()
	internalPool, err := openPool(ctx, configuration.internalDatabaseURL, 5, veladb.RoleInternal)
	if err != nil {
		return fmt.Errorf("open internal database pool: %w", err)
	}
	defer internalPool.Close()

	natsConnection, err := connectNATS(configuration)
	if err != nil {
		return err
	}
	defer natsConnection.Close()
	broker, err := outbox.NewJetStreamBroker(natsConnection)
	if err != nil {
		return err
	}
	publisher, err := outbox.NewPublisher(internalPool, broker, outbox.Config{
		InstanceID: "vela-control-" + uuid.NewString(),
		BatchSize:  configuration.publisherBatchSize,
		ClaimTTL:   30 * time.Second,
		RetryDelay: 5 * time.Second,
	})
	if err != nil {
		return err
	}

	apiHandler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator: identity.NewAuthenticator(authPool, configuration.credentialPepper),
		Admission:     admission.NewService(requestPool),
	})
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /readyz", readinessHandler(authPool, requestPool, internalPool))
	mux.Handle("/", apiHandler)
	httpServer := &http.Server{
		Addr:              configuration.httpAddress,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		runPublisher(ctx, publisher, configuration.publisherTick)
	}()
	serverErrors := make(chan error, 1)
	go func() {
		slog.Info("vela-control HTTP server started", "address", configuration.httpAddress)
		serverErrors <- httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			stop()
			return fmt.Errorf("serve HTTP: %w", err)
		}
	}
	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelShutdown()
	if err := httpServer.Shutdown(shutdownContext); err != nil {
		return fmt.Errorf("shut down HTTP server: %w", err)
	}
	stop()
	select {
	case <-publisherDone:
	case <-shutdownContext.Done():
		return errors.New("outbox Publisher did not stop before shutdown deadline")
	}
	if err := natsConnection.Drain(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
		return fmt.Errorf("drain NATS connection: %w", err)
	}
	return nil
}

func loadConfig() (config, error) {
	configuration := config{
		httpAddress:         envOrDefault("VELA_HTTP_ADDRESS", defaultHTTPAddress),
		authDatabaseURL:     os.Getenv("VELA_AUTH_DATABASE_URL"),
		requestDatabaseURL:  os.Getenv("VELA_REQUEST_DATABASE_URL"),
		internalDatabaseURL: os.Getenv("VELA_INTERNAL_DATABASE_URL"),
		natsURL:             os.Getenv("VELA_NATS_URL"),
		natsCredentials:     os.Getenv("VELA_NATS_CREDENTIALS_FILE"),
		natsRootCA:          os.Getenv("VELA_NATS_ROOT_CA_FILE"),
		natsClientCert:      os.Getenv("VELA_NATS_CLIENT_CERT_FILE"),
		natsClientKey:       os.Getenv("VELA_NATS_CLIENT_KEY_FILE"),
		publisherBatchSize:  defaultPublisherBatch,
		publisherTick:       defaultPublisherTick,
	}
	for name, value := range map[string]string{
		"VELA_AUTH_DATABASE_URL":     configuration.authDatabaseURL,
		"VELA_REQUEST_DATABASE_URL":  configuration.requestDatabaseURL,
		"VELA_INTERNAL_DATABASE_URL": configuration.internalDatabaseURL,
		"VELA_NATS_URL":              configuration.natsURL,
		"VELA_NATS_CREDENTIALS_FILE": configuration.natsCredentials,
		"VELA_NATS_ROOT_CA_FILE":     configuration.natsRootCA,
	} {
		if value == "" {
			return config{}, fmt.Errorf("%s is required", name)
		}
	}
	encodedPepper := os.Getenv("VELA_CREDENTIAL_PEPPER_BASE64")
	pepper, err := base64.StdEncoding.DecodeString(encodedPepper)
	if err != nil || len(pepper) < 32 {
		return config{}, errors.New("environment variable VELA_CREDENTIAL_PEPPER_BASE64 must encode at least 32 random bytes")
	}
	configuration.credentialPepper = pepper
	if value := os.Getenv("VELA_OUTBOX_BATCH_SIZE"); value != "" {
		batchSize, err := strconv.ParseInt(value, 10, 32)
		if err != nil || batchSize < 1 || batchSize > 1000 {
			return config{}, errors.New("environment variable VELA_OUTBOX_BATCH_SIZE must be between 1 and 1000")
		}
		configuration.publisherBatchSize = int32(batchSize)
	}
	return configuration, nil
}

func openPool(
	ctx context.Context,
	databaseURL string,
	maxConnections int32,
	role veladb.Role,
) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	poolConfig.MaxConns = maxConnections
	poolConfig.MinConns = 1
	poolConfig.MaxConnLifetime = 30 * time.Minute
	poolConfig.MaxConnIdleTime = 5 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := veladb.VerifyRole(ctx, pool, role); err != nil {
		pool.Close()
		return nil, fmt.Errorf("verify %s database pool: %w", role, err)
	}
	return pool, nil
}

func connectNATS(configuration config) (*nats.Conn, error) {
	options := []nats.Option{
		nats.Name("vela-control-outbox"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.Timeout(5 * time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				slog.Warn("NATS disconnected; Outbox will remain durable", "error", err)
			}
		}),
		nats.ReconnectHandler(func(connection *nats.Conn) {
			slog.Info("NATS reconnected", "url", connection.ConnectedUrl())
		}),
	}
	options = append(
		options,
		nats.UserCredentials(configuration.natsCredentials),
		nats.RootCAs(configuration.natsRootCA),
	)
	if configuration.natsClientCert != "" || configuration.natsClientKey != "" {
		if configuration.natsClientCert == "" || configuration.natsClientKey == "" {
			return nil, errors.New("both VELA_NATS_CLIENT_CERT_FILE and VELA_NATS_CLIENT_KEY_FILE are required")
		}
		options = append(options, nats.ClientCert(configuration.natsClientCert, configuration.natsClientKey))
	}
	connection, err := nats.Connect(configuration.natsURL, options...)
	if err != nil {
		return nil, fmt.Errorf("connect NATS: %w", err)
	}
	return connection, nil
}

func readinessHandler(pools ...*pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		for _, pool := range pools {
			if err := pool.Ping(ctx); err != nil {
				http.Error(w, "database unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func runPublisher(ctx context.Context, publisher *outbox.Publisher, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		published, err := publisher.PublishBatch(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("Outbox publish batch incomplete", "published", published, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
