package natsauth

import (
	"errors"
	"sync"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const schedulerWorkloadName = "vela-scheduler-consumer"

var (
	ErrInvalidSchedulerConfig     = errors.New("invalid NATS Scheduler configuration")
	ErrInvalidSchedulerCredential = errors.New("invalid NATS Scheduler workload credential")
	ErrSchedulerConnection        = errors.New("authenticated NATS Scheduler transport failed")
)

var schedulerPublishSubjects = []string{
	"$JS.API.STREAM.INFO.VELA_EVENTS",
	"$JS.API.CONSUMER.INFO.VELA_EVENTS.VELA_SCHEDULER",
	"$JS.API.CONSUMER.MSG.NEXT.VELA_EVENTS.VELA_SCHEDULER",
	"$JS.ACK.VELA_EVENTS.VELA_SCHEDULER.>",
}

type SchedulerConfig struct {
	URL                             string
	CredentialsFile                 string
	RootCAFile                      string
	ExpectedAccountPublicKey        string
	ExpectedAccountSignerPublicKeys []string
	ExpectedUserPublicKeys          []string
	ClientCertificateFile           string
	ClientKeyFile                   string
}

type SchedulerConnection struct {
	*nats.Conn
	pendingServers []string
	activateOnce   sync.Once
	activateErr    error
}

func (c *SchedulerConnection) Activate() error {
	if c == nil || c.Conn == nil {
		return ErrSchedulerConnection
	}
	c.activateOnce.Do(func() {
		if len(c.pendingServers) == 0 {
			return
		}
		if err := c.SetServerPool(c.pendingServers); err != nil {
			c.activateErr = ErrSchedulerConnection
			return
		}
		c.pendingServers = nil
	})
	return c.activateErr
}

func ConnectScheduler(config SchedulerConfig, handlers Handlers) (*SchedulerConnection, error) {
	baseConfig := schedulerOutboxConfig(config)
	if err := validateConfig(baseConfig); err != nil {
		return nil, ErrInvalidSchedulerConfig
	}
	config.ExpectedAccountSignerPublicKeys = append(
		[]string(nil),
		config.ExpectedAccountSignerPublicKeys...,
	)
	config.ExpectedUserPublicKeys = append([]string(nil), config.ExpectedUserPublicKeys...)
	credential, err := loadSchedulerCredential(config)
	if err != nil {
		return nil, err
	}
	credential.close()
	tlsConfiguration, err := loadTLSConfig(baseConfig)
	if err != nil {
		return nil, ErrInvalidSchedulerConfig
	}
	baseOptions := []nats.Option{
		nats.Name(schedulerWorkloadName),
		nats.Secure(tlsConfiguration),
		nats.UserJWT(
			func() (string, error) {
				current, loadErr := loadSchedulerCredential(config)
				if loadErr != nil {
					return "", ErrInvalidSchedulerCredential
				}
				defer current.close()
				return current.userJWT, nil
			},
			func(nonce []byte) ([]byte, error) {
				current, loadErr := loadSchedulerCredential(config)
				if loadErr != nil {
					return nil, ErrInvalidSchedulerCredential
				}
				defer current.close()
				return current.userKey.Sign(nonce)
			},
		),
		nats.MaxReconnects(-1),
		nats.IgnoreAuthErrorAbort(),
		nats.ReconnectWait(time.Second),
		nats.Timeout(5 * time.Second),
	}
	connection, err := connectInitialOutbox(config.URL, baseOptions)
	var pendingServers []string
	if errors.Is(err, nats.ErrNoServers) {
		options := append([]nats.Option(nil), baseOptions...)
		options = append(options, nats.RetryOnFailedConnect(true))
		connection, err = nats.Connect(degradedBootstrapURL, options...)
		pendingServers = splitServerList(config.URL)
	}
	if err != nil {
		return nil, ErrSchedulerConnection
	}
	setOutboxHandlers(connection, handlers)
	return &SchedulerConnection{Conn: connection, pendingServers: pendingServers}, nil
}

func schedulerOutboxConfig(config SchedulerConfig) OutboxConfig {
	return OutboxConfig(config)
}

func loadSchedulerCredential(config SchedulerConfig) (*loadedCredential, error) {
	contents, err := readBoundedRegularFile(config.CredentialsFile, maxCredentialBytes, true)
	if err != nil {
		return nil, ErrInvalidSchedulerCredential
	}
	credential := &loadedCredential{contents: contents}
	fail := func() (*loadedCredential, error) {
		credential.close()
		return nil, ErrInvalidSchedulerCredential
	}
	userJWT, err := nkeys.ParseDecoratedJWT(contents)
	if err != nil {
		return fail()
	}
	claims, err := jwt.DecodeUserClaims(userJWT)
	if err != nil || !validSchedulerClaims(claims, config, time.Now()) {
		return fail()
	}
	userKey, err := nkeys.ParseDecoratedUserNKey(contents)
	if err != nil {
		return fail()
	}
	credential.userKey = userKey
	seedPublicKey, err := userKey.PublicKey()
	if err != nil || !contains(config.ExpectedUserPublicKeys, seedPublicKey) ||
		seedPublicKey != claims.Subject {
		return fail()
	}
	credential.userJWT = userJWT
	return credential, nil
}

func validSchedulerClaims(claims *jwt.UserClaims, config SchedulerConfig, now time.Time) bool {
	if claims == nil || !contains(config.ExpectedUserPublicKeys, claims.Subject) ||
		claims.IssuerAccount != config.ExpectedAccountPublicKey ||
		!contains(config.ExpectedAccountSignerPublicKeys, claims.Issuer) ||
		claims.Name != schedulerWorkloadName || claims.BearerToken || claims.ProxyRequired ||
		claims.Expires == 0 || now.Unix() >= claims.Expires ||
		claims.NotBefore > now.Unix() || claims.IssuedAt == 0 ||
		claims.IssuedAt > now.Add(time.Minute).Unix() || claims.Resp != nil ||
		!exactSubjects(claims.AllowedConnectionTypes, jwt.ConnectionTypeStandard) ||
		!exactSubjectSet(claims.Pub.Allow, schedulerPublishSubjects) ||
		!exactSubjects(claims.Sub.Allow, outboxSubscribeSubject) ||
		len(claims.Pub.Deny) != 0 || len(claims.Sub.Deny) != 0 {
		return false
	}
	return true
}

func exactSubjectSet(actual jwt.StringList, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	remaining := make(map[string]struct{}, len(expected))
	for _, subject := range expected {
		remaining[subject] = struct{}{}
	}
	for _, subject := range actual {
		if _, ok := remaining[subject]; !ok {
			return false
		}
		delete(remaining, subject)
	}
	return len(remaining) == 0
}
