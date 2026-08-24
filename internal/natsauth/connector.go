package natsauth

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const (
	outboxWorkloadName     = "vela-outbox-dispatcher"
	outboxPublishSubject   = "vela.events.>"
	outboxSubscribeSubject = "_INBOX.>"
	maxCredentialBytes     = 64 * 1024
	maxRootCABytes         = 1024 * 1024
)

var (
	ErrInvalidOutboxConfig     = errors.New("invalid NATS Outbox configuration")
	ErrInvalidOutboxCredential = errors.New("invalid NATS Outbox workload credential")
	ErrOutboxConnection        = errors.New("authenticated NATS Outbox transport failed")
)

type OutboxConfig struct {
	URL                      string
	CredentialsFile          string
	RootCAFile               string
	ExpectedAccountPublicKey string
	ExpectedUserPublicKeys   []string
	ClientCertificateFile    string
	ClientKeyFile            string
}

type Handlers struct {
	Disconnect func(error)
	Reconnect  func(connectedURL string)
	AsyncError func(error)
	Closed     func(error)
}

func ConnectOutbox(config OutboxConfig, handlers Handlers) (*nats.Conn, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	credential, err := loadCredential(config)
	if err != nil {
		return nil, err
	}
	credential.close()

	tlsConfiguration, err := loadTLSConfig(config)
	if err != nil {
		return nil, err
	}
	options := []nats.Option{
		nats.Name(outboxWorkloadName),
		nats.Secure(tlsConfiguration),
		nats.UserJWT(
			func() (string, error) {
				current, loadErr := loadCredential(config)
				if loadErr != nil {
					return "", ErrInvalidOutboxCredential
				}
				defer current.close()
				return current.userJWT, nil
			},
			func(nonce []byte) ([]byte, error) {
				current, loadErr := loadCredential(config)
				if loadErr != nil {
					return nil, ErrInvalidOutboxCredential
				}
				defer current.close()
				return current.userKey.Sign(nonce)
			},
		),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.Timeout(5 * time.Second),
	}
	if handlers.Disconnect != nil {
		options = append(options, nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			handlers.Disconnect(err)
		}))
	}
	if handlers.Reconnect != nil {
		options = append(options, nats.ReconnectHandler(func(connection *nats.Conn) {
			handlers.Reconnect(connection.ConnectedUrl())
		}))
	}
	if handlers.AsyncError != nil {
		options = append(options, nats.ErrorHandler(func(
			_ *nats.Conn,
			_ *nats.Subscription,
			err error,
		) {
			handlers.AsyncError(err)
		}))
	}
	if handlers.Closed != nil {
		options = append(options, nats.ClosedHandler(func(connection *nats.Conn) {
			handlers.Closed(connection.LastError())
		}))
	}
	connection, err := nats.Connect(config.URL, options...)
	if err != nil {
		return nil, ErrOutboxConnection
	}
	return connection, nil
}

func validateConfig(config OutboxConfig) error {
	if config.URL == "" || config.CredentialsFile == "" || config.RootCAFile == "" ||
		!nkeys.IsValidPublicAccountKey(config.ExpectedAccountPublicKey) ||
		!validExpectedUsers(config.ExpectedUserPublicKeys) {
		return ErrInvalidOutboxConfig
	}
	if (config.ClientCertificateFile == "") != (config.ClientKeyFile == "") {
		return ErrInvalidOutboxConfig
	}
	for _, serverURL := range strings.Split(config.URL, ",") {
		parsed, err := url.Parse(strings.TrimSpace(serverURL))
		if err != nil || parsed.Scheme != "tls" || parsed.Host == "" || parsed.User != nil ||
			parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return ErrInvalidOutboxConfig
		}
	}
	return nil
}

type loadedCredential struct {
	contents []byte
	userJWT  string
	userKey  nkeys.KeyPair
}

func (c *loadedCredential) close() {
	if c == nil {
		return
	}
	if c.userKey != nil {
		c.userKey.Wipe()
	}
	clear(c.contents)
	*c = loadedCredential{}
}

func loadCredential(config OutboxConfig) (*loadedCredential, error) {
	contents, err := readBoundedRegularFile(config.CredentialsFile, maxCredentialBytes, true)
	if err != nil {
		return nil, ErrInvalidOutboxCredential
	}
	credential := &loadedCredential{contents: contents}
	fail := func() (*loadedCredential, error) {
		credential.close()
		return nil, ErrInvalidOutboxCredential
	}
	userJWT, err := nkeys.ParseDecoratedJWT(contents)
	if err != nil {
		return fail()
	}
	claims, err := jwt.DecodeUserClaims(userJWT)
	if err != nil || !validOutboxClaims(claims, config, time.Now()) {
		return fail()
	}
	userKey, err := nkeys.ParseDecoratedUserNKey(contents)
	if err != nil {
		return fail()
	}
	credential.userKey = userKey
	seedPublicKey, err := userKey.PublicKey()
	if err != nil || !contains(config.ExpectedUserPublicKeys, seedPublicKey) || seedPublicKey != claims.Subject {
		return fail()
	}
	credential.userJWT = userJWT
	return credential, nil
}

func validOutboxClaims(claims *jwt.UserClaims, config OutboxConfig, now time.Time) bool {
	if claims == nil || !contains(config.ExpectedUserPublicKeys, claims.Subject) ||
		claims.IssuerAccount != config.ExpectedAccountPublicKey ||
		claims.Name != outboxWorkloadName || claims.BearerToken || claims.ProxyRequired ||
		claims.Expires == 0 || now.Unix() >= claims.Expires ||
		claims.NotBefore > now.Unix() || claims.IssuedAt > now.Add(time.Minute).Unix() ||
		claims.Resp != nil {
		return false
	}
	if !exactSubjects(claims.AllowedConnectionTypes, jwt.ConnectionTypeStandard) ||
		!exactSubjects(claims.Pub.Allow, outboxPublishSubject) ||
		!exactSubjects(claims.Sub.Allow, outboxSubscribeSubject) ||
		len(claims.Pub.Deny) != 0 || len(claims.Sub.Deny) != 0 {
		return false
	}
	return true
}

func validExpectedUsers(publicKeys []string) bool {
	if len(publicKeys) < 1 || len(publicKeys) > 2 {
		return false
	}
	seen := make(map[string]struct{}, len(publicKeys))
	for _, publicKey := range publicKeys {
		if !nkeys.IsValidPublicUserKey(publicKey) {
			return false
		}
		if _, duplicate := seen[publicKey]; duplicate {
			return false
		}
		seen[publicKey] = struct{}{}
	}
	return true
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func exactSubjects(subjects jwt.StringList, expected string) bool {
	return len(subjects) == 1 && subjects[0] == expected
}

func loadTLSConfig(config OutboxConfig) (*tls.Config, error) {
	rootPEM, err := readBoundedRegularFile(config.RootCAFile, maxRootCABytes, false)
	if err != nil {
		return nil, ErrInvalidOutboxConfig
	}
	defer clear(rootPEM)
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(rootPEM) {
		return nil, ErrInvalidOutboxConfig
	}
	tlsConfiguration := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    rootCAs,
	}
	if config.ClientCertificateFile != "" {
		certificate, loadErr := tls.LoadX509KeyPair(
			config.ClientCertificateFile,
			config.ClientKeyFile,
		)
		if loadErr != nil {
			return nil, ErrInvalidOutboxConfig
		}
		tlsConfiguration.Certificates = []tls.Certificate{certificate}
	}
	return tlsConfiguration, nil
}

func readBoundedRegularFile(path string, maximum int64, secret bool) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	information, err := file.Stat()
	if err != nil || !information.Mode().IsRegular() || information.Size() < 1 ||
		information.Size() > maximum || secret && information.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("file does not satisfy the NATS security contract")
	}
	contents, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(contents)) > maximum {
		clear(contents)
		return nil, errors.New("file does not satisfy the NATS size contract")
	}
	return contents, nil
}
