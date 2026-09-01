package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/vivym/vela/internal/h3membercampaign"
	"github.com/vivym/vela/internal/securefile"
)

const (
	campaignAddressEnvironment                 = "VELA_H3_MEMBER_CAMPAIGN_ADDRESS"
	campaignListenAddressEnvironment           = "VELA_H3_MEMBER_CAMPAIGN_LISTEN_ADDRESS"
	campaignServerNameEnvironment              = "VELA_H3_MEMBER_CAMPAIGN_SERVER_NAME"
	campaignClientCertificateEnvironment       = "VELA_H3_MEMBER_CAMPAIGN_CLIENT_TLS_CERT_FILE"
	campaignClientPrivateKeyEnvironment        = "VELA_H3_MEMBER_CAMPAIGN_CLIENT_TLS_KEY_FILE"
	campaignServerCertificateEnvironment       = "VELA_H3_MEMBER_CAMPAIGN_SERVER_TLS_CERT_FILE"
	campaignServerPrivateKeyEnvironment        = "VELA_H3_MEMBER_CAMPAIGN_SERVER_TLS_KEY_FILE"
	campaignClientCAEnvironment                = "VELA_H3_MEMBER_CAMPAIGN_CLIENT_CA_FILE"
	campaignServerCAEnvironment                = "VELA_H3_MEMBER_CAMPAIGN_SERVER_CA_FILE"
	campaignAuthorityKeyFileEnvironment        = "VELA_H3_MEMBER_CAMPAIGN_AUTHORITY_KEY_FILE"
	campaignDialTimeoutEnvironment             = "VELA_H3_MEMBER_CAMPAIGN_DIAL_TIMEOUT"
	campaignCommandTimeoutEnvironment          = "VELA_H3_MEMBER_CAMPAIGN_COMMAND_TIMEOUT"
	campaignRemoteStartDelayEnvironment        = "VELA_H3_MEMBER_CAMPAIGN_REMOTE_START_DELAY"
	campaignAuthorityKeyID                     = "campaign-authority-v1"
	maximumCampaignAuthorityKeyBytes     int64 = 4 << 10
)

type serveCampaign func(context.Context, h3membercampaign.ServerConfig) error

type executeCampaign func(
	context.Context,
	h3membercampaign.ClientConfig,
) (h3membercampaign.Receipt, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(
		ctx,
		os.Args[1:],
		os.Getenv,
		os.Stdout,
		os.Stderr,
		h3membercampaign.Serve,
		h3membercampaign.Run,
	))
}

func run(
	ctx context.Context,
	arguments []string,
	getenv func(string) string,
	stdout io.Writer,
	stderr io.Writer,
	serve serveCampaign,
	execute executeCampaign,
) int {
	if ctx == nil || getenv == nil || stdout == nil || stderr == nil || serve == nil || execute == nil ||
		len(arguments) != 1 {
		writeUsage(stderr)
		return 2
	}
	switch arguments[0] {
	case "serve":
		config, err := loadServerConfig(getenv)
		if err != nil {
			writeConfigurationError(stderr, err)
			return 2
		}
		defer clear(config.AuthorityKey)
		config.Logf = func(format string, arguments ...any) {
			_, _ = fmt.Fprintf(stderr, "campaign event: "+format+"\n", arguments...)
		}
		if err := serve(ctx, config); err != nil {
			_, _ = io.WriteString(stderr, "member campaign server failed\n")
			return 1
		}
		return 0
	case "run", "run-fault":
		config, err := loadClientConfig(getenv, arguments[0] == "run-fault")
		if err != nil {
			writeConfigurationError(stderr, err)
			return 2
		}
		defer clear(config.AuthorityKey)
		receipt, err := execute(ctx, config)
		if err != nil {
			_, _ = io.WriteString(stderr, "member campaign execution failed\n")
			return 1
		}
		if err := writeReceipt(stdout, receipt); err != nil {
			_, _ = io.WriteString(stderr, "member campaign receipt encoding failed\n")
			return 1
		}
		return 0
	default:
		writeUsage(stderr)
		return 2
	}
}

func loadServerConfig(getenv func(string) string) (h3membercampaign.ServerConfig, error) {
	listenAddress, err := requiredText(getenv, campaignListenAddressEnvironment, 512)
	if err != nil {
		return h3membercampaign.ServerConfig{}, err
	}
	certificate, err := requiredAbsolutePath(getenv, campaignServerCertificateEnvironment)
	if err != nil {
		return h3membercampaign.ServerConfig{}, err
	}
	privateKey, err := requiredAbsolutePath(getenv, campaignServerPrivateKeyEnvironment)
	if err != nil {
		return h3membercampaign.ServerConfig{}, err
	}
	clientCA, err := requiredAbsolutePath(getenv, campaignClientCAEnvironment)
	if err != nil {
		return h3membercampaign.ServerConfig{}, err
	}
	authorityKey, err := readAuthorityKey(getenv)
	if err != nil {
		return h3membercampaign.ServerConfig{}, err
	}
	return h3membercampaign.ServerConfig{
		ListenAddress: listenAddress,
		TLS: h3membercampaign.ServerTLSFiles{
			Certificate: certificate,
			PrivateKey:  privateKey,
			ClientCA:    clientCA,
		},
		AuthorityKeyID: campaignAuthorityKeyID,
		AuthorityKey:   authorityKey,
	}, nil
}

func loadClientConfig(
	getenv func(string) string,
	expectedStartFailure bool,
) (h3membercampaign.ClientConfig, error) {
	address, err := requiredText(getenv, campaignAddressEnvironment, 512)
	if err != nil {
		return h3membercampaign.ClientConfig{}, err
	}
	serverName, err := requiredText(getenv, campaignServerNameEnvironment, 253)
	if err != nil {
		return h3membercampaign.ClientConfig{}, err
	}
	certificate, err := requiredAbsolutePath(getenv, campaignClientCertificateEnvironment)
	if err != nil {
		return h3membercampaign.ClientConfig{}, err
	}
	privateKey, err := requiredAbsolutePath(getenv, campaignClientPrivateKeyEnvironment)
	if err != nil {
		return h3membercampaign.ClientConfig{}, err
	}
	serverCA, err := requiredAbsolutePath(getenv, campaignServerCAEnvironment)
	if err != nil {
		return h3membercampaign.ClientConfig{}, err
	}
	authorityKey, err := readAuthorityKey(getenv)
	if err != nil {
		return h3membercampaign.ClientConfig{}, err
	}
	clearAuthorityKey := true
	defer func() {
		if clearAuthorityKey {
			clear(authorityKey)
		}
	}()
	dialTimeout, err := requiredDuration(
		getenv, campaignDialTimeoutEnvironment, time.Millisecond, time.Minute,
	)
	if err != nil {
		return h3membercampaign.ClientConfig{}, err
	}
	commandTimeout, err := requiredDuration(
		getenv, campaignCommandTimeoutEnvironment, time.Millisecond, 10*time.Minute,
	)
	if err != nil {
		return h3membercampaign.ClientConfig{}, err
	}
	remoteStartDelay := time.Duration(0)
	if expectedStartFailure {
		remoteStartDelay, err = requiredDuration(
			getenv, campaignRemoteStartDelayEnvironment, time.Millisecond, commandTimeout,
		)
		if err != nil {
			return h3membercampaign.ClientConfig{}, err
		}
	} else if getenv(campaignRemoteStartDelayEnvironment) != "" {
		return h3membercampaign.ClientConfig{}, fmt.Errorf(
			"%s must be unset for run", campaignRemoteStartDelayEnvironment,
		)
	}
	clearAuthorityKey = false
	return h3membercampaign.ClientConfig{
		Address:    address,
		ServerName: serverName,
		TLS: h3membercampaign.ClientTLSFiles{
			Certificate: certificate,
			PrivateKey:  privateKey,
			ServerCA:    serverCA,
		},
		AuthorityKeyID:       campaignAuthorityKeyID,
		AuthorityKey:         authorityKey,
		DialTimeout:          dialTimeout,
		CommandTimeout:       commandTimeout,
		RemoteStartDelay:     remoteStartDelay,
		ExpectedStartFailure: expectedStartFailure,
	}, nil
}

func readAuthorityKey(getenv func(string) string) ([]byte, error) {
	path, err := requiredAbsolutePath(getenv, campaignAuthorityKeyFileEnvironment)
	if err != nil {
		return nil, err
	}
	key, err := securefile.Read(path, maximumCampaignAuthorityKeyBytes, true)
	if err != nil || len(key) < 32 {
		clear(key)
		return nil, fmt.Errorf(
			"%s must name a private regular file containing 32..%d bytes",
			campaignAuthorityKeyFileEnvironment,
			maximumCampaignAuthorityKeyBytes,
		)
	}
	return key, nil
}

func requiredText(getenv func(string) string, name string, maximum int) (string, error) {
	value := getenv(name)
	if value == "" || value != strings.TrimSpace(value) || len(value) > maximum ||
		strings.ContainsRune(value, '\x00') {
		return "", fmt.Errorf("%s is required and must be canonical text", name)
	}
	return value, nil
}

func requiredAbsolutePath(getenv func(string) string, name string) (string, error) {
	value := getenv(name)
	if value == "" || !filepath.IsAbs(value) || filepath.Clean(value) != value ||
		strings.ContainsRune(value, '\x00') {
		return "", fmt.Errorf("%s must be a canonical absolute path", name)
	}
	return value, nil
}

func requiredDuration(
	getenv func(string) string,
	name string,
	minimum time.Duration,
	maximum time.Duration,
) (time.Duration, error) {
	value, err := time.ParseDuration(getenv(name))
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s is outside the supported duration range", name)
	}
	return value, nil
}

func writeReceipt(writer io.Writer, receipt h3membercampaign.Receipt) error {
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	written, err := writer.Write(encoded)
	if err != nil {
		return err
	}
	if written != len(encoded) {
		return io.ErrShortWrite
	}
	return nil
}

func writeConfigurationError(writer io.Writer, err error) {
	if err == nil {
		err = errors.New("unknown configuration error")
	}
	_, _ = fmt.Fprintf(writer, "configuration error: %s\n", err)
}

func writeUsage(writer io.Writer) {
	_, _ = io.WriteString(writer, "usage: vela-h3-member-campaign <serve|run|run-fault>\n")
}
