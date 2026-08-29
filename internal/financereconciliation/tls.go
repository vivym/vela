package financereconciliation

import (
	"crypto/tls"
	"fmt"

	"github.com/vivym/vela/internal/privilegedlistener"
)

func NewServerTLSConfig(
	certificatePath string,
	privateKeyPath string,
	clientCAPath string,
) (*tls.Config, error) {
	tlsConfig, err := privilegedlistener.NewServerTLSConfig(certificatePath, privateKeyPath, clientCAPath)
	if err != nil {
		return nil, fmt.Errorf("configure Finance Reconciliation server TLS: %w", err)
	}
	return tlsConfig, nil
}
