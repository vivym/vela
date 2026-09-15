// Verify the actual Kubernetes webhook auth resolver without changing a cluster.
package main

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/admission"
	webhookconfig "k8s.io/apiserver/pkg/admission/plugin/webhook/config"
	"k8s.io/apiserver/pkg/apis/apiserver/install"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/transport"
)

const target = "vela-fleet-admission.vela-system.svc:443"
const identity = "spiffe://vela.internal/kube-apiserver/admission"

type check struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
}

func fileOnlyAuth(info *clientcmdapi.AuthInfo, certificatePath string) bool {
	if info == nil {
		return false
	}
	info = info.DeepCopy()
	info.LocationOfOrigin = ""
	// The upstream decoder initializes empty maps; they carry no credentials.
	if len(info.Extensions) == 0 {
		info.Extensions = nil
	}
	if len(info.ImpersonateUserExtra) == 0 {
		info.ImpersonateUserExtra = nil
	}
	if len(info.ImpersonateGroups) == 0 {
		info.ImpersonateGroups = nil
	}
	if len(info.ClientCertificateData) == 0 {
		info.ClientCertificateData = nil
	}
	if len(info.ClientKeyData) == 0 {
		info.ClientKeyData = nil
	}
	return reflect.DeepEqual(info, &clientcmdapi.AuthInfo{ClientCertificate: certificatePath, ClientKey: certificatePath})
}

func run() error {
	configPath := flag.String("kubeconfig", "", "staged webhook kubeconfig")
	caPath := flag.String("client-ca", "", "pinned admission client CA file")
	expected := flag.String("certificate-sha256", "", "expected DER certificate SHA256")
	originalAdmission := flag.String("original-admission", "", "currently active admission configuration")
	flag.Parse()
	if !filepath.IsAbs(*configPath) || !filepath.IsAbs(*caPath) || len(*expected) != 64 {
		return errors.New("absolute input paths and certificate SHA256 required")
	}
	certPath := filepath.Join(filepath.Dir(*configPath), "client.pem")
	if !filepath.IsAbs(*originalAdmission) {
		return errors.New("absolute original admission path required")
	}
	scheme := runtime.NewScheme()
	install.Install(scheme)
	pluginNames := []string{"PodSecurity", "ValidatingAdmissionWebhook"}
	provider, err := admission.ReadAdmissionConfiguration(pluginNames, filepath.Join(filepath.Dir(*configPath), "admission.yaml"), scheme)
	if err != nil {
		return err
	}
	original, err := admission.ReadAdmissionConfiguration(pluginNames, *originalAdmission, scheme)
	if err != nil {
		return err
	}
	for _, name := range []string{"PodSecurity", "MutatingAdmissionWebhook"} {
		old, e := original.ConfigFor(name)
		if e != nil {
			return e
		}
		current, e := provider.ConfigFor(name)
		if e != nil {
			return e
		}
		if old == nil || current == nil {
			if old != nil || current != nil || name == "PodSecurity" {
				return errors.New("existing admission plugin configuration changed")
			}
			continue
		}
		oldData, e := io.ReadAll(old)
		if e != nil {
			return e
		}
		newData, e := io.ReadAll(current)
		if e != nil {
			return e
		}
		var oldObject, newObject any
		if json.Unmarshal(oldData, &oldObject) != nil || json.Unmarshal(newData, &newObject) != nil || !reflect.DeepEqual(oldObject, newObject) {
			return errors.New("existing admission plugin data changed")
		}
	}
	webhookReader, err := provider.ConfigFor("ValidatingAdmissionWebhook")
	if err != nil {
		return err
	}
	loadedPath, err := webhookconfig.LoadConfig(webhookReader)
	if err != nil {
		return err
	}
	if loadedPath != *configPath {
		return errors.New("native webhook loader resolved a different kubeconfig")
	}
	for _, p := range []string{*configPath, certPath} {
		st, err := os.Lstat(p)
		if err != nil {
			return err
		}
		if !st.Mode().IsRegular() || st.Mode().Perm() != 0400 {
			return errors.New("staged files must be regular and mode 0400")
		}
	}
	raw, err := clientcmd.LoadFromFile(*configPath)
	if err != nil {
		return err
	}
	if len(raw.AuthInfos) != 1 || len(raw.Clusters) != 0 || len(raw.Contexts) != 0 || raw.CurrentContext != "" {
		return errors.New("unexpected default or additional credential scope")
	}
	info := raw.AuthInfos[target]
	if info == nil {
		return errors.New("exact service credential is missing")
	}
	if !fileOnlyAuth(info, certPath) {
		return errors.New("only the combined certificate file may configure authentication")
	}
	resolver, err := webhook.NewDefaultAuthenticationInfoResolver(*configPath)
	if err != nil {
		return err
	}
	checks := []check{{"native AdmissionConfiguration parser", true}, {"existing PodSecurity and mutating webhook preserved", true}, {"native WebhookAdmissionConfiguration loader", true}, {"single exact service auth entry without fallback context", true}}
	cases := []struct {
		name string
		want bool
		get  func() (*rest.Config, error)
	}{
		{"exact service and port", true, func() (*rest.Config, error) {
			return resolver.ClientConfigForService("vela-fleet-admission", "vela-system", 443)
		}},
		{"exact hostname and port", true, func() (*rest.Config, error) { return resolver.ClientConfigFor(target) }},
		{"different service", false, func() (*rest.Config, error) {
			return resolver.ClientConfigForService("other-admission", "vela-system", 443)
		}},
		{"different namespace", false, func() (*rest.Config, error) {
			return resolver.ClientConfigForService("vela-fleet-admission", "other", 443)
		}},
		{"different port", false, func() (*rest.Config, error) {
			return resolver.ClientConfigForService("vela-fleet-admission", "vela-system", 8443)
		}},
		{"hostname without explicit port", false, func() (*rest.Config, error) { return resolver.ClientConfigFor("vela-fleet-admission.vela-system.svc") }},
		{"unrelated webhook URL", false, func() (*rest.Config, error) { return resolver.ClientConfigFor("hooks.other.internal:443") }},
		{"hostname suffix impersonation", false, func() (*rest.Config, error) {
			return resolver.ClientConfigFor("vela-fleet-admission.vela-system.svc.attacker.invalid:443")
		}},
		{"direct management IP", false, func() (*rest.Config, error) { return resolver.ClientConfigFor("10.1.201.70:443") }},
		{"Kubernetes default service", false, func() (*rest.Config, error) { return resolver.ClientConfigForService("kubernetes", "default", 443) }},
	}
	roots := x509.NewCertPool()
	ca, err := os.ReadFile(*caPath)
	if err != nil {
		return err
	}
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("invalid client CA")
	}
	for _, c := range cases {
		config, err := c.get()
		if err != nil {
			return fmt.Errorf("%s: %w", c.name, err)
		}
		hasCert := config.CertFile != "" || config.KeyFile != "" || len(config.CertData) > 0 || len(config.KeyData) > 0
		if hasCert != c.want {
			return fmt.Errorf("credential scope mismatch: %s", c.name)
		}
		if c.want {
			if config.CertFile != certPath || config.KeyFile != certPath {
				return errors.New("resolved certificate path differs")
			}
			tc, err := config.TransportConfig()
			if err != nil {
				return err
			}
			tlsConfig, err := transport.TLSConfigFor(tc)
			if err != nil {
				return err
			}
			if tlsConfig == nil || tlsConfig.GetClientCertificate == nil {
				return errors.New("file certificate loader missing")
			}
			certificate, err := tlsConfig.GetClientCertificate(&tls.CertificateRequestInfo{})
			if err != nil {
				return err
			}
			if len(certificate.Certificate) == 0 {
				return errors.New("empty client certificate")
			}
			sum := sha256.Sum256(certificate.Certificate[0])
			if hex.EncodeToString(sum[:]) != *expected {
				return errors.New("loaded certificate differs from pinned input")
			}
			leaf, err := x509.ParseCertificate(certificate.Certificate[0])
			if err != nil {
				return err
			}
			if len(leaf.URIs) != 1 || leaf.URIs[0].String() != identity {
				return errors.New("loaded client identity differs")
			}
			if _, err = leaf.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, CurrentTime: time.Now()}); err != nil {
				return err
			}
		}
		checks = append(checks, check{c.name, true})
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		At      string  `json:"at"`
		Passed  bool    `json:"passed"`
		Library string  `json:"library"`
		Checks  []check `json:"checks"`
		Scope   string  `json:"scope"`
	}{time.Now().UTC().Format(time.RFC3339Nano), true, "k8s.io/apiserver v0.35.7", checks, "native auth resolver and TLS material loading; not live admission activation"})
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
