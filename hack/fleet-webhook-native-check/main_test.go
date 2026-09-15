package main

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"testing"
)

func TestDecoderEmptyMapsDoNotCreateCredentials(t *testing.T) {
	raw, err := clientcmd.Load([]byte(`apiVersion: v1
kind: Config
users:
- name: vela-fleet-admission.vela-system.svc:443
  user:
    client-certificate: /private/client.pem
    client-key: /private/client.pem
`))
	if err != nil {
		t.Fatal(err)
	}
	info := raw.AuthInfos[target]
	if !fileOnlyAuth(info, "/private/client.pem") {
		t.Fatal("native decoder's empty metadata maps rejected")
	}
	if len(info.Extensions) != 0 {
		t.Fatal("unexpected fixture extension")
	}
	for name, mutate := range map[string]func(*clientcmdapi.AuthInfo){
		"bearer token":             func(a *clientcmdapi.AuthInfo) { a.Token = "fixture-forbidden" },
		"token file":               func(a *clientcmdapi.AuthInfo) { a.TokenFile = "/other/token" },
		"username":                 func(a *clientcmdapi.AuthInfo) { a.Username = "other" },
		"password":                 func(a *clientcmdapi.AuthInfo) { a.Password = "fixture-forbidden" },
		"embedded certificate":     func(a *clientcmdapi.AuthInfo) { a.ClientCertificateData = []byte("unexpected") },
		"embedded key":             func(a *clientcmdapi.AuthInfo) { a.ClientKeyData = []byte("unexpected") },
		"other key path":           func(a *clientcmdapi.AuthInfo) { a.ClientKey = "/other/key" },
		"exec plugin":              func(a *clientcmdapi.AuthInfo) { a.Exec = &clientcmdapi.ExecConfig{Command: "unused"} },
		"impersonation":            func(a *clientcmdapi.AuthInfo) { a.Impersonate = "other" },
		"impersonation groups":     func(a *clientcmdapi.AuthInfo) { a.ImpersonateGroups = []string{"other"} },
		"impersonation attributes": func(a *clientcmdapi.AuthInfo) { a.ImpersonateUserExtra = map[string][]string{"other": {"value"}} },
		"extension": func(a *clientcmdapi.AuthInfo) {
			a.Extensions = map[string]runtime.Object{"other": &runtime.Unknown{Raw: []byte(`{}`)}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			altered := info.DeepCopy()
			mutate(altered)
			if fileOnlyAuth(altered, "/private/client.pem") {
				t.Fatal("additional auth settings accepted")
			}
		})
	}
}
