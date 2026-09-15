# Native Fleet webhook configuration validation

This standalone Go module pins `k8s.io/apiserver` and `client-go` to v0.35.7,
matching the deployed Kubernetes v1.35.7 API Server. It does not change Vela's
application dependency versions or connect to the Kubernetes API.

Build for a control host:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o /tmp/vela-fleet-webhook-native-check-v2 .
```

Run as root against a candidate prepared by `../prepare-fleet-webhook-host.py`:

```sh
/opt/vela/fleet-webhook-preparation-20260915/native-check-v2 \
  --kubeconfig /var/lib/rancher/rke2/server/vela-fleet-admission/candidates/REVISION/kubeconfig \
  --client-ca /opt/vela/fleet-webhook-preparation-20260915/client-ca.crt \
  --certificate-sha256 DER_CERTIFICATE_SHA256 \
  --original-admission /etc/rancher/rke2/rke2-pss.yaml
```

Use the exact paths and certificate hash from the preparation receipt. The tool
uses upstream admission parsers, verifies preservation of existing PodSecurity
and mutating webhook configuration, resolves the exact Fleet service credential,
checks unrelated service/namespace/port/URL cases receive no client certificate,
and loads the real combined PEM using the upstream TLS loader. It verifies the
certificate chain and SPIFFE identity without printing certificate/key contents.

The three control hosts passed all 14 checks each. The initial validator rejected
empty metadata maps created by the native clientcmd decoder; v2 normalizes only
empty collections and retains rejection of additional authentication settings.
Both attempts have separate receipts. See the
[deployment preparation record](../../docs/fleet-controller-deployment-preparation-2026-09-15.md).

These checks establish configuration parsing and credential scope. They do not
activate the API Server configuration, register a webhook, demonstrate a live
AdmissionReview, or prove certificate renewal adoption. Keep those deployment
and rotation gates open until separately completed against the running cluster.
