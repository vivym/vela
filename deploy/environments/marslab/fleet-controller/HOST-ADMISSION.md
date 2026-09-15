# Fleet webhook API Server client preparation

Prepare each control host independently with `hack/prepare-fleet-webhook-host.py`.
The tool reads the current static Pod's `--admission-control-config-file` path,
retains all existing admission plugin settings, and stages a new immutable
candidate beneath `/var/lib/rancher/rke2/server/vela-fleet-admission/candidates/`.
It does not edit RKE2 configuration, install the candidate drop-in, or restart a
service. The existing server-directory mount makes the staged files accessible
to the API Server only after the separate configuration rollout.

Inputs are a root-only JSON export of the immutable client Secret, its expected
name/UID/release revision, and the separately pinned Fleet admission client CA.
The tool checks the signing CA, exact SPIFFE identity, exclusive clientAuth EKU,
key pair and validity before writing any candidate. Each private input must be a
regular root-owned file with no group/other access. Deliver private JSON/PEM via
protected files or framed stdin, never command-line literals.

Candidate files are mode 0400 in a mode 0700 root-owned directory. `client.pem`
contains both certificate and key; the kubeconfig maps only
`vela-fleet-admission.vela-system.svc:443` to this file. It has no default context,
wildcard or other user entries. Existing conflicting webhook client settings are
rejected for explicit reconciliation. Repeated preparation accepts only identical
files and permissions. A new certificate or admission configuration gets a new
candidate directory, preserving prior material for controlled rollback.

After preparation, run the standalone
[`fleet-webhook-native-check`](../../../../hack/fleet-webhook-native-check/README.md)
on each host with the receipt's exact paths and DER certificate SHA256. Preserve
the current boot ID, RKE2 InvocationID and static Pod manifest hash to establish
that preparation did not activate anything.

Before activation, all control/storage members and node pressure checks must be
healthy. Then publish the reviewed Control trust snapshot, start both CPU Fleet
replicas, and verify Fleet health before configuring the API Server clients one
host at a time. The `rke2-drop-in.candidate.yaml` is a reviewed input, not a file
to install automatically while a quorum is degraded. Keep the old admission
configuration for rollback, preserve PodSecurity, and validate two healthy peer
API Servers before changing the next host. Only register the protected-resource
webhook after all three clients have adopted the configuration and passed actual
network/mTLS checks. Complete real Kubernetes admission allow/deny tests before
claiming the gate is closed.

Certificate renewal requires another immutable snapshot, preparation and
controlled consumer adoption. cert-manager renewing its source Secret does not
change these files or the Fleet controller's materialized TLS automatically.
