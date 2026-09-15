# Runtime startup package transfer validation (2026-09-13)

This evidence records artifact verification and a source-matched binary install on
`marslab`. The install replaced the three helper binaries and refreshed the two
systemd unit files, but did not enable or start the policy issuer or Node Agent.

## Build

The package builder completed successfully with revision `validation-20260913`:

```text
go run ./cmd/vela-release-artifacts build-runtime-startup-packages \
  /Users/viv/projs/vela validation-20260913 <temporary-output>
```

The package manifest contains exactly these three Linux amd64 artifacts:

```text
vela-pidfd-broker
vela-runtime-launcher
vela-runtime-policy-issuer
```

The builder's candidate verification also checked the contracts, systemd units,
environment examples, provisioning contract and exact inventory.

## Target transfer

The package was copied to `marslab` and inspected before installation. Local and
remote SHA256 matched:

```text
92ab57deb6f86400c02720a208eadd32bc2b6920862419c050fb47cbd337305e
```

The package contained 13 expected entries, including the manifest and all three
binary artifacts. It contained no macOS `._*` AppleDouble entries. The package
directory remains under `/tmp` on `marslab` as the source recorded by the
install receipt.

The package was transferred to `/tmp/vela-runtime-startup-packages-validation-20260913`
on `marslab`; the release-artifacts verifier was transferred separately and the
installer verified the exact revision before touching system paths.

## Target install

The installer ran with `--apply --root /` and without `--enable-services` or
`--reload-node`. Receipt:

```text
operation_id=8d5d5ae4-aed7-45e2-a0e0-2683375c94ec
status=installed
applied=true
revision=validation-2026-09-13
receipt=/var/lib/vela/runtime-startup/receipts/install-8d5d5ae4-aed7-45e2-a0e0-2683375c94ec.json
```

The installed binary digests are:

```text
vela-pidfd-broker       224f53945aa9478445b5edd4fcd6b29ab233af0cc823da09ed27071b86a591b4
vela-runtime-launcher   6257a033c629fb05e129622b7d0241a3fd78adc02500fe9866624234bcf70390
vela-runtime-policy-issuer ccbb62ad704e12ba0012606af0776278cae8e5eb5efb196151766624e84ecfea
```

After a manual `systemctl daemon-reload`, the already configured
`vela-pidfd-broker.service` was restarted once to load the installed binary and
returned `active` with its socket present. `vela-runtime-policy-issuer.service`
and `vela-node-agent.service` remained `inactive`. No RKE2 or business workload
was restarted. This proves the reversible artifact install and broker reload
boundary only; issuer/Node configuration, CRI composition, Permit issuance and
rollback remain outstanding, so
`Production Gates` remains `0/9`.

After the restart, a source-matched Linux amd64 `runtimechannel` test binary was
run in a root, network-disabled container with the host broker socket mounted.
`TestPIDFDBrokerExternalSocket` passed all four cases (`same`, `different`,
`nested-same`, and `wrong-gid`), confirming that the newly loaded broker still
accepts only the expected retained pidfd identity and rejects the wrong
credential path. This is broker-level evidence and does not create a Node
startup Permit.

## Installer validation

The disposable installer tests now cover three release safety boundaries:

```text
python3 -m unittest hack/install_runtime_startup_packages_test.py
```

The suite verifies that a `systemctl` failure writes a `status=failed` receipt
and restores all targets, that a successful rollback marks its receipt
`status=rolled-back` and cannot be replayed, that recorded service enablement
is restored during rollback, and that a symlink in the target parent chain is
rejected before installation. These are local temporary-root checks; they do
not authorize installation on `marslab`.
