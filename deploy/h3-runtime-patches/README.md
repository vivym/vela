# H3 Runtime cancellation finalization

This patch applies to the **installed** `fast_h3.vela.driver` from the exact
base image in `cancel-finalization.json`. The source checkout inside that image
is a different version. Do not replace the installed package with that checkout:
the installed driver also contains execution-authority renewal handling.

The patch schedules cancellation finalization behind compute on the existing
single-thread executor. It publishes STOPPED only after compute and output
cleanup complete. A writer-join failure continues to reject drain. Shutdown
joins the executor outside the command gate to avoid a lock cycle.

To reproduce the source change, extract the installed `driver.py` from the
pinned base image into `build/fast_h3/vela/driver.py`. Check its SHA-256 against
`before_sha256`, then run from the repository root:

```sh
patch --directory=build --strip=1 < deploy/h3-runtime-patches/cancel-finalization.patch
```

Check the result against `after_sha256` before building. Build from the same
pinned base image and copy that patched file to both locations:

```dockerfile
COPY --chmod=0444 fast_h3/vela/driver.py /opt/fast-h3/src/fast_h3/vela/driver.py
COPY --chmod=0444 fast_h3/vela/driver.py /opt/venv/lib/python3.12/site-packages/fast_h3/vela/driver.py
```

Before pushing, use the built image's `/opt/venv/bin/python` to import
`fast_h3.vela.driver` and verify both `__file__` and the installed file's SHA-256.
Run `hack/h3_cancel_drain_contract_test.py` inside that image with networking
disabled; these three tests require neither GPU nor model weights. The same
import-path and hash check must also run in each deployed GPU Runtime container.

Use the concrete `linux/amd64` manifest digest for deployment, and retain any
OCI provenance index separately. New Runtime digests require new StageProfiles
and immutable launch identities. Existing accepted Jobs retain their original
profiles until completion.

The Python tests cover the driver boundary, not the composed API. Final
acceptance must submit a second Job after DiT has been busy for over 150 seconds,
cancel the first, submit again after automatic capacity recovery, verify complete
video/audio/thumbnail and billing, and compare container IDs and restart counts.
See [the repair report](../../docs/h3-live-admission-cancellation-repair-2026-09-17.md)
for deployment evidence and the preserved failed campaigns.
