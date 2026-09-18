# Ref2VA/FL2VA native condition bridge

This patch updates the installed `fast_h3.vela.sglang_native` bridge used by
Vela's disaggregated Runtime. The previous bridge accepted only text-only
`t2va`, even though the signed H3 runtime and the canonical Vela request schema
already supported `fl2va` and `ref2va`.

The bridge now accepts both condition tasks, verifies that the root material
list is a one-to-one ordered match for the canonical conditions, and seeds the
request-local `minimax_h3_material_localization` cache with the already verified
regular files materialized by Vela. The canonical condition URI remains the
identity carried through the encoder/DiT boundary. Encoder pre-queue probing is
deferred until after this cache is populated; later stages restore the resolved
plan from the signed boundary artifact and never fetch the user material again.

The patch is applied to both the source and installed Python copies in the
Runtime image. It is based on the exact image and file hashes in
`ref2va-conditions.json`. The image digest in that file is a canary build only;
it must receive a new immutable StageProfile and WorkerBundle before use.
