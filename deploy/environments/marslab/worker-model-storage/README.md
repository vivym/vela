# GPU worker local model storage

Deferred by the user: continue other cluster work. Formatting was not authorized
and has not been executed. Keep this plan for a later explicit resumption.

The exact 14-device initialization plan awaits explicit authorization to erase the listed disks. They have no recognized filesystem signatures, but sparse samples contain nonzero data. Do not infer empty disks from lsblk.

Use `hack/initialize-worker-model-storage.py prepare` for a read-only preflight. Only use apply with the exact approved plan hash after the user authorizes formatting. Management storage stays on .70/.71/.66. See `docs/worker-model-storage-plan-2026-09-14.md`.
