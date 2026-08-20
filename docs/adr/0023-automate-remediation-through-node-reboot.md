# Automate certified remediation through node reboot

Production remediation automatically performs process restart and CUDA cleanup, certified GPU reset and PCIe FLR, and fenced, rate-limited driver reload or node reboot. BMC power cycle requires human approval or two-person confirmation at launch, while ambiguous identity, uncertified topology, repeated recovery failure, or failed validation automatically quarantines the Worker.

## Consequences

Every Remediation Operation binds node identity, GPU UUID or PCI BDF, worker epoch, idempotency key, certified action matrix, and audit receipt. A recovered node passes device checks, runner checks, model warm-up, and canary before READY; an uncertified action fails closed instead of escalating automatically.
