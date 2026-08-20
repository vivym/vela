# Separate generation presets from service classes

GenerationPresetRevision defines the customer-visible generation quality and speed contract, while ServiceClassRevision independently defines admission, concurrency, queue weight, and statistical completion SLO. PricingSnapshot and execution policy lock both revisions for an Accepted Job; Retry preserves both, and ExecutionProfileRevision remains the internal certified means of satisfying the selected generation preset.

## Consequences

Queue priority is not embedded in `fast`, `balanced`, or `quality`, and adding a new Service Class does not require cloning every generation preset. Launch may expose only `standard` Service Class while retaining the separate model for later contracted priority capacity.
