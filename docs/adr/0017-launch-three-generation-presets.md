# Launch three generation presets

Vela exposes stable `quality`, `balanced`, and `fast` Generation Preset IDs. `quality` follows the reference-quality path without uncertified lossy acceleration, `balanced` is the default certified moderate-acceleration offer, and `fast` uses more aggressive acceleration while still satisfying its own minimum quality contract; internal ExecutionProfileRevisions remain hidden behind these product contracts.

## Consequences

Each new GenerationPresetRevision requires independent quality, success-rate, p95, and cost evidence before ACTIVE promotion. The intended price and runtime order is `quality` above `balanced` above `fast`, but no preset is launched or priced from an assumed multiplier without certification receipts.
