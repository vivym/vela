# Price certified output SKUs

Vela accepts and prices only certified discrete OutputSpecs. An immutable RateCardRevision maps Model, GenerationPresetRevision, ServiceClassRevision, and OutputSpec to a fixed integer-minor-unit price, generation count multiplies that line price, and Admission stores the resolved line, quantity, currency, and amount in PricingSnapshot.

## Consequences

Arbitrary output combinations without an ACTIVE rate line are rejected before Admission. Actual GPU time, platform retries, and hardware changes do not alter customer price, and future capabilities such as LoRA or extended retention enter the quote as explicit line items rather than hidden continuous multipliers.
