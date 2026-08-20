# Use contract credit billing for launch

Vela will launch as a production B2B service for invited Customer Organizations. Each accepted Job reserves part of a contractual credit limit, a billable outcome creates one immutable Charge, and an external finance process invoices accumulated Charges monthly. Vela will not own a prepaid wallet, card authorization, payment capture, or invoice settlement; this accepts bounded customer credit exposure in exchange for a smaller and more reliable launch billing path.

## Consequences

Artifact access depends on Vela committing the Charge, not on external payment settlement. Credit-limit changes, overdue-account suspension, invoice generation, collection, refunds, and credit notes remain outside the Job execution transaction.
