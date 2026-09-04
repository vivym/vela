package authoritypolicy

import "time"

// ProductionMaxClockSkew is the shared bound for authorities crossing process boundaries.
const ProductionMaxClockSkew = 30 * time.Second
