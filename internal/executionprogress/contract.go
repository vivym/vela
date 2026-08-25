package executionprogress

import (
	"math"
	"time"
)

const MaxEstimatedRemainingSeconds int64 = math.MaxInt64 / int64(time.Second)

func ValidStageProgress(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0 && value < 1
}

func ValidEstimatedRemainingSeconds(value int64) bool {
	return value >= 0 && value <= MaxEstimatedRemainingSeconds
}
