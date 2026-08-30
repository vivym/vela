package stagescheduler

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
)

type metricOutcome string

const (
	metricOutcomeAssigned metricOutcome = "ASSIGNED"
	metricOutcomeMatched  metricOutcome = "MATCHED"
	metricOutcomeDiverged metricOutcome = "DIVERGED"
	metricOutcomeNoWork   metricOutcome = "NO_WORK"
	metricOutcomeRejected metricOutcome = "REJECTED"
	metricOutcomeError    metricOutcome = "ERROR"
	metricOutcomeSuccess  metricOutcome = "SUCCESS"
)

type metricReason string

const (
	metricReasonNone               metricReason = "NONE"
	metricReasonInvalidAuthority   metricReason = "INVALID_AUTHORITY"
	metricReasonStaleAuthority     metricReason = "STALE_AUTHORITY"
	metricReasonAssignmentRejected metricReason = "ASSIGNMENT_REJECTED"
	metricReasonReplayDiverged     metricReason = "REPLAY_DIVERGED"
	metricReasonInternal           metricReason = "INTERNAL_ERROR"
)

type Metrics struct {
	acquire        *prometheus.CounterVec
	shadowReplay   *prometheus.CounterVec
	claimReconcile *prometheus.CounterVec
}

func NewMetrics() *Metrics {
	labels := []string{"outcome", "reason", "algorithm_revision"}
	return &Metrics{
		acquire: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "vela",
			Subsystem: "stage_scheduler",
			Name:      "acquire_total",
			Help:      "StageScheduler acquire operations by bounded outcome and reason.",
		}, labels),
		shadowReplay: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "vela",
			Subsystem: "stage_scheduler",
			Name:      "shadow_replay_total",
			Help:      "StageScheduler shadow replay operations by bounded outcome and reason.",
		}, labels),
		claimReconcile: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "vela",
			Subsystem: "stage_scheduler",
			Name:      "claim_reconcile_total",
			Help:      "StageScheduler expired-claim reconciliation by bounded outcome and reason.",
		}, labels),
	}
}

func (metrics *Metrics) Describe(output chan<- *prometheus.Desc) {
	if metrics == nil {
		return
	}
	metrics.acquire.Describe(output)
	metrics.shadowReplay.Describe(output)
	metrics.claimReconcile.Describe(output)
}

func (metrics *Metrics) Collect(output chan<- prometheus.Metric) {
	if metrics == nil {
		return
	}
	metrics.acquire.Collect(output)
	metrics.shadowReplay.Collect(output)
	metrics.claimReconcile.Collect(output)
}

func (metrics *Metrics) observeAcquire(outcome metricOutcome, reason metricReason) {
	if metrics == nil {
		return
	}
	metrics.acquire.WithLabelValues(
		string(outcome), string(reason), AlgorithmRevisionV1,
	).Inc()
}

func (metrics *Metrics) observeShadowReplay(outcome metricOutcome, reason metricReason) {
	if metrics == nil {
		return
	}
	metrics.shadowReplay.WithLabelValues(
		string(outcome), string(reason), AlgorithmRevisionV1,
	).Inc()
}

func (metrics *Metrics) observeClaimReconcile(outcome metricOutcome, reason metricReason) {
	if metrics == nil {
		return
	}
	metrics.claimReconcile.WithLabelValues(
		string(outcome), string(reason), AlgorithmRevisionV1,
	).Inc()
}

func metricReasonForError(err error) metricReason {
	if err == nil {
		return metricReasonNone
	}
	if errors.Is(err, ErrStaleSnapshot) {
		return metricReasonStaleAuthority
	}
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return metricReasonInternal
	}
	switch postgresError.ConstraintName {
	case "stage_scheduler_capacity_pool_stale",
		"stage_scheduler_worker_authority_stale",
		"stage_scheduler_model_residency_stale",
		"stage_scheduler_capacity_observation_stale",
		"stage_scheduler_snapshot_stale",
		"stage_scheduler_candidate_stale",
		"stage_scheduler_candidate_identity_stale":
		return metricReasonStaleAuthority
	default:
		return metricReasonInternal
	}
}
