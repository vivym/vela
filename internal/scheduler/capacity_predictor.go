package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/admission"
	store "github.com/vivym/vela/internal/store/sqlc"
)

const maxDurationSeconds = int64(9_223_372_036)

type CapacityPredictor struct {
	pool *pgxpool.Pool
}

func NewCapacityPredictor(pool *pgxpool.Pool) (*CapacityPredictor, error) {
	if pool == nil {
		return nil, errors.New("scheduler PostgreSQL pool is required for capacity prediction")
	}
	return &CapacityPredictor{pool: pool}, nil
}

func (predictor *CapacityPredictor) PredictCapacity(
	ctx context.Context,
	request admission.CapacityPredictionRequest,
) (admission.CapacityPrediction, error) {
	if predictor == nil || predictor.pool == nil {
		return admission.CapacityPrediction{}, errors.New("scheduler capacity predictor is not configured")
	}
	if request.WorkerPoolID == uuid.Nil || request.ModelRevisionID == uuid.Nil ||
		request.GenerationPresetRevisionID == uuid.Nil ||
		request.ServiceClassRevisionID == uuid.Nil || request.OutputSpecID == uuid.Nil {
		return admission.CapacityPrediction{}, errors.New("capacity prediction requires complete immutable request identity")
	}
	if request.GenerationCount < 1 {
		return admission.CapacityPrediction{}, errors.New("capacity prediction generation count must be positive")
	}

	prediction, err := store.New(predictor.pool).PredictAdmissionCapacity(
		ctx,
		store.PredictAdmissionCapacityParams{
			WorkerPoolID:               request.WorkerPoolID,
			ModelRevisionID:            request.ModelRevisionID,
			GenerationPresetRevisionID: request.GenerationPresetRevisionID,
			ServiceClassRevisionID:     request.ServiceClassRevisionID,
			OutputSpecID:               request.OutputSpecID,
			GenerationCount:            request.GenerationCount,
		},
	)
	if err != nil {
		return admission.CapacityPrediction{}, err
	}
	if prediction.PredictedQueueWaitSeconds < 0 ||
		prediction.PredictedQueueWaitSeconds > maxDurationSeconds {
		return admission.CapacityPrediction{}, fmt.Errorf(
			"predicted queue wait seconds %d cannot be represented",
			prediction.PredictedQueueWaitSeconds,
		)
	}
	if !prediction.PredictedFinishAt.Valid {
		return admission.CapacityPrediction{}, errors.New(
			"predicted Admission finish timestamp is unavailable",
		)
	}
	return admission.CapacityPrediction{
		QueueWait:         time.Duration(prediction.PredictedQueueWaitSeconds) * time.Second,
		EstimatedFinishAt: prediction.PredictedFinishAt.Time,
	}, nil
}

func (predictor *CapacityPredictor) PredictJobDynamicETA(
	ctx context.Context,
	jobID uuid.UUID,
) (time.Time, error) {
	if predictor == nil || predictor.pool == nil {
		return time.Time{}, errors.New("scheduler capacity predictor is not configured")
	}
	if jobID == uuid.Nil {
		return time.Time{}, errors.New("dynamic ETA requires a Job identity")
	}
	predictedFinish, err := store.New(predictor.pool).PredictJobDynamicETA(ctx, jobID)
	if err != nil {
		return time.Time{}, err
	}
	if !predictedFinish.Valid {
		return time.Time{}, errors.New("predicted Job finish timestamp is unavailable")
	}
	return predictedFinish.Time, nil
}
