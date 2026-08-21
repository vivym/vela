package workertransport

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	store "github.com/vivym/vela/internal/store/sqlc"
	"github.com/vivym/vela/internal/workercontrol"
)

type PostgresIdentityResolver struct {
	pool *pgxpool.Pool
}

func NewPostgresIdentityResolver(pool *pgxpool.Pool) (*PostgresIdentityResolver, error) {
	if pool == nil {
		return nil, errors.New("worker identity database pool is required")
	}
	return &PostgresIdentityResolver{pool: pool}, nil
}

func (resolver *PostgresIdentityResolver) ResolveWorker(
	ctx context.Context,
	spiffeID string,
) (workercontrol.AuthenticatedWorker, error) {
	if resolver == nil || resolver.pool == nil {
		return workercontrol.AuthenticatedWorker{}, errors.New("worker identity resolver is not configured")
	}
	if ctx == nil || len(spiffeID) < len("spiffe://a/b") || len(spiffeID) > 500 ||
		!strings.HasPrefix(spiffeID, "spiffe://") || strings.ContainsRune(spiffeID, '\x00') {
		return workercontrol.AuthenticatedWorker{}, errors.New("worker SPIFFE ID is invalid")
	}
	workerID, err := store.New(resolver.pool).ResolveWorkerBySPIFFEID(ctx, spiffeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return workercontrol.AuthenticatedWorker{}, errors.New("worker SPIFFE ID is not registered")
	}
	if err != nil {
		return workercontrol.AuthenticatedWorker{}, fmt.Errorf("resolve Worker SPIFFE ID: %w", err)
	}
	if workerID == uuid.Nil {
		return workercontrol.AuthenticatedWorker{}, errors.New("registered Worker identity is invalid")
	}
	return workercontrol.AuthenticatedWorker{ID: workerID}, nil
}

var _ WorkerIdentityResolver = (*PostgresIdentityResolver)(nil)
