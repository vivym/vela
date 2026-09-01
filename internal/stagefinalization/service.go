package stagefinalization

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgxpool"
	store "github.com/vivym/vela/internal/store/sqlc"
)

type Config struct {
	LeaseTTL          time.Duration
	ActiveLeaseKeyID  string
	LeaseKeys         map[string][]byte
	ArtifactInspector ArtifactInspector
}

type Service struct {
	pool              *pgxpool.Pool
	leaseTTL          time.Duration
	activeLeaseKeyID  string
	leaseKeys         map[string][]byte
	artifactInspector ArtifactInspector
}

func NewService(ctx context.Context, pool *pgxpool.Pool, config Config) (*Service, error) {
	if ctx == nil {
		return nil, errors.New("stage finalization startup context is required")
	}
	if pool == nil {
		return nil, errors.New("stage finalization database pool is required")
	}
	if config.LeaseTTL < time.Second || config.LeaseTTL%time.Second != 0 {
		return nil, errors.New("stage finalization claim TTL must be a positive whole number of seconds")
	}
	if config.ActiveLeaseKeyID == "" {
		return nil, errors.New("active Stage finalization signing-key id is required")
	}
	keys := make(map[string][]byte, len(config.LeaseKeys))
	for keyID, key := range config.LeaseKeys {
		if keyID == "" || len(key) < 32 {
			return nil, errors.New("every Stage finalization signing key must have an id and at least 32 bytes")
		}
		keys[keyID] = append([]byte(nil), key...)
	}
	if _, ok := keys[config.ActiveLeaseKeyID]; !ok {
		return nil, errors.New("active Stage finalization signing key is absent from the keyring")
	}
	return &Service{
		pool: pool, leaseTTL: config.LeaseTTL,
		activeLeaseKeyID: config.ActiveLeaseKeyID, leaseKeys: keys,
		artifactInspector: config.ArtifactInspector,
	}, nil
}

func postgresTime(ctx context.Context, queries *store.Queries) (time.Time, error) {
	wallClock, err := queries.GetPostgresTime(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("read PostgreSQL time: %w", err)
	}
	if !wallClock.Valid {
		return time.Time{}, errors.New("PostgreSQL time is null")
	}
	return wallClock.Time, nil
}

func changedRowsError(operation string, rows int64, err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return fmt.Errorf("%s changed %d rows, want 1", operation, rows)
}

func validPrintableText(value string, maxRunes int) bool {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" {
		return false
	}
	count := 0
	for _, character := range value {
		if !unicode.IsPrint(character) {
			return false
		}
		count++
		if count > maxRunes {
			return false
		}
	}
	return count > 0
}
