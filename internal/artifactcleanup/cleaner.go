package artifactcleanup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vivym/vela/internal/artifactstore"
)

const multipartClaimSafetyFloor = time.Minute

type MultipartStore interface {
	ListIncompleteMultipartUploads(
		context.Context,
		string,
	) ([]artifactstore.IncompleteMultipartUpload, error)
	AbortMultipartUpload(context.Context, artifactstore.MultipartUpload) error
}

type Registry interface {
	IsMultipartUploadRecorded(context.Context, string, string) (bool, error)
}

type Config struct {
	ObjectPrefix string
	MinimumAge   time.Duration
	MaxAborts    int
}

type Result struct {
	Listed   int
	Eligible int
	Recorded int
	Aborted  int
}

type Cleaner struct {
	store        MultipartStore
	registry     Registry
	objectPrefix string
	minimumAge   time.Duration
	maxAborts    int
	now          func() time.Time
}

func New(store MultipartStore, registry Registry, config Config) (*Cleaner, error) {
	if store == nil || registry == nil ||
		!strings.HasPrefix(config.ObjectPrefix, "artifacts/") ||
		len(config.ObjectPrefix) > 1024 || strings.ContainsRune(config.ObjectPrefix, '\x00') ||
		strings.Contains(config.ObjectPrefix, "//") ||
		config.MinimumAge < multipartClaimSafetyFloor ||
		config.MaxAborts <= 0 || config.MaxAborts > 1000 {
		return nil, errors.New("invalid Artifact multipart cleaner configuration")
	}
	return &Cleaner{
		store:        store,
		registry:     registry,
		objectPrefix: config.ObjectPrefix,
		minimumAge:   config.MinimumAge,
		maxAborts:    config.MaxAborts,
		now:          time.Now,
	}, nil
}

func (cleaner *Cleaner) Reconcile(ctx context.Context) (Result, error) {
	if cleaner == nil || cleaner.store == nil || cleaner.registry == nil {
		return Result{}, errors.New("artifact multipart cleaner is not configured")
	}
	if ctx == nil {
		return Result{}, errors.New("artifact multipart cleanup context is required")
	}
	uploads, err := cleaner.store.ListIncompleteMultipartUploads(ctx, cleaner.objectPrefix)
	if err != nil {
		return Result{}, fmt.Errorf("list incomplete Artifact multipart uploads: %w", err)
	}
	result := Result{Listed: len(uploads)}
	cutoff := cleaner.now().UTC().Add(-cleaner.minimumAge)
	candidates := make([]artifactstore.IncompleteMultipartUpload, 0, len(uploads))
	for _, upload := range uploads {
		if upload.InitiatedAt.IsZero() || upload.InitiatedAt.After(cutoff) {
			continue
		}
		candidates = append(candidates, upload)
	}
	result.Eligible = len(candidates)

	orphans := make([]artifactstore.IncompleteMultipartUpload, 0, len(candidates))
	for _, upload := range candidates {
		recorded, lookupErr := cleaner.registry.IsMultipartUploadRecorded(
			ctx,
			upload.ObjectKey,
			upload.UploadID,
		)
		if lookupErr != nil {
			return Result{}, fmt.Errorf("check durable Artifact multipart upload: %w", lookupErr)
		}
		if recorded {
			result.Recorded++
			continue
		}
		orphans = append(orphans, upload)
	}
	if len(orphans) > cleaner.maxAborts {
		orphans = orphans[:cleaner.maxAborts]
	}
	for _, orphan := range orphans {
		if err := cleaner.store.AbortMultipartUpload(ctx, artifactstore.MultipartUpload{
			ObjectKey: orphan.ObjectKey,
			UploadID:  orphan.UploadID,
		}); err != nil {
			return result, fmt.Errorf("abort orphan Artifact multipart upload: %w", err)
		}
		result.Aborted++
	}
	return result, nil
}
