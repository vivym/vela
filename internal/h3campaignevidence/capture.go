package h3campaignevidence

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
)

var ErrUnstableCampaignAuthority = errors.New("H3 campaign authority changed during capture")

type DatabaseReader interface {
	Capture(context.Context, Selection) (DatabaseSnapshot, error)
}

func Capture(
	ctx context.Context,
	reader DatabaseReader,
	request CaptureRequest,
) (Evidence, error) {
	if ctx == nil || reader == nil || !validEvidenceBinding(request.EvidenceBinding) ||
		!validSelection(request.Selection) {
		return Evidence{}, errors.New("H3 campaign capture input is invalid")
	}
	first, err := reader.Capture(ctx, request.Selection)
	if err != nil {
		return Evidence{}, fmt.Errorf("capture initial H3 campaign authority: %w", err)
	}
	second, err := reader.Capture(ctx, request.Selection)
	if err != nil {
		return Evidence{}, fmt.Errorf("capture final H3 campaign authority: %w", err)
	}
	if !reflect.DeepEqual(first.Runs, second.Runs) ||
		!reflect.DeepEqual(first.CacheRun, second.CacheRun) {
		return Evidence{}, ErrUnstableCampaignAuthority
	}
	return verify(Input{
		EvidenceBinding:     request.EvidenceBinding,
		CapturedAt:          time.Now().UTC(),
		InitialDatabaseRead: first.Provenance,
		FinalDatabaseRead:   second.Provenance,
		Runs:                second.Runs,
		CacheRun:            second.CacheRun,
	})
}

func validSelection(selection Selection) bool {
	return selection.SameNodeJobID != uuid.Nil && selection.CrossNodeJobID != uuid.Nil &&
		selection.CacheJobID != uuid.Nil && selection.SameNodeJobID != selection.CrossNodeJobID &&
		selection.SameNodeJobID != selection.CacheJobID && selection.CrossNodeJobID != selection.CacheJobID
}
