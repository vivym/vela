package retention

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestScanDeletionRequestRejectsUnknownState(t *testing.T) {
	_, err := scanDeletionRequest(deletionRequestRow{state: "UNKNOWN"})
	if err == nil {
		t.Fatal("scan Content Deletion request with unknown state succeeded")
	}
}

type deletionRequestRow struct {
	state string
}

func (row deletionRequestRow) Scan(destinations ...any) error {
	if len(destinations) != 13 {
		return errors.New("unexpected Content Deletion projection width")
	}
	requestID := uuid.New()
	projectID := uuid.New()
	jobID := uuid.New()
	requestedAt := time.Now()
	deadlineAt := requestedAt.Add(24 * time.Hour)
	*destinations[0].(*uuid.UUID) = requestID
	*destinations[1].(*uuid.UUID) = projectID
	*destinations[2].(*uuid.UUID) = jobID
	*destinations[3].(*string) = row.state
	*destinations[4].(*time.Time) = requestedAt
	*destinations[5].(*time.Time) = deadlineAt
	*destinations[6].(**time.Time) = nil
	*destinations[7].(*bool) = false
	*destinations[8].(*int64) = 1
	*destinations[9].(*int64) = 0
	*destinations[10].(*int64) = 0
	*destinations[11].(**string) = nil
	*destinations[12].(**string) = nil
	return nil
}
