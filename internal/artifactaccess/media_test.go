package artifactaccess

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	store "github.com/vivym/vela/internal/store/sqlc"
)

func TestArtifactDownloadMetadataKeepsActualDurationsAndHidesReceiptInternals(t *testing.T) {
	now := time.Now()
	row := store.ListReadableArtifactSetRow{
		ArtifactSetID: uuid.New(), JobID: uuid.New(), ArtifactID: uuid.New(), Kind: store.ArtifactKindVIDEO,
		RetentionExpiresAt: pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true}, CommittedAt: pgtype.Timestamptz{Time: now, Valid: true},
		ObjectKey: "private/internal", ObjectVersionID: "full-av", SizeBytes: 1000, Sha256: make([]byte, 32), ContentType: "video/mp4",
		ValidationReceipt: []byte(`{"requested_duration_milliseconds":5000,"duration_milliseconds":5167,"container_duration_milliseconds":5175,"frame_count":124,"frame_rate_milli":24000,"audio":{"codec":"aac","sample_rate":32000,"channels":2,"duration_milliseconds":5175},"object_key":"private/internal","validator_revision":"private-validator"}`),
	}
	result, err := artifactSetFromRows([]store.ListReadableArtifactSetRow{row}, now)
	if err != nil {
		t.Fatal(err)
	}
	m := result.Artifacts[0].Media
	if m == nil || m.RequestedDurationMillis != 5000 || m.DurationMillis != 5167 || m.ContainerDurationMillis != 5175 || m.FrameCount != 124 || m.Audio == nil || m.Audio.DurationMillis != 5175 {
		t.Fatalf("media facts changed: %+v", m)
	}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("private")) || bytes.Contains(data, []byte("validator")) {
		t.Fatalf("internal receipt leaked: %s", data)
	}
	row.ValidationReceipt = []byte(`{"duration_milliseconds":0}`)
	if _, err := artifactSetFromRows([]store.ListReadableArtifactSetRow{row}, now); err == nil {
		t.Fatal("accepted invalid committed facts")
	}
}
