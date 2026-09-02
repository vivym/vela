package stageworkeragent_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFileMaterializationJournalSurvivesAgentProcessRestart(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	manifestDigest := sha256.Sum256(fixture.manifest)
	committedAt := time.Date(2026, 8, 30, 8, 45, 0, 123, time.UTC)
	record := stageworkeragent.PendingMaterialization{
		ID:             "92000000-0000-0000-0000-000000000001",
		StageAuthority: fixture.authority,
		LocalReceipt: &velav1.LocalMaterializationReceipt{
			ReceiptId:          "92000000-0000-0000-0000-000000000001",
			ManifestSha256:     manifestDigest[:],
			TotalSizeBytes:     int64(len(fixture.payload)),
			SealedAt:           timestamppb.Now(),
			OutputManifestJson: fixture.manifest,
		},
		MaterializationAuthority: &velav1.MaterializationAuthority{SchemaVersion: 1},
		ObjectVersion:            "l2-version-before-restart",
		CommittedAt:              committedAt,
	}
	root := t.TempDir()
	journal, err := stageworkeragent.NewFileMaterializationJournal(root, 1)
	if err != nil {
		t.Fatalf("NewFileMaterializationJournal: %v", err)
	}
	if err := journal.Put(context.Background(), record); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := journal.EnsureCapacity(context.Background()); !errors.Is(err, stageworkeragent.ErrMaterializationJournalFull) {
		t.Fatalf("full file journal capacity error = %v", err)
	}

	restarted, err := stageworkeragent.NewFileMaterializationJournal(root, 1)
	if err != nil {
		t.Fatalf("restart FileMaterializationJournal: %v", err)
	}
	records, err := restarted.List(context.Background())
	if err != nil || len(records) != 1 || records[0].ID != record.ID ||
		!proto.Equal(records[0].StageAuthority, record.StageAuthority) ||
		!proto.Equal(records[0].LocalReceipt, record.LocalReceipt) ||
		!proto.Equal(records[0].MaterializationAuthority, record.MaterializationAuthority) ||
		records[0].ObjectVersion != record.ObjectVersion ||
		!records[0].CommittedAt.Equal(committedAt) {
		t.Fatalf("restarted List = %#v error=%v", records, err)
	}
	if err := restarted.EnsureCapacity(context.Background()); !errors.Is(err, stageworkeragent.ErrMaterializationJournalFull) {
		t.Fatalf("restarted full file journal capacity error = %v", err)
	}
	if err := restarted.Delete(context.Background(), record.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	empty, err := stageworkeragent.NewFileMaterializationJournal(root, 1)
	if err != nil {
		t.Fatalf("restart empty FileMaterializationJournal: %v", err)
	}
	records, err = empty.List(context.Background())
	if err != nil || len(records) != 0 {
		t.Fatalf("List after durable delete = %#v error=%v", records, err)
	}
	if err := empty.EnsureCapacity(context.Background()); err != nil {
		t.Fatalf("empty file journal capacity: %v", err)
	}
}
