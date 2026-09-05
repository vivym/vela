package stageworkeragent_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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
		ConfirmedDisposition:     stageworkeragent.MaterializationCommitted,
	}
	root := t.TempDir()
	journal, err := stageworkeragent.NewFileMaterializationJournal(root, 1)
	if err != nil {
		t.Fatalf("NewFileMaterializationJournal: %v", err)
	}
	if err := journal.Put(context.Background(), record); err != nil {
		t.Fatalf("Put: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 {
		t.Fatalf("journal entries: %v %v", entries, err)
	}
	encoded, err := os.ReadFile(filepath.Join(root, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	if string(document["schema_version"]) != "2" {
		t.Fatalf("new journal format = %s", document["schema_version"])
	}
	for _, confirmed := range []bool{false, true} {
		t.Run("schema1 confirmed="+map[bool]string{false: "false", true: "true"}[confirmed], func(t *testing.T) {
			legacyRoot := t.TempDir()
			legacy := make(map[string]json.RawMessage, len(document))
			for field, value := range document {
				legacy[field] = value
			}
			legacy["schema_version"] = json.RawMessage("1")
			if !confirmed {
				delete(legacy, "confirmed_disposition")
			}
			legacyBytes, err := json.Marshal(legacy)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(legacyRoot, entries[0].Name()), legacyBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			legacyJournal, err := stageworkeragent.NewFileMaterializationJournal(legacyRoot, 1)
			if confirmed {
				if err == nil {
					t.Fatal("schema 1 accepted schema 2 confirmation state")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			legacyRecords, err := legacyJournal.List(context.Background())
			if err != nil || len(legacyRecords) != 1 || legacyRecords[0].ConfirmedDisposition != "" {
				t.Fatalf("legacy unconfirmed recovery: %+v %v", legacyRecords, err)
			}
		})
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
	if records[0].ConfirmedDisposition != stageworkeragent.MaterializationCommitted {
		t.Fatalf("confirmed disposition lost on restart: %+v", records[0])
	}
	for _, mutation := range []string{"missing version", "missing commit time", "source loss without evidence", "unknown disposition"} {
		t.Run(mutation, func(t *testing.T) {
			invalid := record
			switch mutation {
			case "missing version":
				invalid.ObjectVersion = ""
			case "missing commit time":
				invalid.CommittedAt = time.Time{}
			case "source loss without evidence":
				invalid.ConfirmedDisposition = stageworkeragent.MaterializationSourceLost
			case "unknown disposition":
				invalid.ConfirmedDisposition = "UNKNOWN"
			}
			if err := restarted.Put(context.Background(), invalid); err == nil {
				t.Fatal("invalid confirmed materialization disposition was persisted")
			}
		})
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
