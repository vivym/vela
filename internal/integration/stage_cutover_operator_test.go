//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stagecutover"
)

func TestStageCutoverOperatorCLISealsTypedEvidence(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	promotion := stageCutoverPromotionPool(t, database)
	activateProductionStageOnlyCutover(t, database, promotion)

	binary := filepath.Join(t.TempDir(), "vela-stage-cutover")
	build := exec.Command("go", "build", "-o", binary, "./cmd/vela-stage-cutover")
	build.Dir = repositoryRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Stage cutover CLI: %v\n%s", err, output)
	}
	databaseURL := roleDatabaseURL(
		t,
		database.DSN,
		"vela_catalog_promotion_login",
		"vela-catalog-promotion-password",
	)
	operator := "integration-stage-cutover-cli"
	var startInventory stagecutover.InventoryResult
	runStageCutoverCLI(
		t,
		binary,
		databaseURL,
		"capture-inventory",
		stagecutover.CaptureInventoryRequest{SnapshotID: uuid.New(), ObservedBy: operator},
		&startInventory,
	)
	manifestDigest := "0200000000000000000000000000000000000000000000000000000000000000"
	var startEvidence stagecutover.ExternalDrainEvidenceResult
	runStageCutoverCLI(
		t,
		binary,
		databaseURL,
		"record-external-evidence",
		stagecutover.RecordExternalDrainEvidenceRequest{
			EvidenceID:             uuid.New(),
			EvidenceManifestDigest: manifestDigest,
			ObservedBy:             operator,
		},
		&startEvidence,
	)
	time.Sleep(1100 * time.Millisecond)
	var endInventory stagecutover.InventoryResult
	runStageCutoverCLI(
		t,
		binary,
		databaseURL,
		"capture-inventory",
		stagecutover.CaptureInventoryRequest{SnapshotID: uuid.New(), ObservedBy: operator},
		&endInventory,
	)
	var endEvidence stagecutover.ExternalDrainEvidenceResult
	runStageCutoverCLI(
		t,
		binary,
		databaseURL,
		"record-external-evidence",
		stagecutover.RecordExternalDrainEvidenceRequest{
			EvidenceID:             uuid.New(),
			EvidenceManifestDigest: manifestDigest,
			ObservedBy:             operator,
		},
		&endEvidence,
	)
	receiptID := uuid.New()
	var receipt stagecutover.ZeroBacklogReceiptResult
	runStageCutoverCLI(
		t,
		binary,
		databaseURL,
		"seal-zero-backlog",
		stagecutover.SealZeroBacklogRequest{
			ReceiptID:               receiptID,
			StartInventoryID:        startInventory.SnapshotID,
			EndInventoryID:          endInventory.SnapshotID,
			StartExternalEvidenceID: startEvidence.EvidenceID,
			EndExternalEvidenceID:   endEvidence.EvidenceID,
			SealedBy:                operator,
		},
		&receipt,
	)
	if startInventory.TotalCount != 0 || startEvidence.TotalCount != 0 ||
		endInventory.TotalCount != 0 || endEvidence.TotalCount != 0 ||
		receipt.ReceiptID != receiptID || receipt.Replayed ||
		receipt.WindowEndedAt.Sub(receipt.WindowStartedAt) < time.Second {
		t.Fatalf(
			"CLI cutover results start=%#v/%#v end=%#v/%#v receipt=%#v",
			startInventory,
			startEvidence,
			endInventory,
			endEvidence,
			receipt,
		)
	}
}

func TestStageCutoverOperatorCapturesEvidenceAndSealsZeroBacklog(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	promotion := stageCutoverPromotionPool(t, database)
	activateProductionStageOnlyCutover(t, database, promotion)

	service, err := stagecutover.New(context.Background(), promotion)
	if err != nil {
		t.Fatalf("configure Stage cutover operator: %v", err)
	}
	startInventory, err := service.CaptureInventory(context.Background(), stagecutover.CaptureInventoryRequest{
		SnapshotID: uuid.New(),
		ObservedBy: "integration-stage-cutover-operator",
	})
	if err != nil {
		t.Fatalf("capture start inventory: %v", err)
	}
	if startInventory.TotalCount != 0 || len(startInventory.ContentDigest) != 64 {
		t.Fatalf("start inventory = %#v", startInventory)
	}
	manifestDigest := "0100000000000000000000000000000000000000000000000000000000000000"
	startEvidenceRequest := stagecutover.RecordExternalDrainEvidenceRequest{
		EvidenceID:             uuid.New(),
		EvidenceManifestDigest: manifestDigest,
		ObservedBy:             "integration-stage-cutover-operator",
	}
	startEvidence, err := service.RecordExternalDrainEvidence(
		context.Background(), startEvidenceRequest,
	)
	if err != nil {
		t.Fatalf("record start external drain evidence: %v", err)
	}
	if startEvidence.TotalCount != 0 || startEvidence.Replayed ||
		len(startEvidence.ContentDigest) != 64 {
		t.Fatalf("start external drain evidence = %#v", startEvidence)
	}
	replayedEvidence, err := service.RecordExternalDrainEvidence(
		context.Background(), startEvidenceRequest,
	)
	if err != nil {
		t.Fatalf("replay start external drain evidence: %v", err)
	}
	if !replayedEvidence.Replayed || replayedEvidence != (stagecutover.ExternalDrainEvidenceResult{
		EvidenceID:    startEvidence.EvidenceID,
		TotalCount:    startEvidence.TotalCount,
		ContentDigest: startEvidence.ContentDigest,
		Replayed:      true,
	}) {
		t.Fatalf("replayed external drain evidence = %#v", replayedEvidence)
	}

	time.Sleep(1100 * time.Millisecond)
	endInventory, err := service.CaptureInventory(context.Background(), stagecutover.CaptureInventoryRequest{
		SnapshotID: uuid.New(),
		ObservedBy: "integration-stage-cutover-operator",
	})
	if err != nil {
		t.Fatalf("capture end inventory: %v", err)
	}
	endEvidence, err := service.RecordExternalDrainEvidence(
		context.Background(),
		stagecutover.RecordExternalDrainEvidenceRequest{
			EvidenceID:             uuid.New(),
			EvidenceManifestDigest: manifestDigest,
			ObservedBy:             "integration-stage-cutover-operator",
		},
	)
	if err != nil {
		t.Fatalf("record end external drain evidence: %v", err)
	}
	receiptRequest := stagecutover.SealZeroBacklogRequest{
		ReceiptID:               uuid.New(),
		StartInventoryID:        startInventory.SnapshotID,
		EndInventoryID:          endInventory.SnapshotID,
		StartExternalEvidenceID: startEvidence.EvidenceID,
		EndExternalEvidenceID:   endEvidence.EvidenceID,
		SealedBy:                "integration-stage-cutover-operator",
	}
	receipt, err := service.SealZeroBacklog(context.Background(), receiptRequest)
	if err != nil {
		t.Fatalf("seal zero backlog: %v", err)
	}
	if receipt.ReceiptID != receiptRequest.ReceiptID || receipt.Replayed ||
		len(receipt.ContentDigest) != 64 ||
		receipt.WindowEndedAt.Sub(receipt.WindowStartedAt) < time.Second {
		t.Fatalf("zero-backlog receipt = %#v", receipt)
	}
	replayedReceipt, err := service.SealZeroBacklog(context.Background(), receiptRequest)
	if err != nil {
		t.Fatalf("replay zero-backlog seal: %v", err)
	}
	if !replayedReceipt.Replayed || replayedReceipt.ReceiptID != receipt.ReceiptID ||
		replayedReceipt.ContentDigest != receipt.ContentDigest ||
		!replayedReceipt.WindowStartedAt.Equal(receipt.WindowStartedAt) ||
		!replayedReceipt.WindowEndedAt.Equal(receipt.WindowEndedAt) {
		t.Fatalf("replayed zero-backlog receipt = %#v", replayedReceipt)
	}
}

func runStageCutoverCLI(
	t *testing.T,
	binary,
	databaseURL,
	command string,
	request,
	result any,
) {
	t.Helper()
	encodedRequest, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("encode %s request: %v", command, err)
	}
	requestPath := filepath.Join(t.TempDir(), command+".json")
	if err := os.WriteFile(requestPath, encodedRequest, 0o600); err != nil {
		t.Fatalf("write %s request: %v", command, err)
	}
	var stderr bytes.Buffer
	process := exec.Command(binary, command, "--request", requestPath)
	process.Env = environmentWith(map[string]string{
		"VELA_STAGE_CUTOVER_DATABASE_URL": databaseURL,
	})
	process.Stderr = &stderr
	stdout, err := process.Output()
	if err != nil {
		t.Fatalf("run %s: %v stderr=%s", command, err, stderr.String())
	}
	decoder := json.NewDecoder(bytes.NewReader(stdout))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(result); err != nil {
		t.Fatalf("decode %s result %q: %v", command, stdout, err)
	}
}
