//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/financereconciliation"
)

const (
	financeReconciliationPrincipalID = "00000000-0000-0000-0000-000000001901"
	financeReconciliationTLSURI      = "spiffe://finance.internal/reconciliation/primary"
)

func TestFinanceReconciliationHTTPPersistsOnceAndPreservesCommercialHistory(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "finance-http-history")
	service := newFinanceReconciliationService(t, fixture.database)
	handler, err := financereconciliation.NewHTTPHandler(service)
	if err != nil {
		t.Fatalf("create Finance Reconciliation HTTP handler: %v", err)
	}
	beforeHistory := financeCommercialHistorySnapshot(t, fixture)
	body := []byte(`{
		"idempotency_key":"settlement-http-integration-1901",
		"source_sequence":1,
		"organization_id":"` + testOrganizationID + `",
		"kind":"SETTLEMENT_POSTED",
		"currency":"CNY",
		"settlement_minor":500,
		"external_reference":"payment-http-integration-1901",
		"effective_at":"2026-08-24T12:00:00Z"
	}`)
	identityURI, err := url.Parse(financeReconciliationTLSURI)
	if err != nil {
		t.Fatalf("parse Finance Reconciliation TLS identity: %v", err)
	}
	certificate := &x509.Certificate{URIs: []*url.URL{identityURI}}
	tlsState := &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{certificate},
		VerifiedChains:   [][]*x509.Certificate{{certificate}},
	}

	var firstRecordID string
	for attempt, wantStatus := range []int{http.StatusCreated, http.StatusOK} {
		request := httptest.NewRequest(
			http.MethodPost,
			"https://finance-listener.internal"+financereconciliation.ReconciliationPath,
			bytes.NewReader(body),
		)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json")
		request.TLS = tlsState
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != wantStatus || response.Header().Get("Content-Type") != "application/json" {
			t.Fatalf(
				"Finance HTTP attempt %d = %d %s body=%s",
				attempt,
				response.Code,
				response.Header(),
				response.Body.String(),
			)
		}
		var result struct {
			RecordID string `json:"record_id"`
			Replayed bool   `json:"replayed"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatalf("decode Finance HTTP attempt %d: %v", attempt, err)
		}
		if result.RecordID == "" || result.Replayed != (attempt == 1) {
			t.Fatalf("Finance HTTP attempt %d result = %#v", attempt, result)
		}
		if attempt == 0 {
			firstRecordID = result.RecordID
		} else if result.RecordID != firstRecordID {
			t.Fatalf("Finance HTTP replay record = %s, want %s", result.RecordID, firstRecordID)
		}
	}

	afterHistory := financeCommercialHistorySnapshot(t, fixture)
	if afterHistory != beforeHistory {
		t.Fatalf(
			"Finance Reconciliation changed Job/Charge/Artifact/Invoice history\nbefore=%s\nafter=%s",
			beforeHistory,
			afterHistory,
		)
	}
	var unsettled, records int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT account.unsettled_posted_minor,
			(SELECT count(*) FROM finance_reconciliation_records)
		FROM organization_credit_accounts AS account
		WHERE account.organization_id = $1
	`, testOrganizationID).Scan(&unsettled, &records); err != nil {
		t.Fatalf("read Finance HTTP durable effect: %v", err)
	}
	if unsettled != 750 || records != 1 {
		t.Fatalf("Finance HTTP durable effect = unsettled %d records %d", unsettled, records)
	}
}

func TestFinanceReconciliationServiceRejectsEveryInvalidAmountShape(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "finance-invalid-amount-shapes")
	service := newFinanceReconciliationService(t, fixture.database)
	negative := int64(-1)
	zero := int64(0)
	one := int64(1)
	base := financereconciliation.Request{
		IdempotencyKey:    "invalid-amount-shape",
		SourceSequence:    1,
		OrganizationID:    uuid.MustParse(testOrganizationID),
		Kind:              financereconciliation.KindSettlementPosted,
		Currency:          "CNY",
		SettlementMinor:   &one,
		ExternalReference: "invalid-amount-shape-reference",
		EffectiveAt:       time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC),
	}
	tests := []struct {
		name string
		edit func(*financereconciliation.Request)
	}{
		{name: "unknown kind", edit: func(request *financereconciliation.Request) { request.Kind = "UNKNOWN" }},
		{name: "settlement missing amount", edit: func(request *financereconciliation.Request) { request.SettlementMinor = nil }},
		{name: "settlement zero", edit: func(request *financereconciliation.Request) { request.SettlementMinor = &zero }},
		{name: "settlement negative", edit: func(request *financereconciliation.Request) { request.SettlementMinor = &negative }},
		{name: "settlement with credit adjustment", edit: func(request *financereconciliation.Request) { request.CreditAdjustmentMinor = &one }},
		{name: "settlement with limit", edit: func(request *financereconciliation.Request) { request.ContractCreditLimitMinor = &one }},
		{name: "credit adjustment missing amount", edit: func(request *financereconciliation.Request) {
			request.Kind = financereconciliation.KindCreditAdjustmentPosted
			request.SettlementMinor = nil
		}},
		{name: "credit adjustment zero", edit: func(request *financereconciliation.Request) {
			request.Kind = financereconciliation.KindCreditAdjustmentPosted
			request.SettlementMinor = nil
			request.CreditAdjustmentMinor = &zero
		}},
		{name: "credit adjustment with settlement", edit: func(request *financereconciliation.Request) {
			request.Kind = financereconciliation.KindCreditAdjustmentPosted
			request.CreditAdjustmentMinor = &one
		}},
		{name: "credit adjustment with limit", edit: func(request *financereconciliation.Request) {
			request.Kind = financereconciliation.KindCreditAdjustmentPosted
			request.SettlementMinor = nil
			request.CreditAdjustmentMinor = &one
			request.ContractCreditLimitMinor = &one
		}},
		{name: "limit missing amount", edit: func(request *financereconciliation.Request) {
			request.Kind = financereconciliation.KindContractCreditLimitChanged
			request.SettlementMinor = nil
		}},
		{name: "limit negative", edit: func(request *financereconciliation.Request) {
			request.Kind = financereconciliation.KindContractCreditLimitChanged
			request.SettlementMinor = nil
			request.ContractCreditLimitMinor = &negative
		}},
		{name: "limit with settlement", edit: func(request *financereconciliation.Request) {
			request.Kind = financereconciliation.KindContractCreditLimitChanged
			request.ContractCreditLimitMinor = &one
		}},
		{name: "limit with credit adjustment", edit: func(request *financereconciliation.Request) {
			request.Kind = financereconciliation.KindContractCreditLimitChanged
			request.SettlementMinor = nil
			request.CreditAdjustmentMinor = &one
			request.ContractCreditLimitMinor = &one
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := base
			request.IdempotencyKey += "-" + uuid.NewString()
			request.ExternalReference += "-" + uuid.NewString()
			test.edit(&request)
			_, err := service.Apply(context.Background(), request)
			assertFinanceReconciliationFailure(t, err, financereconciliation.FailureInvalid)
		})
	}
	var unsettled, records, cursors int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT account.unsettled_posted_minor,
			(SELECT count(*) FROM finance_reconciliation_records),
			(SELECT count(*) FROM finance_reconciliation_cursors)
		FROM organization_credit_accounts AS account
		WHERE account.organization_id = $1
	`, testOrganizationID).Scan(&unsettled, &records, &cursors); err != nil {
		t.Fatalf("read invalid amount-shape effects: %v", err)
	}
	if unsettled != 1250 || records != 0 || cursors != 0 {
		t.Fatalf(
			"invalid amount-shape effects = unsettled %d records %d cursors %d",
			unsettled,
			records,
			cursors,
		)
	}
}

func TestFinanceReconciliationSettlementAppliesOnceAndReplays(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "finance-settlement-replay")
	service := newFinanceReconciliationService(t, fixture.database)
	ctx := context.Background()

	var beforeLimit, beforeReserved, beforeUnsettled, beforeVersion int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT contract_credit_limit_minor, reserved_minor,
			unsettled_posted_minor, version
		FROM organization_credit_accounts
		WHERE organization_id = $1
	`, testOrganizationID).Scan(
		&beforeLimit, &beforeReserved, &beforeUnsettled, &beforeVersion,
	); err != nil {
		t.Fatalf("read credit account before settlement: %v", err)
	}
	if beforeUnsettled != 1250 || beforeReserved != 0 {
		t.Fatalf(
			"settlement fixture credit = reserved %d unsettled %d, want 0/1250",
			beforeReserved,
			beforeUnsettled,
		)
	}

	settlementMinor := int64(500)
	request := financereconciliation.Request{
		IdempotencyKey:    "settlement-payment-1901",
		SourceSequence:    1,
		OrganizationID:    uuid.MustParse(testOrganizationID),
		Kind:              financereconciliation.KindSettlementPosted,
		Currency:          "CNY",
		SettlementMinor:   &settlementMinor,
		ExternalReference: "payment-1901",
		EffectiveAt:       time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC),
	}
	created, err := service.Apply(ctx, request)
	if err != nil {
		t.Fatalf("apply settlement: %v", err)
	}
	if created.Replayed || created.RecordID == uuid.Nil ||
		created.OrganizationID != uuid.MustParse(testOrganizationID) ||
		created.Kind != financereconciliation.KindSettlementPosted ||
		created.Currency != "CNY" ||
		created.ContractCreditLimitMinor != beforeLimit ||
		created.UnsettledPostedMinor != 750 ||
		created.AccountVersion != beforeVersion+1 || created.PostedAt.IsZero() {
		t.Fatalf("created settlement result = %#v", created)
	}

	replayed, err := service.Apply(ctx, request)
	if err != nil {
		t.Fatalf("replay settlement: %v", err)
	}
	if !replayed.Replayed || replayed.RecordID != created.RecordID ||
		replayed.UnsettledPostedMinor != 750 ||
		replayed.AccountVersion != created.AccountVersion ||
		!replayed.PostedAt.Equal(created.PostedAt) {
		t.Fatalf("replayed settlement result = %#v, want record %#v", replayed, created)
	}

	var afterLimit, afterReserved, afterUnsettled, afterVersion, records int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT account.contract_credit_limit_minor, account.reserved_minor,
			account.unsettled_posted_minor, account.version,
			(SELECT count(*) FROM finance_reconciliation_records)
		FROM organization_credit_accounts AS account
		WHERE account.organization_id = $1
	`, testOrganizationID).Scan(
		&afterLimit, &afterReserved, &afterUnsettled, &afterVersion, &records,
	); err != nil {
		t.Fatalf("read credit account after settlement replay: %v", err)
	}
	if afterLimit != beforeLimit || afterReserved != beforeReserved ||
		afterUnsettled != 750 || afterVersion != beforeVersion+1 || records != 1 {
		t.Fatalf(
			"settlement replay state = limit %d reserved %d unsettled %d version %d records %d",
			afterLimit,
			afterReserved,
			afterUnsettled,
			afterVersion,
			records,
		)
	}
	for _, statement := range []string{
		"UPDATE finance_reconciliation_records SET external_reference = 'forged' WHERE id = $1",
		"DELETE FROM finance_reconciliation_records WHERE id = $1",
	} {
		_, err := fixture.database.Admin.Exec(statement, created.RecordID)
		assertPostgresConstraint(t, err, "finance_reconciliation_record_is_immutable")
	}
}

func TestFinanceReconciliationPositiveCreditAdjustmentReducesUnsettledCredit(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "finance-positive-credit-adjustment")
	service := newFinanceReconciliationService(t, fixture.database)
	creditMinor := int64(250)

	result, err := service.Apply(context.Background(), financereconciliation.Request{
		IdempotencyKey:        "credit-note-1902",
		SourceSequence:        1,
		OrganizationID:        uuid.MustParse(testOrganizationID),
		Kind:                  financereconciliation.KindCreditAdjustmentPosted,
		Currency:              "CNY",
		CreditAdjustmentMinor: &creditMinor,
		ExternalReference:     "credit-note-reference-1902",
		EffectiveAt:           time.Date(2026, 8, 24, 12, 30, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("apply positive credit adjustment: %v", err)
	}
	if result.Replayed || result.Kind != financereconciliation.KindCreditAdjustmentPosted ||
		result.UnsettledPostedMinor != 1000 {
		t.Fatalf("positive credit adjustment result = %#v", result)
	}

	var unsettled, records, charges, receipts int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT unsettled_posted_minor FROM organization_credit_accounts
			 WHERE organization_id = $1),
			(SELECT count(*) FROM finance_reconciliation_records),
			(SELECT count(*) FROM charges WHERE id = $2),
			(SELECT count(*) FROM invoice_export_receipts WHERE charge_id = $2)
	`, testOrganizationID, fixture.chargeID).Scan(
		&unsettled, &records, &charges, &receipts,
	); err != nil {
		t.Fatalf("read positive credit adjustment effects: %v", err)
	}
	if unsettled != 1000 || records != 1 || charges != 1 || receipts != 0 {
		t.Fatalf(
			"positive credit adjustment effects = unsettled %d records %d charges %d receipts %d",
			unsettled,
			records,
			charges,
			receipts,
		)
	}
}

func TestFinanceReconciliationNegativeCreditAdjustmentRestoresReceivable(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "finance-negative-credit-adjustment")
	service := newFinanceReconciliationService(t, fixture.database)
	debitMinor := int64(-400)

	result, err := service.Apply(context.Background(), financereconciliation.Request{
		IdempotencyKey:        "credit-reversal-1903",
		SourceSequence:        1,
		OrganizationID:        uuid.MustParse(testOrganizationID),
		Kind:                  financereconciliation.KindCreditAdjustmentPosted,
		Currency:              "CNY",
		CreditAdjustmentMinor: &debitMinor,
		ExternalReference:     "credit-reversal-reference-1903",
		EffectiveAt:           time.Date(2026, 8, 24, 12, 45, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("apply negative credit adjustment: %v", err)
	}
	if result.Replayed || result.Kind != financereconciliation.KindCreditAdjustmentPosted ||
		result.UnsettledPostedMinor != 1650 {
		t.Fatalf("negative credit adjustment result = %#v", result)
	}

	var unsettled, recordedAdjustment int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT account.unsettled_posted_minor, reconciliation.credit_adjustment_minor
		FROM organization_credit_accounts AS account
		JOIN finance_reconciliation_records AS reconciliation
		  ON reconciliation.organization_id = account.organization_id
		WHERE account.organization_id = $1
	`, testOrganizationID).Scan(&unsettled, &recordedAdjustment); err != nil {
		t.Fatalf("read negative credit adjustment effect: %v", err)
	}
	if unsettled != 1650 || recordedAdjustment != -400 {
		t.Fatalf(
			"negative credit adjustment effect = unsettled %d adjustment %d",
			unsettled,
			recordedAdjustment,
		)
	}
}

func TestFinanceReconciliationChangesAbsoluteContractCreditLimit(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "finance-contract-credit-limit")
	service := newFinanceReconciliationService(t, fixture.database)
	newLimit := int64(50000)

	result, err := service.Apply(context.Background(), financereconciliation.Request{
		IdempotencyKey:           "contract-limit-1904",
		SourceSequence:           1,
		OrganizationID:           uuid.MustParse(testOrganizationID),
		Kind:                     financereconciliation.KindContractCreditLimitChanged,
		Currency:                 "CNY",
		ContractCreditLimitMinor: &newLimit,
		ExternalReference:        "contract-amendment-reference-1904",
		EffectiveAt:              time.Date(2026, 8, 24, 13, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("apply Contract Credit Limit change: %v", err)
	}
	if result.Replayed || result.Kind != financereconciliation.KindContractCreditLimitChanged ||
		result.ContractCreditLimitMinor != 50000 || result.UnsettledPostedMinor != 1250 {
		t.Fatalf("Contract Credit Limit result = %#v", result)
	}

	var limit, unsettled, recordedBefore, recordedAfter int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT account.contract_credit_limit_minor, account.unsettled_posted_minor,
			reconciliation.contract_credit_limit_minor_before,
			reconciliation.contract_credit_limit_minor_after
		FROM organization_credit_accounts AS account
		JOIN finance_reconciliation_records AS reconciliation
		  ON reconciliation.organization_id = account.organization_id
		WHERE account.organization_id = $1
	`, testOrganizationID).Scan(
		&limit, &unsettled, &recordedBefore, &recordedAfter,
	); err != nil {
		t.Fatalf("read Contract Credit Limit change: %v", err)
	}
	if limit != 50000 || unsettled != 1250 || recordedBefore != 100000 || recordedAfter != 50000 {
		t.Fatalf(
			"Contract Credit Limit state = limit %d unsettled %d snapshots %d/%d",
			limit,
			unsettled,
			recordedBefore,
			recordedAfter,
		)
	}
}

func TestFinanceReconciliationRejectsInvalidLedgerEffectsWithoutDurableChange(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "finance-reconciliation-rejections")
	service := newFinanceReconciliationService(t, fixture.database)
	overAppliedSettlement := int64(1251)
	overAppliedCredit := int64(1251)
	debitBeyondLimit := int64(-100000)
	limitBelowUsage := int64(1249)
	overflowingDebit := int64(math.MinInt64)
	representableSettlement := int64(1)
	base := financereconciliation.Request{
		IdempotencyKey:    "rejection-base",
		SourceSequence:    1,
		OrganizationID:    uuid.MustParse(testOrganizationID),
		Kind:              financereconciliation.KindSettlementPosted,
		Currency:          "CNY",
		SettlementMinor:   &overAppliedSettlement,
		ExternalReference: "rejection-reference-base",
		EffectiveAt:       time.Date(2026, 8, 24, 13, 15, 0, 0, time.UTC),
	}
	tests := []struct {
		name string
		code financereconciliation.FailureCode
		edit func(*financereconciliation.Request)
	}{
		{name: "settlement over-application", code: financereconciliation.FailureConflict},
		{
			name: "credit over-application", code: financereconciliation.FailureConflict,
			edit: func(request *financereconciliation.Request) {
				request.Kind = financereconciliation.KindCreditAdjustmentPosted
				request.SettlementMinor = nil
				request.CreditAdjustmentMinor = &overAppliedCredit
			},
		},
		{
			name: "debit exceeds Contract Credit Limit", code: financereconciliation.FailureConflict,
			edit: func(request *financereconciliation.Request) {
				request.Kind = financereconciliation.KindCreditAdjustmentPosted
				request.SettlementMinor = nil
				request.CreditAdjustmentMinor = &debitBeyondLimit
			},
		},
		{
			name: "limit below committed usage", code: financereconciliation.FailureConflict,
			edit: func(request *financereconciliation.Request) {
				request.Kind = financereconciliation.KindContractCreditLimitChanged
				request.SettlementMinor = nil
				request.ContractCreditLimitMinor = &limitBelowUsage
			},
		},
		{
			name: "currency mismatch", code: financereconciliation.FailureInvalid,
			edit: func(request *financereconciliation.Request) { request.Currency = "USD" },
		},
		{
			name: "unknown Organization", code: financereconciliation.FailureNotFound,
			edit: func(request *financereconciliation.Request) {
				request.OrganizationID = uuid.New()
			},
		},
		{
			name: "source sequence gap", code: financereconciliation.FailureConflict,
			edit: func(request *financereconciliation.Request) { request.SourceSequence = 2 },
		},
		{
			name: "signed amount cannot overflow bigint", code: financereconciliation.FailureConflict,
			edit: func(request *financereconciliation.Request) {
				request.Kind = financereconciliation.KindCreditAdjustmentPosted
				request.SettlementMinor = nil
				request.CreditAdjustmentMinor = &overflowingDebit
			},
		},
		{
			name: "timestamp beyond PostgreSQL precision", code: financereconciliation.FailureInvalid,
			edit: func(request *financereconciliation.Request) {
				request.SettlementMinor = &representableSettlement
				request.EffectiveAt = time.Date(2026, 8, 24, 13, 15, 0, 1, time.UTC)
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := base
			request.IdempotencyKey += "-" + uuid.NewString()
			request.ExternalReference += "-" + uuid.NewString()
			if test.edit != nil {
				test.edit(&request)
			}
			_, err := service.Apply(context.Background(), request)
			assertFinanceReconciliationFailure(t, err, test.code)

			var unsettled, records, cursors int64
			if err := fixture.database.Admin.QueryRow(`
				SELECT
					(SELECT unsettled_posted_minor FROM organization_credit_accounts
					 WHERE organization_id = $1),
					(SELECT count(*) FROM finance_reconciliation_records),
					(SELECT count(*) FROM finance_reconciliation_cursors)
			`, testOrganizationID).Scan(&unsettled, &records, &cursors); err != nil {
				t.Fatalf("read rejected reconciliation effects: %v", err)
			}
			if unsettled != 1250 || records != 0 || cursors != 0 {
				t.Fatalf(
					"rejected reconciliation %d effects = unsettled %d records %d cursors %d",
					index,
					unsettled,
					records,
					cursors,
				)
			}
		})
	}
}

func TestFinanceReconciliationCannotConsumeActiveReservations(t *testing.T) {
	fixture := newStartFixture(t, "finance-active-reservation", 7)
	service := newFinanceReconciliationService(t, fixture.database)
	debitMinor := int64(-99000)
	tooLowLimit := int64(1000)
	requests := []financereconciliation.Request{
		{
			IdempotencyKey:        "active-reservation-debit",
			SourceSequence:        1,
			OrganizationID:        uuid.MustParse(testOrganizationID),
			Kind:                  financereconciliation.KindCreditAdjustmentPosted,
			Currency:              "CNY",
			CreditAdjustmentMinor: &debitMinor,
			ExternalReference:     "active-reservation-debit-reference",
			EffectiveAt:           time.Date(2026, 8, 24, 13, 20, 0, 0, time.UTC),
		},
		{
			IdempotencyKey:           "active-reservation-limit",
			SourceSequence:           1,
			OrganizationID:           uuid.MustParse(testOrganizationID),
			Kind:                     financereconciliation.KindContractCreditLimitChanged,
			Currency:                 "CNY",
			ContractCreditLimitMinor: &tooLowLimit,
			ExternalReference:        "active-reservation-limit-reference",
			EffectiveAt:              time.Date(2026, 8, 24, 13, 21, 0, 0, time.UTC),
		},
	}
	for _, request := range requests {
		_, err := service.Apply(context.Background(), request)
		assertFinanceReconciliationFailure(t, err, financereconciliation.FailureConflict)
	}

	var limit, reserved, unsettled, records, cursors int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT account.contract_credit_limit_minor, account.reserved_minor,
			account.unsettled_posted_minor,
			(SELECT count(*) FROM finance_reconciliation_records),
			(SELECT count(*) FROM finance_reconciliation_cursors)
		FROM organization_credit_accounts AS account
		WHERE account.organization_id = $1
	`, testOrganizationID).Scan(
		&limit, &reserved, &unsettled, &records, &cursors,
	); err != nil {
		t.Fatalf("read active-reservation Finance effects: %v", err)
	}
	if limit != 100000 || reserved != 1250 || unsettled != 0 || records != 0 || cursors != 0 {
		t.Fatalf(
			"active-reservation Finance effects = limit %d reserved %d unsettled %d records %d cursors %d",
			limit,
			reserved,
			unsettled,
			records,
			cursors,
		)
	}
}

func TestFinanceReconciliationRejectsCommittedKeySequenceAndReferenceConflicts(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "finance-reconciliation-conflicts")
	service := newFinanceReconciliationService(t, fixture.database)
	settlementMinor := int64(100)
	committed := financereconciliation.Request{
		IdempotencyKey:    "committed-reconciliation-1905",
		SourceSequence:    1,
		OrganizationID:    uuid.MustParse(testOrganizationID),
		Kind:              financereconciliation.KindSettlementPosted,
		Currency:          "CNY",
		SettlementMinor:   &settlementMinor,
		ExternalReference: "committed-reference-1905",
		EffectiveAt:       time.Date(2026, 8, 24, 13, 30, 0, 0, time.UTC),
	}
	if _, err := service.Apply(context.Background(), committed); err != nil {
		t.Fatalf("commit reconciliation conflict fixture: %v", err)
	}

	differentAmount := int64(101)
	keyConflict := committed
	keyConflict.SettlementMinor = &differentAmount
	_, err := service.Apply(context.Background(), keyConflict)
	assertFinanceReconciliationFailure(t, err, financereconciliation.FailureConflict)

	sequenceConflict := committed
	sequenceConflict.IdempotencyKey = "different-key-same-sequence-1905"
	sequenceConflict.ExternalReference = "different-reference-same-sequence-1905"
	_, err = service.Apply(context.Background(), sequenceConflict)
	assertFinanceReconciliationFailure(t, err, financereconciliation.FailureConflict)

	referenceConflict := committed
	referenceConflict.IdempotencyKey = "different-key-same-reference-1905"
	referenceConflict.SourceSequence = 2
	_, err = service.Apply(context.Background(), referenceConflict)
	assertFinanceReconciliationFailure(t, err, financereconciliation.FailureConflict)

	gap := committed
	gap.IdempotencyKey = "gap-key-1905"
	gap.SourceSequence = 3
	gap.ExternalReference = "gap-reference-1905"
	_, err = service.Apply(context.Background(), gap)
	assertFinanceReconciliationFailure(t, err, financereconciliation.FailureConflict)

	var unsettled, records, lastSequence int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT account.unsettled_posted_minor,
			(SELECT count(*) FROM finance_reconciliation_records), cursor.last_sequence
		FROM organization_credit_accounts AS account
		JOIN finance_reconciliation_cursors AS cursor
		  ON cursor.principal_id = $2
		WHERE account.organization_id = $1
	`, testOrganizationID, financeReconciliationPrincipalID).Scan(
		&unsettled, &records, &lastSequence,
	); err != nil {
		t.Fatalf("read reconciliation conflict effects: %v", err)
	}
	if unsettled != 1150 || records != 1 || lastSequence != 1 {
		t.Fatalf(
			"reconciliation conflicts changed state = unsettled %d records %d sequence %d",
			unsettled,
			records,
			lastSequence,
		)
	}
}

func TestFinanceReconciliationPrincipalBindingAndRoleFailClosed(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	financePool := newRolePool(
		t,
		database.DSN,
		"vela_finance_reconciliation_login",
		"vela-finance-reconciliation-password",
	)
	if _, err := financereconciliation.NewService(context.Background(), financePool); err == nil {
		t.Fatal("unbound Finance Reconciliation login resolved a Principal")
	}
	if _, err := database.Admin.Exec(`
		CREATE ROLE vela_finance_reconciliation_unbound_login
		LOGIN PASSWORD 'vela-finance-reconciliation-unbound-password'
		IN ROLE vela_finance_reconciliation
	`); err != nil {
		t.Fatalf("create unbound Finance Reconciliation login: %v", err)
	}
	unboundPool := newRolePool(
		t,
		database.DSN,
		"vela_finance_reconciliation_unbound_login",
		"vela-finance-reconciliation-unbound-password",
	)
	if _, err := financereconciliation.NewService(context.Background(), unboundPool); err == nil {
		t.Fatal("cross-login Finance Reconciliation role resolved another login's Principal")
	}

	service := newFinanceReconciliationService(t, database)
	if err := veladb.VerifyRole(
		context.Background(),
		financePool,
		veladb.RoleFinanceReconciliation,
	); err != nil {
		t.Fatalf("verify Finance Reconciliation runtime role: %v", err)
	}
	for _, table := range []string{
		"organization_credit_accounts",
		"finance_reconciliation_principals",
		"finance_reconciliation_database_bindings",
		"finance_reconciliation_cursors",
		"finance_reconciliation_records",
		"charges",
		"invoice_exports",
		"invoice_export_receipts",
		"jobs",
	} {
		var canRead, canInsert, canUpdate, canDelete bool
		if err := financePool.QueryRow(context.Background(), `
			SELECT
				has_table_privilege(current_user, $1, 'SELECT'),
				has_table_privilege(current_user, $1, 'INSERT'),
				has_table_privilege(current_user, $1, 'UPDATE'),
				has_table_privilege(current_user, $1, 'DELETE')
		`, table).Scan(&canRead, &canInsert, &canUpdate, &canDelete); err != nil {
			t.Fatalf("inspect Finance Reconciliation table privileges on %s: %v", table, err)
		}
		if canRead || canInsert || canUpdate || canDelete {
			t.Fatalf("Finance Reconciliation runtime has direct privileges on %s", table)
		}
	}
	var runtimeInheritsOwner, ownerCanLogin, ownerBypassesRLS bool
	if err := database.Admin.QueryRow(`
		SELECT
			pg_has_role('vela_finance_reconciliation_login',
				'vela_finance_reconciliation_owner', 'MEMBER'),
			owner.rolcanlogin,
			owner.rolbypassrls
		FROM pg_roles AS owner
		WHERE owner.rolname = 'vela_finance_reconciliation_owner'
	`).Scan(&runtimeInheritsOwner, &ownerCanLogin, &ownerBypassesRLS); err != nil {
		t.Fatalf("inspect Finance Reconciliation owner boundary: %v", err)
	}
	if runtimeInheritsOwner || ownerCanLogin || !ownerBypassesRLS {
		t.Fatalf(
			"Finance Reconciliation owner boundary = inherits %t login %t bypass %t",
			runtimeInheritsOwner,
			ownerCanLogin,
			ownerBypassesRLS,
		)
	}

	if _, err := database.Admin.Exec(`
		UPDATE finance_reconciliation_principals
		SET status = 'DISABLED', disabled_at = clock_timestamp()
		WHERE id = $1
	`, financeReconciliationPrincipalID); err != nil {
		t.Fatalf("disable Finance Reconciliation Principal: %v", err)
	}
	settlementMinor := int64(1)
	_, err := service.Apply(context.Background(), financereconciliation.Request{
		IdempotencyKey:    "disabled-principal-1906",
		SourceSequence:    1,
		OrganizationID:    uuid.MustParse(testOrganizationID),
		Kind:              financereconciliation.KindSettlementPosted,
		Currency:          "CNY",
		SettlementMinor:   &settlementMinor,
		ExternalReference: "disabled-principal-reference-1906",
		EffectiveAt:       time.Date(2026, 8, 24, 14, 0, 0, 0, time.UTC),
	})
	assertFinanceReconciliationFailure(t, err, financereconciliation.FailureUnauthorized)

	for _, login := range []string{
		"vela_billing_login",
		"vela_organization_billing_request_login",
		"vela_request_login",
		"vela_internal_login",
	} {
		var canApply bool
		if err := database.Admin.QueryRow(`
			SELECT has_function_privilege(
				$1,
				'vela_apply_finance_reconciliation(uuid,text,bigint,uuid,finance_reconciliation_kind,text,bigint,bigint,bigint,text,timestamptz)',
				'EXECUTE'
			)
		`, login).Scan(&canApply); err != nil {
			t.Fatalf("inspect %s Finance Reconciliation execute privilege: %v", login, err)
		}
		if canApply {
			t.Fatalf("non-Finance login %s can apply Finance Reconciliation", login)
		}
	}

	if _, err := financePool.Exec(
		context.Background(),
		"SELECT * FROM finance_reconciliation_records",
	); err == nil {
		t.Fatal("Finance Reconciliation runtime read records directly")
	} else {
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
			t.Fatalf("direct Finance Reconciliation read error = %v", err)
		}
	}
}

func TestFinanceReconciliationProvisioningIdentityIsImmutableAndDisablementPermanent(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	if _, err := database.Admin.Exec(`
		INSERT INTO finance_reconciliation_principals (
			id, stable_id, tls_uri_identity
		) VALUES ($1, 'immutable-finance-principal', $2)
	`, financeReconciliationPrincipalID, financeReconciliationTLSURI); err != nil {
		t.Fatalf("provision immutable Finance Principal: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO finance_reconciliation_database_bindings (
			database_role, principal_id
		) VALUES ('vela_finance_reconciliation_login', $1)
	`, financeReconciliationPrincipalID); err != nil {
		t.Fatalf("bind immutable Finance Principal: %v", err)
	}

	for _, test := range []struct {
		name       string
		statement  string
		constraint string
	}{
		{
			name: "rewrite Principal stable id",
			statement: `UPDATE finance_reconciliation_principals
				SET stable_id = 'forged' WHERE id = '` + financeReconciliationPrincipalID + `'`,
			constraint: "finance_reconciliation_principal_identity_is_immutable",
		},
		{
			name: "rewrite Principal TLS URI",
			statement: `UPDATE finance_reconciliation_principals
				SET tls_uri_identity = 'spiffe://forged/identity'
				WHERE id = '` + financeReconciliationPrincipalID + `'`,
			constraint: "finance_reconciliation_principal_identity_is_immutable",
		},
		{
			name: "rewrite database binding",
			statement: `UPDATE finance_reconciliation_database_bindings
				SET principal_id = gen_random_uuid()
				WHERE database_role = 'vela_finance_reconciliation_login'`,
			constraint: "finance_reconciliation_database_binding_is_immutable",
		},
		{
			name: "delete database binding",
			statement: `DELETE FROM finance_reconciliation_database_bindings
				WHERE database_role = 'vela_finance_reconciliation_login'`,
			constraint: "finance_reconciliation_database_binding_is_immutable",
		},
		{
			name: "delete Principal",
			statement: `DELETE FROM finance_reconciliation_principals
				WHERE id = '` + financeReconciliationPrincipalID + `'`,
			constraint: "finance_reconciliation_principal_identity_is_immutable",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := database.Admin.Exec(test.statement)
			assertPostgresConstraint(t, err, test.constraint)
		})
	}

	if _, err := database.Admin.Exec(`
		UPDATE finance_reconciliation_database_bindings
		SET disabled_at = clock_timestamp()
		WHERE database_role = 'vela_finance_reconciliation_login'
	`); err != nil {
		t.Fatalf("disable Finance Principal binding: %v", err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE finance_reconciliation_principals
		SET status = 'DISABLED', disabled_at = clock_timestamp()
		WHERE id = $1
	`, financeReconciliationPrincipalID); err != nil {
		t.Fatalf("disable Finance Principal: %v", err)
	}
	for _, statement := range []string{
		`UPDATE finance_reconciliation_database_bindings SET disabled_at = NULL
		 WHERE database_role = 'vela_finance_reconciliation_login'`,
		`UPDATE finance_reconciliation_principals SET status = 'ACTIVE', disabled_at = NULL
		 WHERE id = '` + financeReconciliationPrincipalID + `'`,
	} {
		_, err := database.Admin.Exec(statement)
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) ||
			(postgresError.ConstraintName != "finance_reconciliation_database_binding_is_immutable" &&
				postgresError.ConstraintName != "finance_reconciliation_principal_disablement_is_permanent") {
			t.Fatalf("Finance provisioning reactivation error = %v", err)
		}
	}
}

func TestFinanceReconciliationMigrationDownUpAndProvisioningRefusal(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")

	if err := goose.DownTo(database.Admin, migrations, 17); err != nil {
		t.Fatalf("migrate empty Finance Reconciliation schema down: %v", err)
	}
	for _, table := range []string{
		"finance_reconciliation_principals",
		"finance_reconciliation_database_bindings",
		"finance_reconciliation_cursors",
		"finance_reconciliation_records",
	} {
		assertTableDoesNotExist(t, database.Admin, table)
	}
	if err := goose.UpTo(database.Admin, migrations, 18); err != nil {
		t.Fatalf("migrate Finance Reconciliation schema up: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO finance_reconciliation_principals (
			id, stable_id, tls_uri_identity
		) VALUES ($1, 'durable-finance-principal', $2)
	`, financeReconciliationPrincipalID, financeReconciliationTLSURI); err != nil {
		t.Fatalf("provision durable Finance Principal: %v", err)
	}

	err := goose.DownTo(database.Admin, migrations, 17)
	assertPostgresConstraint(t, err, "finance_reconciliation_contract_has_durable_evidence")
	var version int64
	if err := database.Admin.QueryRow(`
		SELECT version_id FROM goose_db_version
		WHERE is_applied ORDER BY id DESC LIMIT 1
	`).Scan(&version); err != nil {
		t.Fatalf("read migration version after refused Down: %v", err)
	}
	if version != 18 {
		t.Fatalf("migration version after refused Finance Down = %d, want 18", version)
	}
}

func TestFinanceReconciliationConcurrentReplicasCommitOneSequence(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "finance-reconciliation-concurrent")
	first := newFinanceReconciliationService(t, fixture.database)
	secondPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_finance_reconciliation_login",
		"vela-finance-reconciliation-password",
	)
	second, err := financereconciliation.NewService(context.Background(), secondPool)
	if err != nil {
		t.Fatalf("create second Finance Reconciliation replica: %v", err)
	}
	firstAmount := int64(100)
	secondAmount := int64(200)
	requests := []financereconciliation.Request{
		{
			IdempotencyKey:    "concurrent-finance-a",
			SourceSequence:    1,
			OrganizationID:    uuid.MustParse(testOrganizationID),
			Kind:              financereconciliation.KindSettlementPosted,
			Currency:          "CNY",
			SettlementMinor:   &firstAmount,
			ExternalReference: "concurrent-reference-a",
			EffectiveAt:       time.Date(2026, 8, 24, 14, 30, 0, 0, time.UTC),
		},
		{
			IdempotencyKey:    "concurrent-finance-b",
			SourceSequence:    1,
			OrganizationID:    uuid.MustParse(testOrganizationID),
			Kind:              financereconciliation.KindSettlementPosted,
			Currency:          "CNY",
			SettlementMinor:   &secondAmount,
			ExternalReference: "concurrent-reference-b",
			EffectiveAt:       time.Date(2026, 8, 24, 14, 30, 1, 0, time.UTC),
		},
	}
	services := []*financereconciliation.Service{first, second}
	type applyResult struct {
		index  int
		result financereconciliation.Result
		err    error
	}
	start := make(chan struct{})
	results := make(chan applyResult, 2)
	var wait sync.WaitGroup
	for index := range services {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			result, err := services[index].Apply(context.Background(), requests[index])
			results <- applyResult{index: index, result: result, err: err}
		}(index)
	}
	close(start)
	wait.Wait()
	close(results)

	winner := -1
	for result := range results {
		if result.err == nil {
			if winner != -1 {
				t.Fatal("both concurrent Finance Reconciliation replicas committed sequence 1")
			}
			winner = result.index
			continue
		}
		assertFinanceReconciliationFailure(t, result.err, financereconciliation.FailureConflict)
	}
	if winner == -1 {
		t.Fatal("neither concurrent Finance Reconciliation replica committed sequence 1")
	}
	winnerAmount := *requests[winner].SettlementMinor
	var unsettled, records, lastSequence int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT account.unsettled_posted_minor,
			(SELECT count(*) FROM finance_reconciliation_records), cursor.last_sequence
		FROM organization_credit_accounts AS account
		JOIN finance_reconciliation_cursors AS cursor
		  ON cursor.principal_id = $2
		WHERE account.organization_id = $1
	`, testOrganizationID, financeReconciliationPrincipalID).Scan(
		&unsettled, &records, &lastSequence,
	); err != nil {
		t.Fatalf("read concurrent Finance Reconciliation result: %v", err)
	}
	if unsettled != 1250-winnerAmount || records != 1 || lastSequence != 1 {
		t.Fatalf(
			"concurrent Finance result = unsettled %d records %d sequence %d winner amount %d",
			unsettled,
			records,
			lastSequence,
			winnerAmount,
		)
	}
	replayed, err := services[winner].Apply(context.Background(), requests[winner])
	if err != nil || !replayed.Replayed || replayed.UnsettledPostedMinor != unsettled {
		t.Fatalf("replay concurrent Finance winner = %#v error=%v", replayed, err)
	}
}

func TestFinanceReconciliationConcurrentExactRetryReplaysCommittedRecord(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "finance-reconciliation-concurrent-replay")
	first := newFinanceReconciliationService(t, fixture.database)
	secondPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_finance_reconciliation_login",
		"vela-finance-reconciliation-password",
	)
	second, err := financereconciliation.NewService(context.Background(), secondPool)
	if err != nil {
		t.Fatalf("create second Finance Reconciliation replay replica: %v", err)
	}
	amount := int64(100)
	request := financereconciliation.Request{
		IdempotencyKey:    "concurrent-finance-exact-retry",
		SourceSequence:    1,
		OrganizationID:    uuid.MustParse(testOrganizationID),
		Kind:              financereconciliation.KindSettlementPosted,
		Currency:          "CNY",
		SettlementMinor:   &amount,
		ExternalReference: "concurrent-finance-exact-retry-reference",
		EffectiveAt:       time.Date(2026, 8, 24, 14, 31, 0, 0, time.UTC),
	}
	services := []*financereconciliation.Service{first, second}
	start := make(chan struct{})
	results := make(chan struct {
		result financereconciliation.Result
		err    error
	}, len(services))
	var wait sync.WaitGroup
	for _, service := range services {
		wait.Add(1)
		go func(service *financereconciliation.Service) {
			defer wait.Done()
			<-start
			result, err := service.Apply(context.Background(), request)
			results <- struct {
				result financereconciliation.Result
				err    error
			}{result: result, err: err}
		}(service)
	}
	close(start)
	wait.Wait()
	close(results)

	var committed, replayed financereconciliation.Result
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent exact Finance retry error: %v", result.err)
		}
		if result.result.Replayed {
			if replayed.RecordID != uuid.Nil {
				t.Fatal("both concurrent exact Finance retries returned replay")
			}
			replayed = result.result
		} else {
			if committed.RecordID != uuid.Nil {
				t.Fatal("both concurrent exact Finance retries committed a record")
			}
			committed = result.result
		}
	}
	if committed.RecordID == uuid.Nil || replayed.RecordID != committed.RecordID ||
		replayed.AccountVersion != committed.AccountVersion ||
		replayed.UnsettledPostedMinor != committed.UnsettledPostedMinor {
		t.Fatalf("concurrent exact Finance retry results = committed %#v replayed %#v", committed, replayed)
	}
	var unsettled, records, lastSequence int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT account.unsettled_posted_minor,
			(SELECT count(*) FROM finance_reconciliation_records), cursor.last_sequence
		FROM organization_credit_accounts AS account
		JOIN finance_reconciliation_cursors AS cursor ON cursor.principal_id = $2
		WHERE account.organization_id = $1
	`, testOrganizationID, financeReconciliationPrincipalID).Scan(
		&unsettled, &records, &lastSequence,
	); err != nil {
		t.Fatalf("read concurrent exact Finance retry effect: %v", err)
	}
	if unsettled != 1150 || records != 1 || lastSequence != 1 {
		t.Fatalf(
			"concurrent exact Finance retry effect = unsettled %d records %d sequence %d",
			unsettled,
			records,
			lastSequence,
		)
	}
}

func TestFinanceReconciliationRejectsExhaustedCountersWithoutDurableChange(t *testing.T) {
	t.Run("source sequence", func(t *testing.T) {
		fixture := newInvoiceExportChargeFixture(t, "finance-source-sequence-exhaustion")
		service := newFinanceReconciliationService(t, fixture.database)
		if _, err := fixture.database.Admin.Exec(`
			INSERT INTO finance_reconciliation_cursors (principal_id, last_sequence)
			VALUES ($1, $2)
		`, financeReconciliationPrincipalID, int64(math.MaxInt64)); err != nil {
			t.Fatalf("seed exhausted Finance source sequence: %v", err)
		}
		amount := int64(1)
		_, err := service.Apply(context.Background(), financereconciliation.Request{
			IdempotencyKey:    "exhausted-finance-source-sequence",
			SourceSequence:    math.MaxInt64,
			OrganizationID:    uuid.MustParse(testOrganizationID),
			Kind:              financereconciliation.KindSettlementPosted,
			Currency:          "CNY",
			SettlementMinor:   &amount,
			ExternalReference: "exhausted-finance-source-sequence-reference",
			EffectiveAt:       time.Date(2026, 8, 24, 15, 0, 0, 0, time.UTC),
		})
		assertFinanceReconciliationFailure(t, err, financereconciliation.FailureConflict)
		var unsettled, records, lastSequence int64
		if err := fixture.database.Admin.QueryRow(`
			SELECT account.unsettled_posted_minor,
				(SELECT count(*) FROM finance_reconciliation_records),
				cursor.last_sequence
			FROM organization_credit_accounts AS account
			JOIN finance_reconciliation_cursors AS cursor ON cursor.principal_id = $2
			WHERE account.organization_id = $1
		`, testOrganizationID, financeReconciliationPrincipalID).Scan(
			&unsettled,
			&records,
			&lastSequence,
		); err != nil {
			t.Fatalf("read exhausted Finance source sequence effects: %v", err)
		}
		if unsettled != 1250 || records != 0 || lastSequence != math.MaxInt64 {
			t.Fatalf(
				"exhausted Finance source effects = unsettled %d records %d sequence %d",
				unsettled,
				records,
				lastSequence,
			)
		}
	})

	t.Run("terminal source sequence exact replay", func(t *testing.T) {
		fixture := newInvoiceExportChargeFixture(t, "finance-source-sequence-terminal-replay")
		service := newFinanceReconciliationService(t, fixture.database)
		if _, err := fixture.database.Admin.Exec(`
			INSERT INTO finance_reconciliation_cursors (principal_id, last_sequence)
			VALUES ($1, $2)
		`, financeReconciliationPrincipalID, int64(math.MaxInt64-1)); err != nil {
			t.Fatalf("seed terminal Finance source sequence: %v", err)
		}
		amount := int64(1)
		request := financereconciliation.Request{
			IdempotencyKey:    "terminal-finance-source-sequence",
			SourceSequence:    math.MaxInt64,
			OrganizationID:    uuid.MustParse(testOrganizationID),
			Kind:              financereconciliation.KindSettlementPosted,
			Currency:          "CNY",
			SettlementMinor:   &amount,
			ExternalReference: "terminal-finance-source-sequence-reference",
			EffectiveAt:       time.Date(2026, 8, 24, 15, 0, 30, 0, time.UTC),
		}
		committed, err := service.Apply(context.Background(), request)
		if err != nil {
			t.Fatalf("commit terminal Finance source sequence: %v", err)
		}
		replayed, err := service.Apply(context.Background(), request)
		if err != nil || !replayed.Replayed || replayed.RecordID != committed.RecordID {
			t.Fatalf("terminal Finance source sequence replay = %#v error=%v", replayed, err)
		}
	})

	t.Run("credit account version", func(t *testing.T) {
		fixture := newInvoiceExportChargeFixture(t, "finance-account-version-exhaustion")
		service := newFinanceReconciliationService(t, fixture.database)
		if _, err := fixture.database.Admin.Exec(`
			UPDATE organization_credit_accounts SET version = $2 WHERE organization_id = $1
		`, testOrganizationID, int64(math.MaxInt64)); err != nil {
			t.Fatalf("seed exhausted credit account version: %v", err)
		}
		amount := int64(1)
		_, err := service.Apply(context.Background(), financereconciliation.Request{
			IdempotencyKey:    "exhausted-credit-account-version",
			SourceSequence:    1,
			OrganizationID:    uuid.MustParse(testOrganizationID),
			Kind:              financereconciliation.KindSettlementPosted,
			Currency:          "CNY",
			SettlementMinor:   &amount,
			ExternalReference: "exhausted-credit-account-version-reference",
			EffectiveAt:       time.Date(2026, 8, 24, 15, 1, 0, 0, time.UTC),
		})
		assertFinanceReconciliationFailure(t, err, financereconciliation.FailureConflict)
		var unsettled, version, records, cursors int64
		if err := fixture.database.Admin.QueryRow(`
			SELECT account.unsettled_posted_minor, account.version,
				(SELECT count(*) FROM finance_reconciliation_records),
				(SELECT count(*) FROM finance_reconciliation_cursors)
			FROM organization_credit_accounts AS account
			WHERE account.organization_id = $1
		`, testOrganizationID).Scan(&unsettled, &version, &records, &cursors); err != nil {
			t.Fatalf("read exhausted credit-account version effects: %v", err)
		}
		if unsettled != 1250 || version != math.MaxInt64 || records != 0 || cursors != 0 {
			t.Fatalf(
				"exhausted credit-account effects = unsettled %d version %d records %d cursors %d",
				unsettled,
				version,
				records,
				cursors,
			)
		}
	})
}

func financeCommercialHistorySnapshot(t *testing.T, fixture invoiceExportFixture) string {
	t.Helper()
	var snapshot string
	if err := fixture.database.Admin.QueryRow(`
		SELECT jsonb_build_object(
			'job', (SELECT to_jsonb(job) FROM jobs AS job WHERE job.id = $1),
			'charges', COALESCE((
				SELECT jsonb_agg(to_jsonb(charge) ORDER BY charge.id)
				FROM charges AS charge WHERE charge.job_id = $1
			), '[]'::jsonb),
			'artifacts', COALESCE((
				SELECT jsonb_agg(to_jsonb(artifact) ORDER BY artifact.id)
				FROM artifacts AS artifact WHERE artifact.job_id = $1
			), '[]'::jsonb),
			'artifact_sets', COALESCE((
				SELECT jsonb_agg(to_jsonb(artifact_set) ORDER BY artifact_set.id)
				FROM artifact_sets AS artifact_set WHERE artifact_set.job_id = $1
			), '[]'::jsonb),
			'artifact_set_items', COALESCE((
				SELECT jsonb_agg(to_jsonb(item) ORDER BY item.artifact_id)
				FROM artifact_set_items AS item WHERE item.job_id = $1
			), '[]'::jsonb),
			'invoice_exports', COALESCE((
				SELECT jsonb_agg(to_jsonb(export) ORDER BY export.charge_id)
				FROM invoice_exports AS export WHERE export.job_id = $1
			), '[]'::jsonb),
			'invoice_export_receipts', COALESCE((
				SELECT jsonb_agg(to_jsonb(receipt) ORDER BY receipt.id)
				FROM invoice_export_receipts AS receipt WHERE receipt.job_id = $1
			), '[]'::jsonb)
		)::text
	`, fixture.assignment.JobID).Scan(&snapshot); err != nil {
		t.Fatalf("snapshot Job/Charge/Artifact/Invoice history: %v", err)
	}
	return snapshot
}

func assertPostgresConstraint(t *testing.T, err error, constraint string) {
	t.Helper()
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.ConstraintName != constraint {
		t.Fatalf("PostgreSQL error = %v, want constraint %s", err, constraint)
	}
}

func assertFinanceReconciliationFailure(
	t *testing.T,
	err error,
	want financereconciliation.FailureCode,
) {
	t.Helper()
	var failure *financereconciliation.Failure
	if !errors.As(err, &failure) || failure.Code != want {
		t.Fatalf("Finance Reconciliation failure = %v, want code %s", err, want)
	}
}

func newFinanceReconciliationService(
	t *testing.T,
	database testDatabase,
) *financereconciliation.Service {
	t.Helper()
	if _, err := database.Admin.Exec(`
		INSERT INTO finance_reconciliation_principals (
			id, stable_id, tls_uri_identity
		) VALUES ($1, 'primary-finance-reconciliation', $2)
	`, financeReconciliationPrincipalID, financeReconciliationTLSURI); err != nil {
		t.Fatalf("provision Finance Reconciliation Principal: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO finance_reconciliation_database_bindings (
			database_role, principal_id
		) VALUES ('vela_finance_reconciliation_login', $1)
	`, financeReconciliationPrincipalID); err != nil {
		t.Fatalf("bind Finance Reconciliation Principal: %v", err)
	}
	pool := newRolePool(
		t,
		database.DSN,
		"vela_finance_reconciliation_login",
		"vela-finance-reconciliation-password",
	)
	service, err := financereconciliation.NewService(context.Background(), pool)
	if err != nil {
		t.Fatalf("create Finance Reconciliation service: %v", err)
	}
	return service
}
