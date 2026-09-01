//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/artifactaccess"
	"github.com/vivym/vela/internal/billingexport"
	"github.com/vivym/vela/internal/cancellation"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/httpapi"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/organizationreporting"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/webhook"
)

func newOrganizationReportingService(
	t *testing.T,
	databaseDSN string,
) (*organizationreporting.Service, error) {
	t.Helper()
	return organizationreporting.NewService(
		newRolePool(
			t,
			databaseDSN,
			"vela_organization_billing_request_login",
			"vela-organization-billing-request-password",
		),
		newRolePool(
			t,
			databaseDSN,
			"vela_organization_audit_request_login",
			"vela-organization-audit-request-password",
		),
		newRolePool(
			t,
			databaseDSN,
			"vela_debug_dump_audit_request_login",
			"vela-debug-dump-audit-request-password",
		),
	)
}

func TestBillingAdminReadsExactOrganizationCreditSummary(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	organizationID := uuid.MustParse(testOrganizationID)
	billingAdminID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		billingAdminID,
		"organization-reporting-billing-admin",
		[]string{"BillingAdmin"},
		nil,
	)

	authenticator := newHumanMembershipAuthenticator(
		t,
		database,
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t,
			database.DSN,
			"vela_human_auth_login",
			"vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "organization-reporting-billing-admin",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	actor, err := authenticator.Authenticate(
		context.Background(),
		"organization-reporting-billing-admin-token",
	)
	if err != nil {
		t.Fatalf("authenticate BillingAdmin: %v", err)
	}
	actor, ok := actor.ForOrganization(organizationID)
	if !ok {
		t.Fatal("BillingAdmin lacks Organization reporting authorization")
	}
	for _, scope := range []string{
		identity.ScopeOrganizationBillingRead,
		identity.ScopeOrganizationBillingContactsManage,
		identity.ScopeOrganizationBillingContactsRead,
		identity.ScopeOrganizationUsageRead,
	} {
		if !actor.HasScope(scope) {
			t.Fatalf("BillingAdmin scopes %v lack %s", actor.Scopes, scope)
		}
	}
	if actor.HasScope(identity.ScopeOrganizationAuditRead) ||
		actor.HasScope(identity.ScopeArtifactsRead) {
		t.Fatalf("BillingAdmin received audit/content scope: %v", actor.Scopes)
	}

	service, err := newOrganizationReportingService(t, database.DSN)
	if err != nil {
		t.Fatalf("create Organization reporting service: %v", err)
	}

	before := readOrganizationReportingAuthoritySnapshot(t, database.Admin, organizationID)
	summary, err := service.GetCreditSummary(
		context.Background(), actor, organizationID,
	)
	if err != nil {
		t.Fatalf("get Organization credit summary: %v", err)
	}
	if summary.OrganizationID != organizationID || summary.Currency != "CNY" ||
		summary.ContractCreditLimitMinor != 100000 || summary.ReservedMinor != 0 ||
		summary.UnsettledPostedMinor != 0 || summary.AvailableMinor != 100000 ||
		summary.Version != 1 || summary.UpdatedAt.IsZero() {
		t.Fatalf("Organization credit summary = %#v", summary)
	}
	if after := readOrganizationReportingAuthoritySnapshot(t, database.Admin, organizationID); after != before {
		t.Fatal("Organization credit read mutated authoritative reporting state")
	}
}

func TestBillingAdminListsChargeWithAvailableInvoiceReference(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "organization-reporting-charge")
	adapter := &recordingInvoiceAdapter{receipt: billingexport.Receipt{
		InvoiceReference: "invoice-2026-08-organization-reporting",
		LineReference:    "line-organization-reporting-charge",
	}}
	exporter, err := billingexport.NewService(
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_billing_login",
			"vela-billing-password",
		),
		adapter,
		billingexport.Config{
			ExporterID: "organization-reporting-exporter",
			BatchSize:  1,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create Invoice exporter: %v", err)
	}
	result, err := exporter.ExportBatch(context.Background())
	if err != nil || result.Claimed != 1 || result.Exported != 1 {
		t.Fatalf("export Charge receipt = %#v error=%v", result, err)
	}

	organizationID := uuid.MustParse(testOrganizationID)
	billingAdminID := uuid.New()
	seedHumanRoleFixture(
		t,
		fixture.database.Admin,
		billingAdminID,
		"organization-reporting-charge-admin",
		[]string{"BillingAdmin"},
		nil,
	)
	authenticator := newHumanMembershipAuthenticator(
		t,
		fixture.database,
		newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_human_auth_login",
			"vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "organization-reporting-charge-admin",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	actor, err := authenticator.Authenticate(
		context.Background(),
		"organization-reporting-charge-admin-token",
	)
	if err != nil {
		t.Fatalf("authenticate BillingAdmin: %v", err)
	}
	actor, ok := actor.ForOrganization(organizationID)
	if !ok {
		t.Fatal("BillingAdmin lacks Organization reporting authorization")
	}
	service, err := newOrganizationReportingService(t, fixture.database.DSN)
	if err != nil {
		t.Fatalf("create Organization reporting service: %v", err)
	}

	before := readOrganizationReportingAuthoritySnapshot(t, fixture.database.Admin, organizationID)
	charges, err := service.ListCharges(
		context.Background(), actor, organizationID, 100,
	)
	if err != nil {
		t.Fatalf("list Organization Charges: %v", err)
	}
	if len(charges) != 1 {
		t.Fatalf("Organization Charges = %#v, want one", charges)
	}
	charge := charges[0]
	if charge.ChargeID != fixture.chargeID ||
		charge.ProjectID != uuid.MustParse(testProjectID) ||
		charge.JobID != fixture.assignment.JobID ||
		charge.Reason != "VISIBLE_COMPLETION" ||
		charge.AmountMinor != 1250 || charge.Currency != "CNY" ||
		charge.PostedAt.IsZero() ||
		charge.InvoiceReference == nil ||
		*charge.InvoiceReference != adapter.receipt.InvoiceReference ||
		charge.LineReference == nil ||
		*charge.LineReference != adapter.receipt.LineReference ||
		charge.ExportedAt == nil || charge.ExportedAt.IsZero() {
		t.Fatalf("Organization Charge projection = %#v", charge)
	}
	if after := readOrganizationReportingAuthoritySnapshot(
		t, fixture.database.Admin, organizationID,
	); after != before {
		t.Fatal("Organization Charge read mutated billing or execution authority")
	}
}

func TestBillingAdminManagesSettlementContactLifecycle(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	organizationID := uuid.MustParse(testOrganizationID)
	billingAdminID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		billingAdminID,
		"organization-reporting-contact-admin",
		[]string{"BillingAdmin"},
		nil,
	)
	authenticator := newHumanMembershipAuthenticator(
		t,
		database,
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t,
			database.DSN,
			"vela_human_auth_login",
			"vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "organization-reporting-contact-admin",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	actor, err := authenticator.Authenticate(
		context.Background(),
		"organization-reporting-contact-admin-token",
	)
	if err != nil {
		t.Fatalf("authenticate BillingAdmin: %v", err)
	}
	actor, ok := actor.ForOrganization(organizationID)
	if !ok {
		t.Fatal("BillingAdmin lacks Organization reporting authorization")
	}
	service, err := newOrganizationReportingService(t, database.DSN)
	if err != nil {
		t.Fatalf("create Organization reporting service: %v", err)
	}

	created, err := service.CreateSettlementContact(
		context.Background(),
		actor,
		organizationID,
		organizationreporting.CreateSettlementContactRequest{
			DisplayName: "  Accounts Payable  ",
			Email:       "BILLING@EXAMPLE.COM",
		},
	)
	if err != nil {
		t.Fatalf("create settlement contact: %v", err)
	}
	if created.ID == uuid.Nil || created.OrganizationID != organizationID ||
		created.DisplayName != "Accounts Payable" || created.Email != "billing@example.com" ||
		created.CreatedByPrincipalID != billingAdminID || created.CreatedAt.IsZero() ||
		created.DisabledAt != nil || created.DisabledByPrincipalID != nil {
		t.Fatalf("created settlement contact = %#v", created)
	}

	replayed, err := service.CreateSettlementContact(
		context.Background(),
		actor,
		organizationID,
		organizationreporting.CreateSettlementContactRequest{
			DisplayName: "Replacement name must not win",
			Email:       "billing@example.com",
		},
	)
	if err != nil || replayed.ID != created.ID ||
		replayed.DisplayName != created.DisplayName ||
		!replayed.CreatedAt.Equal(created.CreatedAt) {
		t.Fatalf("settlement contact create replay = %#v error=%v, want %#v", replayed, err, created)
	}

	beforeList := readOrganizationReportingAuthoritySnapshot(t, database.Admin, organizationID)
	contacts, err := service.ListSettlementContacts(
		context.Background(), actor, organizationID, 100,
	)
	if err != nil || len(contacts) != 1 || contacts[0].ID != created.ID {
		t.Fatalf("settlement contact list = %#v error=%v", contacts, err)
	}
	if afterList := readOrganizationReportingAuthoritySnapshot(
		t, database.Admin, organizationID,
	); afterList != beforeList {
		t.Fatal("settlement contact list mutated contact identity or evidence")
	}

	disabled, err := service.DisableSettlementContact(
		context.Background(), actor, organizationID, created.ID,
	)
	if err != nil {
		t.Fatalf("disable settlement contact: %v", err)
	}
	if disabled.DisabledAt == nil || disabled.DisabledAt.IsZero() ||
		disabled.DisabledByPrincipalID == nil ||
		*disabled.DisabledByPrincipalID != billingAdminID {
		t.Fatalf("disabled settlement contact = %#v", disabled)
	}
	replayedDisable, err := service.DisableSettlementContact(
		context.Background(), actor, organizationID, created.ID,
	)
	if err != nil || replayedDisable.DisabledAt == nil ||
		!replayedDisable.DisabledAt.Equal(*disabled.DisabledAt) {
		t.Fatalf("settlement contact disable replay = %#v error=%v", replayedDisable, err)
	}
}

func TestSettlementContactValidationAndConcurrentReplayAreDeterministic(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	organizationID := uuid.MustParse(testOrganizationID)
	actorID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		actorID,
		"organization-reporting-concurrent-contact",
		[]string{"BillingAdmin"},
		nil,
	)
	authenticator := newHumanMembershipAuthenticator(
		t,
		database,
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t,
			database.DSN,
			"vela_human_auth_login",
			"vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "organization-reporting-concurrent-contact",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	actor, err := authenticator.Authenticate(
		context.Background(), "organization-reporting-concurrent-contact-token",
	)
	if err != nil {
		t.Fatalf("authenticate concurrent contact BillingAdmin: %v", err)
	}
	actor, ok := actor.ForOrganization(organizationID)
	if !ok {
		t.Fatal("concurrent contact BillingAdmin lacks Organization authorization")
	}
	service, err := newOrganizationReportingService(t, database.DSN)
	if err != nil {
		t.Fatalf("create concurrent contact reporting service: %v", err)
	}

	invalidContacts := []organizationreporting.CreateSettlementContactRequest{
		{},
		{DisplayName: strings.Repeat("界", 201), Email: "valid@example.com"},
		{DisplayName: "Valid", Email: " leading@example.com"},
		{DisplayName: "Valid", Email: "embedded space@example.com"},
		{DisplayName: "Valid", Email: "missing-at.example.com"},
		{DisplayName: "Valid", Email: "two@@example.com"},
		{DisplayName: "Valid", Email: "@example.com"},
		{DisplayName: "Valid", Email: "local@"},
		{DisplayName: "Valid", Email: strings.Repeat("a", 310) + "@example.com"},
	}
	for index, request := range invalidContacts {
		if _, err := service.CreateSettlementContact(
			context.Background(), actor, organizationID, request,
		); !organizationReportingFailureHasCode(err, organizationreporting.FailureInvalid) {
			t.Fatalf("invalid settlement contact %d error = %v, want invalid_request", index, err)
		}
	}
	for label, call := range map[string]func() error{
		"Charge limit zero": func() error {
			_, err := service.ListCharges(context.Background(), actor, organizationID, 0)
			return err
		},
		"Contact limit high": func() error {
			_, err := service.ListSettlementContacts(context.Background(), actor, organizationID, 101)
			return err
		},
		"Usage non-UTC": func() error {
			from := time.Now().In(time.FixedZone("UTC+1", 3600))
			_, err := service.GetUsage(context.Background(), actor, organizationID, from, from.Add(time.Hour))
			return err
		},
		"Usage too wide": func() error {
			from := time.Now().UTC()
			_, err := service.GetUsage(context.Background(), actor, organizationID, from, from.Add(367*24*time.Hour))
			return err
		},
		"Usage empty interval": func() error {
			from := time.Now().UTC()
			_, err := service.GetUsage(context.Background(), actor, organizationID, from, from)
			return err
		},
		"Usage reversed interval": func() error {
			from := time.Now().UTC()
			_, err := service.GetUsage(context.Background(), actor, organizationID, from, from.Add(-time.Hour))
			return err
		},
	} {
		if err := call(); !organizationReportingFailureHasCode(err, organizationreporting.FailureInvalid) {
			t.Fatalf("%s error = %v, want invalid_request", label, err)
		}
	}

	const concurrentCreates = 16
	results := make(chan organizationreporting.SettlementContact, concurrentCreates)
	errors := make(chan error, concurrentCreates)
	var waitGroup sync.WaitGroup
	for index := 0; index < concurrentCreates; index++ {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			contact, err := service.CreateSettlementContact(
				context.Background(),
				actor,
				organizationID,
				organizationreporting.CreateSettlementContactRequest{
					DisplayName: "Concurrent Contact " + string(rune('A'+index)),
					Email:       "Concurrent@Example.com",
				},
			)
			if err != nil {
				errors <- err
				return
			}
			results <- contact
		}(index)
	}
	waitGroup.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Errorf("concurrent settlement contact create: %v", err)
	}
	var committed organizationreporting.SettlementContact
	for contact := range results {
		if committed.ID == uuid.Nil {
			committed = contact
			continue
		}
		if contact.ID != committed.ID || contact.DisplayName != committed.DisplayName ||
			contact.Email != "concurrent@example.com" ||
			!contact.CreatedAt.Equal(committed.CreatedAt) {
			t.Fatalf("concurrent replay = %#v, want committed %#v", contact, committed)
		}
	}
	if committed.ID == uuid.Nil {
		t.Fatal("concurrent settlement contact create returned no committed contact")
	}

	var contactCount, createEventCount int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM organization_settlement_contacts
			 WHERE organization_id = $1 AND normalized_email = 'concurrent@example.com'),
			(SELECT count(*) FROM organization_settlement_contact_events
			 WHERE organization_id = $1 AND contact_id = $2
			   AND action = 'SETTLEMENT_CONTACT_CREATED')
	`, organizationID, committed.ID).Scan(&contactCount, &createEventCount); err != nil {
		t.Fatalf("read concurrent settlement contact evidence: %v", err)
	}
	if contactCount != 1 || createEventCount != 1 {
		t.Fatalf("concurrent settlement contact rows/events = %d/%d, want 1/1", contactCount, createEventCount)
	}

	if _, err := service.DisableSettlementContact(
		context.Background(), actor, organizationID, committed.ID,
	); err != nil {
		t.Fatalf("disable concurrent settlement contact: %v", err)
	}
	if _, err := service.DisableSettlementContact(
		context.Background(), actor, organizationID, committed.ID,
	); err != nil {
		t.Fatalf("replay concurrent settlement contact disablement: %v", err)
	}
	var disableEventCount int
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM organization_settlement_contact_events
		WHERE organization_id = $1 AND contact_id = $2
		  AND action = 'SETTLEMENT_CONTACT_DISABLED'
	`, organizationID, committed.ID).Scan(&disableEventCount); err != nil {
		t.Fatalf("count settlement contact disablement events: %v", err)
	}
	if disableEventCount != 1 {
		t.Fatalf("settlement contact disablement events = %d, want 1", disableEventCount)
	}
}

func TestOrganizationReportingRequiresCurrentExactHumanAuthorization(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	organizationID := uuid.MustParse(testOrganizationID)
	projectID := uuid.MustParse(testProjectID)
	type actorFixture struct {
		id      uuid.UUID
		subject string
		roles   []string
		project map[string][]string
	}
	fixtures := []actorFixture{
		{id: uuid.New(), subject: "reporting-union", roles: []string{"BillingAdmin", "OrganizationAuditor"}},
		{id: uuid.New(), subject: "reporting-billing-a", roles: []string{"BillingAdmin"}},
		{id: uuid.New(), subject: "reporting-billing-b", roles: []string{"BillingAdmin"}},
		{id: uuid.New(), subject: "reporting-auditor", roles: []string{"OrganizationAuditor"}},
		{id: uuid.New(), subject: "reporting-project-admin", project: map[string][]string{testProjectID: {"ProjectAdmin"}}},
		{id: uuid.New(), subject: "reporting-disabled", roles: []string{"BillingAdmin"}},
		{id: uuid.New(), subject: "reporting-expired", roles: []string{"BillingAdmin"}},
	}
	for _, fixture := range fixtures {
		seedHumanRoleFixture(
			t,
			database.Admin,
			fixture.id,
			fixture.subject,
			fixture.roles,
			fixture.project,
		)
	}
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	humanAuthPool := newRolePool(
		t, database.DSN, "vela_human_auth_login", "vela-human-auth-password",
	)
	authenticate := func(subject string, expiresAt time.Time) identity.Principal {
		t.Helper()
		actor, err := newHumanMembershipAuthenticator(
			t,
			database,
			authPool,
			humanAuthPool,
			testCredentialPepper,
			staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
				Issuer: "https://identity.example.com", Subject: subject, ExpiresAt: expiresAt,
			}},
		).Authenticate(context.Background(), subject+"-token")
		if err != nil {
			t.Fatalf("authenticate %s: %v", subject, err)
		}
		return actor
	}
	contextualOrganization := func(subject string) identity.Principal {
		t.Helper()
		actor, ok := authenticate(subject, time.Now().UTC().Add(time.Hour)).ForOrganization(organizationID)
		if !ok {
			t.Fatalf("%s lacks Organization authorization", subject)
		}
		return actor
	}
	service, err := newOrganizationReportingService(t, database.DSN)
	if err != nil {
		t.Fatalf("create exact-authorization reporting service: %v", err)
	}

	union := contextualOrganization("reporting-union")
	expectedUnionScopes := []string{
		identity.ScopeOrganizationAuditRead,
		identity.ScopeOrganizationBillingRead,
		identity.ScopeOrganizationBillingContactsManage,
		identity.ScopeOrganizationBillingContactsRead,
		identity.ScopeOrganizationUsageRead,
	}
	assertExactHumanScopes(t, union, expectedUnionScopes)
	if union.HasScope(identity.ScopeArtifactsRead) || union.HasScope(identity.ScopeJobsRead) ||
		union.HasScope(identity.ScopeOrganizationMembersManage) {
		t.Fatalf("BillingAdmin/OrganizationAuditor union received unrelated scopes: %v", union.Scopes)
	}
	if _, err := service.GetCreditSummary(context.Background(), union, organizationID); err != nil {
		t.Fatalf("role union read credit summary: %v", err)
	}
	if _, err := service.ListAuditEvents(context.Background(), union, organizationID, 100); err != nil {
		t.Fatalf("role union read audit events: %v", err)
	}
	now := time.Now().UTC()
	if _, err := service.GetUsage(context.Background(), union, organizationID, now.Add(-time.Hour), now); err != nil {
		t.Fatalf("role union read usage: %v", err)
	}

	billingA := contextualOrganization("reporting-billing-a")
	billingB := contextualOrganization("reporting-billing-b")
	auditor := contextualOrganization("reporting-auditor")
	assertExactHumanScopes(t, billingA, []string{
		identity.ScopeOrganizationBillingRead,
		identity.ScopeOrganizationBillingContactsManage,
		identity.ScopeOrganizationBillingContactsRead,
		identity.ScopeOrganizationUsageRead,
	})
	assertExactHumanScopes(t, auditor, []string{
		identity.ScopeOrganizationAuditRead,
		identity.ScopeOrganizationUsageRead,
	})
	if _, err := service.GetUsage(
		context.Background(), billingA, organizationID, now.Add(-time.Hour), now,
	); err != nil {
		t.Fatalf("BillingAdmin read Organization usage: %v", err)
	}
	if _, err := service.ListAuditEvents(
		context.Background(), billingA, organizationID, 100,
	); !organizationReportingFailureHasCode(err, organizationreporting.FailureForbidden) {
		t.Fatalf("BillingAdmin audit error = %v, want forbidden", err)
	}
	if _, err := service.GetCreditSummary(
		context.Background(), auditor, organizationID,
	); !organizationReportingFailureHasCode(err, organizationreporting.FailureForbidden) {
		t.Fatalf("OrganizationAuditor billing error = %v, want forbidden", err)
	}
	projectActor, ok := authenticate(
		"reporting-project-admin", time.Now().UTC().Add(time.Hour),
	).ForProject(projectID)
	if !ok {
		t.Fatal("ProjectAdmin lacks Project authorization fixture")
	}
	if _, err := service.GetUsage(
		context.Background(), projectActor, organizationID, now.Add(-time.Hour), now,
	); !organizationReportingFailureHasCode(err, organizationreporting.FailureForbidden) {
		t.Fatalf("ProjectAdmin Organization usage error = %v, want forbidden", err)
	}
	serviceActor := identity.Principal{
		Kind:           identity.PrincipalKindService,
		CredentialID:   uuid.New(),
		OrganizationID: organizationID,
		PrincipalID:    uuid.New(),
		Scopes:         []string{identity.ScopeOrganizationUsageRead},
	}
	if _, err := service.GetUsage(
		context.Background(), serviceActor, organizationID, now.Add(-time.Hour), now,
	); !organizationReportingFailureHasCode(err, organizationreporting.FailureForbidden) {
		t.Fatalf("Service Principal Organization usage error = %v, want forbidden", err)
	}
	if _, err := service.CreateSettlementContact(
		context.Background(),
		projectActor,
		organizationID,
		organizationreporting.CreateSettlementContactRequest{
			DisplayName: "Forbidden Project Contact", Email: "forbidden-project@example.com",
		},
	); !organizationReportingFailureHasCode(err, organizationreporting.FailureForbidden) {
		t.Fatalf("ProjectAdmin settlement contact error = %v, want forbidden", err)
	}
	var forbiddenContactCount int
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM organization_settlement_contacts
		WHERE normalized_email = 'forbidden-project@example.com'
	`).Scan(&forbiddenContactCount); err != nil {
		t.Fatalf("count forbidden settlement contacts: %v", err)
	}
	if forbiddenContactCount != 0 {
		t.Fatalf("forbidden actor created %d settlement contacts", forbiddenContactCount)
	}

	substituted := billingA
	substituted.CredentialID = billingB.CredentialID
	if _, err := service.GetCreditSummary(
		context.Background(), substituted, organizationID,
	); !organizationReportingFailureHasCode(err, organizationreporting.FailureUnauthorized) {
		t.Fatalf("substituted Organization proof error = %v, want unauthorized", err)
	}
	if _, err := database.Admin.Exec(`
		DELETE FROM organization_role_bindings
		WHERE organization_id = $1 AND principal_id = $2 AND role = 'BillingAdmin'
	`, organizationID, fixtures[1].id); err != nil {
		t.Fatalf("remove BillingAdmin role after authentication: %v", err)
	}
	if _, err := service.GetCreditSummary(
		context.Background(), billingA, organizationID,
	); !organizationReportingFailureHasCode(err, organizationreporting.FailureUnauthorized) {
		t.Fatalf("stale BillingAdmin role error = %v, want unauthorized", err)
	}

	disabled := contextualOrganization("reporting-disabled")
	if _, err := database.Admin.Exec(`
		UPDATE human_oidc_bindings
		SET disabled_at = clock_timestamp()
		WHERE organization_id = $1 AND principal_id = $2
	`, organizationID, fixtures[5].id); err != nil {
		t.Fatalf("disable reporting Human after authentication: %v", err)
	}
	if _, err := service.GetCreditSummary(
		context.Background(), disabled, organizationID,
	); !organizationReportingFailureHasCode(err, organizationreporting.FailureUnauthorized) {
		t.Fatalf("disabled Human reporting error = %v, want unauthorized", err)
	}

	expiredBase := authenticate(
		"reporting-expired", time.Now().UTC().Add(150*time.Millisecond),
	)
	expired, ok := expiredBase.ForOrganization(organizationID)
	if !ok {
		t.Fatal("expiring BillingAdmin lacks Organization authorization")
	}
	time.Sleep(200 * time.Millisecond)
	if _, err := service.GetCreditSummary(
		context.Background(), expired, organizationID,
	); !organizationReportingFailureHasCode(err, organizationreporting.FailureUnauthorized) {
		t.Fatalf("expired Human reporting error = %v, want unauthorized", err)
	}

	otherOrganizationID := uuid.New()
	otherPrincipalID := uuid.New()
	otherContactID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO customer_organizations (id, display_name)
		VALUES ($1, 'Other reporting Organization')
	`, otherOrganizationID); err != nil {
		t.Fatalf("seed cross-Organization Customer Organization: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO principals (id, organization_id, kind, display_name)
		VALUES ($1, $2, 'HUMAN', 'Other reporting Human')
	`, otherPrincipalID, otherOrganizationID); err != nil {
		t.Fatalf("seed cross-Organization Human Principal: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO organization_settlement_contacts (
			id, organization_id, display_name, normalized_email, created_by_principal_id
		) VALUES ($1, $2, 'Other settlement contact', 'other@example.com', $3)
	`, otherContactID, otherOrganizationID, otherPrincipalID); err != nil {
		t.Fatalf("seed cross-Organization settlement contact: %v", err)
	}
	if _, err := service.DisableSettlementContact(
		context.Background(), union, organizationID, otherContactID,
	); !organizationReportingFailureHasCode(err, organizationreporting.FailureNotFound) {
		t.Fatalf("cross-Organization settlement contact error = %v, want not_found", err)
	}
	if _, err := service.DisableSettlementContact(
		context.Background(), union, organizationID, uuid.New(),
	); !organizationReportingFailureHasCode(err, organizationreporting.FailureNotFound) {
		t.Fatalf("unknown settlement contact error = %v, want not_found", err)
	}
}

func TestSettlementContactIdentityAndEvidenceAreImmutable(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	organizationID := uuid.MustParse(testOrganizationID)
	actorID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		actorID,
		"organization-reporting-immutable-contact",
		[]string{"BillingAdmin"},
		nil,
	)
	authenticator := newHumanMembershipAuthenticator(
		t,
		database,
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t,
			database.DSN,
			"vela_human_auth_login",
			"vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "organization-reporting-immutable-contact",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	actor, err := authenticator.Authenticate(
		context.Background(), "organization-reporting-immutable-contact-token",
	)
	if err != nil {
		t.Fatalf("authenticate immutable contact BillingAdmin: %v", err)
	}
	actor, ok := actor.ForOrganization(organizationID)
	if !ok {
		t.Fatal("immutable contact BillingAdmin lacks Organization authorization")
	}
	service, err := newOrganizationReportingService(t, database.DSN)
	if err != nil {
		t.Fatalf("create immutable contact reporting service: %v", err)
	}
	created, err := service.CreateSettlementContact(
		context.Background(),
		actor,
		organizationID,
		organizationreporting.CreateSettlementContactRequest{
			DisplayName: "Immutable Settlement", Email: "immutable@example.com",
		},
	)
	if err != nil {
		t.Fatalf("create immutable settlement contact: %v", err)
	}
	internalPool := newRolePool(
		t, database.DSN, "vela_internal_login", "vela-internal-password",
	)
	for _, attempt := range []struct {
		name      string
		statement string
		args      []any
	}{
		{
			name: "create", statement: `
				INSERT INTO organization_settlement_contacts (
					id, organization_id, display_name, normalized_email,
					created_by_principal_id
				) VALUES ($1, $2, 'Forged Contact', 'forged@example.com', $3)
			`, args: []any{uuid.New(), organizationID, actorID},
		},
		{
			name: "first disable", statement: `
				UPDATE organization_settlement_contacts
				SET disabled_at = clock_timestamp(), disabled_by_principal_id = $2
				WHERE id = $1
			`, args: []any{created.ID, actorID},
		},
		{
			name: "event insertion", statement: `
				INSERT INTO organization_settlement_contact_events (
					id, organization_id, actor_principal_id, actor_session_id,
					action, contact_id
				) VALUES ($1, $2, $3, $4, 'SETTLEMENT_CONTACT_CREATED', $5)
			`, args: []any{
				uuid.New(), organizationID, actorID, actor.CredentialID, created.ID,
			},
		},
	} {
		t.Run("internal role cannot "+attempt.name, func(t *testing.T) {
			if _, err := internalPool.Exec(
				context.Background(), attempt.statement, attempt.args...,
			); !isPermissionDenied(err) {
				t.Fatalf(
					"internal settlement-contact %s error = %v, want permission denied",
					attempt.name,
					err,
				)
			}
		})
	}
	disabled, err := service.DisableSettlementContact(
		context.Background(), actor, organizationID, created.ID,
	)
	if err != nil {
		t.Fatalf("disable immutable settlement contact: %v", err)
	}

	type eventEvidence struct {
		id        uuid.UUID
		action    string
		principal uuid.UUID
		session   uuid.UUID
		createdAt time.Time
	}
	rows, err := database.Admin.Query(`
		SELECT id, action::text, actor_principal_id, actor_session_id, created_at
		FROM organization_settlement_contact_events
		WHERE organization_id = $1 AND contact_id = $2
		ORDER BY created_at, id
	`, organizationID, created.ID)
	if err != nil {
		t.Fatalf("read immutable settlement contact events: %v", err)
	}
	defer rows.Close()
	events := make([]eventEvidence, 0, 2)
	for rows.Next() {
		var event eventEvidence
		if err := rows.Scan(
			&event.id, &event.action, &event.principal, &event.session, &event.createdAt,
		); err != nil {
			t.Fatalf("scan immutable settlement contact event: %v", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate immutable settlement contact events: %v", err)
	}
	if len(events) != 2 ||
		events[0].action != "SETTLEMENT_CONTACT_CREATED" ||
		events[1].action != "SETTLEMENT_CONTACT_DISABLED" {
		t.Fatalf("settlement contact event sequence = %#v", events)
	}
	for _, event := range events {
		if event.id == uuid.Nil || event.principal != actorID ||
			event.session != actor.CredentialID || event.createdAt.IsZero() {
			t.Fatalf("settlement contact event attribution = %#v", event)
		}
	}
	if disabled.DisabledAt == nil || disabled.DisabledByPrincipalID == nil ||
		*disabled.DisabledByPrincipalID != actorID {
		t.Fatalf("disabled immutable settlement contact = %#v", disabled)
	}

	for _, attempt := range []struct {
		name       string
		statement  string
		args       []any
		constraint string
	}{
		{
			name: "display name", statement: `
				UPDATE organization_settlement_contacts SET display_name = 'Rewritten'
				WHERE id = $1
			`, args: []any{created.ID}, constraint: "settlement_contact_identity_is_immutable",
		},
		{
			name: "email", statement: `
				UPDATE organization_settlement_contacts SET normalized_email = 'rewritten@example.com'
				WHERE id = $1
			`, args: []any{created.ID}, constraint: "settlement_contact_identity_is_immutable",
		},
		{
			name: "permanent disablement", statement: `
				UPDATE organization_settlement_contacts
				SET disabled_at = NULL, disabled_by_principal_id = NULL WHERE id = $1
			`, args: []any{created.ID}, constraint: "settlement_contact_disablement_is_permanent",
		},
		{
			name: "contact deletion", statement: `
				DELETE FROM organization_settlement_contacts WHERE id = $1
			`, args: []any{created.ID}, constraint: "settlement_contact_evidence_is_immutable",
		},
		{
			name: "event update", statement: `
				UPDATE organization_settlement_contact_events
				SET created_at = created_at + interval '1 second' WHERE id = $1
			`, args: []any{events[0].id}, constraint: "settlement_contact_evidence_is_immutable",
		},
		{
			name: "event deletion", statement: `
				DELETE FROM organization_settlement_contact_events WHERE id = $1
			`, args: []any{events[0].id}, constraint: "settlement_contact_evidence_is_immutable",
		},
	} {
		t.Run(attempt.name, func(t *testing.T) {
			tx, err := database.Admin.Begin()
			if err != nil {
				t.Fatalf("begin settlement contact rewrite: %v", err)
			}
			defer func() { _ = tx.Rollback() }()
			_, err = tx.Exec(attempt.statement, attempt.args...)
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
				postgresError.ConstraintName != attempt.constraint {
				t.Fatalf(
					"rewrite %s error = %v, want SQLSTATE 55000 constraint %s",
					attempt.name,
					err,
					attempt.constraint,
				)
			}
		})
	}
}

func TestOrganizationReportingMigrationAllowsEmptyDownUp(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 15)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 14); err != nil {
		t.Fatalf("contract empty Organization reporting migration: %v", err)
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 14 {
		t.Fatalf("Organization reporting migration version after Down = %d error=%v", version, err)
	}
	var contracted bool
	if err := database.Admin.QueryRow(`
		SELECT
			to_regclass('public.organization_settlement_contacts') IS NULL
			AND to_regclass('public.organization_settlement_contact_events') IS NULL
			AND to_regprocedure('vela_get_organization_credit_summary(uuid)') IS NULL
			AND to_regprocedure('vela_list_organization_charges(uuid,integer)') IS NULL
			AND to_regprocedure(
				'vela_get_organization_usage(uuid,timestamp with time zone,timestamp with time zone)'
			) IS NULL
			AND to_regprocedure('vela_list_organization_audit_events(uuid,integer)') IS NULL
			AND to_regprocedure(
				'vela_private.require_organization_billing_context(uuid)'
			) IS NULL
			AND vela_organization_role_scopes('BillingAdmin'::organization_role)
				= ARRAY[]::text[]
			AND vela_organization_role_scopes('OrganizationAuditor'::organization_role)
				= ARRAY[]::text[]
			AND NOT ('organization_billing:read' = ANY(
				vela_organization_role_scopes('OrganizationOwner'::organization_role)
			))
	`).Scan(&contracted); err != nil {
		t.Fatalf("inspect contracted Organization reporting surface: %v", err)
	}
	if !contracted {
		t.Fatal("Organization reporting migration Down left expanded schema or scope surface")
	}
	for _, role := range []string{
		"vela_organization_billing_request",
		"vela_organization_audit_request",
	} {
		var hasExplicitSchemaUsage bool
		if err := database.Admin.QueryRow(`
			SELECT EXISTS (
				SELECT 1
				FROM pg_catalog.pg_namespace AS namespace
				CROSS JOIN LATERAL pg_catalog.aclexplode(
					coalesce(
						namespace.nspacl,
						pg_catalog.acldefault('n', namespace.nspowner)
					)
				) AS privilege
				JOIN pg_catalog.pg_roles AS grantee ON grantee.oid = privilege.grantee
				WHERE namespace.nspname = 'public'
				  AND grantee.rolname = $1
				  AND privilege.privilege_type = 'USAGE'
			)
		`, role).Scan(&hasExplicitSchemaUsage); err != nil {
			t.Fatalf("inspect contracted %s schema privilege: %v", role, err)
		}
		if hasExplicitSchemaUsage {
			t.Fatalf("contracted role %s retains public schema usage", role)
		}
	}
	if err := veladb.VerifyRole(
		context.Background(),
		newRolePool(
			t,
			database.DSN,
			"vela_human_membership_request_login",
			"vela-human-membership-request-password",
		),
		veladb.RoleHumanMembershipRequest,
	); err != nil {
		t.Fatalf("verify preserved Human membership role after Organization reporting Down: %v", err)
	}

	if err := goose.UpTo(database.Admin, migrations, 15); err != nil {
		t.Fatalf("re-expand Organization reporting migration: %v", err)
	}
	version, err = goose.GetDBVersion(database.Admin)
	if err != nil || version != 15 {
		t.Fatalf("Organization reporting migration version after Down/Up = %d error=%v", version, err)
	}
	for _, runtime := range []struct {
		name     string
		login    string
		password string
		role     veladb.Role
	}{
		{
			name: "billing", login: "vela_organization_billing_request_login",
			password: "vela-organization-billing-request-password",
			role:     veladb.RoleOrganizationBillingRequest,
		},
		{
			name: "audit", login: "vela_organization_audit_request_login",
			password: "vela-organization-audit-request-password",
			role:     veladb.RoleOrganizationAuditRequest,
		},
	} {
		if err := veladb.VerifyRole(
			context.Background(),
			newRolePool(t, database.DSN, runtime.login, runtime.password),
			runtime.role,
		); err != nil {
			t.Fatalf("verify re-expanded Organization %s request role: %v", runtime.name, err)
		}
	}
}

func TestOrganizationReportingMigrationDownRefusesDurableEvidence(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 15)
	seedAdmissionFixture(t, database.Admin)
	organizationID := uuid.MustParse(testOrganizationID)
	actorID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		actorID,
		"organization-reporting-migration-evidence",
		[]string{"BillingAdmin"},
		nil,
	)
	authenticator := newHumanMembershipAuthenticator(
		t,
		database,
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t,
			database.DSN,
			"vela_human_auth_login",
			"vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "organization-reporting-migration-evidence",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	actor, err := authenticator.Authenticate(
		context.Background(), "organization-reporting-migration-evidence-token",
	)
	if err != nil {
		t.Fatalf("authenticate migration evidence BillingAdmin: %v", err)
	}
	actor, ok := actor.ForOrganization(organizationID)
	if !ok {
		t.Fatal("migration evidence BillingAdmin lacks Organization authorization")
	}
	service, err := newOrganizationReportingService(t, database.DSN)
	if err != nil {
		t.Fatalf("create migration evidence reporting service: %v", err)
	}
	contact, err := service.CreateSettlementContact(
		context.Background(),
		actor,
		organizationID,
		organizationreporting.CreateSettlementContactRequest{
			DisplayName: "Durable Settlement Evidence", Email: "durable@example.com",
		},
	)
	if err != nil {
		t.Fatalf("create durable settlement contact evidence: %v", err)
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err = goose.DownTo(database.Admin, migrations, 14)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
		postgresError.ConstraintName != "settlement_contact_contract_requires_empty_evidence" {
		t.Fatalf("Organization reporting migration Down error = %v, want named SQLSTATE 55000", err)
	}
	version, versionErr := goose.GetDBVersion(database.Admin)
	if versionErr != nil || version != 15 {
		t.Fatalf("Organization reporting version after refused Down = %d error=%v", version, versionErr)
	}
	var contacts, events int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM organization_settlement_contacts WHERE id = $1),
			(SELECT count(*) FROM organization_settlement_contact_events WHERE contact_id = $1)
	`, contact.ID).Scan(&contacts, &events); err != nil {
		t.Fatalf("read preserved settlement contact evidence: %v", err)
	}
	if contacts != 1 || events != 1 {
		t.Fatalf("preserved settlement contact rows/events = %d/%d, want 1/1", contacts, events)
	}
}

func TestOrganizationAuditorReadsBoundedNonContentUsageByProject(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "organization-reporting-usage")
	seedSecondProjectAndPool(t, fixture.database.Admin)
	to := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	from := to.Add(-2 * time.Hour)
	insideSecondProjectJobID := uuid.New()
	outsideJobID := uuid.New()
	fixtureTx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin usage fixture transaction: %v", err)
	}
	defer func() { _ = fixtureTx.Rollback() }()
	if _, err := fixtureTx.Exec(`
		INSERT INTO jobs (
			id, organization_id, project_id, created_by_principal_id, state, version,
			model_revision_id, generation_preset_revision_id, service_class_revision_id,
			output_spec_id, worker_pool_id, request_hash, request_content,
			request_content_expires_at, pricing_rate_card_revision_id, pricing_rate_line_id,
			pricing_unit_amount_minor, pricing_quantity, pricing_quoted_amount_minor,
			pricing_currency, execution_max_attempts, execution_max_total_compute_seconds,
			execution_max_finalization_seconds_per_attempt, execution_retry_backoff_policy,
			execution_retryable_failure_classes, execution_circuit_breaker_policy,
			job_expires_at, created_at, updated_at
		)
		SELECT
			$1::uuid, organization_id, $2, $3, 'QUEUED', 1,
			model_revision_id, generation_preset_revision_id, service_class_revision_id,
			output_spec_id, $4, sha256(convert_to(($1::uuid)::text, 'UTF8')), request_content,
			request_content_expires_at, pricing_rate_card_revision_id, pricing_rate_line_id,
			pricing_unit_amount_minor, pricing_quantity, pricing_quoted_amount_minor,
			pricing_currency, execution_max_attempts, execution_max_total_compute_seconds,
			execution_max_finalization_seconds_per_attempt, execution_retry_backoff_policy,
			execution_retryable_failure_classes, execution_circuit_breaker_policy,
			job_expires_at, $5, $5
		FROM jobs WHERE id = $6
	`,
		insideSecondProjectJobID,
		uuid.MustParse(testProjectTwoID),
		uuid.MustParse(testPrincipalTwoID),
		uuid.MustParse("00000000-0000-0000-0000-000000000105"),
		from,
		fixture.assignment.JobID,
	); err != nil {
		t.Fatalf("seed second-Project usage Job: %v", err)
	}
	if _, err := fixtureTx.Exec(`
		INSERT INTO jobs (
			id, organization_id, project_id, created_by_principal_id, state, version,
			model_revision_id, generation_preset_revision_id, service_class_revision_id,
			output_spec_id, worker_pool_id, request_hash, request_content,
			request_content_expires_at, pricing_rate_card_revision_id, pricing_rate_line_id,
			pricing_unit_amount_minor, pricing_quantity, pricing_quoted_amount_minor,
			pricing_currency, execution_max_attempts, execution_max_total_compute_seconds,
			execution_max_finalization_seconds_per_attempt, execution_retry_backoff_policy,
			execution_retryable_failure_classes, execution_circuit_breaker_policy,
			job_expires_at, created_at, updated_at
		)
		SELECT
			$1::uuid, organization_id, project_id, created_by_principal_id, 'FAILED', 1,
			model_revision_id, generation_preset_revision_id, service_class_revision_id,
			output_spec_id, worker_pool_id,
			sha256(convert_to(($1::uuid)::text, 'UTF8')), request_content,
			request_content_expires_at, pricing_rate_card_revision_id, pricing_rate_line_id,
			pricing_unit_amount_minor, pricing_quantity, pricing_quoted_amount_minor,
			pricing_currency, execution_max_attempts, execution_max_total_compute_seconds,
			execution_max_finalization_seconds_per_attempt, execution_retry_backoff_policy,
			execution_retryable_failure_classes, execution_circuit_breaker_policy,
			job_expires_at, $2, $2
		FROM jobs WHERE id = $3
	`, outsideJobID, to, fixture.assignment.JobID); err != nil {
		t.Fatalf("seed upper-bound usage Job: %v", err)
	}
	insertCanonicalTerminalJobEvent(t, fixtureTx, outsideJobID, "FAILED", &to)
	for _, jobID := range []uuid.UUID{insideSecondProjectJobID, outsideJobID} {
		if _, err := fixtureTx.Exec(`
			INSERT INTO credit_reservations (
				id, organization_id, project_id, job_id, amount_minor, currency
			)
			SELECT $1, organization_id, project_id, id,
				pricing_quoted_amount_minor, pricing_currency
			FROM jobs WHERE id = $2
		`, uuid.New(), jobID); err != nil {
			t.Fatalf("seed usage Job CreditReservation: %v", err)
		}
		if _, err := fixtureTx.Exec(`
			INSERT INTO retry_runtime_states (job_id, organization_id, project_id)
			SELECT id, organization_id, project_id FROM jobs WHERE id = $1
		`, jobID); err != nil {
			t.Fatalf("seed usage Job RetryRuntimeState: %v", err)
		}
	}
	if err := fixtureTx.Commit(); err != nil {
		t.Fatalf("commit usage fixture: %v", err)
	}

	organizationID := uuid.MustParse(testOrganizationID)
	auditorID := uuid.New()
	seedHumanRoleFixture(
		t,
		fixture.database.Admin,
		auditorID,
		"organization-reporting-usage-auditor",
		[]string{"OrganizationAuditor"},
		nil,
	)
	authenticator := newHumanMembershipAuthenticator(
		t,
		fixture.database,
		newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_human_auth_login",
			"vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "organization-reporting-usage-auditor",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	actor, err := authenticator.Authenticate(
		context.Background(),
		"organization-reporting-usage-auditor-token",
	)
	if err != nil {
		t.Fatalf("authenticate OrganizationAuditor: %v", err)
	}
	actor, ok := actor.ForOrganization(organizationID)
	if !ok || !actor.HasScope(identity.ScopeOrganizationUsageRead) ||
		actor.HasScope(identity.ScopeOrganizationBillingRead) {
		t.Fatalf("OrganizationAuditor scopes = %v", actor.Scopes)
	}
	service, err := newOrganizationReportingService(t, fixture.database.DSN)
	if err != nil {
		t.Fatalf("create Organization reporting service: %v", err)
	}

	usage, err := service.GetUsage(
		context.Background(), actor, organizationID, from, to,
	)
	if err != nil {
		t.Fatalf("get Organization usage: %v", err)
	}
	if usage.OrganizationID != organizationID || !usage.From.Equal(from) ||
		!usage.To.Equal(to) || usage.Currency != "CNY" ||
		usage.Total.TotalJobs != 2 || usage.Total.QueuedJobs != 1 ||
		usage.Total.CancelingJobs != 1 || usage.Total.FailedJobs != 0 ||
		usage.Total.QuotedAmountMinor != 2500 ||
		usage.Total.PostedChargeAmountMinor != 1250 || len(usage.Projects) != 2 {
		t.Fatalf("Organization usage = %#v", usage)
	}
	byProject := make(map[uuid.UUID]organizationreporting.UsageAggregate)
	for _, project := range usage.Projects {
		byProject[project.ProjectID] = project.UsageAggregate
	}
	if first := byProject[uuid.MustParse(testProjectID)]; first.TotalJobs != 1 || first.CancelingJobs != 1 ||
		first.QuotedAmountMinor != 1250 || first.PostedChargeAmountMinor != 1250 {
		t.Fatalf("first Project usage = %#v", first)
	}
	if second := byProject[uuid.MustParse(testProjectTwoID)]; second.TotalJobs != 1 || second.QueuedJobs != 1 ||
		second.QuotedAmountMinor != 1250 || second.PostedChargeAmountMinor != 0 {
		t.Fatalf("second Project usage = %#v", second)
	}
}

func TestOrganizationAuditorReadsUnifiedSafeIdentityAndContactAuditStream(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	organizationID := uuid.MustParse(testOrganizationID)
	projectID := uuid.MustParse(testProjectID)
	ownerID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		ownerID,
		"organization-reporting-audit-owner",
		[]string{"OrganizationOwner"},
		map[string][]string{testProjectID: {"ProjectAdmin"}},
	)
	ownerAuthenticator := newHumanMembershipAuthenticator(
		t,
		database,
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t,
			database.DSN,
			"vela_human_auth_login",
			"vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "organization-reporting-audit-owner",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	owner, err := ownerAuthenticator.Authenticate(
		context.Background(), "organization-reporting-audit-owner-token",
	)
	if err != nil {
		t.Fatalf("authenticate audit fixture owner: %v", err)
	}
	organizationOwner, ok := owner.ForOrganization(organizationID)
	if !ok {
		t.Fatal("audit fixture owner lacks Organization authorization")
	}
	projectAdmin, ok := owner.ForProject(projectID)
	if !ok {
		t.Fatal("audit fixture owner lacks Project authorization")
	}
	identityService, err := newHumanMembershipAdministrationService(
		t,
		database,
		newRolePool(
			t,
			database.DSN,
			"vela_identity_request_login",
			"vela-identity-request-password",
		),
		testCredentialPepper,
		"https://identity.example.com",
	)
	if err != nil {
		t.Fatalf("create audit fixture identity service: %v", err)
	}
	humanTarget, err := identityService.CreateHumanMember(
		context.Background(),
		organizationOwner,
		organizationID,
		identity.CreateHumanMemberRequest{
			OIDCSubject: "organization-reporting-audit-human-target",
			DisplayName: "Audit Human Target",
		},
	)
	if err != nil {
		t.Fatalf("create audited Human member: %v", err)
	}
	if _, err := identityService.AssignOrganizationRole(
		context.Background(),
		organizationOwner,
		organizationID,
		humanTarget.ID,
		identity.OrganizationRoleBillingAdmin,
	); err != nil {
		t.Fatalf("assign audited Human Organization role: %v", err)
	}
	serviceTarget, err := identityService.CreateServicePrincipal(
		context.Background(),
		projectAdmin,
		projectID,
		identity.CreateServicePrincipalRequest{DisplayName: "Audit Service Target"},
	)
	if err != nil {
		t.Fatalf("create audited Service Principal: %v", err)
	}
	issuedCredential, err := identityService.IssueCredential(
		context.Background(),
		projectAdmin,
		projectID,
		serviceTarget.ID,
		identity.IssueCredentialRequest{
			Scopes:    []string{identity.ScopeJobsRead},
			ExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		},
	)
	if err != nil {
		t.Fatalf("issue audited Service Credential: %v", err)
	}
	reportingService, err := newOrganizationReportingService(t, database.DSN)
	if err != nil {
		t.Fatalf("create Organization reporting service: %v", err)
	}
	contactTarget, err := reportingService.CreateSettlementContact(
		context.Background(),
		organizationOwner,
		organizationID,
		organizationreporting.CreateSettlementContactRequest{
			DisplayName: "Audit Settlement Target",
			Email:       "audit-settlement@example.com",
		},
	)
	if err != nil {
		t.Fatalf("create audited settlement contact: %v", err)
	}

	auditorID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		auditorID,
		"organization-reporting-audit-reader",
		[]string{"OrganizationAuditor"},
		nil,
	)
	auditorAuthenticator := newHumanMembershipAuthenticator(
		t,
		database,
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t,
			database.DSN,
			"vela_human_auth_login",
			"vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "organization-reporting-audit-reader",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	auditor, err := auditorAuthenticator.Authenticate(
		context.Background(), "organization-reporting-audit-reader-token",
	)
	if err != nil {
		t.Fatalf("authenticate OrganizationAuditor: %v", err)
	}
	auditor, ok = auditor.ForOrganization(organizationID)
	if !ok || !auditor.HasScope(identity.ScopeOrganizationAuditRead) ||
		auditor.HasScope(identity.ScopeOrganizationBillingRead) {
		t.Fatalf("OrganizationAuditor scopes = %v", auditor.Scopes)
	}

	before := readOrganizationReportingAuthoritySnapshot(t, database.Admin, organizationID)
	events, err := reportingService.ListAuditEvents(
		context.Background(), auditor, organizationID, 100,
	)
	if err != nil {
		t.Fatalf("list Organization audit events: %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("Organization audit events = %#v, want five", events)
	}
	byActionAndTarget := make(map[string]organizationreporting.AuditEvent)
	for index, event := range events {
		if event.EventID == uuid.Nil || event.ActorSessionID == uuid.Nil ||
			event.CreatedAt.IsZero() {
			t.Fatalf("incomplete Organization audit event = %#v", event)
		}
		if index > 0 {
			previous := events[index-1]
			if previous.CreatedAt.Before(event.CreatedAt) ||
				(previous.CreatedAt.Equal(event.CreatedAt) &&
					bytes.Compare(previous.EventID[:], event.EventID[:]) <= 0) {
				t.Fatalf("Organization audit events are not in reverse (created_at, event_id) order: %#v", events)
			}
		}
		byActionAndTarget[event.Action+":"+event.TargetID.String()] = event
	}
	if event := byActionAndTarget["HUMAN_MEMBER_CREATED:"+humanTarget.ID.String()]; event.Source != "HUMAN_IDENTITY" || event.Action != "HUMAN_MEMBER_CREATED" ||
		event.ProjectID != nil || event.ActorPrincipalID != ownerID ||
		event.ActorSessionID != organizationOwner.CredentialID ||
		event.TargetKind != "HUMAN_PRINCIPAL" {
		t.Fatalf("Human identity audit projection = %#v", event)
	}
	if event := byActionAndTarget["ORGANIZATION_ROLE_ASSIGNED:"+humanTarget.ID.String()]; event.Source != "HUMAN_IDENTITY" || event.Action != "ORGANIZATION_ROLE_ASSIGNED" ||
		event.ProjectID != nil || event.ActorPrincipalID != ownerID ||
		event.ActorSessionID != organizationOwner.CredentialID ||
		event.TargetKind != "HUMAN_PRINCIPAL" {
		t.Fatalf("Human role audit projection = %#v", event)
	}
	if event := byActionAndTarget["SERVICE_PRINCIPAL_CREATED:"+serviceTarget.ID.String()]; event.Source != "PROJECT_IDENTITY" ||
		event.Action != "SERVICE_PRINCIPAL_CREATED" || event.ProjectID == nil ||
		*event.ProjectID != projectID || event.ActorPrincipalID != ownerID ||
		event.ActorSessionID != projectAdmin.CredentialID ||
		event.TargetKind != "SERVICE_PRINCIPAL" {
		t.Fatalf("Service identity audit projection = %#v", event)
	}
	if event := byActionAndTarget["CREDENTIAL_ISSUED:"+issuedCredential.Credential.ID.String()]; event.Source != "PROJECT_IDENTITY" || event.Action != "CREDENTIAL_ISSUED" ||
		event.ProjectID == nil || *event.ProjectID != projectID ||
		event.ActorPrincipalID != ownerID ||
		event.ActorSessionID != projectAdmin.CredentialID || event.TargetKind != "CREDENTIAL" {
		t.Fatalf("Credential audit projection = %#v", event)
	}
	if event := byActionAndTarget["SETTLEMENT_CONTACT_CREATED:"+contactTarget.ID.String()]; event.Source != "SETTLEMENT_CONTACT" ||
		event.Action != "SETTLEMENT_CONTACT_CREATED" || event.ProjectID != nil ||
		event.ActorPrincipalID != ownerID ||
		event.ActorSessionID != organizationOwner.CredentialID ||
		event.TargetKind != "SETTLEMENT_CONTACT" {
		t.Fatalf("settlement-contact audit projection = %#v", event)
	}
	if after := readOrganizationReportingAuthoritySnapshot(t, database.Admin, organizationID); after != before {
		t.Fatal("Organization audit read mutated predecessor or contact evidence")
	}
}

func TestOrganizationReportingProductionHTTPPath(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "organization-reporting-http")
	adapter := &recordingInvoiceAdapter{receipt: billingexport.Receipt{
		InvoiceReference: "invoice-organization-reporting-http",
		LineReference:    "line-organization-reporting-http",
	}}
	exporter, err := billingexport.NewService(
		newRolePool(
			t, fixture.database.DSN, "vela_billing_login", "vela-billing-password",
		),
		adapter,
		billingexport.Config{
			ExporterID: "organization-reporting-http-exporter",
			BatchSize:  1,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create HTTP fixture Invoice exporter: %v", err)
	}
	if result, err := exporter.ExportBatch(context.Background()); err != nil ||
		result.Exported != 1 {
		t.Fatalf("export HTTP fixture Charge = %#v error=%v", result, err)
	}
	organizationID := uuid.MustParse(testOrganizationID)
	ownerID := uuid.New()
	seedHumanRoleFixture(
		t,
		fixture.database.Admin,
		ownerID,
		"organization-reporting-http-owner",
		[]string{"OrganizationOwner"},
		nil,
	)
	authenticator := newHumanMembershipAuthenticator(
		t,
		fixture.database,
		newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_human_auth_login",
			"vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "organization-reporting-http-owner",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	reporting, err := newOrganizationReportingService(t, fixture.database.DSN)
	if err != nil {
		t.Fatalf("create HTTP Organization reporting service: %v", err)
	}
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          authenticator,
		IdentityAdministration: &identity.AdministrationService{},
		OrganizationReporting:  reporting,
		Retention:              &retention.Service{},
		Admission:              &admission.Service{},
		Cancellation:           &cancellation.Service{},
		Artifacts:              &artifactaccess.Service{},
		Webhooks:               &webhook.Service{},
	})
	if err != nil {
		t.Fatalf("create Organization reporting HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	basePath := "/v1/organizations/" + organizationID.String()
	doRequest := func(method, path string, body []byte) httpResult {
		t.Helper()
		request, err := http.NewRequest(method, server.URL+path, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("create Organization reporting request: %v", err)
		}
		request.Header.Set("Authorization", "Bearer organization-reporting-http-owner-token")
		if body != nil {
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatalf("execute Organization reporting request: %v", err)
		}
		defer response.Body.Close()
		responseBody, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatalf("read Organization reporting response: %v", err)
		}
		return httpResult{
			StatusCode: response.StatusCode,
			Header:     response.Header.Clone(),
			Body:       responseBody,
		}
	}

	credit := doRequest(http.MethodGet, basePath+"/billing/credit", nil)
	if credit.StatusCode != http.StatusOK || !bytes.Contains(credit.Body, []byte(`"available_minor":98750`)) {
		t.Fatalf("credit HTTP response = %d %s", credit.StatusCode, credit.Body)
	}
	charges := doRequest(http.MethodGet, basePath+"/billing/charges", nil)
	if charges.StatusCode != http.StatusOK ||
		!bytes.Contains(charges.Body, []byte(adapter.receipt.InvoiceReference)) ||
		bytes.Contains(charges.Body, []byte("last_error")) ||
		bytes.Contains(charges.Body, []byte("request_content")) {
		t.Fatalf("Charge HTTP response = %d %s", charges.StatusCode, charges.Body)
	}
	created := doRequest(
		http.MethodPost,
		basePath+"/billing/settlement-contacts",
		[]byte(`{"display_name":"HTTP Settlement","email":"HTTP@EXAMPLE.COM"}`),
	)
	if created.StatusCode != http.StatusCreated {
		t.Fatalf("create contact HTTP response = %d %s", created.StatusCode, created.Body)
	}
	var contact struct {
		ContactID string `json:"contact_id"`
		Email     string `json:"email"`
	}
	if err := json.Unmarshal(created.Body, &contact); err != nil ||
		contact.Email != "http@example.com" {
		t.Fatalf("decode created contact = %#v error=%v body=%s", contact, err, created.Body)
	}
	listed := doRequest(http.MethodGet, basePath+"/billing/settlement-contacts", nil)
	if listed.StatusCode != http.StatusOK || !bytes.Contains(listed.Body, []byte(contact.ContactID)) {
		t.Fatalf("list contacts HTTP response = %d %s", listed.StatusCode, listed.Body)
	}
	disabled := doRequest(
		http.MethodPost,
		basePath+"/billing/settlement-contacts/"+contact.ContactID+"/disable",
		nil,
	)
	if disabled.StatusCode != http.StatusOK ||
		!bytes.Contains(disabled.Body, []byte("disabled_at")) {
		t.Fatalf("disable contact HTTP response = %d %s", disabled.StatusCode, disabled.Body)
	}
	from := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)
	to := time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)
	usage := doRequest(
		http.MethodGet,
		basePath+"/usage?from="+url.QueryEscape(from)+"&to="+url.QueryEscape(to),
		nil,
	)
	if usage.StatusCode != http.StatusOK ||
		!bytes.Contains(usage.Body, []byte(`"posted_charge_amount_minor":1250`)) ||
		bytes.Contains(usage.Body, []byte("request_content")) {
		t.Fatalf("usage HTTP response = %d %s", usage.StatusCode, usage.Body)
	}
	audit := doRequest(http.MethodGet, basePath+"/audit-events", nil)
	if audit.StatusCode != http.StatusOK ||
		!bytes.Contains(audit.Body, []byte("SETTLEMENT_CONTACT_CREATED")) ||
		!bytes.Contains(audit.Body, []byte("SETTLEMENT_CONTACT_DISABLED")) ||
		strings.Contains(string(audit.Body), `"details"`) ||
		strings.Contains(string(audit.Body), "http@example.com") {
		t.Fatalf("audit HTTP response = %d %s", audit.StatusCode, audit.Body)
	}

	unauthenticatedRequest, err := http.NewRequest(
		http.MethodGet, server.URL+basePath+"/billing/credit", nil,
	)
	if err != nil {
		t.Fatalf("create unauthenticated Organization reporting request: %v", err)
	}
	unauthenticatedResponse, err := server.Client().Do(unauthenticatedRequest)
	if err != nil {
		t.Fatalf("execute unauthenticated Organization reporting request: %v", err)
	}
	unauthenticatedBody, readErr := io.ReadAll(unauthenticatedResponse.Body)
	_ = unauthenticatedResponse.Body.Close()
	if readErr != nil {
		t.Fatalf("read unauthenticated Organization reporting response: %v", readErr)
	}
	if unauthenticatedResponse.StatusCode != http.StatusUnauthorized ||
		!strings.HasPrefix(unauthenticatedResponse.Header.Get("Content-Type"), "application/json") ||
		!bytes.Contains(unauthenticatedBody, []byte(`"code":"unauthorized"`)) {
		t.Fatalf(
			"unauthenticated Organization reporting response = %d %s %s",
			unauthenticatedResponse.StatusCode,
			unauthenticatedResponse.Header.Get("Content-Type"),
			unauthenticatedBody,
		)
	}
	invalidLimit := doRequest(http.MethodGet, basePath+"/billing/charges?limit=0", nil)
	if invalidLimit.StatusCode != http.StatusBadRequest ||
		!bytes.Contains(invalidLimit.Body, []byte(`"code":"invalid_request"`)) {
		t.Fatalf("invalid Charge limit HTTP response = %d %s", invalidLimit.StatusCode, invalidLimit.Body)
	}
	invalidContact := doRequest(
		http.MethodPost,
		basePath+"/billing/settlement-contacts",
		[]byte(`{"display_name":"Missing Email"}`),
	)
	if invalidContact.StatusCode != http.StatusBadRequest ||
		!bytes.Contains(invalidContact.Body, []byte(`"code":"invalid_request"`)) {
		t.Fatalf("invalid settlement contact HTTP response = %d %s", invalidContact.StatusCode, invalidContact.Body)
	}
	unknownContact := doRequest(
		http.MethodPost,
		basePath+"/billing/settlement-contacts/"+uuid.NewString()+"/disable",
		nil,
	)
	if unknownContact.StatusCode != http.StatusNotFound ||
		!bytes.Contains(unknownContact.Body, []byte(`"code":"not_found"`)) {
		t.Fatalf("unknown settlement contact HTTP response = %d %s", unknownContact.StatusCode, unknownContact.Body)
	}
	wrongOrganization := doRequest(
		http.MethodGet,
		"/v1/organizations/"+uuid.NewString()+"/billing/credit",
		nil,
	)
	if wrongOrganization.StatusCode != http.StatusForbidden ||
		!bytes.Contains(wrongOrganization.Body, []byte(`"code":"forbidden"`)) {
		t.Fatalf("unselected Organization HTTP response = %d %s", wrongOrganization.StatusCode, wrongOrganization.Body)
	}
	if _, err := fixture.database.Admin.Exec(`
		DELETE FROM organization_role_bindings
		WHERE organization_id = $1 AND principal_id = $2 AND role = 'OrganizationOwner'
	`, organizationID, ownerID); err != nil {
		t.Fatalf("remove HTTP OrganizationOwner role after authentication: %v", err)
	}
	removedRole := doRequest(http.MethodGet, basePath+"/billing/credit", nil)
	if removedRole.StatusCode != http.StatusForbidden ||
		!bytes.Contains(removedRole.Body, []byte(`"code":"forbidden"`)) {
		t.Fatalf("removed Organization role HTTP response = %d %s", removedRole.StatusCode, removedRole.Body)
	}
}

func organizationReportingFailureHasCode(err error, code organizationreporting.FailureCode) bool {
	var failure *organizationreporting.Failure
	return errors.As(err, &failure) && failure.Code == code
}

func readOrganizationReportingAuthoritySnapshot(
	t *testing.T,
	database *sql.DB,
	organizationID uuid.UUID,
) string {
	t.Helper()
	var snapshot string
	if err := database.QueryRow(`
		SELECT jsonb_build_object(
			'credit_account', (
				SELECT to_jsonb(account)
				FROM organization_credit_accounts AS account
				WHERE account.organization_id = $1
			),
			'credit_reservations', COALESCE((
				SELECT jsonb_agg(to_jsonb(reservation) ORDER BY reservation.id)
				FROM credit_reservations AS reservation
				WHERE reservation.organization_id = $1
			), '[]'::jsonb),
			'jobs', COALESCE((
				SELECT jsonb_agg(to_jsonb(job) ORDER BY job.id)
				FROM jobs AS job
				WHERE job.organization_id = $1
			), '[]'::jsonb),
			'charges', COALESCE((
				SELECT jsonb_agg(to_jsonb(charge) ORDER BY charge.id)
				FROM charges AS charge
				WHERE charge.organization_id = $1
			), '[]'::jsonb),
			'invoice_exports', COALESCE((
				SELECT jsonb_agg(to_jsonb(export) ORDER BY export.charge_id)
				FROM invoice_exports AS export
				WHERE export.organization_id = $1
			), '[]'::jsonb),
			'invoice_export_receipts', COALESCE((
				SELECT jsonb_agg(to_jsonb(receipt) ORDER BY receipt.charge_id)
				FROM invoice_export_receipts AS receipt
				WHERE receipt.organization_id = $1
			), '[]'::jsonb),
			'settlement_contacts', COALESCE((
				SELECT jsonb_agg(to_jsonb(contact) ORDER BY contact.id)
				FROM organization_settlement_contacts AS contact
				WHERE contact.organization_id = $1
			), '[]'::jsonb),
			'human_identity_events', COALESCE((
				SELECT jsonb_agg(to_jsonb(event) ORDER BY event.id)
				FROM human_identity_events AS event
				WHERE event.organization_id = $1
			), '[]'::jsonb),
			'project_identity_events', COALESCE((
				SELECT jsonb_agg(to_jsonb(event) ORDER BY event.id)
				FROM project_identity_events AS event
				WHERE event.organization_id = $1
			), '[]'::jsonb),
			'settlement_contact_events', COALESCE((
				SELECT jsonb_agg(to_jsonb(event) ORDER BY event.id)
				FROM organization_settlement_contact_events AS event
				WHERE event.organization_id = $1
			), '[]'::jsonb)
		)::text
	`, organizationID).Scan(&snapshot); err != nil {
		t.Fatalf("read Organization reporting authority snapshot: %v", err)
	}
	return snapshot
}
