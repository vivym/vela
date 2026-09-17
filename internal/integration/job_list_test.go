//go:build integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"

	api "github.com/vivym/vela/api/gen"
)

func TestProjectJobListFiltersPaginationAndIsolation(t *testing.T) {
	server, admin := newAdmissionServer(t)
	get := func(project, token, query string, want int) api.JobList {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, server.URL+"/v1/projects/"+project+"/jobs"+query, nil)
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		wire, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != want {
			t.Fatalf("GET %s %s: %d want %d: %s", project, query, res.StatusCode, want, wire)
		}
		var page api.JobList
		if want == 200 {
			if err := json.Unmarshal(wire, &page); err != nil {
				t.Fatal(err)
			}
			if page.Jobs == nil {
				t.Fatal("empty jobs must be an array")
			}
			if strings.Contains(string(wire), `"prompt"`) || strings.Contains(string(wire), `"client_metadata"`) {
				t.Fatal("list exposed request content")
			}
		}
		return page
	}
	token := testBearerCredential()
	if page := get(testProjectID, token, "?active=true", 200); len(page.Jobs) != 0 || page.NextCursor != nil {
		t.Fatalf("empty page=%+v", page)
	}
	body := []byte(`{"model":"minimax-h3","generation_preset":"balanced","service_class":"standard","output_spec":"video-1080p-5s-24fps","generation_count":1,"prompt":"private listing fixture"}`)
	states := []string{"QUEUED", "ASSIGNED", "RUNNING", "FINALIZING", "RETRY_WAIT", "CANCELING", "SUCCEEDED", "FAILED", "CANCELED"}
	ids := make([]string, 0, len(states))
	for i := range states {
		result := submitJob(t, server.URL, fmt.Sprintf("list-%d", i), body)
		if result.StatusCode != 202 {
			t.Fatalf("submit: %d %s", result.StatusCode, result.Body)
		}
		var job jobResponse
		if err := json.Unmarshal(result.Body, &job); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, job.JobID)
	}
	// Read-side fixtures only, in this disposable database: freeze equal timestamps
	// and materialize every lifecycle state without driving nine GPU workflows.
	tx, err := admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("SET LOCAL session_replication_role='replica'"); err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		if _, err := tx.Exec("UPDATE jobs SET state=$2::text::job_state, billable_started_at=CASE WHEN $2::text IN ('RUNNING','FINALIZING','RETRY_WAIT','CANCELING','SUCCEEDED') THEN transaction_timestamp() ELSE NULL END, created_at=date_trunc('second',transaction_timestamp())-interval '1 hour' WHERE id=$1", id, states[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	seedSecondProject(t, admin)
	seedOtherOrganization(t, admin)
	authorizeCurrentStageCutoverProject(t, admin, testOtherOrganizationID, testOtherProjectID)
	for _, caller := range []struct{ project, token string }{{testProjectTwoID, bearerCredential(testCredentialTwoID, testCredentialTwoSecret)}, {testOtherProjectID, bearerCredential(testOtherCredentialID, testOtherSecret)}} {
		result, err := doSubmitJob(server.URL, caller.project, caller.token, "foreign-list", body)
		if err != nil || result.StatusCode != 202 {
			t.Fatalf("foreign submit: %v %+v", err, result)
		}
		get(caller.project, token, "", 403)
		page := get(caller.project, caller.token, "", 200)
		if len(page.Jobs) != 1 {
			t.Fatalf("foreign own list=%+v", page)
		}
	}
	active := get(testProjectID, token, "?active=true", 200)
	if len(active.Jobs) != 6 {
		t.Fatalf("active count=%d", len(active.Jobs))
	}
	for _, j := range active.Jobs {
		if j.State == "SUCCEEDED" || j.State == "FAILED" || j.State == "CANCELED" {
			t.Fatal("terminal Job in active results")
		}
	}
	for _, state := range states {
		page := get(testProjectID, token, "?state="+state, 200)
		if len(page.Jobs) != 1 || string(page.Jobs[0].State) != state || page.Jobs[0].Model == nil || *page.Jobs[0].Model != "minimax-h3" {
			t.Fatalf("state %s: %+v", state, page)
		}
	}
	first := get(testProjectID, token, "?limit=2", 200)
	if first.NextCursor == nil {
		t.Fatal("missing cursor")
	}
	// A new Job must not displace or duplicate older Jobs while paging.
	inserted := submitJob(t, server.URL, "insert-during-pagination", body)
	if inserted.StatusCode != 202 {
		t.Fatalf("insert: %s", inserted.Body)
	}
	seen := []string{}
	page := first
	for n := 0; n < 10; n++ {
		for _, job := range page.Jobs {
			seen = append(seen, job.JobId.String())
			if job.ProjectId.String() != testProjectID {
				t.Fatal("Project leak")
			}
		}
		if page.NextCursor == nil {
			break
		}
		page = get(testProjectID, token, "?limit=2&cursor="+url.QueryEscape(*page.NextCursor), 200)
	}
	want := append([]string(nil), ids...)
	sort.Sort(sort.Reverse(sort.StringSlice(want)))
	if fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("tie-break pagination=%v want %v", seen, want)
	}
	if len(get(testProjectID, token, "?active=false", 200).Jobs) != 10 {
		t.Fatal("false must include all states")
	}
	get(testProjectID, token, "?active=true&cursor="+*first.NextCursor, 400)
	get(testProjectTwoID, bearerCredential(testCredentialTwoID, testCredentialTwoSecret), "?cursor="+*first.NextCursor, 400)
	for _, query := range []string{"?limit=0", "?limit=101", "?limit=-1", "?active=bad", "?state=UNKNOWN", "?cursor=bad", "?cursor=", "?active=true&state=SUCCEEDED"} {
		get(testProjectID, token, query, 400)
	}
	get(testProjectID, "", "", 401)
	if _, err := admin.Exec("UPDATE credentials SET scopes=ARRAY['jobs:submit'] WHERE id=$1", testCredentialID); err != nil {
		t.Fatal(err)
	}
	get(testProjectID, token, "", 403)
	if _, err := admin.Exec("UPDATE credentials SET scopes=ARRAY['jobs:read'],revoked_at=clock_timestamp() WHERE id=$1", testCredentialID); err != nil {
		t.Fatal(err)
	}
	get(testProjectID, token, "", 401)
}
