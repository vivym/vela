//go:build integration

package integration_test

import (
	"path/filepath"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestH3NativeMediaContractMigrationPreservesExistingRevisions(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 95)
	const insert = `INSERT INTO output_specs (id,stable_id,revision,state,width,height,duration_milliseconds,frame_rate_milli,codec) VALUES ('80000000-0000-0000-0000-000000000099','h3-full',1,'REGISTERED',1344,768,5000,24000,'h264')`
	if _, err := database.Admin.Exec(insert); err != nil {
		t.Fatal(err)
	}
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	for range 2 {
		if err := goose.UpTo(database.Admin, migrations, 99); err != nil {
			t.Fatal(err)
		}
		var contract string
		if err := database.Admin.QueryRow(`SELECT media_contract FROM output_specs WHERE stable_id='h3-full'`).Scan(&contract); err != nil || contract != "exact-video-v1" {
			t.Fatalf("existing contract changed: %q, %v", contract, err)
		}
		if _, err := database.Admin.Exec(`UPDATE output_specs SET media_contract='h3-native-av-v1' WHERE stable_id='h3-full'`); err == nil {
			t.Fatal("mutated immutable media contract")
		}
		if _, err := database.Admin.Exec(`INSERT INTO output_specs (id,stable_id,revision,state,width,height,duration_milliseconds,frame_rate_milli,codec,media_contract) VALUES ('80000000-0000-0000-0000-000000000098','h3-full',2,'REGISTERED',1344,768,5000,24000,'h264','h3-native-av-v1')`); err != nil {
			t.Fatal(err)
		}
		if _, err := database.Admin.Exec(`INSERT INTO output_specs (id,stable_id,revision,state,width,height,duration_milliseconds,frame_rate_milli,codec,media_contract) VALUES ('80000000-0000-0000-0000-000000000097','h3-invalid',1,'REGISTERED',1344,768,5000,30000,'h264','h3-native-av-v1')`); err == nil {
			t.Fatal("accepted unsupported H3 media contract")
		}
		if _, err := database.Admin.Exec(`DELETE FROM output_specs WHERE revision=2 AND stable_id='h3-full'`); err != nil {
			t.Fatal(err)
		}
		if err := goose.Down(database.Admin, migrations); err != nil {
			t.Fatal(err)
		}
	}
}
