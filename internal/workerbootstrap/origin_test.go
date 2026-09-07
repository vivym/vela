package workerbootstrap

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestBootstrapRejectsLostOrInvalidOriginalStorage(t *testing.T) {
	for _, fault := range []string{"missing", "partial", "unknown-field", "duplicate-field", "mode", "hardlink", "zero-identity", "wrong-root", "wrong-pair"} {
		t.Run(fault, func(t *testing.T) {
			config, registry := bootstrapFixture(t)
			first, err := Prepare(t.Context(), config, registry)
			mustDo(t, err)
			path := filepath.Join(config.ScratchDirectory, "bootstrap", originName)
			wire, err := os.ReadFile(path)
			mustDo(t, err)
			var origin journalOrigin
			mustDo(t, json.Unmarshal(wire, &origin))
			if !first.Worker.Storage.Valid() || !first.Runtime.Storage.Valid() ||
				origin.Worker != first.Worker.Storage || origin.Runtime != first.Runtime.Storage {
				t.Fatal("original storage did not come from the actual prepared journals")
			}
			switch fault {
			case "missing":
				mustDo(t, os.Remove(path))
			case "partial":
				mustDo(t, os.WriteFile(path, wire[:len(wire)-1], 0o600))
			case "unknown-field":
				mustDo(t, os.WriteFile(path, append([]byte(`{"unknown":true,`), wire[1:]...), 0o600))
			case "duplicate-field":
				mustDo(t, os.WriteFile(path, append([]byte(`{"schema_version":1,`), wire[1:]...), 0o600))
			case "mode":
				mustDo(t, os.Chmod(path, 0o644))
			case "hardlink":
				mustDo(t, os.Link(path, path+".alias"))
			default:
				switch fault {
				case "zero-identity":
					origin.Runtime.Lock.Inode = 0
				case "wrong-root":
					origin.Runtime.Root.Inode++
				case "wrong-pair":
					origin.Pair.RuntimeScope[0] ^= 1
				}
				changed, err := json.Marshal(origin)
				mustDo(t, err)
				mustDo(t, os.WriteFile(path, changed, 0o600))
			}
			before := snapshotFiles(t, config.ScratchDirectory)
			if result, err := Prepare(t.Context(), config, registry); err == nil || result != (Result{}) {
				t.Fatal("invalid original identity acquired receipt replay")
			}
			if !reflect.DeepEqual(before, snapshotFiles(t, config.ScratchDirectory)) {
				t.Fatal("replay repaired unproven original identity")
			}
			pairPath := filepath.Join(config.ScratchDirectory, "bootstrap", pairName)
			mustDo(t, os.Remove(pairPath))
			before = snapshotFiles(t, config.ScratchDirectory)
			if result, err := ReconcileRecordedPair(t.Context(), config, constantHistory(recordedHistory(config, registry))); err == nil || result != (Result{}) {
				t.Fatal("invalid original identity reconstructed pair metadata")
			}
			if _, err := os.Stat(pairPath); !errors.Is(err, os.ErrNotExist) || registry.claimCalls != 1 || registry.receiptCalls != 1 ||
				!reflect.DeepEqual(before, snapshotFiles(t, config.ScratchDirectory)) {
				t.Fatal("reconciliation changed local evidence or Registry authority")
			}
		})
	}
}

func TestPrepareRejectsReboundOriginalJournalLock(t *testing.T) {
	for _, journal := range []struct{ directory, prefix, prepared string }{
		{"worker-admission", "assignment-admission", "worker-prepared"},
		{"runtime-admission", "execution-admission", "runtime-prepared"},
	} {
		for _, phase := range []string{journal.prepared, "pair-durable", "replay", "reconcile"} {
			t.Run(journal.directory+"/"+phase, func(t *testing.T) {
				config, registry := bootstrapFixture(t)
				replace := func() {
					rebindJournalLock(t, filepath.Join(config.ScratchDirectory, journal.directory, journal.prefix))
				}
				var result Result
				var err error
				priorReceipts := 0
				if phase == "replay" || phase == "reconcile" {
					_, err = Prepare(t.Context(), config, registry)
					mustDo(t, err)
					priorReceipts = registry.receiptCalls
					replace()
					if phase == "reconcile" {
						mustDo(t, os.Remove(filepath.Join(config.ScratchDirectory, "bootstrap", pairName)))
						result, err = ReconcileRecordedPair(t.Context(), config, constantHistory(recordedHistory(config, registry)))
					} else {
						result, err = Prepare(t.Context(), config, registry)
					}
				} else {
					result, err = prepare(t.Context(), config, registry, func(current string) error {
						if current == phase {
							replace()
						}
						return nil
					})
				}
				if err == nil || result != (Result{}) || registry.receiptCalls != priorReceipts || registry.claimCalls != 1 {
					t.Fatalf("rebound lock was accepted as the original journal: result=%+v err=%v claims=%d receipts=%d", result, err, registry.claimCalls, registry.receiptCalls)
				}
			})
		}
	}
}

// Keep UUID, scope and all other fields intact while replacing only the lock
// inode and the mutable journal's self-declared lock binding. Decode members in
// order so that the counterexample preserves the journal's canonical encoding.
func rebindJournalLock(t *testing.T, prefix string) {
	t.Helper()
	lockPath := prefix + ".lock"
	lockBytes, err := os.ReadFile(lockPath)
	mustDo(t, err)
	mustDo(t, os.Rename(lockPath, lockPath+".original"))
	mustDo(t, os.WriteFile(lockPath, lockBytes, 0o600))
	info, err := os.Stat(lockPath)
	mustDo(t, err)
	encodedIdentity, err := json.Marshal(identity(info))
	mustDo(t, err)
	wire, err := os.ReadFile(prefix + ".json")
	mustDo(t, err)
	decoder := json.NewDecoder(bytes.NewReader(wire))
	first, err := decoder.Token()
	mustDo(t, err)
	if first != json.Delim('{') {
		t.Fatal("journal is not a JSON object")
	}
	var rewritten bytes.Buffer
	rewritten.WriteByte('{')
	count, changed := 0, false
	for decoder.More() {
		key, err := decoder.Token()
		mustDo(t, err)
		var value json.RawMessage
		mustDo(t, decoder.Decode(&value))
		if key == "lock" {
			value, changed = encodedIdentity, true
		}
		if count > 0 {
			rewritten.WriteByte(',')
		}
		encodedKey, err := json.Marshal(key)
		mustDo(t, err)
		rewritten.Write(encodedKey)
		rewritten.WriteByte(':')
		rewritten.Write(value)
		count++
	}
	last, err := decoder.Token()
	mustDo(t, err)
	if last != json.Delim('}') || !changed {
		t.Fatal("journal has no complete lock binding")
	}
	rewritten.WriteByte('}')
	mustDo(t, os.WriteFile(prefix+".json", rewritten.Bytes(), 0o600))
}
