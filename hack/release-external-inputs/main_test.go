package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func fixture() (plan, corev1.Secret) {
	item := source{Kind: "Secret", Namespace: "vela-system", Name: "nats-source", UID: "source-uid", ResourceVersion: "1", RequiredKeys: []string{"nats.conf"}, Consumers: []string{"StatefulSet/vela-system/nats"}}
	value := corev1.Secret{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}, ObjectMeta: metav1.ObjectMeta{Name: item.Name, Namespace: item.Namespace, UID: "source-uid", ResourceVersion: "1"}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{"nats.conf": []byte("PRIVATE_SERVER_CONFIGURATION"), "bootstrap.creds": []byte("PRIVATE_BOOTSTRAP_CREDENTIAL")}}
	return plan{SchemaVersion: 1, Resources: []source{item}}, value
}

type fakeCluster struct {
	objects     map[string][]byte
	sourceReads int
	staleAt     int
	writes      int
}

func (f *fakeCluster) call(ctx context.Context, object any, args ...string) ([]byte, error) {
	if args[0] == "create" {
		if args[1] == "--dry-run=server" {
			return []byte("secret/candidate"), nil
		}
		data, _ := json.Marshal(object)
		var value map[string]any
		_ = json.Unmarshal(data, &value)
		meta := value["metadata"].(map[string]any)
		key := value["kind"].(string) + "/" + meta["namespace"].(string) + "/" + meta["name"].(string)
		if f.objects[key] != nil {
			return nil, fmt.Errorf("already exists")
		}
		f.writes++
		meta["uid"] = fmt.Sprintf("created-%d", f.writes)
		meta["resourceVersion"] = "2"
		data, _ = json.Marshal(value)
		f.objects[key] = data
		return data, nil
	}
	key := args[3] + "/" + args[1] + "/" + args[4]
	data := f.objects[key]
	if args[4] == "nats-source" {
		f.sourceReads++
		if f.staleAt > 0 && f.sourceReads >= f.staleAt {
			var value corev1.Secret
			_ = json.Unmarshal(data, &value)
			value.ResourceVersion = "99"
			return json.Marshal(value)
		}
	}
	return data, nil
}
func TestSnapshotAppliesOnlyRequiredMaterialAndReusesVerifiedCopies(t *testing.T) {
	p, value := fixture()
	data, _ := json.Marshal(value)
	cluster := fakeCluster{objects: map[string][]byte{"Secret/vela-system/nats-source": data}}
	var dry receipt
	if err := execute(context.Background(), cluster.call, p, false, &dry); err != nil || cluster.writes != 0 {
		t.Fatalf("prepare: %v, writes=%d", err, cluster.writes)
	}
	var live receipt
	if err := execute(context.Background(), cluster.call, p, true, &live); err != nil {
		t.Fatal(err)
	}
	if cluster.writes != 1 || len(live.Evidence) != 1 || len(live.Created) != 1 || !reflect.DeepEqual(dry.Resources, live.Resources) {
		t.Fatal("prepared identity was not preserved through creation and canonical verification")
	}
	var snapshot corev1.Secret
	if err := json.Unmarshal(cluster.objects["Secret/vela-system/"+live.Resources[0].Name], &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Data) != 1 || string(snapshot.Data["nats.conf"]) != "PRIVATE_SERVER_CONFIGURATION" || snapshot.Immutable == nil || !*snapshot.Immutable {
		t.Fatal("snapshot did not isolate the exact immutable server key")
	}
	encoded, _ := json.Marshal(live)
	if strings.Contains(string(encoded), "PRIVATE_") || strings.Contains(string(encoded), "UFJJVkFURV8") {
		t.Fatal("receipt leaked source payload")
	}
	var retry receipt
	if err := execute(context.Background(), cluster.call, p, true, &retry); err != nil || cluster.writes != 1 || len(retry.Created) != 0 {
		t.Fatalf("idempotent retry: %v", err)
	}
	if !reflect.DeepEqual(cluster.objects["Secret/vela-system/nats-source"], data) {
		t.Fatal("source was mutated")
	}
}
func TestSnapshotRejectsDriftMissingKeysAndConflictingTargetsBeforeWrites(t *testing.T) {
	for _, scenario := range []string{"source-drift", "missing-key", "conflicting-target", "duplicate-source", "unsorted-keys"} {
		t.Run(scenario, func(t *testing.T) {
			p, value := fixture()
			data, _ := json.Marshal(value)
			cluster := fakeCluster{objects: map[string][]byte{"Secret/vela-system/nats-source": data}}
			switch scenario {
			case "source-drift":
				cluster.staleAt = 2
			case "missing-key":
				p.Resources[0].RequiredKeys = []string{"absent"}
			case "conflicting-target":
				c, err := material(p.Resources[0], value)
				if err != nil {
					t.Fatal(err)
				}
				bad := c.object.(corev1.Secret)
				bad.UID = "other"
				bad.ResourceVersion = "2"
				bad.Data["nats.conf"] = []byte("changed")
				encoded, _ := json.Marshal(bad)
				cluster.objects["Secret/vela-system/"+c.entry.Name] = encoded
			case "duplicate-source":
				p.Resources = append(p.Resources, p.Resources[0])
			case "unsorted-keys":
				p.Resources[0].RequiredKeys = []string{"nats.conf", "bootstrap.creds"}
			}
			var result receipt
			if err := execute(context.Background(), cluster.call, p, true, &result); err == nil || cluster.writes != 0 {
				t.Fatalf("invalid input caused mutation: %v, writes=%d", err, cluster.writes)
			}
		})
	}
}
func TestSubsetAndConfigMapIdentitiesUseCanonicalMaterial(t *testing.T) {
	p, value := fixture()
	one, err := material(p.Resources[0], value)
	if err != nil {
		t.Fatal(err)
	}
	subset := p.Resources[0]
	subset.RequiredKeys = []string{"bootstrap.creds"}
	two, err := material(subset, value)
	if err != nil {
		t.Fatal(err)
	}
	if one.entry.Name == two.entry.Name || one.entry.Revision == two.entry.Revision {
		t.Fatal("different subsets reused the same identity")
	}
	cm := corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "runtime-v1", Namespace: "vela-system"}, Data: map[string]string{"policy": "<strict>\u2028"}, BinaryData: map[string][]byte{"binary": {0, 255}}}
	c, err := material(source{Kind: "ConfigMap", Namespace: cm.Namespace, Name: cm.Name}, cm)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := c.object.(corev1.ConfigMap)
	snapshot.UID = "new"
	snapshot.ResourceVersion = "3"
	encoded, _ := json.Marshal(snapshot)
	if _, err := verifyObject(c, encoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot.Data, cm.Data) || !reflect.DeepEqual(snapshot.BinaryData, cm.BinaryData) {
		t.Fatal("ConfigMap material changed")
	}
}
