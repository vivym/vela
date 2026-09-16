package main

import (
	"encoding/json"
	"testing"
)

func TestBootstrapRejectsCapacityAndQuorumFailures(t *testing.T) {
	const healthy = `[
 {"config":{"max_storage":39378487296},"meta_cluster":{"leader":"nats-2","cluster_size":3}},
 {"config":{"max_storage":39378487296},"meta_cluster":{"leader":"nats-2","cluster_size":3}},
 {"config":{"max_storage":39378487296},"meta_cluster":{"leader":"nats-2","cluster_size":3,"replicas":[{"name":"nats-0","current":true},{"name":"nats-1","current":true}]}}
 ]`
	for _, test := range []struct {
		name   string
		mutate func([]memberCapacity)
		valid  bool
	}{
		{"healthy", func([]memberCapacity) {}, true},
		{"reserved quota", func(m []memberCapacity) { m[1].Reserved = 8 << 30 }, false},
		{"stored bytes", func(m []memberCapacity) { m[1].Storage = 8 << 30 }, false},
		{"quota underflow", func(m []memberCapacity) { m[1].Reserved = 1 << 63 }, false},
		{"missing leader", func(m []memberCapacity) {
			for i := range m {
				m[i].Meta.Leader = "nats-9"
			}
		}, false},
		{"split leaders", func(m []memberCapacity) { m[1].Meta.Leader = "nats-1" }, false},
		{"stale replica", func(m []memberCapacity) { m[2].Meta.Replicas[0].Current = false }, false},
		{"offline replica", func(m []memberCapacity) { m[2].Meta.Replicas[0].Offline = true }, false},
		{"duplicate replica", func(m []memberCapacity) { m[2].Meta.Replicas[0].Name = "nats-1" }, false},
		{"missing replica", func(m []memberCapacity) { m[2].Meta.Replicas = m[2].Meta.Replicas[:1] }, false},
		{"duplicate member", func(m []memberCapacity) { m[0].Name = "nats-1" }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var members []memberCapacity
			if err := json.Unmarshal([]byte(healthy), &members); err != nil {
				t.Fatal(err)
			}
			for i, name := range []string{"nats-0", "nats-1", "nats-2"} {
				members[i].Name = name
			}
			test.mutate(members)
			if err := validateCapacity(members); (err == nil) != test.valid {
				t.Fatalf("capacity validation: %v", err)
			}
		})
	}
}
