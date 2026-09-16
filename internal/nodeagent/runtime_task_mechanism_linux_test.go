package nodeagent

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"

	"github.com/containerd/containerd/api/types/runc/options"
	"github.com/opencontainers/runtime-spec/specs-go"
)

func runtimeTaskPolicyFixture() RuntimeTaskRuntimePolicy {
	return RuntimeTaskRuntimePolicy{ShimBinaryPath: "/usr/local/bin/containerd-shim-runc-v2", RuntimeBinaryPath: "/usr/local/bin/runc", RuntimeStateRoot: "/run/vela-approved-runc"}
}

func TestRuntimeTaskMechanismPolicy(t *testing.T) {
	policy := runtimeTaskPolicyFixture()
	makeLaunch := func(value *options.Options, hooks *specs.Hooks) *RuntimeTaskLaunch {
		t.Helper()
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		configuration, err := json.Marshal(specs.Spec{Hooks: hooks})
		if err != nil {
			t.Fatal(err)
		}
		return &RuntimeTaskLaunch{encoded: configuration, optionsEncoded: encoded, runtimeEncoded: []byte(value.BinaryName), shimEncoded: []byte(policy.ShimBinaryPath)}
	}
	valid := func() *options.Options {
		return &options.Options{BinaryName: policy.RuntimeBinaryPath, Root: policy.RuntimeStateRoot}
	}
	launch := makeLaunch(valid(), nil)
	if err := launch.CheckRuntimeMechanism(policy); err != nil {
		t.Fatal(err)
	}
	copy, err := launch.RuntimeOptions()
	if err != nil {
		t.Fatal(err)
	}
	copy.NoPivotRoot = true
	launch.RuntimeBinaryPath, launch.ShimBinaryPath = "/changed-public-field", "/changed-public-field"
	if err := launch.CheckRuntimeMechanism(policy); err != nil {
		t.Fatal("public metadata or returned options mutated retained bytes")
	}
	for _, test := range []struct {
		name   string
		change func(*options.Options)
	}{
		{"implicit-binary", func(o *options.Options) { o.BinaryName = "" }},
		{"different-binary", func(o *options.Options) { o.BinaryName = "/another/runc" }},
		{"implicit-root", func(o *options.Options) { o.Root = "" }},
		{"different-root", func(o *options.Options) { o.Root = "/another/root" }},
		{"no-pivot", func(o *options.Options) { o.NoPivotRoot = true }},
		{"no-keyring", func(o *options.Options) { o.NoNewKeyring = true }},
		{"shim-cgroup", func(o *options.Options) { o.ShimCgroup = "unapproved" }},
		{"io-uid", func(o *options.Options) { o.IoUid = 10001 }},
		{"io-gid", func(o *options.Options) { o.IoGid = 10001 }},
		{"cgroup-mode", func(o *options.Options) { o.SystemdCgroup = true }},
		{"criu-image", func(o *options.Options) { o.CriuImagePath = "/checkpoint" }},
		{"criu-work", func(o *options.Options) { o.CriuWorkPath = "/checkpoint" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := valid()
			test.change(value)
			changed := makeLaunch(value, nil)
			changed.RuntimeBinaryPath = policy.RuntimeBinaryPath
			if err := changed.CheckRuntimeMechanism(policy); err == nil {
				t.Fatal("unsupported retained options were approved")
			}
		})
	}
	if err := makeLaunch(valid(), &specs.Hooks{}).CheckRuntimeMechanism(policy); err == nil {
		t.Fatal("hook structure accepted")
	}
	hooks := &specs.Hooks{CreateContainer: []specs.Hook{{Path: "/usr/bin/nvidia-cdi-hook", Args: []string{"nvidia-cdi-hook", "update-ldcache"}}}}
	approved, err := json.Marshal(hooks)
	if err != nil {
		t.Fatal(err)
	}
	qualified := policy
	qualified.HooksDigest = sha256.Sum256(approved)
	if err := makeLaunch(valid(), hooks).CheckRuntimeMechanism(qualified); err != nil {
		t.Fatal(err)
	}
	hooks.CreateContainer[0].Args = append(hooks.CreateContainer[0].Args, "unapproved")
	if err := makeLaunch(valid(), hooks).CheckRuntimeMechanism(qualified); err == nil {
		t.Fatal("altered hook was accepted")
	}
	if err := makeLaunch(valid(), nil).CheckRuntimeMechanism(qualified); err == nil {
		t.Fatal("missing required hook was accepted")
	}

	for _, extra := range []string{`"task_api_address":"unix:///unapproved",`, `"task_api_version":3,`} {
		changed := makeLaunch(valid(), nil)
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(append(append([]byte{'{'}, []byte(extra)...), changed.optionsEncoded[1:]...), &fields); err != nil {
			t.Fatal(err)
		}
		// Canonicalize through the actual options type so this exercises policy
		// rejection, not merely the serializer's field-order check.
		wire, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		var value options.Options
		if err := json.Unmarshal(wire, &value); err != nil {
			t.Fatal(err)
		}
		changed = makeLaunch(&value, nil)
		if err := changed.CheckRuntimeMechanism(policy); err == nil {
			t.Fatal("deprecated task API override accepted")
		}
	}
	value := valid()
	value.SystemdCgroup = true
	systemdPolicy := policy
	systemdPolicy.SystemdCgroup = true
	if err := makeLaunch(value, nil).CheckRuntimeMechanism(systemdPolicy); err != nil {
		t.Fatal("matching configured cgroup mode rejected")
	}
	for _, value := range []string{"", "relative", "/", "/run/../root", "/run/\x00/root"} {
		changed := policy
		changed.RuntimeStateRoot = value
		if err := launch.CheckRuntimeMechanism(changed); err == nil {
			t.Fatal("invalid trusted policy accepted")
		}
	}
	if err := (&RuntimeTaskLaunch{RuntimeBinaryPath: policy.RuntimeBinaryPath, ShimBinaryPath: policy.ShimBinaryPath}).CheckRuntimeMechanism(policy); err == nil {
		t.Fatal("public fields fabricated an observed mechanism")
	}
	if err := (*RuntimeTaskLaunch)(nil).CheckRuntimeMechanism(policy); err == nil {
		t.Fatal("nil launch accepted")
	}
}

func TestRuntimeTaskOptionsCanonicalEncoding(t *testing.T) {
	for _, encoded := range []string{"", "null", "[]", `{} {}`, `{"unknown":true}`, `{"binary_name":null}`, `{"binary_name":"x","binary_name":"x"}`, `{"binary_name":"x","BINARY_NAME":"x"}`, `{"BinaryName":"x"}`, `{"no_pivot_root":false}`, `{"binary_name":"x"} `, strings.Repeat(" ", (64<<10)+1)} {
		if value, err := parseRuntimeTaskOptions([]byte(encoded)); err == nil || value != nil {
			t.Fatal("noncanonical options accepted")
		}
	}
	for _, encoded := range [][]byte{[]byte(`{}`), []byte(`{"binary_name":"/usr/bin/runc","root":"/run/runc"}`)} {
		value, err := parseRuntimeTaskOptions(encoded)
		if err != nil {
			t.Fatal(err)
		}
		fresh, err := json.Marshal(value)
		if err != nil || !bytes.Equal(encoded, fresh) {
			t.Fatal("canonical options changed")
		}
	}
}

func TestRuntimeTaskBootstrapBinding(t *testing.T) {
	for _, data := range []string{`null`, `{}`, `{"version":2,"address":"unix:///shim","protocol":"ttrpc"}`, `{"version":3,"address":"unix:///shim","protocol":"grpc"}`, `{"version":3,"protocol":"ttrpc"}`, `{"version":3,"version":3,"address":"unix:///shim","protocol":"ttrpc"}`, `{"version":3,"address":"unix:///shim","protocol":"ttrpc","unknown":true}`, `{"version":3,"address":"unix:///shim","protocol":"ttrpc","capabilities":[1]}`, `{"version":3,"address":"unix:///shim","protocol":"ttrpc","metadata":{"extra":"value"}}`} {
		if err := validateRuntimeTaskBootstrap([]byte(data)); err == nil {
			t.Fatal("unsupported bootstrap record accepted")
		}
	}
	if err := validateRuntimeTaskBootstrap([]byte(`{"version":3,"address":"unix:///shim","protocol":"ttrpc"}`)); err != nil {
		t.Fatal(err)
	}
}
