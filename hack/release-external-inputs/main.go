// Command release-external-inputs freezes explicitly inventoried Kubernetes
// material into immutable, content-annotated copies. Values stay in memory and
// kubectl stdin; stdout contains only names, keys, identities and digests.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/vivym/vela/internal/h3launchevidence"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const revisionAnnotation = "vela.ai/release-revision"

type source struct {
	Kind            string   `json:"kind"`
	Namespace       string   `json:"namespace"`
	Name            string   `json:"name"`
	UID             string   `json:"uid"`
	ResourceVersion string   `json:"resource_version"`
	RequiredKeys    []string `json:"required_keys,omitempty"`
	Consumers       []string `json:"consumers"`
}
type plan struct {
	SchemaVersion int      `json:"schema_version"`
	Resources     []source `json:"resources"`
}
type entry struct {
	Source         source `json:"source"`
	SourceRevision string `json:"source_revision"`
	Name           string `json:"name"`
	Revision       string `json:"revision"`
}
type receipt struct {
	At        time.Time                                   `json:"at"`
	Applied   bool                                        `json:"applied"`
	Passed    bool                                        `json:"passed"`
	Resources []entry                                     `json:"resources"`
	Created   []string                                    `json:"created,omitempty"`
	Evidence  []h3launchevidence.ExternalResourceEvidence `json:"evidence,omitempty"`
	Error     string                                      `json:"error,omitempty"`
}
type candidate struct {
	entry  entry
	object any
}
type kubectl func(context.Context, any, ...string) ([]byte, error)

func invoke(ctx context.Context, object any, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	if object != nil {
		data, err := json.Marshal(object)
		if err != nil {
			return nil, errors.New("encode Kubernetes material")
		}
		cmd.Stdin = bytes.NewReader(data)
	}
	data, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("kubectl %s failed; payload and API error suppressed", args[0])
	}
	return data, nil
}
func readSource(ctx context.Context, call kubectl, item source) (any, error) {
	data, err := call(ctx, nil, "-n", item.Namespace, "get", item.Kind, item.Name, "-o", "json")
	if err != nil {
		return nil, err
	}
	if item.Kind == "Secret" {
		var value corev1.Secret
		if json.Unmarshal(data, &value) != nil {
			return nil, errors.New("decode source Secret")
		}
		if value.Name != item.Name || value.Namespace != item.Namespace || string(value.UID) != item.UID || value.ResourceVersion != item.ResourceVersion || value.DeletionTimestamp != nil {
			return nil, errors.New("source Secret identity changed")
		}
		return value, nil
	}
	var value corev1.ConfigMap
	if json.Unmarshal(data, &value) != nil {
		return nil, errors.New("decode source ConfigMap")
	}
	if value.Name != item.Name || value.Namespace != item.Namespace || string(value.UID) != item.UID || value.ResourceVersion != item.ResourceVersion || value.DeletionTimestamp != nil {
		return nil, errors.New("source ConfigMap identity changed")
	}
	return value, nil
}
func material(item source, live any) (candidate, error) {
	var originalRevision string
	var err error
	switch value := live.(type) {
	case corev1.Secret:
		originalRevision, err = h3launchevidence.SecretContentRevision(value)
	case corev1.ConfigMap:
		originalRevision, err = h3launchevidence.ConfigMapContentRevision(value)
	default:
		return candidate{}, errors.New("unsupported material")
	}
	if err != nil {
		return candidate{}, err
	}
	immutable := true
	meta := metav1.ObjectMeta{Name: item.Name, Namespace: item.Namespace, Labels: map[string]string{"app.kubernetes.io/part-of": "vela", "vela.ai/material-snapshot": "true"}}
	c := candidate{entry: entry{Source: item, SourceRevision: originalRevision}}
	switch value := live.(type) {
	case corev1.Secret:
		if value.Type != corev1.SecretTypeOpaque && value.Type != corev1.SecretTypeTLS && value.Type != corev1.SecretTypeDockerConfigJson {
			return candidate{}, errors.New("unsupported Secret type")
		}
		result := corev1.Secret{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}, ObjectMeta: meta, Immutable: &immutable, Type: value.Type, Data: map[string][]byte{}}
		for _, key := range item.RequiredKeys {
			data, exists := value.Data[key]
			if !exists {
				return candidate{}, fmt.Errorf("required key %s is absent", key)
			}
			result.Data[key] = slices.Clone(data)
		}
		selectedRevision, err := h3launchevidence.SecretContentRevision(result)
		if err != nil {
			return candidate{}, err
		}
		result.Name = snapshotName(item.Name, selectedRevision)
		c.entry.Name = result.Name
		c.entry.Revision, err = h3launchevidence.SecretContentRevision(result)
		if err != nil {
			return candidate{}, err
		}
		result.Annotations = map[string]string{revisionAnnotation: c.entry.Revision}
		c.object = result
	case corev1.ConfigMap:
		result := value.DeepCopy()
		result.ObjectMeta = meta
		result.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}
		result.Immutable = &immutable
		result.Name = snapshotName(item.Name, originalRevision)
		c.entry.Name = result.Name
		c.entry.Revision, err = h3launchevidence.ConfigMapContentRevision(*result)
		if err != nil {
			return candidate{}, err
		}
		result.Annotations = map[string]string{revisionAnnotation: c.entry.Revision}
		c.object = *result
	}
	return c, nil
}

func snapshotName(name, revision string) string {
	if len(name) > 230 {
		name = strings.TrimRight(name[:230], ".-")
	}
	return name + "-r-" + strings.TrimPrefix(revision, "sha256:")[:12]
}
func validate(p plan) error {
	if p.SchemaVersion != 1 || len(p.Resources) == 0 || len(p.Resources) > 128 {
		return errors.New("invalid source plan cardinality")
	}
	seen := map[string]bool{}
	for _, item := range p.Resources {
		if (item.Kind != "Secret" && item.Kind != "ConfigMap") || len(validation.IsDNS1123Label(item.Namespace)) != 0 || len(validation.IsDNS1123Subdomain(item.Name)) != 0 || item.UID == "" || item.ResourceVersion == "" || len(item.Consumers) == 0 {
			return errors.New("incomplete source identity")
		}
		identity := item.Kind + "/" + item.Namespace + "/" + item.Name
		if seen[identity] {
			return errors.New("duplicate source")
		}
		seen[identity] = true
		if (item.Kind == "Secret") != (len(item.RequiredKeys) > 0) {
			return errors.New("secret keys must be explicit; ConfigMaps bind whole content")
		}
		for i, key := range item.RequiredKeys {
			if len(validation.IsConfigMapKey(key)) != 0 || (i > 0 && key <= item.RequiredKeys[i-1]) {
				return errors.New("keys must be valid, unique and sorted")
			}
		}
	}
	return nil
}
func expected(c candidate) h3launchevidence.ExternalResourceExpectation {
	return h3launchevidence.ExternalResourceExpectation{Kind: c.entry.Source.Kind, Namespace: c.entry.Source.Namespace, Name: c.entry.Name, Revision: c.entry.Revision, RequiredKeys: c.entry.Source.RequiredKeys}
}
func verifyObject(c candidate, data []byte) ([]h3launchevidence.ExternalResourceEvidence, error) {
	var cms []corev1.ConfigMap
	var secrets []corev1.Secret
	if c.entry.Source.Kind == "Secret" {
		var value corev1.Secret
		if json.Unmarshal(data, &value) != nil {
			return nil, errors.New("decode snapshot Secret")
		}
		secrets = append(secrets, value)
	} else {
		var value corev1.ConfigMap
		if json.Unmarshal(data, &value) != nil {
			return nil, errors.New("decode snapshot ConfigMap")
		}
		cms = append(cms, value)
	}
	return h3launchevidence.VerifyExternalResources([]h3launchevidence.ExternalResourceExpectation{expected(c)}, cms, secrets)
}
func execute(ctx context.Context, call kubectl, p plan, apply bool, r *receipt) error {
	if err := validate(p); err != nil {
		return err
	}
	var candidates []candidate
	for _, item := range p.Resources {
		live, err := readSource(ctx, call, item)
		if err != nil {
			return err
		}
		c, err := material(item, live)
		if err != nil {
			return err
		}
		candidates = append(candidates, c)
		r.Resources = append(r.Resources, c.entry)
		current, err := call(ctx, nil, "-n", item.Namespace, "get", item.Kind, c.entry.Name, "--ignore-not-found", "-o", "json")
		if err != nil {
			return err
		}
		if len(bytes.TrimSpace(current)) > 0 {
			if _, err := verifyObject(c, current); err != nil {
				return fmt.Errorf("existing snapshot failed verification: %w", err)
			}
		} else if _, err := call(ctx, c.object, "create", "--dry-run=server", "-f", "-", "-o", "name"); err != nil {
			return err
		}
	}
	// Re-read every source before any mutation. Metadata/content drift requires a
	// new reviewed inventory rather than silently freezing different material.
	for _, c := range candidates {
		live, err := readSource(ctx, call, c.entry.Source)
		if err != nil {
			return err
		}
		again, err := material(c.entry.Source, live)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(c.entry, again.entry) {
			return errors.New("source content changed")
		}
	}
	if !apply {
		return nil
	}
	for _, c := range candidates {
		item := c.entry.Source
		current, err := call(ctx, nil, "-n", item.Namespace, "get", item.Kind, c.entry.Name, "--ignore-not-found", "-o", "json")
		if err != nil {
			return err
		}
		if len(bytes.TrimSpace(current)) == 0 {
			current, err = call(ctx, c.object, "create", "-f", "-", "-o", "json")
			if err != nil {
				return err
			}
			r.Created = append(r.Created, item.Kind+"/"+item.Namespace+"/"+c.entry.Name)
		}
		evidence, err := verifyObject(c, current)
		if err != nil {
			return err
		}
		fresh, err := call(ctx, nil, "-n", item.Namespace, "get", item.Kind, c.entry.Name, "-o", "json")
		if err != nil {
			return err
		}
		verified, err := verifyObject(c, fresh)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(evidence, verified) {
			return errors.New("snapshot identity changed during verification")
		}
		r.Evidence = append(r.Evidence, verified...)
	}
	return nil
}
func main() {
	apply := false
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "--apply" {
		apply = true
		args = args[1:]
	}
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: release-external-inputs [--apply] <source-plan.json>")
		os.Exit(2)
	}
	file, err := os.Open(args[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, "open source plan failed")
		os.Exit(1)
	}
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	var p plan
	err = decoder.Decode(&p)
	if err == nil {
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			err = errors.New("trailing or oversized input")
		}
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid source plan")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	r := receipt{At: time.Now().UTC(), Applied: apply}
	err = execute(ctx, invoke, p, apply, &r)
	r.Passed = err == nil
	if err != nil {
		r.Error = err.Error()
	}
	if json.NewEncoder(os.Stdout).Encode(r) != nil {
		os.Exit(1)
	}
	if err != nil {
		os.Exit(1)
	}
}
