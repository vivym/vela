package h3launchevidence

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const externalResourceRevisionAnnotation = "vela.ai/release-revision"

type externalResourceKey struct {
	kind      string
	namespace string
	name      string
}

type externalResourceContent struct {
	SchemaVersion int               `json:"schema_version"`
	Kind          string            `json:"kind"`
	Namespace     string            `json:"namespace"`
	Name          string            `json:"name"`
	SecretType    corev1.SecretType `json:"secret_type,omitempty"`
	StringData    map[string]string `json:"string_data,omitempty"`
	BinaryData    map[string][]byte `json:"binary_data,omitempty"`
}

// VerifyExternalResources binds canonical release declarations to exact live
// Kubernetes objects while returning only sanitized identities and digests.
func VerifyExternalResources(
	expected []ExternalResourceExpectation,
	configMaps []corev1.ConfigMap,
	secrets []corev1.Secret,
) ([]ExternalResourceEvidence, error) {
	if len(expected) == 0 || len(configMaps)+len(secrets) != len(expected) {
		return nil, invalid("external resource cardinality does not match the canonical release")
	}
	expectations := make(map[externalResourceKey]ExternalResourceExpectation, len(expected))
	for _, resource := range expected {
		if err := validateExternalResourceExpectation(resource); err != nil {
			return nil, err
		}
		key := externalResourceKey{kind: resource.Kind, namespace: resource.Namespace, name: resource.Name}
		if _, duplicate := expectations[key]; duplicate {
			return nil, invalid("external resource %s %s/%s is duplicated", resource.Kind, resource.Namespace, resource.Name)
		}
		resource.RequiredKeys = append([]string(nil), resource.RequiredKeys...)
		expectations[key] = resource
	}
	result := make([]ExternalResourceEvidence, 0, len(expected))
	seen := make(map[externalResourceKey]struct{}, len(expected))
	for _, value := range configMaps {
		key := externalResourceKey{kind: "ConfigMap", namespace: value.Namespace, name: value.Name}
		expectation, ok := expectations[key]
		if !ok {
			return nil, invalid("live ConfigMap %s/%s is not declared by the canonical release", value.Namespace, value.Name)
		}
		if err := validateExternalObject(expectation, value.UID, value.ResourceVersion, value.DeletionTimestamp,
			value.Immutable, value.Annotations); err != nil {
			return nil, err
		}
		digest, err := digestExternalResourceContent(externalResourceContent{
			SchemaVersion: 1, Kind: "ConfigMap", Namespace: value.Namespace, Name: value.Name,
			StringData: value.Data, BinaryData: value.BinaryData,
		})
		if err != nil {
			return nil, err
		}
		if digest != expectation.Revision {
			return nil, invalid("live ConfigMap %s/%s content does not match the canonical release", value.Namespace, value.Name)
		}
		result = append(result, externalResourceEvidence(expectation, string(value.UID), value.ResourceVersion, digest))
		seen[key] = struct{}{}
	}
	for _, value := range secrets {
		key := externalResourceKey{kind: "Secret", namespace: value.Namespace, name: value.Name}
		expectation, ok := expectations[key]
		if !ok {
			return nil, invalid("live Secret %s/%s is not declared by the canonical release", value.Namespace, value.Name)
		}
		if err := validateExternalObject(expectation, value.UID, value.ResourceVersion, value.DeletionTimestamp,
			value.Immutable, value.Annotations); err != nil {
			return nil, err
		}
		keys := make([]string, 0, len(value.Data))
		for key := range value.Data {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if !reflect.DeepEqual(keys, expectation.RequiredKeys) {
			return nil, invalid("live Secret %s/%s key set does not match the canonical release", value.Namespace, value.Name)
		}
		digest, err := digestExternalResourceContent(externalResourceContent{
			SchemaVersion: 1, Kind: "Secret", Namespace: value.Namespace, Name: value.Name,
			SecretType: value.Type, BinaryData: value.Data,
		})
		if err != nil {
			return nil, err
		}
		if digest != expectation.Revision {
			return nil, invalid("live Secret %s/%s content does not match the canonical release", value.Namespace, value.Name)
		}
		result = append(result, externalResourceEvidence(expectation, string(value.UID), value.ResourceVersion, digest))
		seen[key] = struct{}{}
	}
	if len(seen) != len(expectations) {
		return nil, invalid("one or more canonical external resources are missing from the live cluster")
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Kind != result[right].Kind {
			return result[left].Kind < result[right].Kind
		}
		if result[left].Namespace != result[right].Namespace {
			return result[left].Namespace < result[right].Namespace
		}
		return result[left].Name < result[right].Name
	})
	return result, nil
}

func validateExternalResourceExpectation(resource ExternalResourceExpectation) error {
	if (resource.Kind != "ConfigMap" && resource.Kind != "Secret") ||
		len(validation.IsDNS1123Label(resource.Namespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(resource.Name)) != 0 ||
		!sha256Pattern.MatchString(resource.Revision) {
		return invalid("canonical external resource declaration is invalid")
	}
	if resource.Kind == "ConfigMap" && len(resource.RequiredKeys) != 0 {
		return invalid("canonical external ConfigMap must not declare Secret keys")
	}
	if resource.Kind == "Secret" && len(resource.RequiredKeys) == 0 {
		return invalid("canonical external Secret has no required keys")
	}
	for index, key := range resource.RequiredKeys {
		if len(validation.IsConfigMapKey(key)) != 0 || strings.TrimSpace(key) != key ||
			(index > 0 && key <= resource.RequiredKeys[index-1]) {
			return invalid("canonical external Secret keys are invalid or not strictly sorted")
		}
	}
	return nil
}

func validateExternalObject(
	expected ExternalResourceExpectation,
	uid types.UID,
	resourceVersion string,
	deletionTimestamp *metav1.Time,
	immutable *bool,
	annotations map[string]string,
) error {
	if uid == "" || resourceVersion == "" || deletionTimestamp != nil || immutable == nil || !*immutable ||
		annotations[externalResourceRevisionAnnotation] != expected.Revision {
		return invalid("live %s %s/%s identity, immutability, or revision is invalid", expected.Kind, expected.Namespace, expected.Name)
	}
	return nil
}

func digestExternalResourceContent(content externalResourceContent) (string, error) {
	encoded, err := json.Marshal(content)
	if err != nil {
		return "", fmt.Errorf("%w: encode external resource content: %v", ErrInvalidLaunchEvidence, err)
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func externalResourceEvidence(
	expected ExternalResourceExpectation,
	uid string,
	resourceVersion string,
	digest string,
) ExternalResourceEvidence {
	return ExternalResourceEvidence{
		Kind: expected.Kind, Namespace: expected.Namespace, Name: expected.Name,
		Revision: expected.Revision, RequiredKeys: append([]string(nil), expected.RequiredKeys...),
		UID: uid, ResourceVersion: resourceVersion, ContentDigest: digest,
	}
}
