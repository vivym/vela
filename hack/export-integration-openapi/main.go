// Export the consumer-facing subset without changing the authoritative API.
// Run from the repository root: go run ./hack/export-integration-openapi
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
)

const (
	source      = "api/openapi/vela.yaml"
	target      = "docs/api-integration.openapi.json"
	projectPath = "/v1/projects/{project_id}"
)

func main() {
	check := flag.Bool("check", false, "check that the exported document is current")
	flag.Parse()
	if err := run(*check); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(check bool) error {
	raw, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	loader := openapi3.NewLoader()
	spec, err := loader.LoadFromData(raw)
	if err != nil {
		return err
	}
	if err := spec.Validate(context.Background()); err != nil {
		return err
	}
	wire, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	var document map[string]any
	if err := json.Unmarshal(wire, &document); err != nil {
		return err
	}
	paths := document["paths"].(map[string]any)
	core := map[string]bool{
		projectPath + "/jobs":                    true,
		projectPath + "/jobs/{job_id}":           true,
		projectPath + "/jobs/{job_id}/cancel":    true,
		projectPath + "/jobs/{job_id}/artifacts": true,
	}
	for path := range paths {
		if !core[path] && path != projectPath+"/webhook-subscriptions" &&
			!strings.HasPrefix(path, projectPath+"/webhook-subscriptions/") {
			delete(paths, path)
		}
	}

	// Resolve the transitive closure of component references from these paths.
	components := document["components"].(map[string]any)
	required := map[string]bool{}
	var visit func(any) error
	visit = func(value any) error {
		switch typed := value.(type) {
		case map[string]any:
			if ref, ok := typed["$ref"].(string); ok && !required[ref] {
				parts := strings.Split(ref, "/")
				if len(parts) != 4 || parts[0] != "#" || parts[1] != "components" {
					return fmt.Errorf("unsupported reference: %s", ref)
				}
				group, ok := components[parts[2]].(map[string]any)
				if !ok || group[parts[3]] == nil {
					return fmt.Errorf("unresolved reference: %s", ref)
				}
				required[ref] = true
				if err := visit(group[parts[3]]); err != nil {
					return err
				}
			}
			for _, child := range typed {
				if err := visit(child); err != nil {
					return err
				}
			}
		case []any:
			for _, child := range typed {
				if err := visit(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(paths); err != nil {
		return err
	}
	for category, entries := range components {
		if category == "securitySchemes" {
			continue
		}
		group := entries.(map[string]any)
		for name := range group {
			if !required["#/components/"+category+"/"+name] {
				delete(group, name)
			}
		}
		if len(group) == 0 {
			delete(components, category)
		}
	}
	document["info"].(map[string]any)["title"] = "Vela Video API - Relay Integration"
	document["servers"] = []map[string]string{{
		"url":         "https://vela-api.example.com/api",
		"description": "Example only; replace with the supplied BASE_URL including its gateway prefix.",
	}}
	document["x-vela-source"] = source
	document["x-vela-source-sha256"] = fmt.Sprintf("%x", sha256.Sum256(raw))
	output, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return err
	}
	output = append(output, '\n')
	exported, err := loader.LoadFromData(output)
	if err != nil {
		return err
	}
	if err := exported.Validate(context.Background()); err != nil {
		return err
	}
	if check {
		existing, err := os.ReadFile(target)
		if err != nil {
			return err
		}
		if !bytes.Equal(existing, output) {
			return fmt.Errorf("%s is stale; run go run ./hack/export-integration-openapi", target)
		}
		fmt.Println("Integration OpenAPI is valid and current.")
		return nil
	}
	return os.WriteFile(target, output, 0o644)
}
