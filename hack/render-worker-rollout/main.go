// Command render-worker-rollout uses Fleet's production validator and renderer.
// It never applies resources or changes catalog states.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/vivym/vela/internal/fleetcontroller"
)

func main() {
	root := flag.String("directory", "", "directory containing rollout-input.json")
	existing := flag.String("existing", "", "existing Fleet rollouts.json to preserve and merge")
	flag.Parse()
	if err := render(*root, *existing); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func render(root, existing string) error {
	if existing == "" {
		return errors.New("existing Fleet rollouts file is required; supply an explicit empty envelope only for a new cluster")
	}
	if root == "" {
		return errors.New("directory is required")
	}
	input, err := os.ReadFile(filepath.Join(root, "rollout-input.json"))
	if err != nil {
		return err
	}
	var rollout fleetcontroller.ResidencyPlanRollout
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&rollout); err != nil {
		return err
	}
	if err = decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("expected one rollout document")
	}
	for i := range rollout.WorkerBundles {
		b := &rollout.WorkerBundles[i]
		b.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(*b)
		if err != nil {
			return err
		}
		for j := range rollout.ApprovedPlan.WorkerBundles {
			if rollout.ApprovedPlan.WorkerBundles[j].ID == b.WorkerBundleID {
				rollout.ApprovedPlan.WorkerBundles[j].LayoutDigest = b.RevisionDigest
			}
		}
	}
	rollout.ApprovedPlan.ContentDigest = ""
	data, err := json.Marshal(rollout.ApprovedPlan)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(data)
	rollout.ApprovedPlan.ContentDigest = hex.EncodeToString(hash[:])
	if err = fleetcontroller.ValidateResidencyPlanRollout(rollout); err != nil {
		return err
	}
	// Preserve every existing JSON entry, including withdrawn bundles and fields.
	merged := struct {
		SchemaVersion int               `json:"schema_version"`
		Rollouts      []json.RawMessage `json:"rollouts"`
	}{SchemaVersion: 1}
	namespace := rollout.WorkerBundles[0].Namespace
	if existing != "" {
		data, err = os.ReadFile(existing)
		if err != nil {
			return err
		}
		if _, err = fleetcontroller.DecodeResidencyPlanRollouts(data, namespace); err != nil {
			return err
		}
		if err = json.Unmarshal(data, &merged); err != nil {
			return err
		}
	}
	data, err = json.Marshal(rollout)
	if err != nil {
		return err
	}
	merged.Rollouts = append(merged.Rollouts, data)
	data, err = json.Marshal(merged)
	if err != nil {
		return err
	}
	if _, err = fleetcontroller.DecodeResidencyPlanRollouts(data, namespace); err != nil {
		return err
	}
	files := map[string][]byte{}
	add := func(name string, value any) error {
		if _, exists := files[name]; exists {
			return fmt.Errorf("duplicate output %s", name)
		}
		encoded, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return err
		}
		files[name] = append(encoded, '\n')
		return nil
	}
	for _, b := range rollout.WorkerBundles {
		manifest, err := fleetcontroller.WorkerBundleActuationManifest(b)
		if err != nil {
			return err
		}
		pods, claims, err := fleetcontroller.MaterializeWorkerInstanceLaunchResources(b)
		if err != nil {
			return err
		}
		dir := filepath.Join("bundles", b.WorkerBundleID.String())
		files[filepath.Join(dir, "bundle.json")] = manifest
		if err = add(filepath.Join(dir, "pods.json"), pods); err != nil {
			return err
		}
		if err = add(filepath.Join(dir, "claims.json"), claims); err != nil {
			return err
		}
		for _, w := range b.WorkerInstances {
			for _, m := range w.Members {
				launch, err := fleetcontroller.WorkerMemberLaunchManifest(b, w.ID, m.ID)
				if err != nil {
					return err
				}
				path := filepath.Join("workers", w.ID.String(), "members", m.ID.String(), "launch.json")
				if err = add(path, launch); err != nil {
					return err
				}
			}
		}
	}
	if err = add("rollouts.json", merged); err != nil {
		return err
	}
	if err = add("approved-plan.json", rollout.ApprovedPlan); err != nil {
		return err
	}
	for name := range files {
		if _, err = os.Lstat(filepath.Join(root, name)); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("output already exists or cannot be inspected: %s", name)
		}
	}
	for name, content := range files {
		path := filepath.Join(root, name)
		if err = os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return err
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		_, err = f.Write(content)
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}
