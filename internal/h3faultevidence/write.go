package h3faultevidence

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
)

const EvidenceFileName = "state-event-fault-injection.json"

type OutputArtifact struct {
	Kind   ArtifactKind `json:"kind"`
	Ref    string       `json:"ref"`
	Digest string       `json:"digest"`
}

func WriteBundle(outputDirectory string, bundle Bundle) ([]OutputArtifact, error) {
	if outputDirectory == "" || !filepath.IsLocal(outputDirectory) && !filepath.IsAbs(outputDirectory) ||
		len(bundle.EvidenceBytes) == 0 || len(bundle.ArtifactBytes) != len(artifactContract) {
		return nil, invalid("fault campaign output or bundle is invalid")
	}
	if _, err := os.Lstat(outputDirectory); err == nil {
		return nil, invalid("output directory already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, invalid("inspect output directory: %v", err)
	}
	parent := filepath.Dir(outputDirectory)
	information, err := os.Stat(parent)
	if err != nil || !information.IsDir() {
		return nil, invalid("output parent must be an existing directory")
	}
	temporary, err := os.MkdirTemp(parent, ".vela-h3-fault-evidence-*")
	if err != nil {
		return nil, invalid("create temporary output directory: %v", err)
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(temporary)
		}
	}()
	if err := os.Chmod(temporary, 0o700); err != nil {
		return nil, invalid("protect temporary output directory: %v", err)
	}
	if err := writeEvidenceFile(temporary, EvidenceFileName, bundle.EvidenceBytes); err != nil {
		return nil, err
	}
	artifacts := make([]OutputArtifact, 0, len(artifactContract))
	for _, item := range artifactContract {
		encoded, present := bundle.ArtifactBytes[item.Kind]
		if !present || len(encoded) == 0 {
			return nil, invalid("bundle artifact %s is missing", item.Kind)
		}
		if err := writeEvidenceFile(temporary, item.Ref, encoded); err != nil {
			return nil, err
		}
		digest := sha256.Sum256(encoded)
		artifacts = append(artifacts, OutputArtifact{
			Kind: item.Kind, Ref: item.Ref, Digest: "sha256:" + hex.EncodeToString(digest[:]),
		})
	}
	if err := syncDirectory(temporary); err != nil {
		return nil, invalid("sync output candidate directory: %v", err)
	}
	if err := renameNoReplace(temporary, outputDirectory); err != nil {
		return nil, invalid("publish output directory atomically: %v", err)
	}
	published = true
	if err := syncDirectory(parent); err != nil {
		return nil, invalid("sync output parent directory: %v", err)
	}
	return artifacts, nil
}

func writeEvidenceFile(directory, name string, encoded []byte) error {
	path := filepath.Join(directory, name)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o400)
	if err != nil {
		return invalid("create evidence file %s: %v", name, err)
	}
	if _, err := file.Write(encoded); err != nil {
		_ = file.Close()
		return invalid("write evidence file %s: %v", name, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return invalid("sync evidence file %s: %v", name, err)
	}
	if err := file.Close(); err != nil {
		return invalid("close evidence file %s: %v", name, err)
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
}
