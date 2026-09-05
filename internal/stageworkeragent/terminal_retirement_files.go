package stageworkeragent

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path"
	"slices"
	"strings"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

type terminalRetirementDirectory struct {
	Root       int                          `json:"root"`
	Path       string                       `json:"path"`
	Components []admissionDirectoryIdentity `json:"components"`
}

// Signed terminal state excludes new materialization; completed command replays
// use durable receipts. Only the explicitly selected attempt-owned contract lets
// that state authorize output-directory retirement alongside unused inputs.
func terminalRetirementDirectories(value *velav1.StageTerminalDisposition) []terminalRetirementDirectory {
	result := []terminalRetirementDirectory{{Root: 1, Path: path.Join("stage-runs", value.GetStageRunId())}}
	attempts := make([]string, 0, len(value.GetAllocations()))
	for _, allocation := range value.GetAllocations() {
		attempts = append(attempts, allocation.GetStageAttemptId())
	}
	slices.Sort(attempts)
	for _, attempt := range slices.Compact(attempts) {
		result = append(result, terminalRetirementDirectory{Root: 2, Path: attempt})
	}
	return result
}

func validateRetirementDirectories(value *velav1.StageTerminalDisposition, directories []terminalRetirementDirectory) error {
	expected := terminalRetirementDirectories(value)
	if len(directories) != len(expected) {
		return ErrScratchRetirementUnproven
	}
	for index, directory := range directories {
		if directory.Root != expected[index].Root || directory.Path != expected[index].Path ||
			len(directory.Components) > len(strings.Split(directory.Path, "/")) {
			return ErrScratchRetirementUnproven
		}
		for _, component := range directory.Components {
			if component.Inode == 0 {
				return ErrScratchRetirementUnproven
			}
		}
	}
	return nil
}

func (gate *FileAssignmentAdmission) bindTerminalDirectories(value *velav1.StageTerminalDisposition) ([]terminalRetirementDirectory, error) {
	directories := terminalRetirementDirectories(value)
	for index, directory := range directories {
		binding, err := bindScratchDirectory(gate.files.roots[directory.Root], directory.Path)
		if err != nil {
			return nil, err
		}
		for _, component := range binding.components {
			directories[index].Components = append(directories[index].Components, admissionFileIdentity(component.info))
		}
		err = validateTerminalDirectoryTree(binding)
		binding.close()
		if err != nil {
			return nil, err
		}
	}
	return directories, nil
}

func validateTerminalDirectoryTree(binding *scratchDirectoryBinding) error {
	for _, component := range binding.components {
		if component.info.Mode().Perm()&0o022 != 0 {
			return errors.New("retirement directory is writable by another principal")
		}
	}
	if binding.target == nil {
		return nil
	}
	return fs.WalkDir(binding.target.FS(), ".", func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if (!info.IsDir() && !info.Mode().IsRegular()) || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 {
			return errors.New("retirement rejects special, linked or externally writable scratch")
		}
		return nil
	})
}

func (gate *FileAssignmentAdmission) retireTerminalDirectories(ctx context.Context, directories []terminalRetirementDirectory) error {
	bindings := make([]*scratchDirectoryBinding, 0, len(directories))
	defer func() {
		for _, binding := range bindings {
			binding.close()
		}
	}()
	// Bind and check every target before deleting any target. Missing components
	// may reflect partial prior deletion; replacement/new components never do.
	for _, directory := range directories {
		if err := ctx.Err(); err != nil {
			return err
		}
		binding, err := bindScratchDirectory(gate.files.roots[directory.Root], directory.Path)
		if err != nil {
			return err
		}
		bindings = append(bindings, binding)
		if len(binding.components) > len(directory.Components) {
			return errors.New("retirement namespace appeared after proof checkpoint")
		}
		for index, component := range binding.components {
			if admissionFileIdentity(component.info) != directory.Components[index] {
				return errors.New("retirement namespace was replaced after proof checkpoint")
			}
		}
		if err := validateTerminalDirectoryTree(binding); err != nil {
			return err
		}
	}
	for index, binding := range bindings {
		for _, component := range binding.components {
			current, err := component.parent.Lstat(component.name)
			if err != nil || !os.SameFile(current, component.info) {
				return errors.Join(err, errors.New("retirement directory changed before deletion"))
			}
		}
		if binding.target != nil {
			entries, err := fs.ReadDir(binding.target.FS(), ".")
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := binding.target.RemoveAll(entry.Name()); err != nil {
					return err
				}
			}
		}
		first := 0
		if directories[index].Root == 1 {
			first = 1 // Keep the shared stage-runs parent.
		}
		if err := binding.pruneEmpty(first); err != nil {
			return err
		}
		if _, err := gate.files.roots[directories[index].Root].Lstat(directories[index].Path); !errors.Is(err, os.ErrNotExist) {
			return errors.Join(err, errors.New("retirement namespace remains after cleanup"))
		}
		if gate.retirementAfterDirectory != nil {
			if err := gate.retirementAfterDirectory(index); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}
