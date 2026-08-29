package releaseartifacts

import (
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const h3BackendArtifactName = "h3-backend"

func VerifyH3Backend(contextDirectory, expectedSHA256 string) error {
	if !filepath.IsAbs(contextDirectory) {
		return errors.New("H3 backend context must be an absolute canonical directory")
	}
	contextDirectory, err := canonicalExistingDirectory(contextDirectory)
	if err != nil {
		return fmt.Errorf("resolve H3 backend context: %w", err)
	}
	if len(expectedSHA256) != sha256.Size*2 || expectedSHA256 != strings.ToLower(expectedSHA256) {
		return errors.New("H3 backend SHA-256 must be 64 lowercase hexadecimal characters")
	}
	if _, err := hex.DecodeString(expectedSHA256); err != nil {
		return errors.New("H3 backend SHA-256 must be 64 lowercase hexadecimal characters")
	}

	entries, err := os.ReadDir(contextDirectory)
	if err != nil {
		return fmt.Errorf("read H3 backend context: %w", err)
	}
	if len(entries) != 1 || entries[0].Name() != h3BackendArtifactName ||
		!entries[0].Type().IsRegular() {
		return errors.New("H3 backend context must contain exactly one regular h3-backend file")
	}
	backendPath := filepath.Join(contextDirectory, h3BackendArtifactName)
	information, err := os.Lstat(backendPath)
	if err != nil {
		return fmt.Errorf("stat H3 backend: %w", err)
	}
	if !information.Mode().IsRegular() || information.Size() <= 0 ||
		information.Mode().Perm()&0o111 == 0 {
		return errors.New("H3 backend must be a non-empty executable regular file")
	}
	digest, _, err := digestFile(backendPath)
	if err != nil {
		return fmt.Errorf("digest H3 backend: %w", err)
	}
	if digest != expectedSHA256 {
		return errors.New("H3 backend SHA-256 does not match the declared digest")
	}

	binary, err := elf.Open(backendPath)
	if err != nil {
		return fmt.Errorf("open H3 backend ELF: %w", err)
	}
	defer func() { _ = binary.Close() }()
	if binary.Class != elf.ELFCLASS64 || binary.Data != elf.ELFDATA2LSB ||
		binary.Machine != elf.EM_X86_64 ||
		(binary.Type != elf.ET_EXEC && binary.Type != elf.ET_DYN) {
		return errors.New("H3 backend must be a little-endian ELF64 x86-64 executable")
	}
	if binary.Entry == 0 {
		return errors.New("H3 backend ELF entrypoint is absent")
	}
	for _, program := range binary.Progs {
		if program.Type == elf.PT_LOAD && program.Flags&elf.PF_X != 0 &&
			binary.Entry >= program.Vaddr && binary.Entry-program.Vaddr < program.Filesz {
			return nil
		}
	}
	return errors.New("H3 backend ELF entrypoint is not in an executable load segment")
}
