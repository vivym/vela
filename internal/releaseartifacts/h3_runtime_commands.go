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

var h3RuntimeCommandNames = [...]string{
	"h3-encoder",
	"h3-dit",
	"h3-vae-decoder",
}

// VerifyH3RuntimeCommands validates the complete command context installed in
// the production H3 ModelRuntime image.
func VerifyH3RuntimeCommands(
	contextDirectory string,
	encoderSHA256 string,
	ditSHA256 string,
	vaeDecoderSHA256 string,
) error {
	if !filepath.IsAbs(contextDirectory) {
		return errors.New("H3 runtime command context must be an absolute canonical directory")
	}
	contextDirectory, err := canonicalExistingDirectory(contextDirectory)
	if err != nil {
		return fmt.Errorf("resolve H3 runtime command context: %w", err)
	}
	digests := [...]string{encoderSHA256, ditSHA256, vaeDecoderSHA256}
	for index, digest := range digests {
		if !validH3CommandDigest(digest) {
			return fmt.Errorf(
				"%s SHA-256 must be 64 lowercase hexadecimal characters",
				h3RuntimeCommandNames[index],
			)
		}
	}

	entries, err := os.ReadDir(contextDirectory)
	if err != nil {
		return fmt.Errorf("read H3 runtime command context: %w", err)
	}
	if len(entries) != len(h3RuntimeCommandNames) {
		return errors.New("H3 runtime command context must contain exactly three commands")
	}
	digestByName := make(map[string]string, len(h3RuntimeCommandNames))
	for index, commandName := range h3RuntimeCommandNames {
		digestByName[commandName] = digests[index]
	}
	for _, entry := range entries {
		digest, expected := digestByName[entry.Name()]
		if !expected || !entry.Type().IsRegular() {
			return errors.New("H3 runtime command context inventory is invalid")
		}
		if err := verifyH3RuntimeCommand(
			filepath.Join(contextDirectory, entry.Name()),
			entry.Name(),
			digest,
		); err != nil {
			return err
		}
	}
	return nil
}

func validH3CommandDigest(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func verifyH3RuntimeCommand(path, name, expectedSHA256 string) error {
	information, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("stat %s: %w", name, err)
	}
	if !information.Mode().IsRegular() || information.Size() <= 0 ||
		information.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("%s must be a non-empty executable regular file", name)
	}
	digest, _, err := digestFile(path)
	if err != nil {
		return fmt.Errorf("digest %s: %w", name, err)
	}
	if digest != expectedSHA256 {
		return fmt.Errorf("%s SHA-256 does not match the declared digest", name)
	}

	binary, err := elf.Open(path)
	if err != nil {
		return fmt.Errorf("open %s ELF: %w", name, err)
	}
	defer func() { _ = binary.Close() }()
	if binary.Class != elf.ELFCLASS64 || binary.Data != elf.ELFDATA2LSB ||
		binary.Machine != elf.EM_X86_64 ||
		(binary.Type != elf.ET_EXEC && binary.Type != elf.ET_DYN) {
		return fmt.Errorf("%s must be a little-endian ELF64 x86-64 executable", name)
	}
	if binary.Entry == 0 {
		return fmt.Errorf("%s ELF entrypoint is absent", name)
	}
	for _, program := range binary.Progs {
		if program.Type == elf.PT_LOAD && program.Flags&elf.PF_X != 0 &&
			binary.Entry >= program.Vaddr && binary.Entry-program.Vaddr < program.Filesz {
			return nil
		}
	}
	return fmt.Errorf("%s ELF entrypoint is not in an executable load segment", name)
}
