package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/vivym/vela/internal/h3stagemock"
	"golang.org/x/sys/unix"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil && err != context.Canceled {
		_, _ = fmt.Fprintf(os.Stderr, "H3 Stage mock runtime stopped: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	if len(os.Args) != 1 {
		return fmt.Errorf("H3 Stage mock runtime does not accept arguments")
	}
	if os.Getenv("VELA_MODEL_DRIVER_PROTOCOL") != "stdio-json-v1" {
		return fmt.Errorf("VELA_MODEL_DRIVER_PROTOCOL must be stdio-json-v1")
	}
	component, err := componentFromExecutable(filepath.Base(os.Args[0]))
	if err != nil {
		return err
	}
	mode := h3stagemock.Mode(os.Getenv("VELA_H3_STAGE_MOCK_MODE"))
	if mode == "" {
		mode = h3stagemock.ModeSuccess
	}
	input := os.Stdin
	if ctx.Done() != nil {
		deadlineInput, err := duplicateDeadlineInput(os.Stdin)
		if err != nil {
			return err
		}
		defer func() { _ = deadlineInput.Close() }()
		input = deadlineInput
	}
	return h3stagemock.Run(ctx, h3stagemock.Config{
		Component: component, Mode: mode, Stdin: input, Stdout: os.Stdout,
	})
}

func duplicateDeadlineInput(input *os.File) (*os.File, error) {
	if input == nil {
		return nil, fmt.Errorf("H3 Stage mock stdin is unavailable")
	}
	fileDescriptor, err := unix.Dup(int(input.Fd()))
	if err != nil {
		return nil, fmt.Errorf("duplicate H3 Stage mock stdin: %w", err)
	}
	unix.CloseOnExec(fileDescriptor)
	if err := unix.SetNonblock(fileDescriptor, true); err != nil {
		_ = unix.Close(fileDescriptor)
		return nil, fmt.Errorf("enable H3 Stage mock stdin deadlines: %w", err)
	}
	duplicated := os.NewFile(uintptr(fileDescriptor), input.Name())
	if duplicated == nil {
		_ = unix.Close(fileDescriptor)
		return nil, fmt.Errorf("open duplicated H3 Stage mock stdin")
	}
	return duplicated, nil
}

func componentFromExecutable(name string) (string, error) {
	switch name {
	case "h3-encoder":
		return "ENCODER", nil
	case "h3-dit":
		return "DIT", nil
	case "h3-vae-decoder":
		return "VAE_DECODER", nil
	default:
		return "", fmt.Errorf("H3 Stage mock executable identity is invalid")
	}
}
