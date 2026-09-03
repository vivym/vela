package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/vivym/vela/internal/h3stagemock"
	"golang.org/x/sys/unix"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	input := os.Stdin
	if ctx.Done() != nil {
		deadlineInput, err := duplicateDeadlineInput(os.Stdin)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Vela lab CPU thumbnail mock stopped: %v\n", err)
			os.Exit(1)
		}
		defer func() { _ = deadlineInput.Close() }()
		input = deadlineInput
	}
	if err := run(
		ctx, os.Args[1:], os.Getenv("VELA_MODEL_DRIVER_PROTOCOL"), input, os.Stdout,
	); err != nil && !errors.Is(err, context.Canceled) {
		_, _ = fmt.Fprintf(os.Stderr, "Vela lab CPU thumbnail mock stopped: %v\n", err)
		os.Exit(1)
	}
}

func run(
	ctx context.Context,
	arguments []string,
	protocol string,
	input io.Reader,
	output io.Writer,
) error {
	if len(arguments) != 0 {
		return errors.New("vela lab CPU thumbnail mock does not accept arguments")
	}
	if protocol != "stdio-json-v1" {
		return errors.New("VELA_MODEL_DRIVER_PROTOCOL must be stdio-json-v1")
	}
	return h3stagemock.Run(ctx, h3stagemock.Config{
		Component: "CPU_MEDIA", Mode: h3stagemock.ModeSuccess,
		Stdin: input, Stdout: output,
	})
}

func duplicateDeadlineInput(input *os.File) (*os.File, error) {
	if input == nil {
		return nil, errors.New("vela lab CPU thumbnail mock stdin is unavailable")
	}
	fileDescriptor, err := unix.Dup(int(input.Fd()))
	if err != nil {
		return nil, fmt.Errorf("duplicate Vela lab CPU thumbnail mock stdin: %w", err)
	}
	unix.CloseOnExec(fileDescriptor)
	if err := unix.SetNonblock(fileDescriptor, true); err != nil {
		_ = unix.Close(fileDescriptor)
		return nil, fmt.Errorf("enable Vela lab CPU thumbnail mock stdin deadlines: %w", err)
	}
	duplicated := os.NewFile(uintptr(fileDescriptor), input.Name())
	if duplicated == nil {
		_ = unix.Close(fileDescriptor)
		return nil, errors.New("open duplicated Vela lab CPU thumbnail mock stdin")
	}
	return duplicated, nil
}
