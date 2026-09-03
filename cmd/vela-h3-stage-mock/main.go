package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/vivym/vela/internal/h3stagemock"
)

func main() {
	if err := run(context.Background()); err != nil {
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
	return h3stagemock.Run(ctx, h3stagemock.Config{
		Component: component, Mode: mode, Stdin: os.Stdin, Stdout: os.Stdout,
	})
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
