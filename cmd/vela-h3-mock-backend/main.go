package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/vivym/vela/internal/h3mockbackend"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := h3mockbackend.Run(ctx, os.Args[1:]); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		_, _ = fmt.Fprintf(os.Stderr, "vela H3 mock backend stopped: %v\n", err)
		if errors.Is(err, h3mockbackend.ErrInjectedFailure) {
			os.Exit(7)
		}
		os.Exit(1)
	}
}
