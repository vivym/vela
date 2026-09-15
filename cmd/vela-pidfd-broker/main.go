package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil { //nolint:staticcheck // The non-Linux stub always rejects; Linux run can succeed.
		fmt.Fprintf(os.Stderr, "vela-pidfd-broker stopped: %v\n", err)
		os.Exit(1)
	}
}
