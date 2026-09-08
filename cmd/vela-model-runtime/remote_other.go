//go:build !linux

package main

import (
	"context"
	"errors"
	"io"
)

func runRemote(context.Context, []string, io.Writer) error {
	return errors.New("remote Runtime startup requires Linux process authentication")
}
