package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"os"

	"github.com/vivym/vela/internal/modelruntime"
)

func runRemote(ctx context.Context, arguments []string, stderr io.Writer) error {
	if ctx == nil || stderr == nil {
		return errors.New("remote Runtime requires context and error output")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := protectRuntimeProcess(); err != nil {
		return err
	}
	flags := flag.NewFlagSet("vela-model-runtime serve-remote", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("bootstrap-file", "", "root-published read-only remote startup snapshot")
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *path == "" || os.Geteuid() == 0 || os.Getegid() == 0 {
		return errors.New("remote Runtime requires a non-root process and one explicit bootstrap file")
	}
	wire, err := modelruntime.ReadRemoteRuntimeBootstrapFile(ctx, *path)
	if err != nil {
		return err
	}
	config, err := modelruntime.RemoteRuntimeServerConfig(wire)
	if err != nil {
		return err
	}
	server, err := modelruntime.StartRuntimeServer(ctx, config)
	if err != nil {
		return err
	}
	return waitRuntimeServer(ctx, server)
}
