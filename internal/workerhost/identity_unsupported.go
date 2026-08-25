//go:build !linux && !darwin

package workerhost

import (
	"errors"
	"net"
	"os"
)

func rootIdentity(string) (uint64, uint64, error) {
	return 0, 0, errors.New("Worker host identity probing is unsupported on this platform")
}

func fileOwner(os.FileInfo) (uint32, uint32, error) {
	return 0, 0, errors.New("Worker host owner probing is unsupported on this platform")
}

func peerUID(net.Conn) (uint32, error) {
	return 0, errors.New("Worker host peer credentials are unsupported on this platform")
}
