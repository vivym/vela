//go:build linux

// vela-validation-runtime-caller is a disposable caller used by the
// validation-only CRI launcher. It implements only the authenticated local
// challenge exchange; it grants no Runtime or journal authority.
package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
)

const callerProtocol = "vela-runtime-caller-v1\x00"

func main() {
	socket := os.Getenv("VELA_VALIDATION_STARTUP_SOCKET")
	if !filepath.IsAbs(socket) || filepath.Clean(socket) != socket {
		fatal("VELA_VALIDATION_STARTUP_SOCKET must be a canonical absolute path")
	}
	payload, err := callerPayload()
	if err != nil {
		fatal(err.Error())
	}
	connection, err := net.Dial("unixpacket", socket)
	if err != nil {
		fatal(fmt.Sprintf("connect validation startup socket: %v", err))
	}
	defer connection.Close()
	challenge := make([]byte, len(callerProtocol)+32)
	if _, err := io.ReadFull(connection, challenge); err != nil {
		fatal(fmt.Sprintf("read validation startup challenge: %v", err))
	}
	if string(challenge[:len(callerProtocol)]) != callerProtocol {
		fatal("validation startup challenge protocol mismatch")
	}
	if _, err := connection.Write(append(challenge, payload...)); err != nil {
		fatal(fmt.Sprintf("send validation startup payload: %v", err))
	}
	// Keep the process alive while Node observes the original CRI task and
	// transfers namespace ownership. Node closes the connection after the one
	// startup exchange; a response is intentionally not interpreted here.
	_, _ = io.Copy(io.Discard, connection)
}

func callerPayload() ([]byte, error) {
	if path := os.Getenv("VELA_VALIDATION_PAYLOAD_FILE"); path != "" {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return nil, errors.New("VELA_VALIDATION_PAYLOAD_FILE must be canonical")
		}
		return os.ReadFile(path)
	}
	if encoded := os.Getenv("VELA_VALIDATION_PAYLOAD_B64"); encoded != "" {
		return base64.StdEncoding.DecodeString(encoded)
	}
	return nil, errors.New("validation caller payload is not configured")
}

func fatal(message string) {
	_, _ = fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
