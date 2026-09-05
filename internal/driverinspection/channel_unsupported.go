//go:build !darwin && !linux

package driverinspection

import (
	"errors"
	"net"
	"os"
)

func Pair() (*net.UnixConn, *os.File, error) {
	return nil, nil, errors.New("driver inspection requires Darwin or Linux")
}

func OpenInherited(value string) (*net.UnixConn, error) {
	if value == "" {
		return nil, nil
	}
	return nil, errors.New("driver inspection requires Darwin or Linux")
}
