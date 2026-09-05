//go:build !darwin && !linux

package driverchannel

import (
	"errors"
	"net"
	"os"
)

func Pair() (*net.UnixConn, *os.File, error) {
	return nil, nil, errors.New("driver channel requires Darwin or Linux")
}

func OpenInherited(value string, _ int) (*net.UnixConn, error) {
	if value == "" {
		return nil, nil
	}
	return nil, errors.New("driver channel requires Darwin or Linux")
}
