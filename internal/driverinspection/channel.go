package driverinspection

import (
	"net"
	"os"

	"github.com/vivym/vela/internal/driverchannel"
)

func Pair() (*net.UnixConn, *os.File, error) { return driverchannel.Pair() }

func OpenInherited(value string) (*net.UnixConn, error) {
	return driverchannel.OpenInherited(value, 3)
}
