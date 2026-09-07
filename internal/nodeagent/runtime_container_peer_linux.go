package nodeagent

import (
	"errors"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

func openRuntimeContainerSocket(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

func runtimeContainerPeerUID(connection net.Conn) (uint32, error) {
	local, ok := connection.(*net.UnixConn)
	if !ok {
		return 0, errors.New("CRI connection is not a Unix socket")
	}
	raw, err := local.SyscallConn()
	if err != nil {
		return 0, err
	}
	var uid uint32
	var peerErr error
	err = raw.Control(func(fd uintptr) {
		peer, err := unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		peerErr = err
		if err == nil {
			uid = peer.Uid
		}
	})
	return uid, errors.Join(err, peerErr)
}
