package nodeagent

import (
	"errors"
	"net"

	"golang.org/x/sys/unix"
)

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
		peer, err := unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		peerErr = err
		if err == nil {
			// Darwin sys/ucred.h defines XUCRED_VERSION as 0.
			if peer.Version != 0 {
				peerErr = errors.New("CRI peer credential version is unsupported")
			}
			uid = peer.Uid
		}
	})
	return uid, errors.Join(err, peerErr)
}
