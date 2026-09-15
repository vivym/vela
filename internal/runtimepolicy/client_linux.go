//go:build linux

package runtimepolicy

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
	"golang.org/x/sys/unix"
)

// Client is the Node-side transport to an independently supervised policy
// issuer. The issuer is deliberately outside this repository's launcher and
// must authenticate the Fleet reservation before signing a Reply.
type Client struct {
	SocketPath string
	PublicKey  ed25519.PublicKey
}

func NewClient(socketPath, publicKeyPath string) (*Client, error) {
	if !canonical(socketPath) || socketPath == "/" || !canonical(publicKeyPath) || publicKeyPath == "/" {
		return nil, errors.New("runtime policy issuer paths are not canonical")
	}
	if _, err := securefile.ResolveTrustedDirectory(filepath.Dir(publicKeyPath)); err != nil {
		return nil, err
	}
	info, err := os.Lstat(publicKeyPath)
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 {
		return nil, errors.New("runtime policy public key must be root-owned")
	}
	key, err := securefile.Read(publicKeyPath, ed25519.PublicKeySize, true)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return nil, errors.New("runtime policy issuer public key is unavailable")
	}
	return &Client{SocketPath: socketPath, PublicKey: ed25519.PublicKey(bytes.Clone(key))}, nil
}

func (client *Client) Issue(ctx context.Context, request Request) (Reply, error) {
	if ctx == nil || client == nil || !request.Valid() {
		return Reply{}, errors.New("runtime policy request is invalid")
	}
	if _, err := securefile.ResolveTrustedDirectory(filepath.Dir(client.SocketPath)); err != nil {
		return Reply{}, err
	}
	connection, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, "unixpacket", client.SocketPath)
	if err != nil {
		return Reply{}, err
	}
	defer connection.Close()
	unixConnection, ok := connection.(*net.UnixConn)
	if !ok {
		return Reply{}, errors.New("runtime policy issuer transport is not Unix")
	}
	if err := requireRootPeer(unixConnection); err != nil {
		return Reply{}, err
	}
	deadline := time.Now().Add(10 * time.Second)
	if limit, ok := ctx.Deadline(); ok && limit.Before(deadline) {
		deadline = limit
	}
	if err := unixConnection.SetDeadline(deadline); err != nil {
		return Reply{}, err
	}
	stop := context.AfterFunc(ctx, func() { _ = unixConnection.SetDeadline(time.Now()) })
	defer stop()
	wire, err := json.Marshal(request)
	if err != nil || len(wire) == 0 || len(wire) > 64<<10 {
		return Reply{}, errors.New("runtime policy request frame is invalid")
	}
	if written, err := unixConnection.Write(wire); err != nil {
		return Reply{}, err
	} else if written != len(wire) {
		return Reply{}, io.ErrShortWrite
	}
	buffer := make([]byte, 64<<10)
	oob := make([]byte, unix.CmsgSpace(16*4))
	raw, err := unixConnection.SyscallConn()
	if err != nil {
		return Reply{}, err
	}
	var count, oobCount, flags int
	var receiveErr error
	if err := raw.Read(func(fd uintptr) bool {
		for {
			count, oobCount, flags, _, receiveErr = unix.Recvmsg(int(fd), buffer, oob, unix.MSG_CMSG_CLOEXEC)
			if errors.Is(receiveErr, unix.EINTR) {
				continue
			}
			return !errors.Is(receiveErr, unix.EAGAIN) && !errors.Is(receiveErr, unix.EWOULDBLOCK)
		}
	}); err != nil {
		return Reply{}, err
	}
	ancillaryErr := closeAncillaryRights(oob[:oobCount])
	if receiveErr != nil {
		return Reply{}, receiveErr
	}
	if count <= 0 || count > len(buffer) || oobCount != 0 || ancillaryErr != nil || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return Reply{}, errors.New("runtime policy issuer reply frame is invalid")
	}
	if err := strictjson.RejectDuplicateKeys(buffer[:count]); err != nil {
		return Reply{}, err
	}
	var reply Reply
	decoder := json.NewDecoder(bytes.NewReader(buffer[:count]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reply); err != nil {
		return Reply{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Reply{}, errors.New("runtime policy issuer reply has trailing data")
	}
	if err := VerifyReply(reply, request, client.PublicKey, time.Now().UTC()); err != nil {
		return Reply{}, err
	}
	return reply, nil
}

func closeAncillaryRights(oob []byte) error {
	var messages []unix.SocketControlMessage
	remaining := oob
	var parseErr error
	for len(remaining) >= unix.CmsgLen(0) {
		var message unix.SocketControlMessage
		message.Header, message.Data, remaining, parseErr = unix.ParseOneSocketControlMessage(remaining)
		if parseErr != nil {
			break
		}
		messages = append(messages, message)
	}
	var closeErr error
	for _, message := range messages {
		if message.Header.Level == unix.SOL_SOCKET && message.Header.Type == unix.SCM_RIGHTS {
			rights, parseErr := unix.ParseUnixRights(&message)
			closeErr = errors.Join(closeErr, parseErr)
			for _, fd := range rights {
				_ = unix.Close(fd)
			}
		}
	}
	return errors.Join(parseErr, closeErr)
}

func requireRootPeer(connection *net.UnixConn) error {
	raw, err := connection.SyscallConn()
	if err != nil {
		return err
	}
	var peer *unix.Ucred
	var peerErr error
	if err := raw.Control(func(fd uintptr) { peer, peerErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) }); err != nil {
		return err
	}
	if peerErr != nil || peer == nil || peer.Uid != 0 || peer.Gid != 0 {
		return errors.New("runtime policy issuer peer is not root")
	}
	return nil
}

func canonical(path string) bool {
	return path != "" && filepath.IsAbs(path) && filepath.Clean(path) == path
}
