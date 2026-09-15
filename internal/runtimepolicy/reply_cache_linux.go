//go:build linux

package runtimepolicy

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
)

// ReplyCache makes a successfully authorized request retryable after a lost
// response or issuer restart. The cache is keyed by the complete request and
// stores exactly one signed reply; it cannot authorize a changed digest.
type ReplyCache interface {
	Load(context.Context, Request, ed25519.PublicKey) (Reply, bool, error)
	Store(context.Context, Request, Reply) error
}

type FileReplyCache struct{ directory string }

func NewFileReplyCache(directory string) (*FileReplyCache, error) {
	if !canonical(directory) || directory == "/" {
		return nil, errors.New("runtime policy reply cache directory is invalid")
	}
	if _, err := securefile.ResolveTrustedDirectory(directory); err != nil {
		return nil, err
	}
	return &FileReplyCache{directory: directory}, nil
}

func (cache *FileReplyCache) path(request Request) (string, error) {
	if cache == nil || !request.Valid() {
		return "", errors.New("runtime policy reply cache request is invalid")
	}
	wire, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(wire)
	return filepath.Join(cache.directory, hex.EncodeToString(digest[:])+".json"), nil
}

func (cache *FileReplyCache) Load(ctx context.Context, request Request, publicKey ed25519.PublicKey) (Reply, bool, error) {
	if ctx == nil {
		return Reply{}, false, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return Reply{}, false, err
	}
	path, err := cache.path(request)
	if err != nil {
		return Reply{}, false, err
	}
	wire, err := securefile.Read(path, 64<<10, true)
	if errors.Is(err, os.ErrNotExist) {
		return Reply{}, false, nil
	}
	if err != nil {
		return Reply{}, false, err
	}
	if err := strictjson.RejectDuplicateKeys(wire); err != nil {
		return Reply{}, false, err
	}
	var reply Reply
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reply); err != nil {
		return Reply{}, false, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Reply{}, false, errors.New("runtime policy cached reply has trailing data")
	}
	if err := VerifyReply(reply, request, publicKey, nowUTC()); err != nil {
		return Reply{}, false, err
	}
	return reply, true, nil
}

func (cache *FileReplyCache) Store(ctx context.Context, request Request, reply Reply) error {
	if ctx == nil {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := cache.path(request)
	if err != nil {
		return err
	}
	wire, err := json.Marshal(reply)
	if err != nil || len(wire) > 64<<10 {
		return errors.New("runtime policy cached reply is invalid")
	}
	if existing, readErr := os.ReadFile(path); readErr == nil {
		if bytes.Equal(existing, wire) {
			return nil
		}
		return errors.New("runtime policy cached reply already differs")
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	temporary, err := os.CreateTemp(cache.directory, ".reply-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer func() { _ = temporary.Close(); _ = os.Remove(temporaryName) }()
	if err := temporary.Chmod(0o600); err != nil {
		return err
	}
	if _, err := temporary.Write(wire); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Link(temporaryName, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			if existing, readErr := os.ReadFile(path); readErr == nil && bytes.Equal(existing, wire) {
				return nil
			}
		}
		return err
	}
	directoryFile, err := os.Open(cache.directory)
	if err != nil {
		return err
	}
	defer directoryFile.Close()
	return directoryFile.Sync()
}
