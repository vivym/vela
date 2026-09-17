//go:build linux

package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/runtimechannel"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/workerjournalwire"
)

func TestWorkerJournalRPCPreActivationAndMutation(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root Node journal server")
	}
	inputRoot := filepath.Join("/var/tmp", fmt.Sprintf("vela-worker-input-%d", os.Getpid()))
	if err := os.RemoveAll(inputRoot); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(inputRoot) })
	input, err := stageworkeragent.NewFileInputTransferJournal(inputRoot)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = input.Close() })

	identity := workerjournalwire.Identity{JournalID: uuid.New(), Scope: sha256.Sum256([]byte("rpc-worker-scope"))}
	socketPath := filepath.Join("/run", fmt.Sprintf("vela-worker-journal-test-%d.sock", os.Getpid()))
	_ = os.Remove(socketPath)
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socketPath, Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close(); _ = os.Remove(socketPath) })
	if err := errors.Join(os.Chown(socketPath, 0, 65532), os.Chmod(socketPath, 0o660)); err != nil {
		t.Fatal(err)
	}

	workerPID := -1
	child := exec.Command(os.Args[0], "-test.run=^TestWorkerJournalRPCClientHelper$")
	readyPath := filepath.Join("/tmp", fmt.Sprintf("vela-worker-journal-ready-%d", os.Getpid()))
	activatePath := filepath.Join("/tmp", fmt.Sprintf("vela-worker-journal-activate-%d", os.Getpid()))
	_ = os.Remove(readyPath)
	_ = os.Remove(activatePath)
	child.Env = append(os.Environ(),
		"VELA_WORKER_RPC_HELPER=1",
		"VELA_WORKER_RPC_SOCKET="+socketPath,
		"VELA_WORKER_RPC_JOURNAL_ID="+identity.JournalID.String(),
		"VELA_WORKER_RPC_SCOPE="+hex.EncodeToString(identity.Scope[:]),
		"VELA_WORKER_RPC_READY="+readyPath,
		"VELA_WORKER_RPC_ACTIVATE="+activatePath,
	)
	child.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65532, Gid: 65532}, PidFD: &workerPID}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	if workerPID < 0 {
		_ = child.Process.Kill()
		_ = child.Wait()
		t.Fatal("child did not provide original pidfd")
	}
	worker := os.NewFile(uintptr(workerPID), "worker-journal-owner")
	defer func(cleanup func() error) { _ = cleanup() }(worker.Close)
	endpoint := &WorkerJournalEndpoint{input: input, worker: worker, identity: identity, uploads: make(map[string]*materializationUpload)}
	server, err := NewJournalServerForHandler(endpoint, JournalServerConfig{Credentials: []RuntimeCallerCredentials{{UID: 65532, GID: 65532}}, MaxConcurrent: 2, ExchangeTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(context.Background(), listener) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, statErr := os.Stat(readyPath); statErr == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = child.Process.Kill()
			_ = child.Wait()
			t.Fatal("Worker did not complete pre-activation exchanges")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := endpoint.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(activatePath, []byte("activate"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err != nil {
		t.Fatal(err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	record, found, err := input.Load(context.Background(), sha256.Sum256([]byte("worker-rpc-token")))
	if err != nil || !found || record.Consumed {
		t.Fatalf("Node-owned journal result: found=%v consumed=%v err=%v", found, record.Consumed, err)
	}
	t.Log("pre-activation mutation rejected; activation enabled the same retained Worker pidfd for mutation")
}

func TestWorkerJournalRPCClientHelper(t *testing.T) {
	if os.Getenv("VELA_WORKER_RPC_HELPER") != "1" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	id, err := uuid.Parse(os.Getenv("VELA_WORKER_RPC_JOURNAL_ID"))
	if err != nil {
		t.Fatal(err)
	}
	scopeBytes, err := hex.DecodeString(os.Getenv("VELA_WORKER_RPC_SCOPE"))
	if err != nil || len(scopeBytes) != sha256.Size {
		t.Fatal("invalid scope")
	}
	var scope [sha256.Size]byte
	copy(scope[:], scopeBytes)
	identity := workerjournalwire.Identity{JournalID: id, Scope: scope}
	exchange := func(input *workerjournalwire.InputRequest) workerjournalwire.Response {
		wire, digest, encodeErr := workerjournalwire.EncodeRequest(identity, input, nil)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		reply, exchangeErr := runtimechannel.ExchangeWithRequestLimit(ctx, os.Getenv("VELA_WORKER_RPC_SOCKET"), wire, workerjournalwire.MaximumRequestBytes)
		if exchangeErr != nil {
			t.Fatal(exchangeErr)
		}
		response, parseErr := workerjournalwire.ParseResponse(reply, digest)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		return response
	}
	token := sha256.Sum256([]byte("worker-rpc-token"))
	encoded, err := stageworkeragent.EncodeInputTransferJournalRecord(stageworkeragent.InputTransferJournalRecord{TokenDigest: token, Command: stageartifact.ConsumeTransferCommand{CommandID: uuid.New(), TicketID: uuid.New(), TokenDigest: token, Destination: stageartifact.TransferDestination{WorkerInstanceID: uuid.New(), WorkerInstanceEpoch: 1, ModelResidencyID: uuid.New(), ModelRuntimeEpoch: 1, ConnectorRevisionID: uuid.New()}, OutcomeDigest: sha256.Sum256([]byte("outcome")), ConsumedAt: time.Now().UTC()}})
	if err != nil {
		t.Fatal(err)
	}
	if response := exchange(&workerjournalwire.InputRequest{Operation: "load", TokenDigest: hex.EncodeToString(token[:])}); response.Found {
		t.Fatal("empty Node journal unexpectedly found a record")
	}
	if response := exchange(&workerjournalwire.InputRequest{Operation: "put_pending", TokenDigest: hex.EncodeToString(token[:]), Record: encoded}); response.Error != "REJECTED" {
		t.Fatalf("pre-activation mutation response = %q", response.Error)
	}
	if err := os.WriteFile(os.Getenv("VELA_WORKER_RPC_READY"), []byte("ready"), 0o644); err != nil {
		t.Fatal(err)
	}
	for {
		if _, statErr := os.Stat(os.Getenv("VELA_WORKER_RPC_ACTIVATE")); statErr == nil {
			break
		}
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if response := exchange(&workerjournalwire.InputRequest{Operation: "put_pending", TokenDigest: hex.EncodeToString(token[:]), Record: encoded}); response.Error != "" {
		t.Fatalf("activated mutation response = %q", response.Error)
	}
	response := exchange(&workerjournalwire.InputRequest{Operation: "load", TokenDigest: hex.EncodeToString(token[:])})
	if !response.Found || len(response.InputRecord) == 0 {
		t.Fatal("activated Node journal record was not readable")
	}
}
