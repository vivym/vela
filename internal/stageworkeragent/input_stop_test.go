package stageworkeragent_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestStreamAgentStopCancelsPendingRootInput(t *testing.T) {
	for _, direct := range []bool{false, true} {
		name := "unsolicited"
		if direct {
			name = "direct"
		}
		t.Run(name, func(t *testing.T) { testStopCancelsPendingRootInput(t, direct) })
	}
}

func testStopCancelsPendingRootInput(t *testing.T, direct bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payload := bytes.Repeat([]byte("input"), 128)
	digest := sha256.Sum256(payload)
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseDownload := func() { releaseOnce.Do(func() { close(release) }) }
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		_, _ = writer.Write(payload[:1])
		writer.(http.Flusher).Flush()
		close(started)
		select {
		case <-request.Context().Done():
			close(canceled)
		case <-release:
			_, _ = writer.Write(payload[1:])
		}
	}))
	defer server.Close()
	defer releaseDownload()
	fixture := newSingleMemberMaterializationFixture(t)
	assignment := rootInputAssignment(t, fixture.assignment, digest, int64(len(payload)), server.URL+"/input")
	inputRoot := t.TempDir()
	resolver, err := stageworkeragent.NewHTTPSRootInputResolver(stageworkeragent.HTTPSRootInputResolverConfig{
		InputRoot: inputRoot, Client: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := stageworkeragent.New(stageworkeragent.Config{Members: []stageworkeragent.RuntimeMember{
		{ID: fixture.memberID, Client: fixture.client},
	}})
	if err != nil {
		t.Fatal(err)
	}
	commands := make(chan *velav1.StageWorkerControlServiceConnectResponse, 1)
	control := &recordingStreamControl{
		decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED, commands: commands,
	}
	agent, err := stageworkeragent.NewInputResolvingStreamAgent(runtime, control, resolver)
	if err != nil {
		t.Fatal(err)
	}
	type executionResult struct {
		result stageworkeragent.AssignmentExecutionResult
		err    error
	}
	finished := make(chan executionResult, 1)
	go func() {
		result, err := agent.ExecuteAssignment(ctx, assignment)
		finished <- executionResult{result: result, err: err}
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("root download did not start")
	}
	stop := &velav1.StopStage{
		Authority: assignment.Authority,
		Reason:    velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_PARENT_CANCELED,
	}
	if direct {
		result, err := agent.HandleStop(ctx, stop)
		if err != nil || result.AllStopped || result.AcknowledgedMembers != 0 {
			t.Errorf("signal pending input Stop: result=%+v error=%v", result, err)
		}
	} else {
		commands <- &velav1.StageWorkerControlServiceConnectResponse{
			Result: &velav1.StageWorkerControlServiceConnectResponse_StopStage{StopStage: stop},
		}
		close(commands)
		if err := agent.RunControlCommands(ctx); err != nil {
			t.Errorf("consume Stop during download: %v", err)
		}
	}
	select {
	case <-canceled:
	case <-time.After(300 * time.Millisecond):
		t.Error("Stop did not cancel the pending HTTPS body")
	}
	releaseDownload()
	select {
	case completed := <-finished:
		if !errors.Is(completed.err, context.Canceled) || completed.result.PreparedMembers != 0 || completed.result.ControlStartAccepted {
			t.Errorf("stopped input entered Runtime: result=%+v error=%v", completed.result, completed.err)
		}
		if control.calls != 0 {
			t.Errorf("stopped input sent %d control operations", control.calls)
		}
	case <-ctx.Done():
		t.Fatal("input resolution did not finish after cancellation")
	}
	if err := filepath.WalkDir(inputRoot, func(path string, entry fs.DirEntry, err error) error {
		if err == nil && entry.Type().IsRegular() {
			t.Errorf("stopped download retained input data: %s", path)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestStreamAgentPendingInputStopIdentityAndLateReturn(t *testing.T) {
	for _, test := range []struct {
		name       string
		mutate     func(*velav1.StopStage)
		wantCancel bool
		wantError  bool
	}{
		{name: "matching", wantCancel: true},
		{name: "mismatched-authority", mutate: func(stop *velav1.StopStage) {
			stop.Authority.ExecutionNonce[0] ^= 1
		}},
		{name: "unspecified-reason", wantError: true, mutate: func(stop *velav1.StopStage) {
			stop.Reason = velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_UNSPECIFIED
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			fixture := newSingleMemberMaterializationFixture(t)
			assignment := rootInputAssignment(t, fixture.assignment, sha256.Sum256([]byte("input")), 5, "https://example.test/input")
			runtime, err := stageworkeragent.New(stageworkeragent.Config{Members: []stageworkeragent.RuntimeMember{
				{ID: fixture.memberID, Client: fixture.client},
			}})
			if err != nil {
				t.Fatal(err)
			}
			started := make(chan context.Context, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseResolver := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseResolver()
			resolver := inputResolverFunc(func(inputCtx context.Context, _ *velav1.StageAssignment) error {
				started <- inputCtx
				select {
				case <-release:
					return nil // A resolver can finish successfully after Stop has canceled it.
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			commands := make(chan *velav1.StageWorkerControlServiceConnectResponse, 1)
			control := &recordingStreamControl{
				decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED, commands: commands,
			}
			agent, err := stageworkeragent.NewInputResolvingStreamAgent(runtime, control, resolver)
			if err != nil {
				t.Fatal(err)
			}
			type executionResult struct {
				result stageworkeragent.AssignmentExecutionResult
				err    error
			}
			finished := make(chan executionResult, 1)
			go func() {
				result, err := agent.ExecuteAssignment(ctx, assignment)
				finished <- executionResult{result: result, err: err}
			}()
			var inputCtx context.Context
			select {
			case inputCtx = <-started:
			case <-ctx.Done():
				t.Fatal("input resolver did not start")
			}
			stop := &velav1.StopStage{
				Authority: proto.Clone(assignment.Authority).(*velav1.StageAuthority),
				Reason:    velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_PARENT_CANCELED,
			}
			if test.mutate != nil {
				test.mutate(stop)
			}
			commands <- &velav1.StageWorkerControlServiceConnectResponse{
				Result: &velav1.StageWorkerControlServiceConnectResponse_StopStage{StopStage: stop},
			}
			close(commands)
			if err := agent.RunControlCommands(ctx); (err != nil) != test.wantError {
				t.Errorf("Stop error = %v, want error %v", err, test.wantError)
			}
			if canceled := errors.Is(inputCtx.Err(), context.Canceled); canceled != test.wantCancel {
				t.Errorf("input canceled = %v, want %v", canceled, test.wantCancel)
			}
			releaseResolver()
			select {
			case completed := <-finished:
				if test.wantCancel {
					if !errors.Is(completed.err, context.Canceled) || completed.result.PreparedMembers != 0 ||
						completed.result.ControlStartAccepted || control.calls != 0 {
						t.Errorf("late resolver entered Runtime: result=%+v error=%v calls=%d", completed.result, completed.err, control.calls)
					}
				} else if completed.err != nil || completed.result.PreparedMembers != 1 || !completed.result.ControlStartAccepted {
					t.Errorf("unrelated or invalid Stop disturbed assignment: result=%+v error=%v", completed.result, completed.err)
				}
			case <-ctx.Done():
				t.Fatal("input resolver did not finish")
			}
		})
	}
}

type inputResolverFunc func(context.Context, *velav1.StageAssignment) error

func (resolve inputResolverFunc) Resolve(ctx context.Context, assignment *velav1.StageAssignment) error {
	return resolve(ctx, assignment)
}
