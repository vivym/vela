package stageworkertransport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

type ClientConfig struct {
	Address                    string
	TransportCredentials       credentials.TransportCredentials
	InitialControlSessionEpoch int64
	ControlSessionEpochSource  ControlSessionEpochSource
}

type ControlSessionEpochSource interface {
	NextControlSessionEpoch(context.Context) (int64, error)
}

type controlSessionEpochObserver interface {
	ObserveControlSessionEpoch(context.Context, int64) error
}

type exchangeResult struct {
	response *velav1.StageWorkerControlServiceConnectResponse
	err      error
}

type controlCommand struct {
	response   *velav1.StageWorkerControlServiceConnectResponse
	generation uint64
}

type Client struct {
	connection *grpc.ClientConn
	service    velav1.StageWorkerControlServiceClient
	ctx        context.Context
	cancel     context.CancelFunc

	mu     sync.Mutex
	stream grpc.BidiStreamingClient[
		velav1.StageWorkerControlServiceConnectRequest,
		velav1.StageWorkerControlServiceConnectResponse,
	]
	streamEpoch       int64
	streamCancel      context.CancelFunc
	epochSource       ControlSessionEpochSource
	synchronizedEpoch int64
	generation        uint64
	pending           map[string]chan exchangeResult
	closed            bool
	sendMu            sync.Mutex
	commands          chan controlCommand
	closeOnce         sync.Once
}

func DialClient(ctx context.Context, config ClientConfig) (*Client, error) {
	hasInitialEpoch := config.InitialControlSessionEpoch > 0
	hasEpochSource := config.ControlSessionEpochSource != nil
	if ctx == nil || config.TransportCredentials == nil || hasInitialEpoch == hasEpochSource {
		return nil, errors.New("incomplete Stage Worker control client configuration")
	}
	host, port, err := net.SplitHostPort(config.Address)
	if err != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
		return nil, errors.New("invalid Stage Worker control address: host and port are required")
	}
	connection, err := grpc.NewClient(
		config.Address,
		grpc.WithTransportCredentials(config.TransportCredentials),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(4<<20),
			grpc.MaxCallSendMsgSize(1<<20),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("configure Stage Worker control client: %w", err)
	}
	connection.Connect()
	for state := connection.GetState(); state != connectivity.Ready; state = connection.GetState() {
		if state == connectivity.Shutdown {
			_ = connection.Close()
			return nil, errors.New("connection shut down during Stage Worker control startup")
		}
		if !connection.WaitForStateChange(ctx, state) {
			_ = connection.Close()
			return nil, fmt.Errorf("connect to Stage Worker control: %w", ctx.Err())
		}
	}
	clientContext, cancel := context.WithCancel(context.Background())
	return &Client{
		connection:  connection,
		service:     velav1.NewStageWorkerControlServiceClient(connection),
		ctx:         clientContext,
		cancel:      cancel,
		streamEpoch: config.InitialControlSessionEpoch,
		epochSource: config.ControlSessionEpochSource,
		pending:     make(map[string]chan exchangeResult),
		commands:    make(chan controlCommand, 64),
	}, nil
}

func (client *Client) Close() error {
	if client == nil {
		return nil
	}
	var closeErr error
	client.closeOnce.Do(func() {
		client.mu.Lock()
		client.closed = true
		stream := client.stream
		streamCancel := client.streamCancel
		client.stream = nil
		client.streamCancel = nil
		pending := client.pending
		client.pending = make(map[string]chan exchangeResult)
		client.mu.Unlock()
		client.cancel()
		if streamCancel != nil {
			streamCancel()
		}
		if stream != nil {
			_ = stream.CloseSend()
		}
		for _, waiter := range pending {
			waiter <- exchangeResult{err: errors.New("closed Stage Worker control client")}
		}
		if client.connection != nil {
			closeErr = client.connection.Close()
		}
	})
	return closeErr
}

func (client *Client) NextCommand(ctx context.Context) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	if client == nil || ctx == nil {
		return nil, errors.New("missing Stage Worker command consumer context")
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-client.ctx.Done():
			return nil, io.EOF
		case command := <-client.commands:
			client.mu.Lock()
			current := !client.closed && client.stream != nil &&
				client.generation == command.generation && client.stream.Context().Err() == nil
			client.mu.Unlock()
			if current {
				return command.response, nil
			}
		}
	}
}

func (client *Client) CurrentControlSessionEpoch() int64 {
	if client == nil {
		return 0
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.streamEpoch
}

func (client *Client) HasActiveControlSession() bool {
	if client == nil {
		return false
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.closed && client.stream != nil
}

func (client *Client) Exchange(
	ctx context.Context,
	request *velav1.StageWorkerControlServiceConnectRequest,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	if client == nil || client.service == nil || ctx == nil || request == nil ||
		request.GetOperation() == nil {
		return nil, errors.New("missing configured Stage Worker control exchange")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	message := proto.Clone(request).(*velav1.StageWorkerControlServiceConnectRequest)
	if message.GetRequestId() == "" {
		message.RequestId = uuid.NewString()
	} else {
		requestID, err := uuid.Parse(message.GetRequestId())
		if err != nil || requestID == uuid.Nil || requestID.String() != message.GetRequestId() {
			return nil, errors.New("stage worker control request ID is not a canonical UUID")
		}
	}
	stream, epoch, generation, err := client.ensureStream()
	if err != nil {
		return nil, err
	}
	message.ControlSessionEpoch = epoch
	waiter := make(chan exchangeResult, 1)
	client.mu.Lock()
	if client.closed || client.stream != stream || client.generation != generation {
		client.mu.Unlock()
		return nil, errors.New("changed Stage Worker control stream before send")
	}
	if _, exists := client.pending[message.GetRequestId()]; exists {
		client.mu.Unlock()
		return nil, errors.New("duplicate in-flight Stage Worker control request ID")
	}
	client.pending[message.GetRequestId()] = waiter
	client.mu.Unlock()

	client.sendMu.Lock()
	sendErr := stream.Send(message)
	client.sendMu.Unlock()
	if sendErr != nil {
		client.removePending(message.GetRequestId(), waiter)
		client.failStream(stream, generation, sendErr)
		return nil, sendErr
	}
	select {
	case result := <-waiter:
		return result.response, result.err
	case <-ctx.Done():
		// The peer remembers request IDs for the lifetime of this stream. An
		// uncertain outcome must reconnect before the same command can replay.
		client.failStream(stream, generation, ctx.Err())
		return nil, ctx.Err()
	case <-client.ctx.Done():
		client.removePending(message.GetRequestId(), waiter)
		return nil, errors.New("closed Stage Worker control client")
	}
}

func (client *Client) ensureStream() (
	grpc.BidiStreamingClient[
		velav1.StageWorkerControlServiceConnectRequest,
		velav1.StageWorkerControlServiceConnectResponse,
	],
	int64,
	uint64,
	error,
) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		return nil, 0, 0, errors.New("closed Stage Worker control client")
	}
	if client.stream != nil {
		return client.stream, client.streamEpoch, client.generation, nil
	}
	if client.epochSource != nil {
		if client.synchronizedEpoch > 0 {
			client.streamEpoch = client.synchronizedEpoch
			client.synchronizedEpoch = 0
		} else {
			nextEpoch, err := client.epochSource.NextControlSessionEpoch(client.ctx)
			if err != nil {
				return nil, 0, 0, fmt.Errorf("allocate Stage Worker control session epoch: %w", err)
			}
			if nextEpoch <= client.streamEpoch {
				return nil, 0, 0, errors.New("stage worker control session epoch did not advance")
			}
			client.streamEpoch = nextEpoch
		}
	}
	streamContext, streamCancel := context.WithCancel(client.ctx)
	stream, err := client.service.Connect(streamContext)
	if err != nil {
		streamCancel()
		return nil, 0, 0, fmt.Errorf("open Stage Worker control stream: %w", err)
	}
	client.generation++
	client.stream = stream
	client.streamCancel = streamCancel
	generation := client.generation
	epoch := client.streamEpoch
	go client.receive(stream, generation)
	return stream, epoch, generation, nil
}

func (client *Client) receive(
	stream grpc.BidiStreamingClient[
		velav1.StageWorkerControlServiceConnectRequest,
		velav1.StageWorkerControlServiceConnectResponse,
	],
	generation uint64,
) {
	for {
		response, err := stream.Recv()
		if err != nil {
			client.failStream(stream, generation, err)
			return
		}
		if response == nil || response.GetResult() == nil {
			client.failStream(stream, generation, errors.New("malformed Stage Worker control response"))
			return
		}
		client.mu.Lock()
		if client.stream != stream || client.generation != generation || stream.Context().Err() != nil {
			client.mu.Unlock()
			return
		}
		if decision := response.GetWorkerReadinessDecision(); decision != nil &&
			decision.GetControlSessionEpoch() != 0 &&
			decision.GetControlSessionEpoch() != client.streamEpoch {
			observer, ok := client.epochSource.(controlSessionEpochObserver)
			if !ok {
				client.mu.Unlock()
				client.failStream(stream, generation, errors.New("invalid durable Stage Worker control session epoch"))
				return
			}
			if err := observer.ObserveControlSessionEpoch(
				stream.Context(),
				decision.GetControlSessionEpoch(),
			); err != nil {
				client.mu.Unlock()
				client.failStream(stream, generation, fmt.Errorf("persist durable Stage Worker control session epoch: %w", err))
				return
			}
			client.synchronizedEpoch = decision.GetControlSessionEpoch()
			client.mu.Unlock()
			client.failStream(stream, generation, errors.New("durable Stage Worker control session synchronized; reconnect required"))
			return
		}
		if response.GetRequestId() == "" {
			if response.GetStopStage() == nil {
				client.mu.Unlock()
				client.failStream(stream, generation, errors.New("unsolicited Stage Worker response is not StopStage"))
				return
			}
			// Admission to the bounded queue is atomic with stream replacement.
			// A stalled consumer must not hold the connection mutex or receiver.
			select {
			case client.commands <- controlCommand{response: response, generation: generation}:
				client.mu.Unlock()
			default:
				client.mu.Unlock()
				client.failStream(stream, generation, errors.New("stage worker command queue is full"))
				return
			}
			continue
		}
		waiter := client.pending[response.GetRequestId()]
		delete(client.pending, response.GetRequestId())
		client.mu.Unlock()
		if waiter != nil {
			waiter <- exchangeResult{response: response}
		}
	}
}

func (client *Client) failStream(
	stream grpc.BidiStreamingClient[
		velav1.StageWorkerControlServiceConnectRequest,
		velav1.StageWorkerControlServiceConnectResponse,
	],
	generation uint64,
	err error,
) {
	client.mu.Lock()
	if client.stream != stream || client.generation != generation {
		client.mu.Unlock()
		return
	}
	client.stream = nil
	streamCancel := client.streamCancel
	client.streamCancel = nil
	if client.epochSource == nil {
		client.streamEpoch++
	}
	pending := client.pending
	client.pending = make(map[string]chan exchangeResult)
	client.mu.Unlock()
	if streamCancel != nil {
		streamCancel()
	}
	_ = stream.CloseSend()
	for _, waiter := range pending {
		waiter <- exchangeResult{err: err}
	}
}

func (client *Client) removePending(requestID string, waiter chan exchangeResult) {
	client.mu.Lock()
	if client.pending[requestID] == waiter {
		delete(client.pending, requestID)
	}
	client.mu.Unlock()
}
