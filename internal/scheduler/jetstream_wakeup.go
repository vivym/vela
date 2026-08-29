package scheduler

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vivym/vela/internal/eventstream"
	"github.com/vivym/vela/internal/inbox"
)

type JetStreamWakeupConsumer struct {
	consumer jetstream.Consumer
	messages *inbox.JetStreamConsumer
	handler  inbox.Handler
}

func BindJetStreamWakeupConsumer(
	ctx context.Context,
	connection *nats.Conn,
	messages *inbox.JetStreamConsumer,
	handler inbox.Handler,
) (*JetStreamWakeupConsumer, error) {
	if connection == nil || messages == nil || handler == nil {
		return nil, errors.New("scheduler JetStream wakeup dependencies are required")
	}
	client, err := jetstream.New(connection)
	if err != nil {
		return nil, fmt.Errorf("create Scheduler JetStream client: %w", err)
	}
	stream, err := client.Stream(ctx, eventstream.StreamName)
	if err != nil {
		return nil, fmt.Errorf("bind release-owned JetStream stream: %w", err)
	}
	streamInformation, err := stream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("read release-owned JetStream stream contract: %w", err)
	}
	if err := eventstream.ValidateStreamConfig(streamInformation.Config); err != nil {
		return nil, fmt.Errorf("reject release-owned JetStream stream drift: %w", err)
	}
	consumer, err := stream.Consumer(ctx, eventstream.SchedulerConsumerName)
	if err != nil {
		return nil, fmt.Errorf("bind release-owned Scheduler durable consumer: %w", err)
	}
	consumerInformation, err := consumer.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("read release-owned Scheduler consumer contract: %w", err)
	}
	if err := eventstream.ValidateSchedulerConsumerConfig(consumerInformation.Config); err != nil {
		return nil, fmt.Errorf("reject release-owned Scheduler consumer drift: %w", err)
	}
	return &JetStreamWakeupConsumer{
		consumer: consumer,
		messages: messages,
		handler:  handler,
	}, nil
}

func (c *JetStreamWakeupConsumer) Run(ctx context.Context, reportError func(error)) error {
	if c == nil || c.consumer == nil || c.messages == nil || c.handler == nil {
		return errors.New("scheduler JetStream wakeup consumer is not configured")
	}
	if reportError == nil {
		reportError = func(error) {}
	}
	requestExpiry := eventstream.SchedulerConsumerConfig().MaxRequestExpires
	for {
		fetchContext, cancelFetch := context.WithTimeout(ctx, requestExpiry)
		message, err := c.consumer.Next(jetstream.FetchContext(fetchContext))
		cancelFetch()
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) ||
			errors.Is(err, jetstream.ErrNoMessages) {
			continue
		}
		if err != nil {
			return fmt.Errorf("fetch Scheduler JetStream wakeup: %w", err)
		}
		_, processErr := c.messages.ProcessMessage(ctx, message, c.handler)
		if processErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			reportError(processErr)
		}
	}
}
