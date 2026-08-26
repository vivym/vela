package outbox

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vivym/vela/internal/eventstream"
)

const natsMessageIDHeader = "Nats-Msg-Id"

type JetStreamBroker struct {
	client jetstream.JetStream
}

func NewJetStreamBroker(connection *nats.Conn) (*JetStreamBroker, error) {
	if connection == nil {
		return nil, errors.New("missing NATS connection")
	}
	client, err := jetstream.New(connection)
	if err != nil {
		return nil, fmt.Errorf("create JetStream client: %w", err)
	}
	return &JetStreamBroker{client: client}, nil
}

func (b *JetStreamBroker) Publish(
	ctx context.Context,
	subject string,
	messageID string,
	payload []byte,
) (Receipt, error) {
	if b == nil || b.client == nil {
		return Receipt{}, errors.New("broker is not configured for JetStream")
	}
	if subject == "" || messageID == "" {
		return Receipt{}, errors.New("subject and message id are required for JetStream")
	}
	stream, err := b.releaseStream(ctx)
	if err != nil {
		return Receipt{}, err
	}
	message := nats.NewMsg(subject)
	message.Header.Set(natsMessageIDHeader, messageID)
	message.Data = payload
	acknowledgement, err := b.client.PublishMsg(
		ctx,
		message,
		jetstream.WithExpectStream(eventstream.StreamName),
	)
	if err != nil {
		return Receipt{}, fmt.Errorf("publish JetStream message: %w", err)
	}
	if acknowledgement.Stream != eventstream.StreamName || acknowledgement.Sequence < 1 {
		return Receipt{}, fmt.Errorf(
			"JetStream PubAck does not identify %s with a positive sequence",
			eventstream.StreamName,
		)
	}
	information, err := stream.Info(ctx)
	if err != nil {
		return Receipt{}, fmt.Errorf("confirm release-owned JetStream stream after PubAck: %w", err)
	}
	if err := eventstream.ValidateStreamConfig(information.Config); err != nil {
		return Receipt{}, fmt.Errorf("reject release-owned JetStream stream drift after PubAck: %w", err)
	}
	return Receipt{
		Stream:   acknowledgement.Stream,
		Sequence: int64(acknowledgement.Sequence),
	}, nil
}

func (b *JetStreamBroker) releaseStream(ctx context.Context) (jetstream.Stream, error) {
	stream, err := b.client.Stream(ctx, eventstream.StreamName)
	if err != nil {
		return nil, fmt.Errorf("bind release-owned JetStream stream before publish: %w", err)
	}
	information, err := stream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("read release-owned JetStream stream before publish: %w", err)
	}
	if err := eventstream.ValidateStreamConfig(information.Config); err != nil {
		return nil, fmt.Errorf("reject release-owned JetStream stream drift before publish: %w", err)
	}
	return stream, nil
}
