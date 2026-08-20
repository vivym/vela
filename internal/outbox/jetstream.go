package outbox

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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
	message := nats.NewMsg(subject)
	message.Header.Set(natsMessageIDHeader, messageID)
	message.Data = payload
	acknowledgement, err := b.client.PublishMsg(ctx, message)
	if err != nil {
		return Receipt{}, fmt.Errorf("publish JetStream message: %w", err)
	}
	return Receipt{
		Stream:   acknowledgement.Stream,
		Sequence: int64(acknowledgement.Sequence),
	}, nil
}
