package inbox

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

type JetStreamConsumer struct {
	processor      *Processor
	postCommitHook PostCommitHook
}

type PostCommitHook func(context.Context, Event, jetstream.Msg) error

func NewJetStreamConsumer(processor *Processor) (*JetStreamConsumer, error) {
	return newJetStreamConsumer(processor, nil)
}

func NewJetStreamConsumerWithPostCommitHook(
	processor *Processor,
	hook PostCommitHook,
) (*JetStreamConsumer, error) {
	if hook == nil {
		return nil, errors.New("JetStream consumer post-commit hook is required")
	}
	return newJetStreamConsumer(processor, hook)
}

func newJetStreamConsumer(processor *Processor, hook PostCommitHook) (*JetStreamConsumer, error) {
	if processor == nil || processor.pool == nil {
		return nil, errors.New("JetStream consumer Inbox processor is required")
	}
	return &JetStreamConsumer{processor: processor, postCommitHook: hook}, nil
}

func (c *JetStreamConsumer) ProcessMessage(
	ctx context.Context,
	message jetstream.Msg,
	handler Handler,
) (bool, error) {
	if c == nil || c.processor == nil {
		return false, errors.New("JetStream Inbox consumer is not configured")
	}
	if message == nil {
		return false, errors.New("JetStream message is required")
	}
	event, err := decodeJetStreamEvent(message)
	if err != nil {
		return false, err
	}
	applied, err := c.processor.ProcessOnce(ctx, event, handler)
	if err != nil {
		return false, err
	}
	if applied && c.postCommitHook != nil {
		if err := c.postCommitHook(ctx, event, message); err != nil {
			return true, fmt.Errorf("run JetStream consumer post-commit hook: %w", err)
		}
	}
	if err := message.DoubleAck(ctx); err != nil {
		return applied, fmt.Errorf("confirm JetStream message after Inbox commit: %w", err)
	}
	return applied, nil
}

func decodeJetStreamEvent(message jetstream.Msg) (Event, error) {
	var envelope velav1.EventEnvelope
	if err := proto.Unmarshal(message.Data(), &envelope); err != nil {
		return Event{}, fmt.Errorf("decode JetStream EventEnvelope: %w", err)
	}
	if envelope.SchemaVersion != 1 || envelope.AggregateVersion < 1 ||
		envelope.AggregateVersion > math.MaxInt64 || envelope.AggregateType != "Job" ||
		envelope.EventType == "" || envelope.OccurredAt == nil ||
		envelope.OccurredAt.CheckValid() != nil {
		return Event{}, errors.New("JetStream EventEnvelope contract is invalid")
	}
	eventID, err := uuid.Parse(envelope.EventId)
	if err != nil || eventID == uuid.Nil ||
		message.Headers().Get(jetstream.MsgIDHeader) != envelope.EventId {
		return Event{}, errors.New("JetStream event id does not match Nats-Msg-Id")
	}
	aggregateID, err := uuid.Parse(envelope.AggregateId)
	if err != nil || aggregateID == uuid.Nil ||
		message.Subject() != "vela.events."+envelope.EventType {
		return Event{}, errors.New("JetStream aggregate identity or subject is invalid")
	}
	organizationText, projectText, jobText, payloadType, err := eventPayloadIdentity(&envelope)
	if err != nil || payloadType != envelope.EventType || jobText != envelope.AggregateId {
		return Event{}, errors.New("JetStream event payload identity is invalid")
	}
	organizationID, organizationErr := uuid.Parse(organizationText)
	projectID, projectErr := uuid.Parse(projectText)
	if organizationErr != nil || projectErr != nil ||
		organizationID == uuid.Nil || projectID == uuid.Nil {
		return Event{}, errors.New("JetStream Organization or Project identity is invalid")
	}
	return Event{
		ID:               eventID,
		OrganizationID:   organizationID,
		ProjectID:        projectID,
		AggregateType:    envelope.AggregateType,
		AggregateID:      aggregateID,
		AggregateVersion: int64(envelope.AggregateVersion),
		Type:             envelope.EventType,
	}, nil
}

func eventPayloadIdentity(envelope *velav1.EventEnvelope) (string, string, string, string, error) {
	switch payload := envelope.Payload.(type) {
	case *velav1.EventEnvelope_JobReady:
		return payload.JobReady.GetOrganizationId(), payload.JobReady.GetProjectId(),
			payload.JobReady.GetJobId(), "job.ready", nil
	case *velav1.EventEnvelope_JobAssigned:
		return payload.JobAssigned.GetOrganizationId(), payload.JobAssigned.GetProjectId(),
			payload.JobAssigned.GetJobId(), "job.assigned", nil
	case *velav1.EventEnvelope_JobStarted:
		return payload.JobStarted.GetOrganizationId(), payload.JobStarted.GetProjectId(),
			payload.JobStarted.GetJobId(), "job.started", nil
	case *velav1.EventEnvelope_JobRetryWait:
		return payload.JobRetryWait.GetOrganizationId(), payload.JobRetryWait.GetProjectId(),
			payload.JobRetryWait.GetJobId(), "job.retry_wait", nil
	case *velav1.EventEnvelope_JobFailed:
		return payload.JobFailed.GetOrganizationId(), payload.JobFailed.GetProjectId(),
			payload.JobFailed.GetJobId(), "job.failed", nil
	case *velav1.EventEnvelope_JobCanceled:
		return payload.JobCanceled.GetOrganizationId(), payload.JobCanceled.GetProjectId(),
			payload.JobCanceled.GetJobId(), "job.canceled", nil
	case *velav1.EventEnvelope_JobCancelRequested:
		return payload.JobCancelRequested.GetOrganizationId(), payload.JobCancelRequested.GetProjectId(),
			payload.JobCancelRequested.GetJobId(), "job.cancel_requested", nil
	case *velav1.EventEnvelope_JobCanceling:
		return payload.JobCanceling.GetOrganizationId(), payload.JobCanceling.GetProjectId(),
			payload.JobCanceling.GetJobId(), "job.canceling", nil
	case *velav1.EventEnvelope_ChargePosted:
		return payload.ChargePosted.GetOrganizationId(), payload.ChargePosted.GetProjectId(),
			payload.ChargePosted.GetJobId(), "charge.posted", nil
	case *velav1.EventEnvelope_InvoiceExportRequested:
		return payload.InvoiceExportRequested.GetOrganizationId(),
			payload.InvoiceExportRequested.GetProjectId(),
			payload.InvoiceExportRequested.GetJobId(), "invoice.export_requested", nil
	case *velav1.EventEnvelope_JobSucceeded:
		return payload.JobSucceeded.GetOrganizationId(), payload.JobSucceeded.GetProjectId(),
			payload.JobSucceeded.GetJobId(), "job.succeeded", nil
	default:
		return "", "", "", "", errors.New("JetStream event payload is missing")
	}
}
