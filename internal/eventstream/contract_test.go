package eventstream_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/vivym/vela/internal/eventstream"
)

func TestReleaseContractUsesThreeReplicatedDurableExplicitAck(t *testing.T) {
	stream := eventstream.StreamConfig()
	if stream.Name != "VELA_EVENTS" ||
		!reflect.DeepEqual(stream.Subjects, []string{"vela.events.>"}) ||
		stream.Retention != jetstream.LimitsPolicy ||
		stream.MaxConsumers != 32 || stream.MaxMsgs != 1_000_000 ||
		stream.MaxBytes != 64<<30 || stream.Discard != jetstream.DiscardOld ||
		stream.MaxAge != 7*24*time.Hour || stream.MaxMsgsPerSubject != -1 ||
		stream.MaxMsgSize != 1<<20 ||
		stream.Storage != jetstream.FileStorage || stream.Replicas != 3 || stream.NoAck ||
		stream.Duplicates != 10*time.Minute || !stream.DenyDelete || !stream.DenyPurge ||
		stream.AllowRollup || stream.AllowDirect || stream.MirrorDirect ||
		!reflect.DeepEqual(stream.Metadata, map[string]string{
			"vela.contract": "event-delivery",
			"vela.revision": "1",
		}) {
		t.Fatalf("release stream contract = %#v", stream)
	}

	consumer := eventstream.SchedulerConsumerConfig()
	if consumer.Name != "VELA_SCHEDULER" || consumer.Durable != consumer.Name ||
		consumer.Description == "" || consumer.DeliverPolicy != jetstream.DeliverAllPolicy ||
		consumer.AckPolicy != jetstream.AckExplicitPolicy || consumer.AckWait != 30*time.Second ||
		consumer.MaxDeliver != -1 || consumer.FilterSubject != "vela.events.job.ready" ||
		consumer.ReplayPolicy != jetstream.ReplayInstantPolicy || consumer.MaxWaiting != 32 ||
		consumer.MaxAckPending != 128 || consumer.MaxRequestBatch != 1 ||
		consumer.MaxRequestExpires != 5*time.Second || consumer.Replicas != 3 ||
		consumer.MemoryStorage || consumer.InactiveThreshold != 0 ||
		!reflect.DeepEqual(consumer.Metadata, map[string]string{
			"vela.contract": "scheduler-wakeup",
			"vela.revision": "1",
		}) {
		t.Fatalf("release Scheduler consumer contract = %#v", consumer)
	}
}

func TestReleaseContractRejectsReliabilityDrift(t *testing.T) {
	streamCases := []struct {
		name   string
		field  string
		mutate func(*jetstream.StreamConfig)
	}{
		{name: "name", field: "name", mutate: func(config *jetstream.StreamConfig) { config.Name = "EVENTS" }},
		{name: "subject", field: "subjects", mutate: func(config *jetstream.StreamConfig) { config.Subjects = []string{"vela.events.job.ready"} }},
		{name: "memory", field: "storage", mutate: func(config *jetstream.StreamConfig) { config.Storage = jetstream.MemoryStorage }},
		{name: "single replica", field: "replicas", mutate: func(config *jetstream.StreamConfig) { config.Replicas = 1 }},
		{name: "no PubAck", field: "acknowledgements", mutate: func(config *jetstream.StreamConfig) { config.NoAck = true }},
		{name: "short duplicate window", field: "duplicate window", mutate: func(config *jetstream.StreamConfig) { config.Duplicates = 30 * time.Second }},
		{name: "unbounded bytes", field: "limits", mutate: func(config *jetstream.StreamConfig) { config.MaxBytes = -1 }},
		{name: "deletion allowed", field: "mutation guards", mutate: func(config *jetstream.StreamConfig) { config.DenyDelete = false }},
		{name: "revision", field: "metadata", mutate: func(config *jetstream.StreamConfig) { config.Metadata["vela.revision"] = "2" }},
		{name: "foreign metadata", field: "metadata", mutate: func(config *jetstream.StreamConfig) { config.Metadata["other.owner"] = "true" }},
		{name: "per-subject limit", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.MaxMsgsPerSubject = 1 }},
		{name: "discard new per subject", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.DiscardNewPerSubject = true }},
		{name: "placement", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.Placement = &jetstream.Placement{Cluster: "other"} }},
		{name: "mirror", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.Mirror = &jetstream.StreamSource{Name: "OTHER"} }},
		{name: "source", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.Sources = []*jetstream.StreamSource{{Name: "OTHER"}} }},
		{name: "sealed", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.Sealed = true }},
		{name: "compression", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.Compression = jetstream.S2Compression }},
		{name: "first sequence", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.FirstSeq = 10 }},
		{name: "subject transform", field: "extended contract", mutate: func(config *jetstream.StreamConfig) {
			config.SubjectTransform = &jetstream.SubjectTransformConfig{Source: "vela.events.>", Destination: "other.>"}
		}},
		{name: "republish", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.RePublish = &jetstream.RePublish{Destination: "copy.>"} }},
		{name: "consumer limits", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.ConsumerLimits.MaxAckPending = 1 }},
		{name: "template", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.Template = "legacy" }},
		{name: "message ttl", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.AllowMsgTTL = true }},
		{name: "delete marker", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.SubjectDeleteMarkerTTL = time.Minute }},
		{name: "message counter", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.AllowMsgCounter = true }},
		{name: "atomic publish", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.AllowAtomicPublish = true }},
		{name: "message schedules", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.AllowMsgSchedules = true }},
		{name: "batch publish", field: "extended contract", mutate: func(config *jetstream.StreamConfig) { config.AllowBatchPublish = true }},
	}
	for _, test := range streamCases {
		t.Run("stream/"+test.name, func(t *testing.T) {
			config := eventstream.StreamConfig()
			test.mutate(&config)
			if err := eventstream.ValidateStreamConfig(config); err == nil || !strings.Contains(err.Error(), test.field) {
				t.Fatalf("ValidateStreamConfig error = %v, want %q drift", err, test.field)
			}
		})
	}

	consumerCases := []struct {
		name   string
		field  string
		mutate func(*jetstream.ConsumerConfig)
	}{
		{name: "ephemeral", field: "durable identity", mutate: func(config *jetstream.ConsumerConfig) { config.Durable = "" }},
		{name: "ack none", field: "ack policy", mutate: func(config *jetstream.ConsumerConfig) { config.AckPolicy = jetstream.AckNonePolicy }},
		{name: "wrong filter", field: "filter", mutate: func(config *jetstream.ConsumerConfig) { config.FilterSubject = "vela.events.>" }},
		{name: "single replica", field: "replicas", mutate: func(config *jetstream.ConsumerConfig) { config.Replicas = 1 }},
		{name: "memory", field: "memory", mutate: func(config *jetstream.ConsumerConfig) { config.MemoryStorage = true }},
		{name: "unbounded pending", field: "pending", mutate: func(config *jetstream.ConsumerConfig) { config.MaxAckPending = -1 }},
		{name: "finite redelivery", field: "redelivery", mutate: func(config *jetstream.ConsumerConfig) { config.MaxDeliver = 5 }},
		{name: "revision", field: "metadata", mutate: func(config *jetstream.ConsumerConfig) { config.Metadata["vela.revision"] = "2" }},
		{name: "description", field: "extended contract", mutate: func(config *jetstream.ConsumerConfig) { config.Description = "other" }},
		{name: "start sequence", field: "extended contract", mutate: func(config *jetstream.ConsumerConfig) { config.OptStartSeq = 2 }},
		{name: "start time", field: "extended contract", mutate: func(config *jetstream.ConsumerConfig) { at := time.Now(); config.OptStartTime = &at }},
		{name: "backoff", field: "extended contract", mutate: func(config *jetstream.ConsumerConfig) { config.BackOff = []time.Duration{time.Second} }},
		{name: "rate limit", field: "extended contract", mutate: func(config *jetstream.ConsumerConfig) { config.RateLimit = 1 }},
		{name: "sampling", field: "extended contract", mutate: func(config *jetstream.ConsumerConfig) { config.SampleFrequency = "100" }},
		{name: "headers only", field: "extended contract", mutate: func(config *jetstream.ConsumerConfig) { config.HeadersOnly = true }},
		{name: "request bytes", field: "extended contract", mutate: func(config *jetstream.ConsumerConfig) { config.MaxRequestMaxBytes = 1 }},
		{name: "pause", field: "extended contract", mutate: func(config *jetstream.ConsumerConfig) { at := time.Now(); config.PauseUntil = &at }},
		{name: "priority", field: "extended contract", mutate: func(config *jetstream.ConsumerConfig) {
			config.PriorityPolicy = jetstream.PriorityPolicyPinned
			config.PinnedTTL = time.Second
			config.PriorityGroups = []string{"other"}
		}},
		{name: "push subject", field: "extended contract", mutate: func(config *jetstream.ConsumerConfig) { config.DeliverSubject = "deliver" }},
		{name: "push group", field: "extended contract", mutate: func(config *jetstream.ConsumerConfig) { config.DeliverGroup = "group" }},
		{name: "flow control", field: "extended contract", mutate: func(config *jetstream.ConsumerConfig) { config.FlowControl = true }},
		{name: "heartbeat", field: "extended contract", mutate: func(config *jetstream.ConsumerConfig) { config.IdleHeartbeat = time.Second }},
	}
	for _, test := range consumerCases {
		t.Run("consumer/"+test.name, func(t *testing.T) {
			config := eventstream.SchedulerConsumerConfig()
			test.mutate(&config)
			if err := eventstream.ValidateSchedulerConsumerConfig(config); err == nil ||
				!strings.Contains(err.Error(), test.field) {
				t.Fatalf("ValidateSchedulerConsumerConfig error = %v, want %q drift", err, test.field)
			}
		})
	}
}
