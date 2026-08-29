package eventstream

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	StreamName             = "VELA_EVENTS"
	StreamSubject          = "vela.events.>"
	SchedulerConsumerName  = "VELA_SCHEDULER"
	SchedulerFilterSubject = "vela.events.job.ready"
)

func StreamConfig() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:              StreamName,
		Subjects:          []string{StreamSubject},
		Retention:         jetstream.LimitsPolicy,
		MaxConsumers:      32,
		MaxMsgs:           1_000_000,
		MaxBytes:          64 << 30,
		Discard:           jetstream.DiscardOld,
		MaxAge:            7 * 24 * time.Hour,
		MaxMsgsPerSubject: -1,
		MaxMsgSize:        1 << 20,
		Storage:           jetstream.FileStorage,
		Replicas:          3,
		Duplicates:        10 * time.Minute,
		DenyDelete:        true,
		DenyPurge:         true,
		Metadata: map[string]string{
			"vela.contract": "event-delivery",
			"vela.revision": "1",
		},
	}
}

func SchedulerConsumerConfig() jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Name:              SchedulerConsumerName,
		Durable:           SchedulerConsumerName,
		Description:       "PostgreSQL-authoritative Scheduler job.ready wakeup",
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           30 * time.Second,
		MaxDeliver:        -1,
		FilterSubject:     SchedulerFilterSubject,
		ReplayPolicy:      jetstream.ReplayInstantPolicy,
		MaxWaiting:        32,
		MaxAckPending:     128,
		MaxRequestBatch:   1,
		MaxRequestExpires: 5 * time.Second,
		Replicas:          3,
		Metadata: map[string]string{
			"vela.contract": "scheduler-wakeup",
			"vela.revision": "1",
		},
	}
}

func ValidateStreamConfig(config jetstream.StreamConfig) error {
	want := StreamConfig()
	if config.Name != want.Name {
		return errors.New("JetStream stream name drift")
	}
	if !slices.Equal(config.Subjects, want.Subjects) {
		return errors.New("JetStream stream subjects drift")
	}
	if config.Retention != want.Retention || config.MaxConsumers != want.MaxConsumers ||
		config.MaxMsgs != want.MaxMsgs || config.MaxBytes != want.MaxBytes ||
		config.Discard != want.Discard || config.MaxAge != want.MaxAge ||
		config.MaxMsgSize != want.MaxMsgSize {
		return errors.New("JetStream stream limits drift")
	}
	if config.Storage != want.Storage {
		return errors.New("JetStream stream storage drift")
	}
	if config.Replicas != want.Replicas {
		return errors.New("JetStream stream replicas drift")
	}
	if config.NoAck != want.NoAck {
		return errors.New("JetStream stream acknowledgements drift")
	}
	if config.Duplicates != want.Duplicates {
		return errors.New("JetStream stream duplicate window drift")
	}
	if config.DenyDelete != want.DenyDelete || config.DenyPurge != want.DenyPurge ||
		config.AllowRollup != want.AllowRollup || config.AllowDirect != want.AllowDirect ||
		config.MirrorDirect != want.MirrorDirect {
		return errors.New("JetStream stream mutation guards drift")
	}
	if !contractMetadataMatches(config.Metadata, want.Metadata) {
		return errors.New("JetStream stream metadata drift")
	}
	normalizeStreamConfig(&config, want.Metadata)
	if !reflect.DeepEqual(config, want) {
		return errors.New("JetStream stream extended contract drift")
	}
	return nil
}

func ValidateSchedulerConsumerConfig(config jetstream.ConsumerConfig) error {
	want := SchedulerConsumerConfig()
	if config.Name != want.Name || config.Durable != want.Durable {
		return errors.New("scheduler consumer durable identity drift")
	}
	if config.DeliverPolicy != want.DeliverPolicy {
		return errors.New("scheduler consumer delivery policy drift")
	}
	if config.AckPolicy != want.AckPolicy || config.AckWait != want.AckWait {
		return errors.New("scheduler consumer ack policy drift")
	}
	if config.MaxDeliver != want.MaxDeliver {
		return errors.New("scheduler consumer redelivery drift")
	}
	if config.FilterSubject != want.FilterSubject || len(config.FilterSubjects) != 0 {
		return errors.New("scheduler consumer filter drift")
	}
	if config.ReplayPolicy != want.ReplayPolicy || config.MaxWaiting != want.MaxWaiting ||
		config.MaxRequestBatch != want.MaxRequestBatch ||
		config.MaxRequestExpires != want.MaxRequestExpires {
		return errors.New("scheduler consumer pull limits drift")
	}
	if config.MaxAckPending != want.MaxAckPending {
		return errors.New("scheduler consumer pending limit drift")
	}
	if config.Replicas != want.Replicas {
		return errors.New("scheduler consumer replicas drift")
	}
	if config.MemoryStorage != want.MemoryStorage {
		return errors.New("scheduler consumer memory storage drift")
	}
	if config.InactiveThreshold != want.InactiveThreshold {
		return errors.New("scheduler consumer inactive threshold drift")
	}
	if !contractMetadataMatches(config.Metadata, want.Metadata) {
		return errors.New("scheduler consumer metadata drift")
	}
	normalizeConsumerConfig(&config, want.Metadata)
	if !reflect.DeepEqual(config, want) {
		return errors.New("scheduler consumer extended contract drift")
	}
	return nil
}

func normalizeStreamConfig(config *jetstream.StreamConfig, metadata map[string]string) {
	if len(config.Sources) == 0 {
		config.Sources = nil
	}
	config.Metadata = metadata
}

func normalizeConsumerConfig(config *jetstream.ConsumerConfig, metadata map[string]string) {
	if len(config.BackOff) == 0 {
		config.BackOff = nil
	}
	if len(config.FilterSubjects) == 0 {
		config.FilterSubjects = nil
	}
	if len(config.PriorityGroups) == 0 {
		config.PriorityGroups = nil
	}
	config.Metadata = metadata
}

func contractMetadataMatches(actual, want map[string]string) bool {
	for key, value := range want {
		if actual[key] != value {
			return false
		}
	}
	for key := range actual {
		if _, owned := want[key]; !owned && !strings.HasPrefix(key, "_nats.") {
			return false
		}
	}
	return true
}
