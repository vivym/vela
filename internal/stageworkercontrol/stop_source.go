package stageworkercontrol

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type AuthorityStopSourceConfig struct {
	PollInterval time.Duration
	Now          func() time.Time
}

type AuthorityStopSource struct {
	validator  *stageauthority.Validator
	authorizer Authorizer
	config     AuthorityStopSourceConfig
	mu         sync.Mutex
	streams    map[authorityStopStreamKey]*authorityStopStream
}

type authorityStopStreamKey struct {
	spiffeID     string
	sessionEpoch int64
}

type authorityStopStream struct {
	key         authorityStopStreamKey
	identity    stageworkertransport.Identity
	ctx         context.Context
	output      chan *velav1.StopStage
	mu          sync.Mutex
	authorities map[string]stageauthority.Verified
	sent        map[string]struct{}
}

func NewAuthorityStopSource(
	validator *stageauthority.Validator,
	authorizer Authorizer,
	config AuthorityStopSourceConfig,
) (*AuthorityStopSource, error) {
	if validator == nil || authorizer == nil || config.PollInterval <= 0 ||
		config.PollInterval > time.Minute {
		return nil, errors.New("stage worker authority StopSource configuration is invalid")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &AuthorityStopSource{
		validator: validator, authorizer: authorizer, config: config,
		streams: make(map[authorityStopStreamKey]*authorityStopStream),
	}, nil
}

func (source *AuthorityStopSource) Stops(
	ctx context.Context,
	identity stageworkertransport.Identity,
	sessionEpoch int64,
) <-chan *velav1.StopStage {
	if source == nil || source.validator == nil || source.authorizer == nil || ctx == nil ||
		strings.TrimSpace(identity.SPIFFEID) == "" || sessionEpoch <= 0 {
		closed := make(chan *velav1.StopStage)
		close(closed)
		return closed
	}
	key := authorityStopStreamKey{spiffeID: identity.SPIFFEID, sessionEpoch: sessionEpoch}
	stream := &authorityStopStream{
		key: key, identity: identity, ctx: ctx, output: make(chan *velav1.StopStage, 8),
		authorities: make(map[string]stageauthority.Verified),
		sent:        make(map[string]struct{}),
	}
	source.mu.Lock()
	source.streams[key] = stream
	source.mu.Unlock()
	go source.runStream(stream)
	return stream.output
}

func (source *AuthorityStopSource) ObserveStageAuthority(
	identity stageworkertransport.Identity,
	sessionEpoch int64,
	authority *velav1.StageAuthority,
) error {
	if source == nil || source.validator == nil || source.authorizer == nil ||
		strings.TrimSpace(identity.SPIFFEID) == "" || sessionEpoch <= 0 || authority == nil {
		return errors.New("stage worker tracked authority is incomplete")
	}
	verified, err := source.validator.ValidateEnvelope(authority)
	if err != nil {
		return err
	}
	leaseID := verified.Authority.GetStageLeaseId()
	if leaseID == "" {
		return errors.New("stage worker tracked authority has no StageLease identity")
	}
	key := authorityStopStreamKey{spiffeID: identity.SPIFFEID, sessionEpoch: sessionEpoch}
	source.mu.Lock()
	stream := source.streams[key]
	source.mu.Unlock()
	if stream == nil {
		return errors.New("stage worker control stream is not registered for authority tracking")
	}
	stream.mu.Lock()
	stream.authorities[leaseID] = verified
	stream.mu.Unlock()
	return nil
}

func (source *AuthorityStopSource) runStream(stream *authorityStopStream) {
	ticker := time.NewTicker(source.config.PollInterval)
	defer func() {
		ticker.Stop()
		source.mu.Lock()
		if source.streams[stream.key] == stream {
			delete(source.streams, stream.key)
		}
		source.mu.Unlock()
		close(stream.output)
	}()
	for {
		select {
		case <-stream.ctx.Done():
			return
		case <-ticker.C:
			source.pollStream(stream)
		}
	}
}

func (source *AuthorityStopSource) pollStream(stream *authorityStopStream) {
	stream.mu.Lock()
	authorities := make(map[string]stageauthority.Verified, len(stream.authorities))
	for leaseID, authority := range stream.authorities {
		if _, alreadySent := stream.sent[leaseID]; !alreadySent {
			authorities[leaseID] = authority
		}
	}
	stream.mu.Unlock()
	for leaseID, authority := range authorities {
		now := source.config.Now().UTC()
		reason := velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_UNSPECIFIED
		if !now.Before(authority.Authority.GetExpiresAt().AsTime().UTC()) {
			reason = velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_LEASE_EXPIRED
		} else {
			active, err := source.authorizer.IsActive(
				stream.ctx,
				stream.identity,
				stream.key.sessionEpoch,
				OperationReattachStage,
				authority,
			)
			if err != nil {
				continue
			}
			if !active {
				reason = velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_AUTHORITY_REVOKED
			}
		}
		if reason == velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_UNSPECIFIED {
			continue
		}
		stream.mu.Lock()
		if _, alreadySent := stream.sent[leaseID]; alreadySent {
			stream.mu.Unlock()
			continue
		}
		stream.sent[leaseID] = struct{}{}
		stream.mu.Unlock()
		stop := &velav1.StopStage{
			Authority: proto.Clone(authority.Authority).(*velav1.StageAuthority),
			Reason:    reason,
			IssuedAt:  timestamppb.New(now),
		}
		select {
		case stream.output <- stop:
		case <-stream.ctx.Done():
			return
		}
	}
}

var (
	_ stageworkertransport.StopSource        = (*AuthorityStopSource)(nil)
	_ stageworkertransport.AuthorityObserver = (*AuthorityStopSource)(nil)
)
