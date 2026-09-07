package nodeagent

import (
	"context"
	"errors"
)

type RuntimeContainerObserver struct{}

func DialRuntimeContainerObserver(context.Context, RuntimeContainerObserverConfig) (*RuntimeContainerObserver, error) {
	return nil, errors.New("runtime container observation requires Linux socket inode pinning")
}

func (*RuntimeContainerObserver) Inspect(context.Context, RuntimeContainerTarget) (RuntimeContainerObservation, error) {
	return RuntimeContainerObservation{}, errors.New("runtime container observation requires Linux socket inode pinning")
}

func (*RuntimeContainerObserver) Close() error { return nil }
