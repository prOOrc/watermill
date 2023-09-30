package saga

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

type remoteStepAction struct {
	predicate func(context.Context, SagaData) bool
	handler   func(context.Context, SagaData) cqrs.Command
}

func (a *remoteStepAction) isInvocable(ctx context.Context, sagaData SagaData) bool {
	if a.predicate == nil {
		return true
	}

	return a.predicate(ctx, sagaData)
}

func (a *remoteStepAction) execute(ctx context.Context, sagaData SagaData) cqrs.Command {
	return a.handler(ctx, sagaData)
}
