package saga

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

type remoteStepAction struct {
	predicate func(context.Context, any) bool
	handler   func(context.Context, any) cqrs.Command
}

func (a *remoteStepAction) isInvocable(ctx context.Context, sagaData any) bool {
	if a.predicate == nil {
		return true
	}

	return a.predicate(ctx, sagaData)
}

func (a *remoteStepAction) execute(ctx context.Context, sagaData any) cqrs.Command {
	return a.handler(ctx, sagaData)
}
