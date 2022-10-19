package saga

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

// Step interface for local, remote, ...other saga steps
type Step interface {
	hasInvocableAction(ctx context.Context, sagaData SagaData, compensating bool) bool
	getReplyHandler(replyName string, compensating bool) func(ctx context.Context, data SagaData, reply cqrs.Reply) error
	execute(ctx context.Context, sagaData SagaData, compensating bool) func(results *stepResults)
}
