package saga

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

// Step interface for local, remote, ...other saga steps
type Step interface {
	hasInvocableAction(ctx context.Context, sagaData any, compensating bool) bool
	getReplyHandler(replyName string, compensating bool) func(ctx context.Context, data any, reply cqrs.Reply) error
	execute(ctx context.Context, sagaData any, compensating bool) func(results *stepResults)
}
