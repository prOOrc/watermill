package saga

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

// LocalStep is used to execute local saga business logic
type LocalStep[Data any] struct {
	actions map[bool]func(context.Context, any) error
}

var _ Step = (*LocalStep[any])(nil)

// NewLocalStep constructor for LocalStep
func NewLocalStep[Data any](action func(context.Context, *Data) error) LocalStep[Data] {
	return LocalStep[Data]{
		actions: map[bool]func(context.Context, any) error{
			notCompensating: func(ctx context.Context, data any) error {
				return action(ctx, data.(*Data))
			},
		},
	}
}

// Compensation sets the compensating action for this step
func (s LocalStep[Data]) Compensation(compensation func(context.Context, *Data) error) LocalStep[Data] {
	s.actions[isCompensating] = func(ctx context.Context, data any) error {
		return compensation(ctx, data.(*Data))
	}
	return s
}

func (s LocalStep[Data]) hasInvocableAction(_ context.Context, _ any, compensating bool) bool {
	return s.actions[compensating] != nil
}

func (s LocalStep[Data]) getReplyHandler(string, bool) func(context.Context, any, cqrs.Reply) error {
	return nil
}

func (s LocalStep[Data]) execute(ctx context.Context, sagaData any, compensating bool) func(results *stepResults) {
	err := s.actions[compensating](ctx, sagaData)
	return func(results *stepResults) {
		results.local = true
		results.failure = err
	}
}
