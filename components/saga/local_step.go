package saga

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

// LocalStep is used to execute local saga business logic
type LocalStep struct {
	actions map[bool]func(context.Context, SagaData) error
}

var _ Step = (*LocalStep)(nil)

// NewLocalStep constructor for LocalStep
func NewLocalStep(action func(context.Context, SagaData) error) LocalStep {
	return LocalStep{
		actions: map[bool]func(context.Context, SagaData) error{
			notCompensating: action,
		},
	}
}

// Compensation sets the compensating action for this step
func (s LocalStep) Compensation(compensation func(context.Context, SagaData) error) LocalStep {
	s.actions[isCompensating] = compensation
	return s
}

func (s LocalStep) hasInvocableAction(_ context.Context, _ SagaData, compensating bool) bool {
	return s.actions[compensating] != nil
}

func (s LocalStep) getReplyHandler(string, bool) func(context.Context, SagaData, cqrs.Reply) error {
	return nil
}

func (s LocalStep) execute(ctx context.Context, sagaData SagaData, compensating bool) func(results *stepResults) {
	err := s.actions[compensating](ctx, sagaData)
	return func(results *stepResults) {
		results.local = true
		results.failure = err
	}
}
