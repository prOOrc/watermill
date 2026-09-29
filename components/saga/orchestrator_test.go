package saga

import (
	"context"
	"testing"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/stretchr/testify/require"
)

// Regression test: when the scan finds no step with an invocable action,
// executeNextStep must end the saga instead of executing the last checked step
// (which dereferences a nil action handler and panics).
func TestExecuteNextStep_NoInvocableStep_EndsSaga(t *testing.T) {
	t.Parallel()

	t.Run("compensating scan without compensation steps ends saga", func(t *testing.T) {
		t.Parallel()

		def := NewDefinition[struct{}]("test-saga", "test-saga.reply", nil).WithSteps([]Step{
			NewRemoteStep[struct{}]().Action(func(ctx context.Context, data *struct{}) cqrs.Command { return nil }),
			NewRemoteStep[struct{}]().Action(func(ctx context.Context, data *struct{}) cqrs.Command { return nil }),
		})

		o := &orchestrator{definition: def}

		results := o.executeNextStep(context.Background(), stepContext{step: 1, compensating: true}, &struct{}{})

		require.True(t, results.updatedStepContext.ended, "saga must end when no compensation step is found")
	})

	t.Run("forward scan with all steps skipped by predicate ends saga", func(t *testing.T) {
		t.Parallel()

		def := NewDefinition[struct{}]("test-saga", "test-saga.reply", nil).WithSteps([]Step{
			NewRemoteStep[struct{}]().Action(func(ctx context.Context, data *struct{}) cqrs.Command { return nil }),
			NewRemoteStep[struct{}]().Action(
				func(ctx context.Context, data *struct{}) cqrs.Command { return nil },
				WithRemoteStepPredicate(func(ctx context.Context, data any) bool { return false }),
			),
		})

		o := &orchestrator{definition: def}

		results := o.executeNextStep(context.Background(), stepContext{step: 0}, &struct{}{})

		require.True(t, results.updatedStepContext.ended, "saga must end when all remaining steps are skipped by predicate")
	})
}
