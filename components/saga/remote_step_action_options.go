package saga

import (
	"context"
)

// RemoteStepActionOption options for remoteStepAction
type RemoteStepActionOption func(action *remoteStepAction)

// WithRemoteStepPredicate sets a predicate function for the action
func WithRemoteStepPredicate(predicate func(context.Context, SagaData) bool) RemoteStepActionOption {
	return func(step *remoteStepAction) {
		step.predicate = predicate
	}
}
