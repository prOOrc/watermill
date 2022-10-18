package saga

import (
	"github.com/ThreeDotsLabs/watermill"
)

// CommandDispatcherOption options for CommandConsumers
type CommandDispatcherOption func(consumer *CommandDispatcher)

// WithCommandDispatcherLogger is an option to set the log.Logger of the CommandDispatcher
func WithCommandDispatcherLogger(logger watermill.LoggerAdapter) CommandDispatcherOption {
	return func(dispatcher *CommandDispatcher) {
		dispatcher.logger = logger
	}
}
