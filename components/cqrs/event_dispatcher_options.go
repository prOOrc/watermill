package cqrs

import (
	"github.com/ThreeDotsLabs/watermill"
)

// EventDispatcherOption options for EventDispatcher
type EventDispatcherOption func(consumer *EventDispatcher)

// WithEventDispatcherLogger is an option to set the log.Logger of the EventDispatcher
func WithEventDispatcherLogger(logger watermill.LoggerAdapter) EventDispatcherOption {
	return func(dispatcher *EventDispatcher) {
		dispatcher.logger = logger
	}
}
