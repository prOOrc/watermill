package cqrs

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// EventDispatcherFactory is a MessageReceiver for Events
type EventDispatcherFactory struct {
	subscriberConstructor EventsSubscriberConstructor
	marshaler             CommandEventMarshaler
	logger                watermill.LoggerAdapter
	options               []EventDispatcherOption
}

// NewEventDispatcherFactory constructs a new EventDispatcherFactory
func NewEventDispatcherFactory(
	subscriberConstructor EventsSubscriberConstructor,
	marshaler CommandEventMarshaler,
	logger watermill.LoggerAdapter,
	options ...EventDispatcherOption,
) *EventDispatcherFactory {
	f := &EventDispatcherFactory{
		subscriberConstructor: subscriberConstructor,
		marshaler:             marshaler,
		logger:                logger,
		options:               options,
	}

	f.logger.Trace("saga.EventDispatcherFactory constructed", watermill.LogFields{})

	return f
}

func (f *EventDispatcherFactory) New() *EventDispatcher {
	return NewEventDispatcher(f.subscriberConstructor, f.marshaler, f.logger, f.options...)
}

// EventDispatcher is a MessageReceiver for Events
type EventDispatcher struct {
	subscriberConstructor EventsSubscriberConstructor
	marshaler             CommandEventMarshaler
	logger                watermill.LoggerAdapter
	handlers              map[string]EventHandler
}

// NewEventDispatcher constructs a new EventDispatcher
func NewEventDispatcher(
	subscriberConstructor EventsSubscriberConstructor,
	marshaler CommandEventMarshaler,
	logger watermill.LoggerAdapter,
	options ...EventDispatcherOption,
) *EventDispatcher {
	c := &EventDispatcher{
		subscriberConstructor: subscriberConstructor,
		marshaler:             marshaler,
		logger:                logger,
		handlers:              map[string]EventHandler{},
	}

	for _, option := range options {
		option(c)
	}

	c.logger.Trace("msg.EventDispatcher constructed", watermill.LogFields{})

	return c
}

// Handle adds a new Event that will be handled by EventMessageFunc handler
func (d *EventDispatcher) Handle(handler EventHandler) *EventDispatcher {
	evt := handler.NewEvent()
	eventName := d.marshaler.Name(evt)
	d.logger.Trace("event handler added", watermill.LogFields{"event_name": eventName})
	d.handlers[eventName] = handler
	return d
}

func (d *EventDispatcher) AddHandlerToRouter(handlerName, topicName string, r *message.Router) error {

	logger := d.logger.With(watermill.LogFields{
		"event_handler_name": handlerName,
		"topic":              topicName,
	})

	logger.Debug("Adding event dispatcher to router", nil)

	subscriber, err := d.subscriberConstructor(handlerName)
	if err != nil {
		return errors.Wrap(err, "cannot create subscriber for event processor")
	}

	r.AddNoPublisherHandler(
		handlerName,
		topicName,
		subscriber,
		d.receiveMessage,
	)

	return nil
}

// ReceiveMessage implements MessageReceiver.ReceiveMessage
func (d *EventDispatcher) receiveMessage(msg *message.Message) error {
	eventName, err := msg.Metadata.GetRequired(MessageEventName)
	if err != nil {
		d.logger.Error("error reading event name", err, watermill.LogFields{})
		return nil
	}

	logger := d.logger.With(
		watermill.LogFields{
			"event_name": eventName,
			"message_id": msg.UUID,
		},
	)

	logger.Debug("received event message", watermill.LogFields{})

	// check first for a handler of the event; It is possible events might be published into channels
	// that haven't been registered in our application
	handler, exists := d.handlers[eventName]
	if !exists {
		return nil
	}

	logger.Trace("event handler found", watermill.LogFields{})

	event := handler.NewEvent()
	err = d.marshaler.Unmarshal(msg, event)
	if err != nil {
		logger.Error("error decoding event message payload", err, watermill.LogFields{})
		return nil
	}

	ctx := msg.Context()
	err = handler.Handle(ctx, event)
	if err != nil {
		logger.Error("event handler returned an error", err, watermill.LogFields{})
	}

	return err
}
