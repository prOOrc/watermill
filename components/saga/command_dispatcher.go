package saga

import (
	"context"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// CommandDispatcherFactory is a MessageReceiver for Commands
type CommandDispatcherFactory struct {
	subscriberConstructor cqrs.CommandsSubscriberConstructor
	publisher             message.Publisher
	marshaler             cqrs.CommandEventMarshaler
	logger                watermill.LoggerAdapter
	options               []CommandDispatcherOption
}

// NewCommandDispatcherFactory constructs a new CommandDispatcherFactory
func NewCommandDispatcherFactory(
	subscriberConstructor cqrs.CommandsSubscriberConstructor,
	publisher message.Publisher,
	marshaler cqrs.CommandEventMarshaler,
	logger watermill.LoggerAdapter,
	options ...CommandDispatcherOption,
) *CommandDispatcherFactory {
	f := &CommandDispatcherFactory{
		subscriberConstructor: subscriberConstructor,
		publisher:             publisher,
		marshaler:             marshaler,
		logger:                logger,
		options:               options,
	}

	f.logger.Trace("saga.CommandDispatcherFactory constructed", watermill.LogFields{})

	return f
}

func (f *CommandDispatcherFactory) New() *CommandDispatcher {
	return NewCommandDispatcher(f.subscriberConstructor, f.publisher, f.marshaler, f.logger, f.options...)
}

// CommandDispatcher is a MessageReceiver for Commands
type CommandDispatcher struct {
	subscriberConstructor cqrs.CommandsSubscriberConstructor
	publisher             message.Publisher
	marshaler             cqrs.CommandEventMarshaler
	handlers              map[string]cqrs.ReplyCommandHandler
	logger                watermill.LoggerAdapter
}

// NewCommandDispatcher constructs a new CommandDispatcher
func NewCommandDispatcher(
	subscriberConstructor cqrs.CommandsSubscriberConstructor,
	publisher message.Publisher,
	marshaler cqrs.CommandEventMarshaler,
	logger watermill.LoggerAdapter,
	options ...CommandDispatcherOption,
) *CommandDispatcher {
	c := &CommandDispatcher{
		subscriberConstructor: subscriberConstructor,
		publisher:             publisher,
		marshaler:             marshaler,
		handlers:              map[string]cqrs.ReplyCommandHandler{},
		logger:                logger,
	}

	for _, option := range options {
		option(c)
	}

	c.logger.Trace("saga.CommandDispatcher constructed", watermill.LogFields{})

	return c
}

// Handle adds a new Command that will be handled by handler
func (d *CommandDispatcher) Handle(handler cqrs.ReplyCommandHandler) *CommandDispatcher {
	cmd := handler.NewCommand()
	commandName := d.marshaler.Name(cmd)
	d.logger.Trace("saga command handler added", watermill.LogFields{"command_name": commandName})
	d.handlers[commandName] = handler
	return d
}

func (d *CommandDispatcher) AddHandlerToRouter(handlerName, topicName string, r *message.Router) error {

	logger := d.logger.With(watermill.LogFields{
		"command_handler_name": handlerName,
		"topic":                topicName,
	})

	logger.Debug("Adding command dispatcher to router", nil)

	subscriber, err := d.subscriberConstructor(handlerName)
	if err != nil {
		return errors.Wrap(err, "cannot create subscriber for command processor")
	}

	r.AddHandler(
		handlerName,
		topicName,
		subscriber,
		"",
		d.publisher,
		d.receiveMessage,
	)

	return nil
}

// receiveMessage implements MessageReceiver.receiveMessage
func (d *CommandDispatcher) receiveMessage(msg *message.Message) (messages []*message.Message, err error) {
	commandName, sagaID, sagaName, err := d.commandMessageInfo(msg)
	if err != nil {
		return messages, nil
	}

	logger := d.logger.With(
		watermill.LogFields{
			"command_name": commandName,
			"saga_name":    sagaName,
			"saga_id":      sagaID,
			"message_id":   msg.UUID,
		},
	)

	logger.Debug("received saga command message", watermill.LogFields{})

	// check first for a handler of the command; It is possible commands might be published into channels
	// that haven't been registered in our application
	handler, exists := d.handlers[commandName]
	if !exists {
		return messages, nil
	}

	logger.Trace("saga command handler found", watermill.LogFields{})

	cmd := handler.NewCommand()
	err = d.marshaler.Unmarshal(msg, cmd)
	if err != nil {
		logger.Error("error decoding saga command message payload", err, watermill.LogFields{})
		return messages, nil
	}

	correlationHeaders := d.correlationHeaders(msg.Metadata)

	command := cmd.(cqrs.Command)

	ctx := msg.Context()
	replies, err := handler.Handle(ctx, command)
	if err != nil {
		logger.Error("saga command handler returned an error", err, watermill.LogFields{})
		messages, rerr := d.buildReplies(ctx, []cqrs.ReplyMessage{cqrs.WithFailure()}, correlationHeaders)
		if rerr != nil {
			logger.Error("error building replies", rerr, watermill.LogFields{})
			return messages, rerr
		}
		return messages, nil
	}

	messages, err = d.buildReplies(ctx, replies, correlationHeaders)
	if err != nil {
		logger.Error("error building replies", err, watermill.LogFields{})
		return messages, err
	}

	return messages, nil
}

func (d *CommandDispatcher) commandMessageInfo(message *message.Message) (string, string, string, error) {
	var err error
	var commandName, sagaID, sagaName string

	commandName, err = message.Metadata.GetRequired(cqrs.MessageCommandName)
	if err != nil {
		d.logger.Error("error reading command name", err, watermill.LogFields{})
		return "", "", "", err
	}

	sagaID, err = message.Metadata.GetRequired(MessageCommandSagaID)
	if err != nil {
		d.logger.Error("error reading saga id", err, watermill.LogFields{})
		return "", "", "", err
	}

	sagaName, err = message.Metadata.GetRequired(MessageCommandSagaName)
	if err != nil {
		d.logger.Error("error reading saga name", err, watermill.LogFields{})
		return "", "", "", err
	}

	return commandName, sagaID, sagaName, nil
}

func (d *CommandDispatcher) buildReplies(ctx context.Context, replies []cqrs.ReplyMessage, correlationHeaders message.Metadata) (messages []*message.Message, err error) {
	messages = make([]*message.Message, len(replies))
	for i, reply := range replies {
		msg, err := d.marshaler.Marshal(reply.Reply())
		if err != nil {
			return messages, err
		}
		message.WithHeaders(correlationHeaders)(msg)
		message.WithHeaders(reply.Headers())(msg)
		msg.SetContext(ctx)
		messages[i] = msg
	}
	return messages, nil
}

func (d *CommandDispatcher) correlationHeaders(headers message.Metadata) message.Metadata {
	replyHeaders := make(map[string]string)
	for key, value := range headers {
		if key == cqrs.MessageCommandName {
			continue
		}

		if strings.HasPrefix(key, cqrs.MessageCommandPrefix) {
			replyHeader := cqrs.MessageReplyPrefix + key[len(cqrs.MessageCommandPrefix):]
			replyHeaders[replyHeader] = value
		}
	}

	return replyHeaders
}
