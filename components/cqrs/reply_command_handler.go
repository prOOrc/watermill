package cqrs

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type ReplyCommandHandleFunc func(ctx context.Context, cmd interface{}) ([]ReplyMessage, error)

// CommandHandler receives a command defined by NewCommand and handles it with the Handle method.
// If using DDD, CommandHandler may modify and persist the aggregate.
//
// In contrast to EventHandler, every Command must have only one CommandHandler.
//
// One instance of CommandHandler is used during handling messages.
// When multiple commands are delivered at the same time, Handle method can be executed multiple times at the same time.
// Because of that, Handle method needs to be thread safe!
type ReplyCommandHandler interface {
	// HandlerName is the name used in message.Router while creating handler.
	//
	// It will be also passed to CommandsSubscriberConstructor.
	// May be useful, for example, to create a consumer group per each handler.
	//
	// WARNING: If HandlerName was changed and is used for generating consumer groups,
	// it may result with **reconsuming all messages**!
	HandlerName() string

	NewCommand() interface{}
	Handle(ctx context.Context, cmd interface{}) ([]ReplyMessage, error)
}

type replyCommandHandler struct {
	name       string
	newCommand func() interface{}
	handle     ReplyCommandHandleFunc
}

// Handle implements CommandHandler
func (c *replyCommandHandler) Handle(ctx context.Context, cmd interface{}) ([]ReplyMessage, error) {
	return c.handle(ctx, cmd)
}

// HandlerName implements CommandHandler
func (c *replyCommandHandler) HandlerName() string {
	return c.name
}

// NewCommand implements CommandHandler
func (c *replyCommandHandler) NewCommand() interface{} {
	return c.newCommand()
}

func NewReplyCommandHandler(
	name string,
	newCommand func() interface{},
	handle ReplyCommandHandleFunc,
) ReplyCommandHandler {
	return &replyCommandHandler{
		name:       name,
		newCommand: newCommand,
		handle:     handle,
	}
}

// ReplyCommandProcessor determines which ReplyCommandHandler should handle the command received from the command bus.
type ReplyCommandProcessor struct {
	handlers      []ReplyCommandHandler
	generateTopic func(commandName string) string

	subscriberConstructor CommandsSubscriberConstructor
	publisher             message.Publisher

	marshaler CommandEventMarshaler
	logger    watermill.LoggerAdapter
}

func NewReplyCommandProcessor(
	handlers []ReplyCommandHandler,
	generateTopic func(commandName string) string,
	subscriberConstructor CommandsSubscriberConstructor,
	publisher message.Publisher,
	marshaler CommandEventMarshaler,
	logger watermill.LoggerAdapter,
) (*ReplyCommandProcessor, error) {
	if len(handlers) == 0 {
		return nil, errors.New("missing handlers")
	}
	if generateTopic == nil {
		return nil, errors.New("missing generateTopic")
	}
	if subscriberConstructor == nil {
		return nil, errors.New("missing subscriberConstructor")
	}
	if marshaler == nil {
		return nil, errors.New("missing marshaler")
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &ReplyCommandProcessor{
		handlers,
		generateTopic,
		subscriberConstructor,
		publisher,
		marshaler,
		logger,
	}, nil
}

type DuplicateReplyCommandHandlerError struct {
	ReplyCommandName string
}

func (d DuplicateReplyCommandHandlerError) Error() string {
	return fmt.Sprintf("command handler for command %s already exists", d.ReplyCommandName)
}

func (p ReplyCommandProcessor) AddHandlersToRouter(r *message.Router) error {
	handledReplyCommands := map[string]struct{}{}

	for i := range p.Handlers() {
		handler := p.handlers[i]
		handlerName := handler.HandlerName()
		commandName := p.marshaler.Name(handler.NewCommand())
		topicName := p.generateTopic(commandName)

		if _, ok := handledReplyCommands[commandName]; ok {
			return DuplicateReplyCommandHandlerError{commandName}
		}
		handledReplyCommands[commandName] = struct{}{}

		logger := p.logger.With(watermill.LogFields{
			"command_handler_name": handlerName,
			"topic":                topicName,
		})

		handlerFunc, err := p.routerHandlerFunc(handler, logger)
		if err != nil {
			return err
		}

		logger.Debug("Adding reply command handler to router", nil)

		subscriber, err := p.subscriberConstructor(handlerName)
		if err != nil {
			return errors.Wrap(err, "cannot create subscriber for command processor")
		}

		r.AddHandler(
			handlerName,
			topicName,
			subscriber,
			"",
			p.publisher,
			handlerFunc,
		)
	}

	return nil
}

func (p ReplyCommandProcessor) Handlers() []ReplyCommandHandler {
	return p.handlers
}

func (p ReplyCommandProcessor) routerHandlerFunc(handler ReplyCommandHandler, logger watermill.LoggerAdapter) (message.HandlerFunc, error) {
	cmd := handler.NewCommand()
	cmdName := p.marshaler.Name(cmd)

	if err := p.validateReplyCommand(cmd); err != nil {
		return nil, err
	}

	return func(msg *message.Message) (replyMessages []*message.Message, err error) {
		cmd := handler.NewCommand()
		messageCmdName := p.marshaler.NameFromMessage(msg)

		if messageCmdName != cmdName {
			logger.Trace("Received different command type than expected, ignoring", watermill.LogFields{
				"message_uuid":          msg.UUID,
				"expected_command_type": cmdName,
				"received_command_type": messageCmdName,
			})
			return replyMessages, nil
		}

		logger.Debug("Handling command", watermill.LogFields{
			"message_uuid":          msg.UUID,
			"received_command_type": messageCmdName,
		})

		if err := p.marshaler.Unmarshal(msg, cmd); err != nil {
			return replyMessages, err
		}
		replies, err := handler.Handle(msg.Context(), cmd)
		if err != nil {
			logger.Debug("Error when handling command", watermill.LogFields{"err": err})
			return replyMessages, err
		}
		replyMessages = make([]*message.Message, len(replies))
		correlationHeaders := p.correlationHeaders(msg.Metadata)
		for i, reply := range replies {
			m, err := p.marshaler.Marshal(reply.Reply())
			message.WithHeaders(correlationHeaders)(m)
			message.WithHeaders(reply.Headers())(m)
			m.SetContext(msg.Context())
			if err != nil {
				return replyMessages, err
			}
			replyMessages[i] = m
		}

		return replyMessages, nil
	}, nil
}

func (p *ReplyCommandProcessor) correlationHeaders(headers message.Metadata) message.Metadata {
	replyHeaders := make(map[string]string)
	for key, value := range headers {
		if key == MessageCommandName {
			continue
		}

		if strings.HasPrefix(key, MessageCommandPrefix) {
			replyHeader := MessageReplyPrefix + key[len(MessageCommandPrefix):]
			replyHeaders[replyHeader] = value
		}
	}

	return replyHeaders
}


func (p ReplyCommandProcessor) validateReplyCommand(cmd interface{}) error {
	// ReplyCommandHandler's NewReplyCommand must return a pointer, because it is used to unmarshal
	if err := isPointer(cmd); err != nil {
		return errors.Wrap(err, "command must be a non-nil pointer")
	}

	return nil
}
