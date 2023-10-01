package cqrs

import (
	stdErrors "errors"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type ReplyCommandProcessorConfig struct {
	// GenerateSubscribeTopic is used to generate topic for subscribing command.
	GenerateSubscribeTopic ReplyCommandProcessorGenerateSubscribeTopicFn

	// SubscriberConstructor is used to create subscriber for ReplyCommandHandler.
	SubscriberConstructor ReplyCommandProcessorSubscriberConstructorFn

	Publisher message.Publisher

	// OnHandle is called before handling command.
	// OnHandle works in a similar way to middlewares: you can inject additional logic before and after handling a command.
	//
	// Because of that, you need to explicitly call params.Handler.Handle() to handle the command.
	//  func(params ReplyCommandProcessorOnHandleParams) (err error) {
	//      // logic before handle
	//      //  (...)
	//
	//      err := params.Handler.Handle(params.Message.Context(), params.ReplyCommand)
	//
	//      // logic after handle
	//      //  (...)
	//
	//      return err
	//  }
	//
	// This option is not required.
	OnHandle ReplyCommandProcessorOnHandleFn

	// Marshaler is used to marshal and unmarshal commands.
	// It is required.
	Marshaler CommandEventMarshaler

	// Logger instance used to log.
	// If not provided, watermill.NopLogger is used.
	Logger watermill.LoggerAdapter

	// If true, ReplyCommandProcessor will ack messages even if ReplyCommandHandler returns an error.
	// If RequestReplyBackend is not null and sending reply fails, the message will be nack-ed anyway.
	//
	// Warning: It's not recommended to use this option when you are using requestreply component
	// (requestreply.NewReplyCommandHandler or requestreply.NewReplyCommandHandlerWithResult), as it may ack the
	// command when sending reply failed.
	//
	// When you are using requestreply, you should use requestreply.PubSubBackendConfig.AckReplyCommandErrors.
	AckReplyCommandHandlingErrors bool

	// disableRouterAutoAddHandlers is used to keep backwards compatibility.
	// it is set when ReplyCommandProcessor is created by NewReplyCommandProcessor.
	// Deprecated: please migrate to NewReplyCommandProcessorWithConfig.
	disableRouterAutoAddHandlers bool
}

func (c *ReplyCommandProcessorConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

func (c ReplyCommandProcessorConfig) Validate() error {
	var err error
	
	if c.Marshaler == nil {
		err = stdErrors.Join(err, errors.New("missing Marshaler"))
	}

	if c.Publisher == nil {
		err = stdErrors.Join(err, errors.New("missing Publisher"))
	}

	if c.GenerateSubscribeTopic == nil {
		err = stdErrors.Join(err, errors.New("missing GenerateSubscribeTopic"))
	}
	if c.SubscriberConstructor == nil {
		err = stdErrors.Join(err, errors.New("missing SubscriberConstructor"))
	}

	return err
}

type ReplyCommandProcessorGenerateSubscribeTopicFn func(ReplyCommandProcessorGenerateSubscribeTopicParams) (string, error)

type ReplyCommandProcessorGenerateSubscribeTopicParams struct {
	CommandName    string
	CommandHandler ReplyCommandHandler
}

// ReplyCommandProcessorSubscriberConstructorFn creates subscriber for ReplyCommandHandler.
// It allows you to create a separate customized Subscriber for every command handler.
type ReplyCommandProcessorSubscriberConstructorFn func(ReplyCommandProcessorSubscriberConstructorParams) (message.Subscriber, error)

type ReplyCommandProcessorSubscriberConstructorParams struct {
	HandlerName string
	Handler     ReplyCommandHandler
}

type ReplyCommandProcessorOnHandleFn func(params ReplyCommandProcessorOnHandleParams) ([]ReplyMessage, error)

type ReplyCommandProcessorOnHandleParams struct {
	Handler ReplyCommandHandler

	CommandName string
	Command     any

	// Message is never nil and can be modified.
	Message *message.Message
}

// ReplyCommandProcessor determines which ReplyCommandHandler should handle the command received from the command bus.
type ReplyCommandProcessor struct {
	router *message.Router

	handlers []ReplyCommandHandler

	config ReplyCommandProcessorConfig
}

func NewReplyCommandProcessorWithConfig(router *message.Router, config ReplyCommandProcessorConfig) (*ReplyCommandProcessor, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if router == nil && !config.disableRouterAutoAddHandlers {
		return nil, errors.New("missing router")
	}

	return &ReplyCommandProcessor{
		router: router,
		config: config,
	}, nil
}

// NewReplyCommandProcessor creates a new ReplyCommandProcessor.
// Deprecated. Use NewReplyCommandProcessorWithConfig instead.
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
	if publisher == nil {
		return nil, errors.New("missing publisher")
	}
	if subscriberConstructor == nil {
		return nil, errors.New("missing subscriberConstructor")
	}

	cp, err := NewReplyCommandProcessorWithConfig(
		nil,
		ReplyCommandProcessorConfig{
			Publisher: publisher,
			GenerateSubscribeTopic: func(params ReplyCommandProcessorGenerateSubscribeTopicParams) (string, error) {
				return generateTopic(params.CommandName), nil
			},
			SubscriberConstructor: func(params ReplyCommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return subscriberConstructor(params.HandlerName)
			},
			Marshaler:                    marshaler,
			Logger:                       logger,
			disableRouterAutoAddHandlers: true,
		},
	)
	if err != nil {
		return nil, err
	}

	for _, handler := range handlers {
		if err := cp.AddHandlers(handler); err != nil {
			return nil, err
		}
	}

	return cp, nil
}

// ReplyCommandsSubscriberConstructor creates subscriber for ReplyCommandHandler.
// It allows you to create a separate customized Subscriber for every command handler.
//
// Deprecated: please use CommandProcessorSubscriberConstructorFn instead.
type ReplyCommandsSubscriberConstructor func(handlerName string) (message.Subscriber, error)

// AddHandlers adds a new ReplyCommandHandler to the ReplyCommandProcessor and adds it to the router.
func (p *ReplyCommandProcessor) AddHandlers(handlers ...ReplyCommandHandler) error {
	handledCommands := map[string]struct{}{}
	for _, handler := range handlers {
		commandName := p.config.Marshaler.Name(handler.NewCommand())
		if _, ok := handledCommands[commandName]; ok {
			return DuplicateCommandHandlerError{commandName}
		}

		handledCommands[commandName] = struct{}{}
	}

	if p.config.disableRouterAutoAddHandlers {
		p.handlers = append(p.handlers, handlers...)
		return nil
	}

	for _, handler := range handlers {
		if err := p.addHandlerToRouter(p.router, handler); err != nil {
			return err
		}

		p.handlers = append(p.handlers, handler)
	}

	return nil
}

type DuplicateReplyCommandHandlerError struct {
	ReplyCommandName string
}

func (d DuplicateReplyCommandHandlerError) Error() string {
	return fmt.Sprintf("command handler for command %s already exists", d.ReplyCommandName)
}

// AddHandlersToRouter adds the ReplyCommandProcessor's handlers to the given router.
// It should be called only once per ReplyCommandProcessor instance.
//
// It is required to call AddHandlersToRouter only if command processor is created with NewReplyCommandProcessor (disableRouterAutoAddHandlers is set to true).
// Deprecated: please migrate to command processor created by NewReplyCommandProcessorWithConfig.
func (p ReplyCommandProcessor) AddHandlersToRouter(r *message.Router) error {
	if !p.config.disableRouterAutoAddHandlers {
		return errors.New("AddHandlersToRouter should be called only when using deprecated NewReplyCommandProcessor")
	}

	for i := range p.Handlers() {
		handler := p.handlers[i]

		if err := p.addHandlerToRouter(r, handler); err != nil {
			return err
		}
	}

	return nil
}

func (p ReplyCommandProcessor) addHandlerToRouter(r *message.Router, handler ReplyCommandHandler) error {
	handlerName := handler.HandlerName()
	commandName := p.config.Marshaler.Name(handler.NewCommand())

	topicName, err := p.config.GenerateSubscribeTopic(ReplyCommandProcessorGenerateSubscribeTopicParams{
		CommandName:    commandName,
		CommandHandler: handler,
	})
	if err != nil {
		return errors.Wrapf(err, "cannot generate topic for command handler %s", handlerName)
	}

	logger := p.config.Logger.With(watermill.LogFields{
		"command_handler_name": handlerName,
		"topic":                topicName,
	})

	handlerFunc, err := p.routerHandlerFunc(handler, logger)
	if err != nil {
		return err
	}

	logger.Debug("Adding reply command handler to router", nil)

	subscriber, err := p.config.SubscriberConstructor(ReplyCommandProcessorSubscriberConstructorParams{
		HandlerName: handlerName,
		Handler:     handler,
	})
	if err != nil {
		return errors.Wrap(err, "cannot create subscriber for command processor")
	}

	r.AddHandler(
		handlerName,
		topicName,
		subscriber,
		"",
		p.config.Publisher,
		handlerFunc,
	)

	return nil
}

func (p ReplyCommandProcessor) Handlers() []ReplyCommandHandler {
	return p.handlers
}

func (p ReplyCommandProcessor) routerHandlerFunc(handler ReplyCommandHandler, logger watermill.LoggerAdapter) (message.HandlerFunc, error) {
	cmd := handler.NewCommand()
	cmdName := p.config.Marshaler.Name(cmd)

	if err := p.validateReplyCommand(cmd); err != nil {
		return nil, err
	}

	return func(msg *message.Message) (replyMessages []*message.Message, err error) {
		cmd := handler.NewCommand()
		messageCmdName := p.config.Marshaler.NameFromMessage(msg)

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

		ctx := CtxWithOriginalMessage(msg.Context(), msg)
		msg.SetContext(ctx)

		if err := p.config.Marshaler.Unmarshal(msg, cmd); err != nil {
			return replyMessages, err
		}

		handle := func(params ReplyCommandProcessorOnHandleParams) (replies []ReplyMessage, err error) {
			return params.Handler.Handle(ctx, params.Command)
		}
		if p.config.OnHandle != nil {
			handle = p.config.OnHandle
		}

		replies, err := handle(ReplyCommandProcessorOnHandleParams{
			Handler:     handler,
			CommandName: messageCmdName,
			Command:     cmd,
			Message:     msg,
		})

		if p.config.AckReplyCommandHandlingErrors && err != nil {
			logger.Error("Error when handling command, acking (AckCommandHandlingErrors is enabled)", err, nil)
			return replyMessages, err
		}
		if err != nil {
			logger.Debug("Error when handling command, nacking", watermill.LogFields{"err": err})
			return replyMessages, err
		}

		replyMessages = make([]*message.Message, len(replies))
		correlationHeaders := p.correlationHeaders(msg.Metadata)
		for i, reply := range replies {
			m, err := p.config.Marshaler.Marshal(reply.Reply())
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
