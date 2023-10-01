package cqrs

import "context"

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

	NewCommand() any
	Handle(ctx context.Context, cmd any) ([]ReplyMessage, error)
}

type genericReplyCommandHandler[Command any] struct {
	handleFunc  func(ctx context.Context, cmd *Command) ([]ReplyMessage, error)
	handlerName string
}

// NewReplyCommandHandler creates a new ReplyCommandHandler implementation based on provided function
// and command type inferred from function argument.
func NewReplyCommandHandler[ReplyCommand any](
	handlerName string,
	handleFunc func(ctx context.Context, cmd *ReplyCommand) ([]ReplyMessage, error),
) ReplyCommandHandler {
	return &genericReplyCommandHandler[ReplyCommand]{
		handleFunc:  handleFunc,
		handlerName: handlerName,
	}
}

// NewSimpleReplyCommandHandler creates a new ReplyCommandHandler implementation based on provided function
// and command type inferred from function argument.
func NewSimpleReplyCommandHandler[ReplyCommand any](
	handlerName string,
	handleFunc func(ctx context.Context, cmd *ReplyCommand) error,
) ReplyCommandHandler {
	return &genericReplyCommandHandler[ReplyCommand]{
		handleFunc: func(ctx context.Context, cmd *ReplyCommand) ([]ReplyMessage, error) {
			err := handleFunc(ctx, cmd)
			if err != nil {
				return []ReplyMessage{WithFailure()}, err
			}
			return []ReplyMessage{WithSuccess()}, nil
		},
		handlerName: handlerName,
	}
}

// NewResultReplyCommandHandler creates a new ReplyCommandHandler implementation based on provided function
// and command type inferred from function argument.
func NewResultReplyCommandHandler[ReplyCommand any, Result Reply](
	handlerName string,
	handleFunc func(ctx context.Context, cmd *ReplyCommand) (Result, error),
) ReplyCommandHandler {
	return &genericReplyCommandHandler[ReplyCommand]{
		handleFunc: func(ctx context.Context, cmd *ReplyCommand) ([]ReplyMessage, error) {
			result, err := handleFunc(ctx, cmd)
			if err != nil {
				return []ReplyMessage{WithFailure()}, err
			}
			return []ReplyMessage{WithReply(result).Success()}, nil
		},
		handlerName: handlerName,
	}
}

func (c genericReplyCommandHandler[Command]) HandlerName() string {
	return c.handlerName
}

func (c genericReplyCommandHandler[Command]) NewCommand() interface{} {
	tVar := new(Command)
	return tVar
}

func (c genericReplyCommandHandler[Command]) Handle(ctx context.Context, cmd any) ([]ReplyMessage, error) {
	command := cmd.(*Command)
	return c.handleFunc(ctx, command)
}
