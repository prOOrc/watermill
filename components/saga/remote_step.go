package saga

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

// RemoteStep is used to execute distributed saga business logic
type RemoteStep[Data any] struct {
	actionHandlers map[bool]*remoteStepAction
	replyHandlers  map[bool]map[string]func(context.Context, any, cqrs.Reply) error
}

var _ Step = (*RemoteStep[any])(nil)

// NewRemoteStep constructor for RemoteStep
func NewRemoteStep[Data any]() RemoteStep[Data] {
	return RemoteStep[Data]{
		actionHandlers: map[bool]*remoteStepAction{
			notCompensating: nil,
			isCompensating:  nil,
		},
		replyHandlers: map[bool]map[string]func(context.Context, any, cqrs.Reply) error{
			notCompensating: {},
			isCompensating:  {},
		},
	}
}

// Action adds a domain command constructor that will be called while the definition is advancing
func (s RemoteStep[Data]) Action(fn func(context.Context, *Data) cqrs.Command, options ...RemoteStepActionOption) RemoteStep[Data] {
	handler := &remoteStepAction{
		handler: func(ctx context.Context, data any) cqrs.Command {
			sagaData := data.(*Data)
			return fn(ctx, sagaData)
		},
	}

	for _, option := range options {
		option(handler)
	}

	s.actionHandlers[notCompensating] = handler

	return s
}

// HandleActionReply adds additional handling for specific replies while advancing
//
// SuccessReply and FailureReply do not require any special handling unless desired
func (s RemoteStep[Data]) HandleActionReply(reply cqrs.Reply, handler func(context.Context, *Data, cqrs.Reply) error) RemoteStep[Data] {
	s.replyHandlers[notCompensating][reply.ReplyName()] = func(ctx context.Context, data any, reply cqrs.Reply) error {
		sagaData := data.(*Data)
		return handler(ctx, sagaData, reply)
	}

	return s
}

// Compensation adds a domain command constructor that will be called while the definition is compensating
func (s RemoteStep[Data]) Compensation(fn func(context.Context, *Data) cqrs.Command, options ...RemoteStepActionOption) RemoteStep[Data] {
	handler := &remoteStepAction{
		handler: func(ctx context.Context, data any) cqrs.Command {
			sagaData := data.(*Data)
			return fn(ctx, sagaData)
		},
	}

	for _, option := range options {
		option(handler)
	}

	s.actionHandlers[isCompensating] = handler

	return s
}

// HandleCompensationReply adds additional handling for specific replies while compensating
//
// SuccessReply does not require any special handling unless desired
func (s RemoteStep[Data]) HandleCompensationReply(reply cqrs.Reply, handler func(context.Context, *Data, cqrs.Reply) error) RemoteStep[Data] {
	s.replyHandlers[isCompensating][reply.ReplyName()] = func(ctx context.Context, data any, reply cqrs.Reply) error {
		sagaData := data.(*Data)
		return handler(ctx, sagaData, reply)
	}

	return s
}

func (s RemoteStep[Data]) hasInvocableAction(ctx context.Context, sagaData any, compensating bool) bool {
	return s.actionHandlers[compensating] != nil && s.actionHandlers[compensating].isInvocable(ctx, sagaData)
}

func (s RemoteStep[Data]) getReplyHandler(replyName string, compensating bool) func(context.Context, any, cqrs.Reply) error {
	return s.replyHandlers[compensating][replyName]
}

func (s RemoteStep[Data]) execute(ctx context.Context, sagaData any, compensating bool) func(results *stepResults) {
	if commandToSend := s.actionHandlers[compensating].execute(ctx, sagaData); commandToSend != nil {
		return func(actions *stepResults) {
			actions.commands = []cqrs.Command{commandToSend}
		}
	}

	return func(actions *stepResults) {}
}
