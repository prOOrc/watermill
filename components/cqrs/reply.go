package cqrs

import "github.com/ThreeDotsLabs/watermill/message"

// Reply interface
type Reply interface {
	ReplyName() string
}

// Success reply type for generic successful replies to commands
type Success struct{}

// ReplyName implements Reply.ReplyName
func (Success) ReplyName() string { return "edat.msg.Success" }

// Failure reply type for generic failure replies to commands
type Failure struct{}

// ReplyName implements Reply.ReplyName
func (Failure) ReplyName() string { return "edat.msg.Failure" }

// Reply outcomes
const (
	ReplyOutcomeSuccess = "SUCCESS"
	ReplyOutcomeFailure = "FAILURE"
)

// Reply interface
type ReplyMessage interface {
	Reply() Reply
	Headers() message.Metadata
}

type replyMessage struct {
	reply   Reply
	headers message.Metadata
}

// NewReply constructs a new reply with headers
func NewReply(reply Reply, headers message.Metadata) ReplyMessage {
	return &replyMessage{reply, headers}
}

// Reply returns the Reply
func (m replyMessage) Reply() Reply {
	return m.reply
}

// Headers returns the msg.Headers
func (m replyMessage) Headers() message.Metadata {
	return m.headers
}

// SuccessReply wraps a reply and returns it as a Success reply
// Deprecated: Use the WithReply() reply builder
func SuccessReply(reply Reply) ReplyMessage {
	if reply == nil {
		return &replyMessage{
			reply: Success{},
			headers: map[string]string{
				MessageReplyOutcome: ReplyOutcomeSuccess,
				MessageReplyName:    Success{}.ReplyName(),
			},
		}
	}

	return &replyMessage{
		reply: reply,
		headers: map[string]string{
			MessageReplyOutcome: ReplyOutcomeSuccess,
			MessageReplyName:    reply.ReplyName(),
		},
	}
}

// FailureReply wraps a reply and returns it as a Failure reply
// Deprecated: Use the WithReply() reply builder
func FailureReply(reply Reply) ReplyMessage {
	if reply == nil {
		return &replyMessage{
			reply: Failure{},
			headers: map[string]string{
				MessageReplyOutcome: ReplyOutcomeFailure,
				MessageReplyName:    Failure{}.ReplyName(),
			},
		}
	}

	return &replyMessage{
		reply: reply,
		headers: map[string]string{
			MessageReplyOutcome: ReplyOutcomeFailure,
			MessageReplyName:    reply.ReplyName(),
		},
	}
}

// WithSuccess returns a generic Success reply
func WithSuccess() ReplyMessage {
	return &replyMessage{
		reply: Success{},
		headers: map[string]string{
			MessageReplyOutcome: ReplyOutcomeSuccess,
			MessageReplyName:    Success{}.ReplyName(),
		},
	}
}

// WithFailure returns a generic Failure reply
func WithFailure() ReplyMessage {
	return &replyMessage{
		reply: Failure{},
		headers: map[string]string{
			MessageReplyOutcome: ReplyOutcomeFailure,
			MessageReplyName:    Failure{}.ReplyName(),
		},
	}
}
