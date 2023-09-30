package cqrs

// WithReply starts a reply builder allowing custom headers to be injected
func WithReply(reply Reply) *ReplyBuilder {
	return &ReplyBuilder{
		reply:   reply,
		headers: map[string]string{},
	}
}

// ReplyBuilder is used to build custom replies
type ReplyBuilder struct {
	reply   Reply
	headers map[string]string
}

// Reply replaces the reply to be wrapped
func (b *ReplyBuilder) Reply(reply Reply) *ReplyBuilder {
	b.reply = reply
	return b
}

// Headers adds headers to include with the reply
func (b *ReplyBuilder) Headers(headers map[string]string) *ReplyBuilder {
	for key, value := range headers {
		b.headers[key] = value
	}
	return b
}

// Success wraps the reply with the custom headers as a Success reply
func (b *ReplyBuilder) Success() ReplyMessage {
	if b.reply == nil {
		b.reply = Success{}
	}

	b.headers[MessageReplyOutcome] = ReplyOutcomeSuccess
	b.headers[MessageReplyName] = b.reply.ReplyName()

	return &replyMessage{
		reply:   b.reply,
		headers: b.headers,
	}
}

// Failure wraps the reply with the custom headers as a Failure reply
func (b *ReplyBuilder) Failure() ReplyMessage {
	if b.reply == nil {
		b.reply = Failure{}
	}

	b.headers[MessageReplyOutcome] = ReplyOutcomeFailure
	b.headers[MessageReplyName] = b.reply.ReplyName()

	return &replyMessage{
		reply:   b.reply,
		headers: b.headers,
	}
}
