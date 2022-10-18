package cqrs

// cqrs message headers
const (
	MessageEventPrefix     = "event_"
	MessageEventName       = MessageEventPrefix + "name"
	MessageEventEntityName = MessageEventPrefix + "entity_name"
	MessageEventEntityID   = MessageEventPrefix + "entity_id"	

	MessageCommandPrefix       = "command_"
	MessageCommandName         = MessageCommandPrefix + "name"
	MessageCommandChannel      = MessageCommandPrefix + "channel"
	MessageCommandReplyChannel = MessageCommandPrefix + "reply_channel"

	MessageReplyPrefix  = "reply_"
	MessageReplyName    = MessageReplyPrefix + "name"
	MessageReplyOutcome = MessageReplyPrefix + "outcome"

	MessageCommandResource = MessageCommandPrefix + "resource"
)
