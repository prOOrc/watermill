package cqrs

import (
	"context"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
)

type CommandBus interface {
	Send(ctx context.Context, cmd interface{}) error
}

// CommandBus transports commands to command handlers.
type commandBus struct {
	publisher     message.Publisher
	generateTopic func(commandName string) string
	marshaler     CommandEventMarshaler
}

func NewCommandBus(
	publisher message.Publisher,
	generateTopic func(commandName string) string,
	marshaler CommandEventMarshaler,
) (CommandBus, error) {
	if publisher == nil {
		return nil, errors.New("missing publisher")
	}
	if generateTopic == nil {
		return nil, errors.New("missing generateTopic")
	}
	if marshaler == nil {
		return nil, errors.New("missing marshaler")
	}

	return &commandBus{publisher, generateTopic, marshaler}, nil
}

// Send sends command to the command bus.
func (c *commandBus) Send(ctx context.Context, cmd interface{}) error {
	msg, err := c.marshaler.Marshal(cmd)
	if err != nil {
		return err
	}

	commandName := c.marshaler.Name(cmd)
	topicName := c.generateTopic(commandName)

	msg.SetContext(ctx)

	return c.publisher.Publish(topicName, msg)
}
