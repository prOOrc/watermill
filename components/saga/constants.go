package saga

import "github.com/ThreeDotsLabs/watermill/components/cqrs"

const (
	notCompensating = false
	isCompensating  = true
)

// LifecycleHook type for hooking in custom code at specific stages of a saga
type LifecycleHook int

// Definition lifecycle hooks
const (
	SagaStarting LifecycleHook = iota
	SagaCompleted
	SagaCompensated
)

// Saga message headers
const (
	MessageCommandSagaID   = cqrs.MessageCommandPrefix + "saga_id"
	MessageCommandSagaName = cqrs.MessageCommandPrefix + "saga_name"
	MessageCommandResource = cqrs.MessageCommandPrefix + "resource"

	MessageReplySagaID   = cqrs.MessageReplyPrefix + "saga_id"
	MessageReplySagaName = cqrs.MessageReplyPrefix + "saga_name"
)
