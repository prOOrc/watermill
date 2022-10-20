package saga

import "context"

// Definition interface
type Definition interface {
	SagaName() string
	ReplyChannel() string
	NewData() SagaData
	NewReply(name string) (interface{}, error)
	Steps() []Step
	OnHook(ctx context.Context, hook LifecycleHook, instance *Instance)
}
