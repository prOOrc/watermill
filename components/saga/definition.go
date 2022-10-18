package saga

// Definition interface
type Definition interface {
	SagaName() string
	ReplyChannel() string
	NewData() SagaData
	NewReply(name string) (interface{}, error)
	Steps() []Step
	OnHook(hook LifecycleHook, instance *Instance)
}
