package saga

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

// Definition interface
type Definition interface {
	SagaName() string
	ReplyChannel() string
	NewData() any
	NewReply(name string) (interface{}, error)
	Steps() []Step
	OnHook(ctx context.Context, hook LifecycleHook, instance *Instance)
}

type genericDefinition[Data any] struct {
	steps            []Step
	name             string
	replyChannel     string
	replyFactoryFunc ReplyFactoryFunc
	onHook           func(ctx context.Context, hook LifecycleHook, instance *Instance)
}

func NewDefinition[Data any](
	name string,
	replyChannel string,
	replyFactories []func() cqrs.Reply,
) *genericDefinition[Data] {
	return &genericDefinition[Data]{
		name:             name,
		replyChannel:     replyChannel,
		replyFactoryFunc: newReplyFactory(replyFactories),
	}
}

// NewData implements Definition.
func (d *genericDefinition[Data]) NewData() any {
	tVar := new(Data)
	return tVar
}

// NewReply implements Definition.
func (d *genericDefinition[Data]) NewReply(name string) (interface{}, error) {
	return d.replyFactoryFunc(name)
}

// OnHook implements Definition.
func (d *genericDefinition[Data]) OnHook(ctx context.Context, hook LifecycleHook, instance *Instance) {
	if d.onHook != nil {
		d.onHook(ctx, hook, instance)
	}
}

// ReplyChannel implements Definition.
func (d *genericDefinition[Data]) ReplyChannel() string {
	return d.replyChannel
}

// SagaName implements Definition.
func (d *genericDefinition[Data]) SagaName() string {
	return d.name
}

// Steps implements Definition.
func (d *genericDefinition[Data]) Steps() []Step {
	return d.steps
}

func (d *genericDefinition[Data]) WithOnHook(fn func(ctx context.Context, hook LifecycleHook, instance *Instance)) *genericDefinition[Data] {
	d.onHook = fn
	return d
}

func (d *genericDefinition[Data]) WithSteps(steps []Step) *genericDefinition[Data] {
	d.steps = steps
	return d
}

func (d *genericDefinition[Data]) NewLocalStep(action func(context.Context, *Data) error) LocalStep[Data] {
	step := NewLocalStep[Data](action)
	return step
}

func (d *genericDefinition[Data]) AddLocalStep(action func(context.Context, *Data) error) LocalStep[Data] {
	step := d.NewLocalStep(action)
	d.steps = append(d.steps, step)
	return step
}

func (d *genericDefinition[Data]) NewRemoteStep() RemoteStep[Data] {
	step := NewRemoteStep[Data]()
	return step
}

func (d *genericDefinition[Data]) AddRemoteStep() RemoteStep[Data] {
	step := d.NewRemoteStep()
	d.steps = append(d.steps, step)
	return step
}

var _ Definition = (*genericDefinition[any])(nil)
