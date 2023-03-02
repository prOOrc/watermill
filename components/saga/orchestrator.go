package saga

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// Orchestrator orchestrates local and distributed processes
type Orchestrator interface {
	Start(ctx context.Context, sagaData SagaData) (*Instance, error)
	ReplyChannel() string
	AddHandlerToRouter(r *message.Router) (*message.Handler, error)
}

type orchestrator struct {
	definition            Definition
	instanceStore         InstanceStore
	subscriberConstructor cqrs.CommandsSubscriberConstructor
	publisher             message.Publisher
	marshaler             cqrs.CommandEventMarshaler
	generateTopic         func(commandName string) string
	logger                watermill.LoggerAdapter
}

const sagaNotStarted = -1

type ReplyFactoryFunc func() interface{}

// NewOrchestrator constructs a new Orchestrator
func NewOrchestrator(
	definition Definition,
	store InstanceStore,
	subscriberConstructor cqrs.CommandsSubscriberConstructor,
	publisher message.Publisher,
	marshaler cqrs.CommandEventMarshaler,
	generateTopic func(commandName string) string,
	logger watermill.LoggerAdapter,
	options ...OrchestratorOption) Orchestrator {
	o := &orchestrator{
		definition:            definition,
		instanceStore:         store,
		subscriberConstructor: subscriberConstructor,
		publisher:             publisher,
		marshaler:             marshaler,
		generateTopic:         generateTopic,
		logger:                logger,
	}

	for _, option := range options {
		option(o)
	}

	o.logger.Trace("saga.Orchestrator constructed", watermill.LogFields{"saga_name": definition.SagaName()})

	return o
}

// OrchestratorFactory orchestrates local and distributed processes
type OrchestratorFactory struct {
	instanceStore         InstanceStore
	subscriberConstructor cqrs.CommandsSubscriberConstructor
	publisher             message.Publisher
	marshaler             cqrs.CommandEventMarshaler
	generateTopic         func(commandName string) string
	logger                watermill.LoggerAdapter
	options               []OrchestratorOption
}

// NewOrchestratorFactory constructs a new OrchestratorFactory
func NewOrchestratorFactory(
	store InstanceStore,
	subscriberConstructor cqrs.CommandsSubscriberConstructor,
	publisher message.Publisher,
	marshaler cqrs.CommandEventMarshaler,
	generateTopic func(commandName string) string,
	logger watermill.LoggerAdapter,
	options []OrchestratorOption,
) *OrchestratorFactory {
	f := &OrchestratorFactory{
		instanceStore:         store,
		subscriberConstructor: subscriberConstructor,
		publisher:             publisher,
		marshaler:             marshaler,
		generateTopic:         generateTopic,
		logger:                logger,
	}

	return f
}

func (f *OrchestratorFactory) New(definition Definition) Orchestrator {
	return NewOrchestrator(
		definition,
		f.instanceStore,
		f.subscriberConstructor,
		f.publisher,
		f.marshaler,
		f.generateTopic,
		f.logger,
		f.options...,
	)
}

// Start creates a new instance of the saga and begins execution
func (o *orchestrator) Start(ctx context.Context, sagaData SagaData) (*Instance, error) {
	instance := &Instance{
		sagaID:   uuid.New().String(),
		sagaName: o.definition.SagaName(),
		sagaData: sagaData,
	}

	err := o.instanceStore.Save(ctx, instance)
	if err != nil {
		return nil, err
	}

	logger := o.logger.With(
		watermill.LogFields{
			"saga_name": o.definition.SagaName(),
			"saga_id":   instance.sagaID,
		},
	)

	logger.Trace("executing saga starting hook", watermill.LogFields{})
	o.definition.OnHook(ctx, SagaStarting, instance)

	results := o.executeNextStep(ctx, stepContext{step: sagaNotStarted}, sagaData)
	if results.failure != nil {
		logger.Error("error while starting saga orchestration", results.failure, watermill.LogFields{})
		return nil, err
	}

	err = o.processResults(ctx, instance, results)
	if err != nil {
		logger.Error("error while processing results", err, watermill.LogFields{})
		return nil, err
	}

	return instance, err
}

// ReplyChannel returns the channel replies are to be received from msg.Subscribers
func (o *orchestrator) ReplyChannel() string {
	return o.definition.ReplyChannel()
}

func (o *orchestrator) AddHandlerToRouter(r *message.Router) (handler *message.Handler, err error) {
	handlerName := o.definition.SagaName()
	topicName := o.ReplyChannel()
	logger := o.logger.With(watermill.LogFields{
		"command_handler_name": handlerName,
		"topic":                topicName,
	})

	logger.Debug("Adding saga to router", nil)

	subscriber, err := o.subscriberConstructor(handlerName)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create subscriber for command processor")
	}

	handler = r.AddNoPublisherHandler(
		handlerName,
		topicName,
		subscriber,
		o.receiveMessage,
	)

	return handler, err
}

// receiveMessage implements message.HandlerFunc
func (o *orchestrator) receiveMessage(message *message.Message) (err error) {
	replyName, sagaID, sagaName, err := o.replyMessageInfo(message)
	if err != nil {
		return nil
	}

	if sagaID == "" || (sagaName == "" || sagaName != o.definition.SagaName()) {
		o.logger.Error(
			"cannot process message",
			fmt.Errorf("cannot process message"),
			watermill.LogFields{
				"name_value": sagaName,
				"id_value":   sagaID,
			},
		)
		return nil
	}

	logger := o.logger.With(
		watermill.LogFields{
			"reply_name": replyName,
			"saga_name":  sagaName,
			"saga_id":    sagaID,
			"message_id": message.UUID,
		},
	)

	logger.Debug("received saga reply message", watermill.LogFields{})

	replyMessage, err := o.definition.NewReply(replyName)
	if err != nil {
		logger.Error("recieve message error", err, watermill.LogFields{})
		return nil
	}

	o.marshaler.Unmarshal(message, replyMessage)
	if err != nil {
		// sagas should not be receiving any replies that have not already been registered
		logger.Error("error decoding reply message payload", err, watermill.LogFields{})
		return nil
	}
	reply := replyMessage.(cqrs.Reply)
	replyMsg := cqrs.NewReply(reply, message.Metadata)
	ctx := message.Context()
	data := o.definition.NewData()
	instance, err := o.instanceStore.Find(ctx, sagaID, data)
	if err != nil {
		logger.Error("failed to locate saga instance data", err, watermill.LogFields{})
		return nil
	}

	stepCtx := instance.getStepContext()

	results, err := o.handleReply(ctx, stepCtx, instance.SagaData(), replyMsg)
	if err != nil {
		logger.Error("saga reply handler returned an error", err, watermill.LogFields{})
		return err
	}

	err = o.processResults(ctx, instance, results)
	if err != nil {
		logger.Error("error while processing results", err, watermill.LogFields{})
		return err
	}

	return nil
}

func (o *orchestrator) replyMessageInfo(message *message.Message) (string, string, string, error) {
	var err error
	var replyName, sagaID, sagaName string

	replyName, err = message.Metadata.GetRequired(cqrs.MessageReplyName)
	if err != nil {
		o.logger.Error("error reading reply name", err, watermill.LogFields{})
		return "", "", "", err
	}

	sagaID, err = message.Metadata.GetRequired(MessageReplySagaID)
	if err != nil {
		o.logger.Error("error reading saga id", err, watermill.LogFields{})
		return "", "", "", err
	}

	sagaName, err = message.Metadata.GetRequired(MessageReplySagaName)
	if err != nil {
		o.logger.Error("error reading saga name", err, watermill.LogFields{})
		return "", "", "", err
	}

	return replyName, sagaID, sagaName, nil
}

func (o *orchestrator) processResults(ctx context.Context, instance *Instance, results *stepResults) (err error) {
	logger := o.logger.With(
		watermill.LogFields{
			"saga_name": o.definition.SagaName(),
			"saga_id":   instance.sagaID,
		},
	)

	for {
		if results.failure != nil {
			logger.Trace("handling local failure result", watermill.LogFields{})
			logger.Error("error while process step", results.failure, watermill.LogFields{})
			results, err = o.handleReply(ctx, results.updatedStepContext, results.updatedSagaData, cqrs.WithFailure())
			if err != nil {
				logger.Error("error handling local failure result", err, watermill.LogFields{})
				return err
			}
		} else {
			for _, command := range results.commands {
				msg, err := o.marshaler.Marshal(command)
				if err != nil {
					return err
				}
				commandName := o.marshaler.Name(command)
				msg.SetContext(ctx)
				message.WithHeaders(map[string]string{
					cqrs.MessageCommandName:         commandName,
					cqrs.MessageCommandReplyChannel: o.definition.ReplyChannel(),
				})(msg)
				WithSagaInfo(instance)(msg)

				topicName := o.generateTopic(commandName)
				err = o.publisher.Publish(topicName, msg)
				if err != nil {
					logger.Error("error while sending commands", err, watermill.LogFields{})
					return err
				}
			}

			instance.updateStepContext(results.updatedStepContext)

			if results.updatedSagaData != nil {
				instance.sagaData = results.updatedSagaData
			}

			if results.updatedStepContext.ended {
				o.processEnd(ctx, instance)
			}

			err = o.instanceStore.Update(ctx, instance)
			if err != nil {
				logger.Error("error saving saga instance", err, watermill.LogFields{})
				return err
			}

			if !results.local {
				logger.Trace("exiting step loop", watermill.LogFields{})
				break
			}

			// handle a local success outcome and kick off the next step
			logger.Trace("handling local success result", watermill.LogFields{})
			results, err = o.handleReply(ctx, results.updatedStepContext, results.updatedSagaData, cqrs.WithSuccess())
			if err != nil {
				logger.Error("error handling local success result", err, watermill.LogFields{})
				return err
			}
		}
	}

	return nil
}

func (o *orchestrator) processEnd(ctx context.Context, instance *Instance) {
	logger := o.logger.With(
		watermill.LogFields{
			"saga_name": o.definition.SagaName(),
			"saga_id":   instance.sagaID,
		},
	)

	if instance.compensating {
		logger.Trace("executing saga compensated hook", watermill.LogFields{})
		o.definition.OnHook(ctx, SagaCompensated, instance)
	} else {
		logger.Trace("executing saga completed hook", watermill.LogFields{})
		o.definition.OnHook(ctx, SagaCompleted, instance)
	}
	logger.Trace("saga has finished all steps", watermill.LogFields{})
}

func (o *orchestrator) handleReply(ctx context.Context, stepCtx stepContext, sagaData SagaData, message cqrs.ReplyMessage) (*stepResults, error) {
	replyName := message.Reply().ReplyName()

	logger := o.logger.With(
		watermill.LogFields{
			"saga_name":  o.definition.SagaName(),
			"saga_id":    message.Headers().Get(MessageReplySagaID),
			"reply_name": replyName,
		},
	)

	if stepCtx.step >= len(o.definition.Steps()) || stepCtx.step < 0 {
		logger.Error("current step is out of bounds", fmt.Errorf("current step is out of bounds"), watermill.LogFields{"Step": fmt.Sprintf("%d", stepCtx.step)})
		return nil, fmt.Errorf("current step is out of bounds: 0-%d, got %d", len(o.definition.Steps()), stepCtx.step)
	}
	step := o.definition.Steps()[stepCtx.step]

	// handle specific replies
	if handler := step.getReplyHandler(replyName, stepCtx.compensating); handler != nil {
		logger.Trace("saga reply handler found", watermill.LogFields{})
		err := handler(ctx, sagaData, message.Reply())
		if err != nil {
			logger.Error("saga reply handler returned an error", err, watermill.LogFields{})
			return nil, err
		}
	}

	outcome, err := message.Headers().GetRequired(cqrs.MessageReplyOutcome)
	if err != nil {
		logger.Error("error reading reply outcome", err, watermill.LogFields{})
		return nil, err
	}

	logger.Trace("reply outcome", watermill.LogFields{"Outcome": outcome})

	success := outcome == cqrs.ReplyOutcomeSuccess

	switch {
	case success:
		logger.Trace("advancing to next step", watermill.LogFields{})
		return o.executeNextStep(ctx, stepCtx, sagaData), nil
	case stepCtx.compensating:
		// we're already failing, we can't fail any more
		logger.Error("received a failure outcome while compensating", fmt.Errorf("received a failure outcome while compensating"), watermill.LogFields{"Step": fmt.Sprintf("%d", stepCtx.step)})
		return nil, fmt.Errorf("received failure outcome while compensating")
	default:
		logger.Trace("compensating to previous step", watermill.LogFields{})
		return o.executeNextStep(ctx, stepCtx.compensate(), sagaData), nil
	}
}

func (o *orchestrator) executeNextStep(ctx context.Context, stepCtx stepContext, sagaData SagaData) *stepResults {
	var stepDelta = 1
	var direction = 1
	var step Step

	if stepCtx.compensating {
		direction = -1
	}

	sagaSteps := o.definition.Steps()

	for i := stepCtx.step + direction; i >= 0 && i < len(sagaSteps); i += direction {
		if step = sagaSteps[i]; step.hasInvocableAction(ctx, sagaData, stepCtx.compensating) {
			break
		}

		// Skips steps that have no action for the direction (compensating or not compensating)
		stepDelta++
	}

	// if no step to execute exists the saga is done
	if step == nil {
		return &stepResults{updatedStepContext: stepCtx.end()}
	}

	nextCtx := stepCtx.next(stepDelta)

	results := &stepResults{
		updatedSagaData:    sagaData,
		updatedStepContext: nextCtx,
	}

	step.execute(ctx, sagaData, stepCtx.compensating)(results)

	return results
}
