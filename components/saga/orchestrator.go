package saga

import (
	"context"
	stdErrors "errors"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// Orchestrator orchestrates local and distributed processes
type Orchestrator interface {
	Start(ctx context.Context, sagaData any) (*Instance, error)
	ReplyChannel() string
	AddHandlerToRouter(r *message.Router) (*message.Handler, error)
}

type ReplyChannelSubscriberConstructor func(handlerName string) (message.Subscriber, error)
type TopicNameGenerator func(commandName string) string

type OrchestratorConfig struct {
	InstanceStore          InstanceStore
	SubscriberConstructor  ReplyChannelSubscriberConstructor
	GenerateSubscribeTopic TopicNameGenerator
	Publisher              message.Publisher
	Marshaler              cqrs.CommandEventMarshaler
	Logger                 watermill.LoggerAdapter
}

func (c OrchestratorConfig) Validate() error {
	var err error

	if c.InstanceStore == nil {
		err = stdErrors.Join(err, errors.New("missing InstanceStore"))
	}

	if c.Marshaler == nil {
		err = stdErrors.Join(err, errors.New("missing Marshaler"))
	}

	if c.Publisher == nil {
		err = stdErrors.Join(err, errors.New("missing Publisher"))
	}

	if c.GenerateSubscribeTopic == nil {
		err = stdErrors.Join(err, errors.New("missing GenerateSubscribeTopic"))
	}
	if c.SubscriberConstructor == nil {
		err = stdErrors.Join(err, errors.New("missing SubscriberConstructor"))
	}

	if c.Logger == nil {
		err = stdErrors.Join(err, errors.New("missing Logger"))
	}

	return err
}

type orchestrator struct {
	definition Definition
	config     OrchestratorConfig
}

const sagaNotStarted = -1

type ReplyFactoryFunc func(string) (interface{}, error)

// NewOrchestrator constructs a new Orchestrator
func NewOrchestrator(
	definition Definition,
	store InstanceStore,
	subscriberConstructor ReplyChannelSubscriberConstructor,
	publisher message.Publisher,
	marshaler cqrs.CommandEventMarshaler,
	generateTopic TopicNameGenerator,
	logger watermill.LoggerAdapter,
) (Orchestrator, error) {
	config := OrchestratorConfig{
		InstanceStore:          store,
		SubscriberConstructor:  subscriberConstructor,
		Publisher:              publisher,
		Marshaler:              marshaler,
		GenerateSubscribeTopic: generateTopic,
		Logger:                 logger,
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	o := &orchestrator{
		definition: definition,
		config: OrchestratorConfig{
			InstanceStore:          store,
			SubscriberConstructor:  subscriberConstructor,
			Publisher:              publisher,
			Marshaler:              marshaler,
			GenerateSubscribeTopic: generateTopic,
			Logger:                 logger,
		},
	}

	o.config.Logger.Trace("saga.Orchestrator constructed", watermill.LogFields{"saga_name": definition.SagaName()})

	return o, nil
}

// NewOrchestratorWithConfig constructs a new Orchestrator
func NewOrchestratorWithConfig(
	definition Definition,
	config OrchestratorConfig,
) (Orchestrator, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	o := &orchestrator{
		definition: definition,
		config:     config,
	}

	o.config.Logger.Trace("saga.Orchestrator constructed", watermill.LogFields{"saga_name": definition.SagaName()})

	return o, nil
}

// OrchestratorFactory orchestrates local and distributed processes
type OrchestratorFactory struct {
	config OrchestratorConfig
}

// NewOrchestratorFactory constructs a new OrchestratorFactory
func NewOrchestratorFactory(
	store InstanceStore,
	subscriberConstructor ReplyChannelSubscriberConstructor,
	publisher message.Publisher,
	marshaler cqrs.CommandEventMarshaler,
	generateTopic TopicNameGenerator,
	logger watermill.LoggerAdapter,
) (*OrchestratorFactory, error) {
	config := OrchestratorConfig{
		InstanceStore:          store,
		SubscriberConstructor:  subscriberConstructor,
		Publisher:              publisher,
		Marshaler:              marshaler,
		GenerateSubscribeTopic: generateTopic,
		Logger:                 logger,
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	f := &OrchestratorFactory{
		config: OrchestratorConfig{
			InstanceStore:          store,
			SubscriberConstructor:  subscriberConstructor,
			Publisher:              publisher,
			Marshaler:              marshaler,
			GenerateSubscribeTopic: generateTopic,
			Logger:                 logger,
		},
	}

	return f, nil
}

// NewOrchestratorFactoryWithConfig constructs a new OrchestratorFactory
func NewOrchestratorFactoryWithConfig(
	config OrchestratorConfig,
) (*OrchestratorFactory, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	f := &OrchestratorFactory{
		config: config,
	}

	return f, nil
}

func (f *OrchestratorFactory) New(definition Definition) (Orchestrator, error) {
	return NewOrchestratorWithConfig(
		definition,
		f.config,
	)
}

// Start creates a new instance of the saga and begins execution
func (o *orchestrator) Start(ctx context.Context, sagaData any) (*Instance, error) {
	instance := &Instance{
		sagaID:   uuid.New().String(),
		sagaName: o.definition.SagaName(),
		sagaData: sagaData,
	}

	err := o.config.InstanceStore.Save(ctx, instance)
	if err != nil {
		return nil, err
	}

	logFields := watermill.LogFields{
		"saga_name": o.definition.SagaName(),
		"saga_id":   instance.sagaID,
	}
	logger := o.config.Logger.With(logFields)

	logger.Trace("executing saga starting hook", logFields)
	o.definition.OnHook(ctx, SagaStarting, instance)

	results := o.executeNextStep(ctx, stepContext{step: sagaNotStarted}, sagaData)
	if results.failure != nil {
		logger.Error("error while starting saga orchestration", results.failure, logFields)
		return nil, err
	}

	err = o.processResults(ctx, instance, results)
	if err != nil {
		logger.Error("error while processing results", err, logFields)
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
	logFields := watermill.LogFields{
		"command_handler_name": handlerName,
		"topic":                topicName,
	}
	logger := o.config.Logger.With(logFields)

	logger.Debug("Adding saga to router", logFields)

	subscriber, err := o.config.SubscriberConstructor(handlerName)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create subscriber for command processor")
	}

	handler = r.AddNoPublisherHandler(
		handlerName,
		topicName,
		subscriber,
		o.receiveMessage,
	)

	return handler, nil
}

// receiveMessage implements message.HandlerFunc
func (o *orchestrator) receiveMessage(message *message.Message) (err error) {
	replyName, sagaID, sagaName, err := o.replyMessageInfo(message)
	if err != nil {
		return nil
	}

	if sagaID == "" || (sagaName == "" || sagaName != o.definition.SagaName()) {
		o.config.Logger.Error(
			"cannot process message",
			fmt.Errorf("cannot process message"),
			watermill.LogFields{
				"name_value": sagaName,
				"id_value":   sagaID,
			},
		)
		return nil
	}

	logFields := watermill.LogFields{
		"reply_name": replyName,
		"saga_name":  sagaName,
		"saga_id":    sagaID,
		"message_id": message.UUID,
	}
	logger := o.config.Logger.With(logFields)

	logger.Debug("received saga reply message", logFields)

	replyMessage, err := o.definition.NewReply(replyName)
	if err != nil {
		logger.Error("recieve message error", err, logFields)
		return nil
	}

	err = o.config.Marshaler.Unmarshal(message, replyMessage)
	if err != nil {
		// sagas should not be receiving any replies that have not already been registered
		logger.Error("error decoding reply message payload", err, logFields)
		return nil
	}
	reply := replyMessage.(cqrs.Reply)
	replyMsg := cqrs.NewReply(reply, message.Metadata)
	ctx := message.Context()
	data := o.definition.NewData()
	instance, err := o.config.InstanceStore.Find(ctx, sagaID, data)
	if err != nil {
		logger.Error("failed to locate saga instance data", err, logFields)
		return nil
	}

	stepCtx := instance.getStepContext()

	results, err := o.handleReply(ctx, stepCtx, instance.SagaData(), replyMsg)
	if err != nil {
		logger.Error("saga reply handler returned an error", err, logFields)
		return err
	}

	err = o.processResults(ctx, instance, results)
	if err != nil {
		logger.Error("error while processing results", err, logFields)
		return err
	}

	return nil
}

func (o *orchestrator) replyMessageInfo(message *message.Message) (string, string, string, error) {
	var err error
	var replyName, sagaID, sagaName string

	replyName, err = message.Metadata.GetRequired(cqrs.MessageReplyName)
	if err != nil {
		o.config.Logger.Error("error reading reply name", err, watermill.LogFields{})
		return "", "", "", err
	}

	sagaID, err = message.Metadata.GetRequired(MessageReplySagaID)
	if err != nil {
		o.config.Logger.Error("error reading saga id", err, watermill.LogFields{})
		return "", "", "", err
	}

	sagaName, err = message.Metadata.GetRequired(MessageReplySagaName)
	if err != nil {
		o.config.Logger.Error("error reading saga name", err, watermill.LogFields{})
		return "", "", "", err
	}

	return replyName, sagaID, sagaName, nil
}

func (o *orchestrator) processResults(ctx context.Context, instance *Instance, results *stepResults) (err error) {
	logFields := watermill.LogFields{
		"saga_name": o.definition.SagaName(),
		"saga_id":   instance.sagaID,
	}
	logger := o.config.Logger.With(logFields)

	for {
		if results.failure != nil {
			logger.Trace("handling local failure result", logFields)
			logger.Error("error while process step", results.failure, logFields)
			results, err = o.handleReply(ctx, results.updatedStepContext, results.updatedSagaData, cqrs.WithFailure())
			if err != nil {
				logger.Error("error handling local failure result", err, logFields)
				return err
			}
		} else {
			for _, command := range results.commands {
				msg, err := o.config.Marshaler.Marshal(command)
				if err != nil {
					return err
				}
				commandName := o.config.Marshaler.Name(command)
				msg.SetContext(ctx)
				message.WithHeaders(map[string]string{
					cqrs.MessageCommandName:         commandName,
					cqrs.MessageCommandReplyChannel: o.definition.ReplyChannel(),
				})(msg)
				WithSagaInfo(instance)(msg)

				topicName := o.config.GenerateSubscribeTopic(commandName)
				err = o.config.Publisher.Publish(topicName, msg)
				if err != nil {
					logger.Error("error while sending commands", err, logFields)
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

			err = o.config.InstanceStore.Update(ctx, instance)
			if err != nil {
				logger.Error("error saving saga instance", err, logFields)
				return err
			}

			if !results.local {
				logger.Trace("exiting step loop", logFields)
				break
			}

			// handle a local success outcome and kick off the next step
			logger.Trace("handling local success result", logFields)
			results, err = o.handleReply(ctx, results.updatedStepContext, results.updatedSagaData, cqrs.WithSuccess())
			if err != nil {
				logger.Error("error handling local success result", err, logFields)
				return err
			}
		}
	}

	return nil
}

func (o *orchestrator) processEnd(ctx context.Context, instance *Instance) {
	logFields := watermill.LogFields{
		"saga_name": o.definition.SagaName(),
		"saga_id":   instance.sagaID,
	}
	logger := o.config.Logger.With(logFields)

	if instance.compensating {
		logger.Trace("executing saga compensated hook", logFields)
		o.definition.OnHook(ctx, SagaCompensated, instance)
	} else {
		logger.Trace("executing saga completed hook", logFields)
		o.definition.OnHook(ctx, SagaCompleted, instance)
	}
	logger.Trace("saga has finished all steps", logFields)
}

func (o *orchestrator) handleReply(ctx context.Context, stepCtx stepContext, sagaData any, message cqrs.ReplyMessage) (*stepResults, error) {
	replyName := message.Reply().ReplyName()

	logFields := watermill.LogFields{
		"saga_name":  o.definition.SagaName(),
		"saga_id":    message.Headers().Get(MessageReplySagaID),
		"reply_name": replyName,
	}
	logger := o.config.Logger.With(logFields)

	if stepCtx.step >= len(o.definition.Steps()) || stepCtx.step < 0 {
		logger.Error(
			"current step is out of bounds",
			fmt.Errorf("current step is out of bounds"),
			logFields.Add(watermill.LogFields{"Step": fmt.Sprintf("%d", stepCtx.step)}),
		)
		return nil, fmt.Errorf("current step is out of bounds: 0-%d, got %d", len(o.definition.Steps()), stepCtx.step)
	}
	step := o.definition.Steps()[stepCtx.step]

	// handle specific replies
	if handler := step.getReplyHandler(replyName, stepCtx.compensating); handler != nil {
		logger.Trace("saga reply handler found", logFields)
		err := handler(ctx, sagaData, message.Reply())
		if err != nil {
			logger.Error("saga reply handler returned an error", err, logFields)
			return nil, err
		}
	} else {
		logger.Trace("saga reply handler not found", logFields)
	}

	outcome, err := message.Headers().GetRequired(cqrs.MessageReplyOutcome)
	if err != nil {
		logger.Error("error reading reply outcome", err, logFields)
		return nil, err
	}

	logger.Trace("reply outcome", logFields.Add(watermill.LogFields{"Outcome": outcome}))

	success := outcome == cqrs.ReplyOutcomeSuccess

	switch {
	case success:
		logger.Trace("advancing to next step", logFields)
		return o.executeNextStep(ctx, stepCtx, sagaData), nil
	case stepCtx.compensating:
		// we're already failing, we can't fail any more
		logger.Error(
			"received a failure outcome while compensating",
			fmt.Errorf("received a failure outcome while compensating"),
			logFields.Add(watermill.LogFields{"Step": fmt.Sprintf("%d", stepCtx.step)}),
		)
		return nil, fmt.Errorf("received failure outcome while compensating")
	default:
		logger.Trace("compensating to previous step", logFields)
		return o.executeNextStep(ctx, stepCtx.compensate(), sagaData), nil
	}
}

func (o *orchestrator) executeNextStep(ctx context.Context, stepCtx stepContext, sagaData any) *stepResults {
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
