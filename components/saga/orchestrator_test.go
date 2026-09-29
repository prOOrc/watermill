package saga

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"
)

// Regression test: when the scan finds no step with an invocable action,
// executeNextStep must end the saga instead of executing the last checked step
// (which dereferences a nil action handler and panics).
func TestExecuteNextStep_NoInvocableStep_EndsSaga(t *testing.T) {
	t.Parallel()

	t.Run("compensating scan without compensation steps ends saga", func(t *testing.T) {
		t.Parallel()

		def := NewDefinition[struct{}]("test-saga", "test-saga.reply", nil).WithSteps([]Step{
			NewRemoteStep[struct{}]().Action(func(ctx context.Context, data *struct{}) cqrs.Command { return nil }),
			NewRemoteStep[struct{}]().Action(func(ctx context.Context, data *struct{}) cqrs.Command { return nil }),
		})

		o := &orchestrator{definition: def}

		results := o.executeNextStep(context.Background(), stepContext{step: 1, compensating: true}, &struct{}{})

		require.True(t, results.updatedStepContext.ended, "saga must end when no compensation step is found")
	})

	t.Run("forward scan with all steps skipped by predicate ends saga", func(t *testing.T) {
		t.Parallel()

		def := NewDefinition[struct{}]("test-saga", "test-saga.reply", nil).WithSteps([]Step{
			NewRemoteStep[struct{}]().Action(func(ctx context.Context, data *struct{}) cqrs.Command { return nil }),
			NewRemoteStep[struct{}]().Action(
				func(ctx context.Context, data *struct{}) cqrs.Command { return nil },
				WithRemoteStepPredicate(func(ctx context.Context, data any) bool { return false }),
			),
		})

		o := &orchestrator{definition: def}

		results := o.executeNextStep(context.Background(), stepContext{step: 0}, &struct{}{})

		require.True(t, results.updatedStepContext.ended, "saga must end when all remaining steps are skipped by predicate")
	})
}

// TestHandleReply_Synchronous verifies that HandleReply drives the saga to an
// end state synchronously on the caller's goroutine, without a router or pubsub.
func TestHandleReply_Synchronous(t *testing.T) {
	t.Parallel()

	type testCommand struct{}

	var _ cqrs.Command = (*testCommand)(nil)

	def := NewDefinition[struct{}](
		"test-saga",
		"test-saga.reply",
		[]func() cqrs.Reply{
			func() cqrs.Reply { return &cqrs.Success{} },
		},
	).WithSteps([]Step{
		NewRemoteStep[struct{}]().Action(func(ctx context.Context, data *struct{}) cqrs.Command {
			return &testCommand{}
		}),
	})

	store := newTestInstanceStore()
	publisher := newTestPublisher()

	orchestrator, err := NewOrchestratorWithConfig(def, OrchestratorConfig{
		InstanceStore: store,
		SubscriberConstructor: func(string) (message.Subscriber, error) {
			return nil, errors.New("subscriber constructor must not be called in synchronous test")
		},
		GenerateSubscribeTopic: func(string) string { return "test.commands" },
		Publisher:              publisher,
		Marshaler:              cqrs.JSONMarshaler{},
		Logger:                 watermill.NopLogger{},
	})
	require.NoError(t, err)

	ctx := context.Background()

	instance, err := orchestrator.Start(ctx, &struct{}{})
	require.NoError(t, err)

	require.Len(t, publisher.messages, 1, "Start must publish the first step command")
	cmdMsg := publisher.messages[0]

	replyMsg := message.NewMessage(watermill.NewUUID(), []byte("{}"))
	replyMsg.Metadata.Set(cqrs.MessageReplyName, cqrs.Success{}.ReplyName())
	replyMsg.Metadata.Set(cqrs.MessageReplyOutcome, cqrs.ReplyOutcomeSuccess)
	replyMsg.Metadata.Set(MessageReplySagaID, cmdMsg.Metadata.Get(MessageCommandSagaID))
	replyMsg.Metadata.Set(MessageReplySagaName, cmdMsg.Metadata.Get(MessageCommandSagaName))
	replyMsg.Metadata.Set(MessageReplySagaStep, cmdMsg.Metadata.Get(MessageCommandSagaStep))

	require.NoError(t, orchestrator.HandleReply(ctx, replyMsg))

	final := store.get(instance.SagaID())
	require.NotNil(t, final)
	require.True(t, final.EndState(), "saga must be in end state synchronously after HandleReply returns")
	require.False(t, final.Compensating())
}

// testInstanceStore is a minimal in-memory InstanceStore for synchronous tests.
type testInstanceStore struct {
	mu        sync.Mutex
	instances map[string]*Instance
}

func newTestInstanceStore() *testInstanceStore {
	return &testInstanceStore{instances: make(map[string]*Instance)}
}

func (s *testInstanceStore) Find(ctx context.Context, sagaID string, data any) (*Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	instance, ok := s.instances[sagaID]
	if !ok {
		return nil, fmt.Errorf("saga instance %s not found", sagaID)
	}
	return instance, nil
}

func (s *testInstanceStore) Save(ctx context.Context, instance *Instance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.instances[instance.SagaID()] = instance
	return nil
}

func (s *testInstanceStore) Update(ctx context.Context, instance *Instance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.instances[instance.SagaID()] = instance
	return nil
}

func (s *testInstanceStore) get(sagaID string) *Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.instances[sagaID]
}

// testPublisher records published messages for synchronous tests.
type testPublisher struct {
	mu       sync.Mutex
	messages []*message.Message
}

func newTestPublisher() *testPublisher {
	return &testPublisher{}
}

func (p *testPublisher) Publish(topic string, messages ...*message.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, messages...)
	return nil
}

func (p *testPublisher) Close() error { return nil }
