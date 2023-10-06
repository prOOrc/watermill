package saga

import (
	"context"
)

// InstanceStore interface
type InstanceStore interface {
	Find(ctx context.Context, sagaID string, data any) (*Instance, error)
	Save(ctx context.Context, sagaInstance *Instance) error
	Update(ctx context.Context, sagaInstance *Instance) error
}
