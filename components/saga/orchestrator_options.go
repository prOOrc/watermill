package saga

import (
	"github.com/ThreeDotsLabs/watermill"
)

// OrchestratorOption options for Orchestrator
type OrchestratorOption func(o *orchestrator)

// WithOrchestratorLogger is an option to set the log.Logger of the Orchestrator
func WithOrchestratorLogger(logger watermill.LoggerAdapter) OrchestratorOption {
	return func(o *orchestrator) {
		o.logger = logger
	}
}
