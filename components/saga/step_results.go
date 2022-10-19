package saga

import "github.com/ThreeDotsLabs/watermill/components/cqrs"

type stepResults struct {
	updatedSagaData    SagaData
	commands           []cqrs.Command
	updatedStepContext stepContext
	local              bool
	failure            error
}
