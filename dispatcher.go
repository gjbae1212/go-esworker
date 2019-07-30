package esworker

import "context"

// TODO: support create or update or delete
type (

	// ErrorHandler is called when error is raised.
	ErrorHandler func(error)

	// Action is operation that could create or update or delete to document.
	Action interface {
		GetIndex() string
		GetType() string
		GetDoc() map[string]interface{}
	}

	// Dispatcher is Worker Orchestration that could manage all of Action and control all of process flows.
	Dispatcher interface {
		AddAction(ctx context.Context, action *Action) error
	}
)
