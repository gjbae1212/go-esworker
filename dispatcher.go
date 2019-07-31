package esworker

import (
	"context"
	"net/http"
)

var (
	defaultQueueSize  = 1000
	defaultWorkerSize = 5
)

// TODO: support create or update or delete
type (
	// ErrorHandler is called when an error is raised.
	ErrorHandler func(error)

	// Action is an operation that could create or update or delete to document.
	Action interface {
		GetIndex() string
		GetType() string
		GetDoc() map[string]interface{}
	}

	// Dispatcher is an interface of workers orchestration that could manage all of Action and control all of the process flows.
	Dispatcher interface {
		AddAction(ctx context.Context, action *Action) error
	}

	// dispatcher is a practical struct to use in internal.
	dispatcher struct {
		cfg *config
	}
)

// NewDispatcher is to make Dispatcher.
func NewDispatcher(opts ...Option) (Dispatcher, error) {
	cfg := &config{}

	o := []Option{
		WithTransportOption(http.DefaultTransport),
		WithQueueSizeOption(defaultQueueSize),
		WithWorkerSizeOption(defaultWorkerSize),
	}

	o = append(o, opts...)
	for _, opt := range o {
		opt.apply(cfg)
	}

	// TODO: 개발

	return nil, nil
}
