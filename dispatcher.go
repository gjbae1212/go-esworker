package esworker

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

var (
	defaultGlobalQueueSize    = 5000
	defaultWorkerSize         = 5
	defaultWorkerQueueSize    = 1000
	defaultWorkerWaitInterval = time.Duration(2 * time.Second)
)

// TODO: support create or update or delete
type (
	// Action is an operation that could create or update or delete to document.
	Action interface {
		GetIndex() string
		GetType() string
		GetDoc() map[string]interface{}
	}

	// Dispatcher is an interface of workers orchestration that could manage all of Action and control all of the process flows.
	Dispatcher interface {
		AddAction(ctx context.Context, action Action) error
		Start() error
		Stop() error
	}

	// dispatcher is a practical struct to use in internal.
	dispatcher struct {
		sync.RWMutex
		cfg *config
		bk  *breaker
	}

	// breaker is a middle struct between dispatcher and worker
	breaker struct {
		queue        chan Action
		pool         chan chan Action
		workers      []*worker
		errorHandler ErrorHandler
		quit         chan bool
		running      bool
	}
)

// AddAction pushes an action to queue.
func (dp *dispatcher) AddAction(ctx context.Context, action Action) error {
	if ctx == nil || action == nil {
		return fmt.Errorf("[err] AddAction empty params")
	}

	select {
	case dp.bk.queue <- action:
	case <-ctx.Done():
		return fmt.Errorf("[err] AddAction timeout")
	}
	return nil
}

// Start is starting to let an action processed.
func (dp *dispatcher) Start() error {
	dp.Lock()
	defer dp.Unlock()
	if dp.bk.running {
		return fmt.Errorf("[err] already runnning dispatcher\n")
	}
	dp.bk.start()
	return nil
}

// Stop is stopping to let an action don't be processed.
func (dp *dispatcher) Stop() error {
	dp.Lock()
	defer dp.Unlock()
	if !dp.bk.running {
		return fmt.Errorf("[err] already stop dispatcher\n")
	}
	dp.bk.stop()
	return nil
}

func (bk *breaker) start() {
	bk.running = true
	for _, w := range bk.workers {
		go w.start()
	}
	go bk.booking()
}

func (bk *breaker) stop() {
	bk.running = false
	for _, w := range bk.workers {
		w.stop()
	}
	bk.quit <- true
}

func (bk *breaker) booking() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[warning] recover booking")
			bk.errorHandler(r.(error))
			// retry booking
			go bk.booking()
		}
	}()

	for {
		select {
		case act := <-bk.queue: // pop action
			// get worker pipe
			workerPipe := <-bk.pool
			// send action to worker pipe
			workerPipe <- act
		case <-bk.quit: // exit breaker
			// delete workers to be waiting
			for len(bk.pool) > 0 {
				<-bk.pool
			}
			return
		}
	}
}

// NewDispatcher is to make Dispatcher.
func NewDispatcher(opts ...Option) (Dispatcher, error) {
	cfg := &config{}

	o := []Option{
		WithErrorHandler(func(err error) {
			fmt.Printf("[err] %+v\n", err)
		}),
		WithTransportOption(http.DefaultTransport),
		WithGlobalQueueSizeOption(defaultGlobalQueueSize),
		WithWorkerSizeOption(defaultWorkerSize),
		WithWorkerQueueSizeOption(defaultWorkerQueueSize),
		WithWorkerWaitInterval(defaultWorkerWaitInterval),
	}

	o = append(o, opts...)
	for _, opt := range o {
		opt.apply(cfg)
	}

	bk, err := createBreaker(cfg)
	if err != nil {
		return nil, err
	}
	return &dispatcher{cfg: cfg, bk: bk}, nil
}

func createBreaker(cfg *config) (*breaker, error) {
	if cfg == nil {
		return nil, fmt.Errorf("[err] createBreaker empty params")
	}

	pool := make(chan chan Action, cfg.workerSize)
	workers := make([]*worker, cfg.workerSize, cfg.workerSize)
	for i := 0; i < cfg.workerSize; i++ {
		w := &worker{
			id:           i,
			pool:         pool,
			pipe:         make(chan Action),
			maxQueueSize: cfg.workerQueueSize,
			waitInterval: cfg.workerWaitInterval,
			quit:         make(chan bool),
			errorHandler: cfg.errorHandler,
		}
		workers = append(workers, w)
	}

	return &breaker{
		queue:        make(chan Action, cfg.globalQueueSize),
		pool:         pool,
		workers:      workers,
		errorHandler: cfg.errorHandler,
		quit:         make(chan bool),
		running:      false,
	}, nil
}
