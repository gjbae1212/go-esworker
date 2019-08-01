package esworker

import "time"

// worker will be processing actions with bulk-operation.
type worker struct {
	id           int
	pool         chan chan Action
	pipe         chan Action
	queue        []Action
	maxQueueSize int
	waitInterval time.Duration
	errorHandler ErrorHandler
	quit         chan bool
}

func (w *worker) start() {}
func (w *worker) stop()  {}
