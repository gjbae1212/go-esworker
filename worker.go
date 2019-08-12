package esworker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// worker will be processing actions with bulk-operation.
type worker struct {
	sync.RWMutex
	esClient     ESProxy
	id           int
	pool         chan chan Action
	pipe         chan Action
	queue        []Action
	maxQueueSize int
	waitInterval time.Duration
	errorHandler ErrorHandler
	quit         chan bool
}

// start is to start loop.
func (w *worker) start() {
	defer func() {
		if r := recover(); r != nil {
			w.errorHandler(r.(error))
			go w.start()
		}
	}()

	// the pipe is inserted to the worker pool in firstly.
	w.pool <- w.pipe
Loop:
	for {
		select {
		case act := <-w.pipe: // get data.
			if err := w.enqueue(act); err != nil { // enqueue a action.
				w.errorHandler(err)
			}

			if w.queueSize() >= w.maxQueueSize { // exceed a threshold.
				if err := w.process(); err != nil { // processing rest jobs
					w.errorHandler(err)
				}
			}

			// ready for getting an action
			w.pool <- w.pipe
		case <-time.After(w.waitInterval): // wait duration interval time.
			if err := w.process(); err != nil {
				w.errorHandler(err)
			}
		case <-w.quit: // exit loop.
			if err := w.process(); err != nil { // processing rest jobs.
				w.errorHandler(err)
			}
			break Loop
		}
	}

}

// stop is to stop loop
func (w *worker) stop() {
	w.quit <- true
}

// enqueue adds action to a queue.
func (w *worker) enqueue(act ...Action) error {
	if len(act) == 0 {
		return fmt.Errorf("[err] enqueue (empty params)")
	}
	w.Lock()
	defer w.Unlock()

	w.queue = append(w.queue, act...)
	return nil
}

// dequeueAll is to pop all of the queue data.
func (w *worker) dequeueAll() {
	w.Lock()
	defer w.Unlock()
	w.queue = w.queue[:0]
}

// queueSize returns queue size.
func (w *worker) queueSize() int {
	w.RLock()
	defer w.RUnlock()
	return len(w.queue)
}

// process is something that total actions get from a queue, processing its actions, respectively.
func (w *worker) process() (err error) {
	if w.queueSize() == 0 {
		return nil
	}

	defer func() {
		w.dequeueAll()
	}()

	// set request timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	resp, err := w.esClient.Bulk(ctx, w.queue)
	if err != nil {
		return err
	}

	success, fail := resp.Count()
	fmt.Printf("[go-esworker-process] success %d, fail %d \n", success, fail)
	return nil
}
