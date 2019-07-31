package esworker

type worker struct {
	id           int
	pool         chan chan Action
	pipe         chan Action
	errorHandler ErrorHandler
	quit         chan bool
}

func (w *worker) start() {}
func (w *worker) stop()  {}
