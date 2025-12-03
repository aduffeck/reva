package workqueue

import (
	"os"

	"github.com/opencloud-eu/reva/v2/pkg/workqueue/task"
)

type Worker struct {
	storage  Storage
	handlers map[string]func(*task.Task) error
}

func NewWorker(wq *WorkQueue) *Worker {
	return &Worker{
		storage: wq.storage,
	}
}

func (w *Worker) Handle(taskType string, handler func(*task.Task) error) {
	if w.handlers == nil {
		w.handlers = make(map[string]func(*task.Task) error)
	}
	w.handlers[taskType] = handler
}

func (w *Worker) Start() {
	for {
		t, err := w.storage.Pull()
		if err != nil {
			continue
		}
		go func(task *task.Task) {
			defer func() {
				if r := recover(); r != nil {
					os.Stderr.WriteString("Recovered in worker: panic processing task\n")
				}
			}()
			if handler, ok := w.handlers[task.Type()]; ok {
				if err := handler(task); err != nil {
					// handle processing error
				}
			} else {
				panic("no handler for task type " + task.Type())
			}
		}(t)
	}
}

func (w *Worker) Stop() {
	// Implementation of worker stop logic goes here
}
