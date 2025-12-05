package workqueue

import (
	"github.com/opencloud-eu/reva/v2/pkg/workqueue/task"
	"github.com/rs/zerolog"
)

type handlerFunc func(*task.Task) error

type Worker struct {
	storage Storage
	log     *zerolog.Logger

	handlers map[string]handlerFunc
}

func (w *Worker) Handle(taskType string, handler handlerFunc) {
	if w.handlers == nil {
		w.handlers = make(map[string]handlerFunc)
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
					w.log.Error().Msg("Recovered in worker: panic processing task\n")
				}
			}()
			if handler, ok := w.handlers[task.Type]; ok {
				if err := handler(task); err != nil {
					w.storage.Nack(task)
					w.log.Error().Msg("error while handling task " + task.Type)
					return
				}
				w.storage.Ack(task)
			} else {
				w.storage.Nack(task)
				w.log.Error().Msg("no handler for task type " + task.Type)
			}
		}(t)
	}
}

func (w *Worker) Stop() {
	// Implementation of worker stop logic goes here
}
