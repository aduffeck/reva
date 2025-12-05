package workqueue

import (
	"github.com/opencloud-eu/reva/v2/pkg/workqueue/task"
	"github.com/rs/zerolog"
)

type Storage interface {
	Queue(t *task.Task) error
	Pull() (*task.Task, error)

	Ack(t *task.Task) error
	Nack(t *task.Task) error

	Len() int
}
type WorkQueue struct {
	storage Storage
	log     *zerolog.Logger
}

func New(opts ...Option) *WorkQueue {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	if o.log == nil {
		o.log = &zerolog.Logger{}
	}

	return &WorkQueue{
		storage: o.storage,
		log:     o.log,
	}
}

func (wq *WorkQueue) Storage() Storage {
	return wq.storage
}

func (wq *WorkQueue) Push(t *task.Task) error {
	return wq.storage.Queue(t)
}

func (wq *WorkQueue) NewWorker() *Worker {
	return &Worker{
		storage: wq.storage,
		log:     wq.log,
	}
}
