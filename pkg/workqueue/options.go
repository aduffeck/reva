package workqueue

import "github.com/rs/zerolog"

type Option func(*options)

type options struct {
	storage Storage
	log     *zerolog.Logger
}

func WithStorage(s Storage) Option {
	return func(o *options) {
		o.storage = s
	}
}

func WithLogger(log *zerolog.Logger) Option {
	return func(o *options) {
		o.log = log
	}
}
