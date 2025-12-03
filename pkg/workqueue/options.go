package workqueue

type Option func(*options)

type options struct {
	storage Storage
}

func WithStorage(s Storage) Option {
	return func(o *options) {
		o.storage = s
	}
}
