package nats

type Option func(*options)

type options struct {
	streamName string
	natsConfig NatsConfig
}

func WithStreamName(name string) Option {
	return func(o *options) {
		o.streamName = name
	}
}

func WithNatsConfig(c NatsConfig) Option {
	return func(o *options) {
		o.natsConfig = c
	}
}
