package nats

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/opencloud-eu/reva/v2/pkg/workqueue/task"
	"github.com/pkg/errors"
)

type NatsConfig struct {
	Endpoint             string        `mapstructure:"address"`          // Endpoint of the nats server
	Cluster              string        `mapstructure:"clusterID"`        // CluserID of the nats cluster
	TLSInsecure          bool          `mapstructure:"tls-insecure"`     // Whether to verify TLS certificates
	TLSRootCACertificate string        `mapstructure:"tls-root-ca-cert"` // The root CA certificate used to validate the TLS certificate
	EnableTLS            bool          `mapstructure:"enable-tls"`       // Enable TLS
	AuthUsername         string        `mapstructure:"username"`         // Username for authentication
	AuthPassword         string        `mapstructure:"password"`         // Password for authentication
	MaxAckPending        int           `mapstructure:"max-ack-pending"`  // Maximum number of unacknowledged messages
	AckWait              time.Duration `mapstructure:"ack-wait"`         // Time to wait for an ack
}

// Nats is a NATS-based implementation of the Storage interface.
type Nats struct {
	js         jetstream.JetStream
	consumer   jetstream.Consumer
	streamName string
	subject    string
	natsConfig NatsConfig

	mu         sync.Mutex
	processing map[string]jetstream.Msg
}

// New creates a new NATS storage backend for the workqueue.
func New(opts ...Option) (*Nats, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	if o.streamName == "" {
		return nil, errors.New("stream name must be provided")
	}

	ctx := context.Background()
	subject := "workqueue.task"
	js, err := jsFromConfig("workqueue-nats-storage", o.natsConfig)
	if err != nil {
		return nil, err
	}

	// Ensure the stream exists
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     o.streamName,
		Subjects: []string{subject},
	})
	if err != nil {
		return nil, err
	}

	// Create or update the consumer
	consumer, err := js.CreateOrUpdateConsumer(ctx, o.streamName, jetstream.ConsumerConfig{
		Durable:   o.streamName + "_WORKER",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return nil, err
	}

	return &Nats{
		natsConfig: o.natsConfig,
		js:         js,
		consumer:   consumer,
		streamName: o.streamName,
		subject:    subject,
		processing: make(map[string]jetstream.Msg),
	}, nil
}

func jsFromConfig(name string, cfg NatsConfig) (jetstream.JetStream, error) {
	var js jetstream.JetStream
	b := backoff.NewExponentialBackOff()

	connect := func() error {
		var tlsConf *tls.Config
		if cfg.EnableTLS {
			var rootCAPool *x509.CertPool
			if cfg.TLSRootCACertificate != "" {
				rootCrtFile, err := os.Open(cfg.TLSRootCACertificate)
				if err != nil {
					return err
				}

				rootCAPool, err = newCertPoolFromPEM(rootCrtFile)
				if err != nil {
					return err
				}
				cfg.TLSInsecure = false
			}

			tlsConf = &tls.Config{
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: cfg.TLSInsecure,
				RootCAs:            rootCAPool,
			}
		}

		nopts := nats.GetDefaultOptions()
		nopts.Name = name
		if tlsConf != nil {
			nopts.Secure = true
			nopts.TLSConfig = tlsConf
		}

		if len(cfg.Endpoint) > 0 {
			nopts.Servers = []string{cfg.Endpoint}
		}

		if cfg.AuthUsername != "" && cfg.AuthPassword != "" {
			nopts.User = cfg.AuthUsername
			nopts.Password = cfg.AuthPassword
		}

		conn, err := nopts.Connect()
		if err != nil {
			return err
		}

		js, err = jetstream.New(conn)
		if err != nil {
			return err
		}

		return nil
	}

	err := backoff.Retry(connect, b)
	if err != nil {
		return nil, err
	}
	return js, nil
}

// Queue adds a task to the queue.
func (n *Nats) Queue(t *task.Task) error {
	data, err := json.Marshal(t)
	if err != nil {
		return err
	}

	_, err = n.js.Publish(context.Background(), n.subject, data)
	return err
}

// Pull retrieves the next task from the queue.
func (n *Nats) Pull() (*task.Task, error) {
	for {
		// Fetch 1 message, waiting up to 5 minutes
		msgs, err := n.consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Minute))
		if err != nil {
			continue
		}

		m := <-msgs.Messages()
		if m == nil {
			continue
		}

		var t task.Task
		if err := json.Unmarshal(m.Data(), &t); err != nil {
			// Malformed data, terminate to prevent redelivery
			_ = m.Term()
			continue
		}

		// Store msg locally to Ack later
		n.mu.Lock()
		n.processing[t.ID] = m
		n.mu.Unlock()

		return &t, nil
	}
}

// Len returns the number of tasks currently in the queue.
func (n *Nats) Len() int {
	ctx := context.Background()
	// Try to get the consumer info first to see how many messages are pending for the worker group
	info, err := n.consumer.Info(ctx)
	if err == nil {
		return int(info.NumPending) + info.NumAckPending
	}

	// Fallback to stream info if consumer doesn't exist yet
	stream, err := n.js.Stream(ctx, n.streamName)
	if err != nil {
		return 0
	}

	sInfo, err := stream.Info(ctx)
	if err != nil {
		return 0
	}
	return int(sInfo.State.Msgs)
}

// Ack acknowledges the successful processing of a task.
func (n *Nats) Ack(t *task.Task) error {
	n.mu.Lock()
	msg, ok := n.processing[t.ID]
	delete(n.processing, t.ID)
	n.mu.Unlock()

	if !ok {
		return nil
	}
	return msg.Ack()
}

// Nack negatively acknowledges the processing of a task.
func (n *Nats) Nack(t *task.Task) error {
	n.mu.Lock()
	msg, ok := n.processing[t.ID]
	delete(n.processing, t.ID)
	n.mu.Unlock()

	if !ok {
		return nil
	}
	return msg.Nak()
}

// newCertPoolFromPEM reads certificates from io.Reader and returns a x509.CertPool
// containing those certificates.
func newCertPoolFromPEM(crts ...io.Reader) (*x509.CertPool, error) {
	certPool := x509.NewCertPool()

	var buf bytes.Buffer
	for _, c := range crts {
		if _, err := io.Copy(&buf, c); err != nil {
			return nil, err
		}
		if !certPool.AppendCertsFromPEM(buf.Bytes()) {
			return nil, errors.New("failed to append cert from PEM")
		}
		buf.Reset()
	}

	return certPool, nil
}
