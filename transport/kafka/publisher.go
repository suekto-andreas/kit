package kafka

import (
	"context"
	"time"

	"github.com/go-kit/kit/endpoint"
	kafka "github.com/segmentio/kafka-go"
)

// Writer is the interface which represents a writing io of Kafka.
// github.com/segmentio/kafka-go Writer is recommended as the concrete
// implementation.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// Publisher wraps a Kafka writer, and provides a method that
// implements endpoint.Endpoint.
type Publisher struct {
	writer  Writer
	enc     EncodeRequestFunc
	before  []RequestFunc
	timeout time.Duration
}

// NewPublisher constructs a Kafka Publisher
// based on the given kafka.WriterConfig.
func NewPublisher(
	writer Writer,
	enc EncodeRequestFunc,
	options ...PublisherOption,
) *Publisher {
	p := &Publisher{
		writer: writer,
		enc:    enc,
	}
	for _, option := range options {
		option(p)
	}
	return p
}

// PublisherOption sets an optional parameter for clients.
type PublisherOption func(*Publisher)

// PublisherBefore sets the RequestFuncs that are applied to the outgoing Kafka
// message before it's being produced.
func PublisherBefore(before ...RequestFunc) PublisherOption {
	return func(p *Publisher) { p.before = append(p.before, before...) }
}

// PublisherTimeout sets the available timeout for an Kafka request.
func PublisherTimeout(timeout time.Duration) PublisherOption {
	return func(p *Publisher) { p.timeout = timeout }
}

// Endpoint returns a usable endpoint that invokes the remote endpoint.
func (p Publisher) Endpoint() endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		ctx, cancel := context.WithTimeout(ctx, p.timeout)
		defer cancel()

		msg := kafka.Message{}

		if err := p.enc(ctx, &msg, request); err != nil {
			return nil, err
		}

		for _, f := range p.before {
			ctx = f(ctx, &msg)
		}

		err := p.writer.WriteMessages(ctx, msg)
		if err != nil {
			return nil, err
		}

		return p.writer, nil
	}
}
