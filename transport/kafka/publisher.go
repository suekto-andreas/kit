package kafka

import (
	"context"
	"time"

	"github.com/go-kit/kit/endpoint"
	kafka "github.com/segmentio/kafka-go"
)

// Publisher wraps a Kafka producer config, and provides a method that
// implements endpoint.Endpoint.
type Publisher struct {
	writer  *kafka.Writer
	enc     EncodeRequestFunc
	dec     DecodeResponseFunc
	before  []RequestFunc
	after   []PublisherResponseFunc
	timeout time.Duration
}

// NewPublisher constructs a Kafka Publisher
// based on the given kafka.WriterConfig.
func NewPublisher(
	writer *kafka.Writer,
	options ...PublisherOption,
) *Publisher {
	p := &Publisher{
		writer: writer,
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

// PublisherAfter sets the ClientResponseFuncs applied to the incoming Kafka
// request prior to it being decoded. This is useful for obtaining anything off
// of the response and adding onto the context prior to decoding.
func PublisherAfter(after ...PublisherResponseFunc) PublisherOption {
	return func(p *Publisher) { p.after = append(p.after, after...) }
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

		msgs := []kafka.Message{
			{Time: time.Now()},
		}

		if err := p.enc(ctx, msgs, request); err != nil {
			return nil, err
		}

		for _, f := range p.before {
			ctx = f(ctx, msgs)
		}

		err := p.writer.WriteMessages(ctx, msgs...)
		if err != nil {
			return nil, err
		}

		stats := p.writer.Stats()
		for _, f := range p.after {
			ctx = f(ctx, &stats)
		}
		response, err := p.dec(ctx, &stats)
		if err != nil {
			return nil, err
		}

		return response, nil
	}
}
