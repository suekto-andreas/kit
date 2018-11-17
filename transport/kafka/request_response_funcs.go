package kafka

import (
	"context"

	kafka "github.com/segmentio/kafka-go"
)

// RequestFunc may take information from publisher request messages
// and put it into a request context. In Subscribers, RequestFuncs
// are executed prior to invoking the endpoint.
type RequestFunc func(context.Context, []kafka.Message) context.Context

// PublisherResponseFunc may take information from Kafka Stats and make the
// response available for consumption. ClientResponseFuncs are only executed in
// clients, after a request has been made, but prior to it being decoded.
type PublisherResponseFunc func(context.Context, *kafka.WriterStats) context.Context
