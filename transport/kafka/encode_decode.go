package kafka

import (
	"context"

	kafka "github.com/segmentio/kafka-go"
)

// EncodeRequestFunc encodes the passed request object into the Kafka request
// object. It's designed to be used in Kafka publishers, for publisher-side
// endpoints.
type EncodeRequestFunc func(context.Context, []kafka.Message, interface{}) error

// DecodeResponseFunc extracts a user-domain response object from
// a Kafka WriterStats object. It is designed to be used in Kafka Publishers.
type DecodeResponseFunc func(context.Context, *kafka.WriterStats) (response interface{}, err error)
