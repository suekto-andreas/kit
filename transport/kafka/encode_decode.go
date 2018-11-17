package kafka

import (
	"context"

	kafka "github.com/segmentio/kafka-go"
)

// EncodeRequestFunc encodes the passed request object into the Kafka message
// object. It's designed to be used in Kafka publishers, for publisher-side
// endpoints.
type EncodeRequestFunc func(context.Context, *kafka.Message, interface{}) error
