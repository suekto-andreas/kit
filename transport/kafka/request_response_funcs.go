package kafka

import (
	"context"

	kafka "github.com/segmentio/kafka-go"
)

// RequestFunc may take information from publisher request message
// and put it into a request context. In Subscribers, RequestFuncs
// are executed prior to invoking the endpoint.
type RequestFunc func(context.Context, *kafka.Message) context.Context
