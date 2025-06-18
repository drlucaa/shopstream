package contracts

import (
	"context"
	"errors"

	"github.com/streadway/amqp"
)

// MessageHandler defines the signature for a function that can process a RabbitMQ message.
// It's the contract between the eventbus consumer and the message processor.
type MessageHandler func(ctx context.Context, delivery amqp.Delivery) error

// ErrPermanentFailure is a special error used by a MessageHandler to signal
// that a message is malformed or cannot be processed and should not be retried or requeued.
// The eventbus consumer will see this error and move the message to a parking lot or DLQ.
var ErrPermanentFailure = errors.New("permanent failure processing message")
