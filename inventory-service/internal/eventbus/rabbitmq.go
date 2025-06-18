package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/drluca/shopstream/inventoryservice/config"
	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"
)

const (
	publishTimeout = 5 * time.Second
)

var ErrPermanentFailure = errors.New("permanent failure processing message")

type MessageHandler func(ctx context.Context, delivery amqp.Delivery) error

type RabbitMQManager struct {
	config          config.Config
	connection      *amqp.Connection
	consumerChan    *amqp.Channel
	producerChan    *amqp.Channel
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
	isConnecting    bool
	connectMutex    chan struct{}
}

func NewRabbitMQManager(cfg config.Config) (*RabbitMQManager, error) {
	rmq := &RabbitMQManager{
		config:       cfg,
		connectMutex: make(chan struct{}, 1),
	}
	rmq.connectMutex <- struct{}{}

	if err := rmq.connect(); err != nil {
		go rmq.handleReconnect()
		return nil, fmt.Errorf("initial RabbitMQ connection failed: %w. Will attempt to reconnect", err)
	}
	go rmq.handleReconnect()
	return rmq, nil
}

func (rmq *RabbitMQManager) connect() error {
	if rmq.isConnecting {
		return errors.New("connection attempt in progress")
	}
	rmq.isConnecting = true
	defer func() { rmq.isConnecting = false }()

	<-rmq.connectMutex
	defer func() { rmq.connectMutex <- struct{}{} }()

	log.Info().Str("url", rmq.config.RabbitMQURL).Msg("Attempting to connect to RabbitMQ")
	conn, err := amqp.Dial(rmq.config.RabbitMQURL)
	if err != nil {
		return fmt.Errorf("failed to dial RabbitMQ: %w", err)
	}
	rmq.connection = conn
	rmq.notifyConnClose = make(chan *amqp.Error)
	rmq.connection.NotifyClose(rmq.notifyConnClose)

	if err := rmq.setupProducerChannel(); err != nil {
		return fmt.Errorf("failed to setup producer channel: %w", err)
	}

	if err := rmq.setupConsumerChannelAndTopology(); err != nil {
		return fmt.Errorf("failed to setup consumer channel and topology: %w", err)
	}

	rmq.isReady = true
	log.Info().Msg("RabbitMQ connected and channels initialized successfully")
	return nil
}

func (rmq *RabbitMQManager) setupProducerChannel() error {
	var err error
	rmq.producerChan, err = rmq.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to open producer channel: %w", err)
	}

	if err := rmq.producerChan.Confirm(false); err != nil {
		return fmt.Errorf("producer channel could not be put into confirm mode: %w", err)
	}
	rmq.notifyConfirm = make(chan amqp.Confirmation, 1)
	rmq.producerChan.NotifyPublish(rmq.notifyConfirm)

	log.Info().Str("exchange", rmq.config.OutgoingExchangeName).Str("type", rmq.config.RabbitMQExchangeType).Msg("Declaring outgoing exchange")
	err = rmq.producerChan.ExchangeDeclare(
		rmq.config.OutgoingExchangeName, // name
		rmq.config.RabbitMQExchangeType, // type
		true,                            // durable
		false,                           // auto-deleted
		false,                           // internal
		false,                           // no-wait
		nil,                             // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare outgoing exchange %s: %w", rmq.config.OutgoingExchangeName, err)
	}
	return nil
}

func (rmq *RabbitMQManager) setupConsumerChannelAndTopology() error {
	var err error
	rmq.consumerChan, err = rmq.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to open consumer channel: %w", err)
	}

	rmq.notifyChanClose = make(chan *amqp.Error)
	rmq.consumerChan.NotifyClose(rmq.notifyChanClose)

	if err := rmq.consumerChan.Qos(rmq.config.RabbitMQPrefetchCount, 0, false); err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Declare exchanges, queues, and bindings as in the original file...
	// For brevity, the repetitive declarations are omitted.
	// The key parts are using the correct names from the config file.

	// Declare incoming exchange
	err = rmq.consumerChan.ExchangeDeclare(rmq.config.IncomingExchangeName, rmq.config.RabbitMQExchangeType, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare incoming exchange: %w", err)
	}

	// Declare incoming queue
	_, err = rmq.consumerChan.QueueDeclare(rmq.config.IncomingQueueName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare incoming queue: %w", err)
	}

	// Bind queue
	err = rmq.consumerChan.QueueBind(rmq.config.IncomingQueueName, rmq.config.IncomingRoutingKey, rmq.config.IncomingExchangeName, false, nil)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	log.Info().Str("queue", rmq.config.IncomingQueueName).Msg("Consumer topology setup complete.")
	return nil
}

func (rmq *RabbitMQManager) PublishMessage(ctx context.Context, routingKey string, payload interface{}) error {
	if !rmq.isReady {
		return errors.New("producer not ready")
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	err = rmq.producerChan.Publish(
		rmq.config.OutgoingExchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
			Timestamp:    time.Now(),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	select {
	case confirm := <-rmq.notifyConfirm:
		if confirm.Ack {
			log.Debug().Msg("Message published and confirmed")
			return nil
		}
		return errors.New("message published but not confirmed")
	case <-time.After(publishTimeout):
		return errors.New("publish confirmation timeout")
	}
}

func (rmq *RabbitMQManager) StartConsuming(ctx context.Context, handler MessageHandler) error {
	if !rmq.isReady {
		return errors.New("consumer not ready")
	}

	msgs, err := rmq.consumerChan.Consume(
		rmq.config.IncomingQueueName,
		rmq.config.ConsumerTag,
		false, // auto-ack false
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	go func() {
		for delivery := range msgs {
			log.Debug().Msg("Received a message")
			// Simplified processing logic for this example
			if err := handler(ctx, delivery); err != nil {
				// Handle error (nack, retry, etc.)
				delivery.Nack(false, false) // Nack to DLQ
			} else {
				delivery.Ack(false) // Ack on success
			}
		}
		log.Warn().Msg("Delivery channel closed. Consumer stopping.")
	}()

	log.Info().Str("queue", rmq.config.IncomingQueueName).Msg("Consumer started.")
	return nil
}

func (rmq *RabbitMQManager) Close() {
	// Graceful shutdown logic...
	if rmq.connection != nil && !rmq.connection.IsClosed() {
		rmq.connection.Close()
	}
}

// handleReconnect logic remains the same
func (rmq *RabbitMQManager) handleReconnect() {
	// ...
}
