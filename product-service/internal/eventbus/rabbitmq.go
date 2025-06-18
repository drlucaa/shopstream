package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/drluca/shopstream/productservice/config"
	"github.com/drluca/shopstream/productservice/internal/contracts"
	// Note: The direct import of "models" is no longer needed here for publishing,
	// making this package more generic. The processor will handle specific model types.
	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"
)

const (
	publishTimeout = 5 * time.Second
)

// ErrPermanentFailure is a special error to indicate a message cannot be processed.
var ErrPermanentFailure = errors.New("permanent failure processing message")

// MessageHandler is a function type that processes a received amqp.Delivery
type MessageHandler func(ctx context.Context, delivery amqp.Delivery) error

// RabbitMQManager handles RabbitMQ connections and operations.
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

// NewRabbitMQManager creates a new RabbitMQManager.
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

// connect establishes the connection and sets up channels and topology.
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

// setupProducerChannel declares the outgoing exchange.
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
		rmq.config.OutgoingExchangeName,
		rmq.config.RabbitMQExchangeType, // "topic"
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

// setupConsumerChannelAndTopology declares the incoming exchange, queues, and bindings.
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

	// Declare incoming exchange
	err = rmq.consumerChan.ExchangeDeclare(rmq.config.IncomingExchangeName, rmq.config.RabbitMQExchangeType, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare incoming exchange: %w", err)
	}

	// Declare incoming queue
	// In a real production system, you would declare DLX/DLQ here as well.
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

// PublishMessage sends a message with a given routing key and payload.
// The payload is generic (interface{}) and will be JSON marshaled.
func (rmq *RabbitMQManager) PublishMessage(ctx context.Context, routingKey string, payload interface{}) error {
	if !rmq.isReady {
		return errors.New("producer not ready")
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal message payload: %w", err)
	}

	log.Debug().Str("exchange", rmq.config.OutgoingExchangeName).Str("routingKey", routingKey).RawJSON("body", body).Msg("Publishing message")

	err = rmq.producerChan.Publish(
		rmq.config.OutgoingExchangeName,
		routingKey,
		false, // mandatory
		false, // immediate
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

	// Wait for confirmation from the broker.
	select {
	case confirm := <-rmq.notifyConfirm:
		if confirm.Ack {
			log.Debug().Msg("Message published and confirmed by broker")
			return nil
		}
		return errors.New("message published but was NACKed by broker")
	case <-time.After(publishTimeout):
		return errors.New("publish confirmation timeout")
	}
}

// StartConsuming registers a consumer for the configured queue.
// It now uses contracts.MessageHandler as the handler type.
func (rmq *RabbitMQManager) StartConsuming(ctx context.Context, handler contracts.MessageHandler) error {
	if !rmq.isReady {
		return errors.New("consumer not ready")
	}

	msgs, err := rmq.consumerChan.Consume(
		rmq.config.IncomingQueueName,
		rmq.config.ConsumerTag,
		false, // auto-ack is false
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	go func() {
		for delivery := range msgs {
			log.Debug().Uint64("tag", delivery.DeliveryTag).Msg("Received a message")
			err := handler(ctx, delivery)

			// Check for the specific permanent failure error from the contracts package.
			if errors.Is(err, contracts.ErrPermanentFailure) {
				log.Error().Err(err).Msg("Permanent failure processing message, NACKing without requeue.")
				delivery.Nack(false, false) // Send to DLQ
			} else if err != nil {
				log.Error().Err(err).Msg("Transient error processing message, NACKing with requeue.")
				delivery.Nack(false, true) // Retry
			} else {
				delivery.Ack(false) // Acknowledge successful processing.
			}
		}
		log.Warn().Msg("Delivery channel was closed. Consumer is stopping.")
	}()

	log.Info().Str("queue", rmq.config.IncomingQueueName).Msg("Consumer started and waiting for messages.")
	return nil
}

// Close gracefully shuts down the connection.
func (rmq *RabbitMQManager) Close() {
	if rmq.connection != nil && !rmq.connection.IsClosed() {
		log.Info().Msg("Closing RabbitMQ connection.")
		rmq.connection.Close()
	}
}

func (rmq *RabbitMQManager) handleReconnect() {
	log.Info().Msg("RabbitMQ connection monitor started.")
	for {
		if rmq.isReady {
			select {
			case err, ok := <-rmq.notifyConnClose:
				if !ok { // Channel closed by Close() method
					log.Info().Msg("RabbitMQ connection close notification channel closed. Exiting reconnect handler.")
					return
				}
				log.Error().Err(err).Msg("RabbitMQ connection lost. Attempting to reconnect...")
				rmq.isReady = false
				// Clear notifyConfirm to prevent deadlock if producerChan was also lost
				// A new one will be created on successful reconnect and producer channel setup
				if rmq.producerChan != nil {
					// Drain any pending confirms to prevent blocking on next publish if channel is reused (it won't be here, but good practice)
					// Or, more simply, ensure notifyConfirm is re-initialized.
					// In our case, setupProducerChannel re-initializes it.
				}

			case err, ok := <-rmq.notifyChanClose:
				if !ok { // Channel closed by Close() method
					log.Info().Msg("RabbitMQ channel close notification channel closed. Exiting reconnect handler.")
					return
				}
				log.Error().Err(err).Msg("RabbitMQ channel lost. Attempting to re-establish channel...")
				rmq.isReady = false // Mark not ready, connection might still be okay but channel needs reset
				// Attempt to re-setup channels if connection is still alive
				// However, a channel error often indicates a connection issue or requires full reset.
				// For simplicity, we trigger a full reconnect sequence.
			}
		}

		// Attempt to reconnect if not ready
		if !rmq.isReady {
			attempts := 0
			for attempts < rmq.config.MaxReconnectAttempts || rmq.config.MaxReconnectAttempts == 0 { // 0 for infinite
				attempts++
				log.Info().Int("attempt", attempts).Msg("Attempting RabbitMQ reconnection...")
				if err := rmq.connect(); err == nil {
					log.Info().Msg("RabbitMQ reconnected successfully.")
					// If consumer needs to be restarted, it should be handled by the component using StartConsuming.
					// For now, this manager just ensures the connection and channels are ready.
					// The consumer loop in StartConsuming might exit and need to be recalled.
					// This part is tricky: StartConsuming might need to be called again from main or another manager.
					// For now, we assume if connect is successful, the consumer part of StartConsuming will be re-initiated if it exited.
					break // Break from retry loop
				}
				if attempts >= rmq.config.MaxReconnectAttempts && rmq.config.MaxReconnectAttempts != 0 {
					log.Error().Int("attempts", attempts).Msg("Max reconnection attempts reached. Will not try further until next event or manual intervention.")
					// Consider exiting the application or having a longer backoff if critical
					// For now, it will just sit and wait for another connection/channel close event to re-trigger.
					// This means if MaxReconnectAttempts is reached, it won't try again until a new NotifyClose event occurs,
					// which won't happen if the connection is permanently down. This might need refinement for long-lived processes.
					break // Break from retry loop, but the outer loop of handleReconnect continues.
				}
				time.Sleep(rmq.config.ReconnectDelay)
			}
		}
		// If still not ready after attempts, wait before checking notifications again
		// This prevents a tight loop if MaxReconnectAttempts is reached.
		if !rmq.isReady {
			time.Sleep(rmq.config.ReconnectDelay * 2) // Longer sleep if failed all attempts
		}
	}
}
