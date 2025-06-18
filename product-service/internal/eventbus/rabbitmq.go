package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/drluca/shopstream/productservice/config"
	"github.com/drluca/shopstream/productservice/internal/models"

	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"
)

const (
	// For publisher confirms
	publishTimeout = 5 * time.Second
)

// RabbitMQManager handles RabbitMQ connections, channels, and operations.
type RabbitMQManager struct {
	config          config.Config
	connection      *amqp.Connection
	consumerChan    *amqp.Channel
	producerChan    *amqp.Channel
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation // For publisher confirms
	isReady         bool
	isConnecting    bool          // To prevent concurrent reconnection attempts
	connectMutex    chan struct{} // Mutex for connect/reconnect logic
}

// MessageHandler is a function type that processes a received amqp.Delivery
// It should return an error if processing fails and the message should be nacked/requeued.
// It can also return a special error `ErrPermanentFailure` to indicate message should go to DLQ/Parking Lot.
type MessageHandler func(ctx context.Context, delivery amqp.Delivery) error

// ErrPermanentFailure is a special error to indicate a message cannot be processed and should not be retried indefinitely.
var ErrPermanentFailure = errors.New("permanent failure processing message")

// NewRabbitMQManager creates a new RabbitMQManager.
func NewRabbitMQManager(cfg config.Config) (*RabbitMQManager, error) {
	rmq := &RabbitMQManager{
		config:       cfg,
		connectMutex: make(chan struct{}, 1), // Buffered channel of size 1 as a mutex
	}
	rmq.connectMutex <- struct{}{} // Acquire mutex initially

	if err := rmq.connect(); err != nil {
		// Start reconnection goroutine even if initial connect fails
		go rmq.handleReconnect()
		return nil, fmt.Errorf("initial RabbitMQ connection failed: %w. Will attempt to reconnect", err)
	}
	// If connect was successful, start monitoring.
	// If it failed, handleReconnect will try to establish it.
	go rmq.handleReconnect()
	return rmq, nil
}

func (rmq *RabbitMQManager) connect() error {
	if rmq.isConnecting {
		log.Warn().Msg("RabbitMQ connection attempt already in progress.")
		return errors.New("connection attempt in progress")
	}
	rmq.isConnecting = true
	defer func() { rmq.isConnecting = false }()

	<-rmq.connectMutex                                // Wait to acquire lock
	defer func() { rmq.connectMutex <- struct{}{} }() // Release lock

	log.Info().Str("url", rmq.config.RabbitMQURL).Msg("Attempting to connect to RabbitMQ")
	conn, err := amqp.Dial(rmq.config.RabbitMQURL)
	if err != nil {
		return fmt.Errorf("failed to dial RabbitMQ: %w", err)
	}
	rmq.connection = conn
	rmq.notifyConnClose = make(chan *amqp.Error)
	rmq.connection.NotifyClose(rmq.notifyConnClose)

	// Setup Producer Channel
	if err := rmq.setupProducerChannel(); err != nil {
		return fmt.Errorf("failed to setup producer channel: %w", err)
	}

	// Setup Consumer Channel (and related topology)
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

	// Enable publisher confirms on this channel
	if err := rmq.producerChan.Confirm(false); err != nil {
		return fmt.Errorf("producer channel could not be put into confirm mode: %w", err)
	}
	rmq.notifyConfirm = make(chan amqp.Confirmation, 1)
	rmq.producerChan.NotifyPublish(rmq.notifyConfirm)

	log.Info().Str("exchange", rmq.config.OutgoingExchangeName).Str("type", rmq.config.OutgoingExchangeType).Msg("Declaring outgoing exchange")
	err = rmq.producerChan.ExchangeDeclare(
		rmq.config.OutgoingExchangeName, // name
		rmq.config.OutgoingExchangeType, // type
		true,                            // durable
		false,                           // auto-deleted
		false,                           // internal
		false,                           // no-wait
		nil,                             // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare outgoing exchange %s: %w", rmq.config.OutgoingExchangeName, err)
	}
	log.Info().Str("exchange", rmq.config.OutgoingExchangeName).Msg("Outgoing exchange declared successfully")
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

	// Set prefetch count (QoS)
	if err := rmq.consumerChan.Qos(
		rmq.config.RabbitMQPrefetchCount, // prefetchCount
		0,                                // prefetchSize
		false,                            // global - false means per consumer
	); err != nil {
		return fmt.Errorf("failed to set QoS on consumer channel: %w", err)
	}

	// Declare Dead Letter Exchange (DLX)
	log.Info().Str("dlx_exchange", rmq.config.DLXName).Msg("Declaring Dead Letter Exchange (DLX)")
	err = rmq.consumerChan.ExchangeDeclare(
		rmq.config.DLXName, // name
		"direct",           // type (DLXs are often direct)
		true,               // durable
		false,              // auto-deleted
		false,              // internal
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare DLX %s: %w", rmq.config.DLXName, err)
	}

	// Declare Dead Letter Queue (DLQ) and bind it to DLX
	// Messages in DLQ will use the original routing key or a specific one if defined
	dlqArgs := amqp.Table{
		// Optionally, set TTL for messages in DLQ to re-route them back or to another queue
		// "x-message-ttl": int32(3600000), // e.g., 1 hour in milliseconds
		// "x-dead-letter-exchange": "some-other-exchange-to-retry", // To re-route from DLQ
	}
	log.Info().Str("dlq_name", rmq.config.IncomingQueueName+".dlq").Msg("Declaring Dead Letter Queue (DLQ)")
	_, err = rmq.consumerChan.QueueDeclare(
		rmq.config.IncomingQueueName+".dlq", // name (e.g., original_queue.dlq)
		true,                                // durable
		false,                               // delete when unused
		false,                               // exclusive
		false,                               // no-wait
		dlqArgs,                             // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare DLQ %s: %w", rmq.config.IncomingQueueName+".dlq", err)
	}
	log.Info().Str("dlq_name", rmq.config.IncomingQueueName+".dlq").Str("dlx_exchange", rmq.config.DLXName).Str("routing_key", rmq.config.DLQRoutingKey).Msg("Binding DLQ to DLX")
	err = rmq.consumerChan.QueueBind(
		rmq.config.IncomingQueueName+".dlq", // queue name
		rmq.config.DLQRoutingKey,            // routing key for DLQ (can be specific or # for all)
		rmq.config.DLXName,                  // exchange name (DLX)
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind DLQ %s to DLX %s: %w", rmq.config.IncomingQueueName+".dlq", rmq.config.DLXName, err)
	}

	// Declare Parking Lot Exchange
	log.Info().Str("exchange", rmq.config.ParkingLotExchangeName).Msg("Declaring Parking Lot Exchange")
	err = rmq.consumerChan.ExchangeDeclare(
		rmq.config.ParkingLotExchangeName, // name
		"direct",                          // type
		true,                              // durable
		false,                             // auto-deleted
		false,                             // internal
		false,                             // no-wait
		nil,                               // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare parking lot exchange %s: %w", rmq.config.ParkingLotExchangeName, err)
	}

	// Declare Parking Lot Queue and bind it
	log.Info().Str("queue", rmq.config.ParkingLotQueueName).Msg("Declaring Parking Lot Queue")
	_, err = rmq.consumerChan.QueueDeclare(
		rmq.config.ParkingLotQueueName, // name
		true,                           // durable
		false,                          // delete when unused
		false,                          // exclusive
		false,                          // no-wait
		nil,                            // arguments for parking lot queue
	)
	if err != nil {
		return fmt.Errorf("failed to declare parking lot queue %s: %w", rmq.config.ParkingLotQueueName, err)
	}

	log.Info().Str("queue", rmq.config.ParkingLotQueueName).Str("exchange", rmq.config.ParkingLotExchangeName).Str("routing_key", rmq.config.ParkingLotRoutingKey).Msg("Binding Parking Lot Queue")
	err = rmq.consumerChan.QueueBind(
		rmq.config.ParkingLotQueueName,    // queue name
		rmq.config.ParkingLotRoutingKey,   // routing key
		rmq.config.ParkingLotExchangeName, // exchange name
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind parking lot queue %s: %w", rmq.config.ParkingLotQueueName, err)
	}

	// Declare the main incoming exchange
	log.Info().Str("exchange", rmq.config.IncomingExchangeName).Str("type", rmq.config.RabbitMQExchangeType).Msg("Declaring incoming exchange")
	err = rmq.consumerChan.ExchangeDeclare(
		rmq.config.IncomingExchangeName, // name
		rmq.config.RabbitMQExchangeType, // type
		true,                            // durable
		false,                           // auto-deleted
		false,                           // internal
		false,                           // no-wait
		nil,                             // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare incoming exchange %s: %w", rmq.config.IncomingExchangeName, err)
	}

	// Declare the main incoming queue with DLX arguments
	queueArgs := amqp.Table{
		"x-dead-letter-exchange":    rmq.config.DLXName,
		"x-dead-letter-routing-key": rmq.config.DLQRoutingKey, // Optional: routing key for messages going to DLX
	}
	log.Info().Str("queue", rmq.config.IncomingQueueName).Msg("Declaring incoming queue")
	_, err = rmq.consumerChan.QueueDeclare(
		rmq.config.IncomingQueueName, // name
		true,                         // durable
		false,                        // delete when unused
		false,                        // exclusive
		false,                        // no-wait
		queueArgs,                    // arguments with DLX
	)
	if err != nil {
		return fmt.Errorf("failed to declare incoming queue %s: %w", rmq.config.IncomingQueueName, err)
	}

	// Bind the incoming queue to the incoming exchange
	log.Info().Str("queue", rmq.config.IncomingQueueName).Str("exchange", rmq.config.IncomingExchangeName).Str("key", rmq.config.IncomingRoutingKey).Msg("Binding incoming queue to exchange")
	err = rmq.consumerChan.QueueBind(
		rmq.config.IncomingQueueName,    // queue name
		rmq.config.IncomingRoutingKey,   // routing key
		rmq.config.IncomingExchangeName, // exchange name
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind incoming queue %s with key %s to exchange %s: %w",
			rmq.config.IncomingQueueName, rmq.config.IncomingRoutingKey, rmq.config.IncomingExchangeName, err)
	}
	log.Info().Str("queue", rmq.config.IncomingQueueName).Msg("Incoming queue declared and bound successfully")
	return nil
}

// PublishMessage sends a message to the configured outgoing exchange and topic.
func (rmq *RabbitMQManager) PublishMessage(ctx context.Context, message models.EnrichedMessage) error {
	if !rmq.isReady || rmq.producerChan == nil {
		log.Error().Msg("RabbitMQ producer not ready or channel is nil. Cannot publish message.")
		return errors.New("producer not ready")
	}

	body, err := json.Marshal(message)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal outgoing message to JSON")
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	log.Debug().Str("exchange", rmq.config.OutgoingExchangeName).Str("topic", rmq.config.OutgoingTopic).RawJSON("body", body).Msg("Publishing message")

	err = rmq.producerChan.Publish(
		rmq.config.OutgoingExchangeName, // exchange
		rmq.config.OutgoingTopic,        // routing key (topic)
		false,                           // mandatory
		false,                           // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // Make message persistent
			Body:         body,
			Timestamp:    time.Now(),
		},
	)
	if err != nil {
		log.Error().Err(err).Msg("Failed to publish message")
		// Consider if this error means connection is lost, trigger reconnect logic or count failures
		return fmt.Errorf("failed to publish message: %w", err)
	}

	// Wait for confirmation
	select {
	case confirm := <-rmq.notifyConfirm:
		if confirm.Ack {
			log.Debug().Uint64("tag", confirm.DeliveryTag).Msg("Message published and confirmed")
			return nil
		}
		log.Error().Uint64("tag", confirm.DeliveryTag).Msg("Message published but not confirmed (Nacked by broker)")
		return errors.New("message published but not confirmed by broker")
	case <-time.After(publishTimeout):
		log.Warn().Msg("Publish confirmation timeout")
		// This is tricky. Message might have been delivered or not.
		// Application needs to decide how to handle: retry, log, alert.
		// For now, we return an error. Could also attempt to republish or store for later.
		return errors.New("publish confirmation timeout")
	}
}

// StartConsuming consumes messages from the configured queue and passes them to the handler.
func (rmq *RabbitMQManager) StartConsuming(ctx context.Context, handler MessageHandler) error {
	if !rmq.isReady || rmq.consumerChan == nil {
		return errors.New("RabbitMQ consumer not ready or channel is nil")
	}

	msgs, err := rmq.consumerChan.Consume(
		rmq.config.IncomingQueueName, // queue
		rmq.config.ConsumerTag,       // consumer tag
		false,                        // auto-ack (false means we manually ack/nack)
		false,                        // exclusive
		false,                        // no-local
		false,                        // no-wait
		nil,                          // args
	)
	if err != nil {
		log.Error().Err(err).Str("queue", rmq.config.IncomingQueueName).Msg("Failed to register a consumer")
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	log.Info().Str("queue", rmq.config.IncomingQueueName).Str("tag", rmq.config.ConsumerTag).Msg("Consumer started, waiting for messages...")

	go func() {
		for {
			select {
			case <-ctx.Done(): // If the context is cancelled, stop consuming
				log.Info().Msg("Context cancelled, stopping consumer.")
				// Attempt to gracefully close the channel, connection will be closed by Close()
				if rmq.consumerChan != nil {
					// Note: Closing the channel will stop the range over msgs
					// but it's good practice to handle it gracefully.
					// The connection itself should be closed by the main Close() method.
					// rmq.consumerChan.Close() // This might be too abrupt.
				}
				return
			case delivery, ok := <-msgs:
				if !ok {
					log.Warn().Msg("Delivery channel was closed. Attempting to re-establish consumer.")
					// This usually means the channel or connection was closed.
					// Reconnect logic should handle re-establishing the consumer.
					// We might need to break this loop and let handleReconnect re-initiate StartConsuming
					rmq.isReady = false // Mark as not ready to trigger reconnect
					// If the channel is closed, the loop will break.
					// handleReconnect will then attempt to re-establish.
					return // Exit this goroutine, handleReconnect will take over.
				}
				log.Debug().Uint64("tag", delivery.DeliveryTag).Str("correlationId", delivery.CorrelationId).Msg("Received a message")

				// Process message with retries and DLQ/Parking Lot logic
				rmq.processMessageWithRetries(ctx, delivery, handler)
			}
		}
	}()

	return nil
}

func (rmq *RabbitMQManager) processMessageWithRetries(ctx context.Context, delivery amqp.Delivery, handler MessageHandler) {
	var processingErr error
	maxRetries := rmq.config.MaxProcessingRetries
	retryCount := 0

	// Check for x-death header to count previous NACKs if message came from DLQ
	// This basic retry is for application-level errors. RabbitMQ requeues are another level.
	if xDeath, ok := delivery.Headers["x-death"].([]interface{}); ok {
		for _, item := range xDeath {
			if deathEvent, ok := item.(amqp.Table); ok {
				if _, ok := deathEvent["count"].(int64); ok {
					// This count is how many times it was in a DLQ for the *same reason*.
					// We are implementing application-level retries here.
					// A more sophisticated approach might distinguish between requeues by RabbitMQ
					// and our application-level retries.
					// For simplicity, let's assume x-death means it's been through a DLQ once.
					// You might need more complex logic if you have multiple DLQs or re-queueing strategies.
					log.Warn().Interface("x-death", xDeath).Msg("Message has x-death header, may have been dead-lettered before.")
					// We could use this count to influence our retryCount, but for now,
					// our internal retry logic is separate.
				}
			}
		}
	}

	for retryCount < maxRetries {
		processingErr = handler(ctx, delivery)
		if processingErr == nil {
			log.Debug().Uint64("deliveryTag", delivery.DeliveryTag).Msg("Message processed successfully, ACKing.")
			if err := delivery.Ack(false); err != nil { // Ack message
				log.Error().Err(err).Uint64("deliveryTag", delivery.DeliveryTag).Msg("Failed to ACK message")
			}
			return // Success
		}

		if errors.Is(processingErr, ErrPermanentFailure) {
			log.Error().Err(processingErr).Uint64("deliveryTag", delivery.DeliveryTag).Msg("Permanent failure processing message. Sending to Parking Lot.")
			if err := rmq.sendToParkingLot(delivery); err != nil {
				log.Error().Err(err).Uint64("deliveryTag", delivery.DeliveryTag).Msg("Failed to send message to Parking Lot. NACKing without requeue (will go to DLX).")
				delivery.Nack(false, false) // Nack, don't requeue (goes to DLX as per queue config)
			} else {
				delivery.Ack(false) // Ack original after sending to parking lot
			}
			return // Sent to Parking Lot
		}

		// For transient errors, log and prepare for retry
		log.Warn().Err(processingErr).Uint64("deliveryTag", delivery.DeliveryTag).Int("attempt", retryCount+1).Int("maxRetries", maxRetries).Msg("Transient error processing message. Will retry if attempts remain.")
		retryCount++
		if retryCount < maxRetries {
			// Optional: add a delay before retrying, though RabbitMQ requeue often handles this
			time.Sleep(time.Second * time.Duration(retryCount*2)) // Exponential backoff example
		}
	}

	// Max retries exceeded for transient errors
	log.Error().Err(processingErr).Uint64("deliveryTag", delivery.DeliveryTag).Int("retries", maxRetries).Msg("Max processing retries exceeded. NACKing message to send to DLX/DLQ.")
	if err := delivery.Nack(false, false); err != nil { // Nack, don't requeue (will go to DLX)
		log.Error().Err(err).Uint64("deliveryTag", delivery.DeliveryTag).Msg("Failed to NACK message after max retries")
	}
}

// sendToParkingLot explicitly sends a message to the parking lot exchange.
func (rmq *RabbitMQManager) sendToParkingLot(originalDelivery amqp.Delivery) error {
	if !rmq.isReady || rmq.producerChan == nil {
		log.Error().Msg("RabbitMQ producer not ready. Cannot send to parking lot.")
		return errors.New("producer not ready for parking lot")
	}

	log.Info().Uint64("originalDeliveryTag", originalDelivery.DeliveryTag).Msg("Sending message to parking lot")

	// Preserve original headers and add parking lot specific ones if needed
	headers := originalDelivery.Headers
	if headers == nil {
		headers = amqp.Table{}
	}
	headers["x-parking-lot-reason"] = "permanent_failure_or_max_retries"
	headers["x-original-exchange"] = originalDelivery.Exchange
	headers["x-original-routing-key"] = originalDelivery.RoutingKey

	err := rmq.producerChan.Publish(
		rmq.config.ParkingLotExchangeName, // parking lot exchange
		rmq.config.ParkingLotRoutingKey,   // parking lot routing key
		false,                             // mandatory
		false,                             // immediate
		amqp.Publishing{
			ContentType:   originalDelivery.ContentType,
			CorrelationId: originalDelivery.CorrelationId,
			MessageId:     originalDelivery.MessageId,
			Timestamp:     time.Now(), // New timestamp for parking lot event
			DeliveryMode:  amqp.Persistent,
			Body:          originalDelivery.Body,
			Headers:       headers,
		},
	)
	if err != nil {
		log.Error().Err(err).Msg("Failed to publish message to parking lot")
		return fmt.Errorf("failed to publish to parking lot: %w", err)
	}

	// Wait for confirmation for the parking lot message
	select {
	case confirm := <-rmq.notifyConfirm:
		if confirm.Ack {
			log.Info().Msg("Message successfully sent to parking lot and confirmed.")
			return nil
		}
		log.Error().Msg("Message sent to parking lot but NACKed by broker.")
		return errors.New("parking lot publish NACKed")
	case <-time.After(publishTimeout): // Use the same publish timeout
		log.Warn().Msg("Parking lot publish confirmation timeout.")
		return errors.New("parking lot publish confirmation timeout")
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

// Close gracefully shuts down the RabbitMQ connection and channels.
func (rmq *RabbitMQManager) Close() {
	log.Info().Msg("Closing RabbitMQ manager...")
	rmq.isReady = false // Stop any operations

	// Stop the reconnect handler by closing its notification channels
	// Check for nil before closing to prevent panic if not initialized
	if rmq.notifyConnClose != nil {
		close(rmq.notifyConnClose)
		rmq.notifyConnClose = nil
	}
	if rmq.notifyChanClose != nil {
		close(rmq.notifyChanClose)
		rmq.notifyChanClose = nil
	}

	if rmq.consumerChan != nil {
		log.Info().Msg("Closing RabbitMQ consumer channel.")
		if err := rmq.consumerChan.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing consumer channel")
		}
		rmq.consumerChan = nil
	}

	if rmq.producerChan != nil {
		log.Info().Msg("Closing RabbitMQ producer channel.")
		// Drain and close notifyConfirm before closing channel
		if rmq.notifyConfirm != nil {
			close(rmq.notifyConfirm)
			rmq.notifyConfirm = nil
		}
		if err := rmq.producerChan.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing producer channel")
		}
		rmq.producerChan = nil
	}

	if rmq.connection != nil && !rmq.connection.IsClosed() {
		log.Info().Msg("Closing RabbitMQ connection.")
		if err := rmq.connection.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing RabbitMQ connection")
		}
		rmq.connection = nil
	}
	log.Info().Msg("RabbitMQ manager closed.")
}

// IsReady checks if the RabbitMQ manager is connected and channels are set up.
func (rmq *RabbitMQManager) IsReady() bool {
	return rmq.isReady && rmq.connection != nil && !rmq.connection.IsClosed() && rmq.producerChan != nil && rmq.consumerChan != nil
}
