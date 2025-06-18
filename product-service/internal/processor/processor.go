package processor

import (
	"context"
	"encoding/json"

	"github.com/drluca/shopstream/productservice/internal/database"
	"github.com/drluca/shopstream/productservice/internal/eventbus"
	"github.com/drluca/shopstream/productservice/internal/models"
	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"
)

// Processor handles the core business logic of the service.
type Processor struct {
	db  *database.DB
	bus *eventbus.RabbitMQManager
}

// New creates a new Processor.
func New(db *database.DB, bus *eventbus.RabbitMQManager) *Processor {
	return &Processor{
		db:  db,
		bus: bus,
	}
}

// MessageHandler is the function that processes incoming RabbitMQ messages.
func (p *Processor) MessageHandler(ctx context.Context, delivery amqp.Delivery) error {
	log.Info().RawJSON("body", delivery.Body).Msg("Received message for processing")

	// 1. Decode the incoming message
	var incomingMsg models.IncomingMessage
	if err := json.Unmarshal(delivery.Body, &incomingMsg); err != nil {
		log.Error().Err(err).Msg("Failed to unmarshal incoming message. Sending to parking lot.")
		// This is a permanent failure because the message is malformed.
		return eventbus.ErrPermanentFailure
	}

	// 2. Fetch user data from the database
	user, err := p.db.GetUserByID(ctx, incomingMsg.UserID)
	if err != nil {
		// This could be a transient error (DB down) or a permanent one (user not found).
		// For this example, we'll treat a DB error as transient and let it retry.
		// A more sophisticated implementation might check for a "not found" error
		// and send that to the parking lot.
		log.Warn().Err(err).Int64("user_id", incomingMsg.UserID).Msg("Failed to get user from DB. Will retry.")
		return err // Return error to trigger retry logic in RabbitMQ manager
	}
	log.Info().Interface("user", user).Msg("Successfully fetched user data")

	// 3. Create the enriched message
	enrichedMsg := models.EnrichedMessage{
		EventID:         incomingMsg.EventID,
		UserID:          incomingMsg.UserID,
		UserData:        user,
		OriginalPayload: incomingMsg.Payload,
	}

	// 4. Publish the enriched message to the outgoing exchange
	if err := p.bus.PublishMessage(ctx, enrichedMsg); err != nil {
		log.Error().Err(err).Msg("Failed to publish enriched message. Will retry.")
		return err // Return error to trigger retry
	}

	log.Info().Str("event_id", enrichedMsg.EventID).Msg("Successfully processed and published enriched message.")
	return nil // Return nil for successful processing
}
