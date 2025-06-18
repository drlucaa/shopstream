package processor

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/drluca/shopstream/inventoryservice/config"
	"github.com/drluca/shopstream/inventoryservice/internal/database"
	"github.com/drluca/shopstream/inventoryservice/internal/eventbus"
	"github.com/drluca/shopstream/inventoryservice/internal/models"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"
)

type Processor struct {
	db  *database.DB
	bus *eventbus.RabbitMQManager
	cfg config.Config
}

func New(db *database.DB, bus *eventbus.RabbitMQManager, cfg config.Config) *Processor {
	return &Processor{db: db, bus: bus, cfg: cfg}
}

func (p *Processor) MessageHandler(ctx context.Context, delivery amqp.Delivery) error {
	log.Info().Msg("Received order.created event")

	var orderEvent models.OrderCreatedEvent
	if err := json.Unmarshal(delivery.Body, &orderEvent); err != nil {
		log.Error().Err(err).Msg("Failed to unmarshal OrderCreatedEvent")
		// This is a permanent failure because the message is malformed.
		return eventbus.ErrPermanentFailure
	}

	checkedProducts := make([]models.CheckedProduct, 0, len(orderEvent.Products))
	allAvailable := true

	// In a real scenario, you'd use a transaction here to ensure all stock updates succeed or fail together.
	for _, product := range orderEvent.Products {
		err := p.db.UpdateStock(ctx, product.ProductID, product.Quantity)
		if err != nil {
			log.Warn().Err(err).Int64("productId", product.ProductID).Msg("Failed to update stock for product.")
			allAvailable = false
			// NOTE: A compensating transaction should be implemented here to roll back
			// any successful stock updates from this order before failing.
			// For this example, we'll break and proceed to publish a failure event.
			break
		}
		// If stock was updated successfully, add it to the list for the outgoing event.
		checkedProducts = append(checkedProducts, models.CheckedProduct{
			ProductID:      product.ProductID,
			Quantity:       product.Quantity,
			StockAvailable: true, // It was available, otherwise UpdateStock would have failed.
		})
	}

	if !allAvailable {
		// Publish an inventory.check.failed event or simply NACK to retry/DLQ.
		log.Error().Str("orderId", orderEvent.OrderID).Msg("Inventory check failed; some items were not available in sufficient quantity.")
		// Returning a normal error will cause the message to be NACK'd and retried or sent to a DLQ,
		// depending on the RabbitMQ handler's logic.
		return errors.New("inventory check failed due to insufficient stock")
	}

	// All stock was available and has been updated. Now, publish the success event.
	inventoryEvent := models.InventoryCheckedEvent{
		EventID:       uuid.New().String(),
		CorrelationID: orderEvent.EventID, // Link this event to the one that triggered it.
		OrderID:       orderEvent.OrderID,
		UserID:        orderEvent.UserID,
		Products:      checkedProducts,
		Timestamp:     time.Now(),
	}

	routingKey := p.cfg.OutgoingTopic // This should be "inventory.checked" from your config.
	if err := p.bus.PublishMessage(ctx, routingKey, inventoryEvent); err != nil {
		log.Error().Err(err).Msg("Failed to publish InventoryCheckedEvent. This is a transient error.")
		// If publishing fails, we should not ACK the original message, so it will be retried.
		// A compensating transaction to revert the stock updates would be ideal here as well.
		return err
	}

	log.Info().Str("orderId", orderEvent.OrderID).Msg("Successfully processed inventory and published inventory.checked event")
	return nil // Return nil on success to ACK the message.
}
