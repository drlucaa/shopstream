package processor

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/drluca/shopstream/inventoryservice/internal/database"
	"github.com/drluca/shopstream/inventoryservice/internal/eventbus"
	"github.com/drluca/shopstream/inventoryservice/internal/models"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"
	"go.trai.ch/gecro/config"
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
		return eventbus.ErrPermanentFailure
	}

	checkedProducts := make([]models.CheckedProduct, 0, len(orderEvent.Products))
	allAvailable := true

	// In a real scenario, you'd use a transaction here
	for _, product := range orderEvent.Products {
		err := p.db.UpdateStock(ctx, product.ProductID, product.Quantity)
		if err != nil {
			log.Warn().Err(err).Int64("productId", product.ProductID).Msg("Failed to update stock")
			allAvailable = false
			// TODO: Implement compensating transaction to roll back previous stock updates
			break
		}
		checkedProducts = append(checkedProducts, models.CheckedProduct{
			ProductID:      product.ProductID,
			Quantity:       product.Quantity,
			StockAvailable: true,
		})
	}

	if !allAvailable {
		// Publish an inventory.check.failed event
		log.Error().Str("orderId", orderEvent.OrderID).Msg("Inventory check failed, some items not available")
		// For now, we just NACK the message. In a real system, you'd publish a specific failure event.
		return errors.New("inventory check failed")
	}

	// All stock updated, publish the success event
	inventoryEvent := models.InventoryCheckedEvent{
		EventID:       uuid.New().String(),
		CorrelationID: orderEvent.EventID,
		OrderID:       orderEvent.OrderID,
		UserID:        orderEvent.UserID,
		Products:      checkedProducts,
		Timestamp:     time.Now(),
	}

	routingKey := p.cfg.OutgoingTopic // "inventory.checked"
	if err := p.bus.PublishMessage(ctx, routingKey, inventoryEvent); err != nil {
		log.Error().Err(err).Msg("Failed to publish InventoryCheckedEvent")
		// TODO: Implement retry logic or compensating transaction
		return err
	}

	log.Info().Str("orderId", orderEvent.OrderID).Msg("Successfully processed inventory and published inventory.checked event")
	return nil
}
