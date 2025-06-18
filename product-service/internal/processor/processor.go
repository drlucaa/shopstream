package processor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/drluca/shopstream/productservice/config"
	"github.com/drluca/shopstream/productservice/internal/contracts"
	"github.com/drluca/shopstream/productservice/internal/database"
	"github.com/drluca/shopstream/productservice/internal/eventbus"
	"github.com/drluca/shopstream/productservice/internal/models"
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

// MessageHandler now returns errors, including the special contracts.ErrPermanentFailure
func (p *Processor) MessageHandler(ctx context.Context, delivery amqp.Delivery) error {
	log.Info().Msg("Received inventory.checked event")

	var inventoryEvent models.InventoryCheckedEvent
	if err := json.Unmarshal(delivery.Body, &inventoryEvent); err != nil {
		log.Error().Err(err).Msg("Failed to unmarshal InventoryCheckedEvent, this is a permanent failure.")
		return contracts.ErrPermanentFailure // Return the error from the contracts package
	}

	detailedProducts := make([]models.DetailedProduct, 0, len(inventoryEvent.Products))
	var totalPrice float64 = 0

	for _, product := range inventoryEvent.Products {
		if !product.StockAvailable {
			continue
		}

		dbProduct, err := p.db.GetProductByID(ctx, product.ProductID)
		if err != nil {
			log.Error().Err(err).Int64("productId", product.ProductID).Msg("Failed to fetch product details from DB. This is a transient error.")
			// Return a normal error to trigger a retry.
			return err
		}

		detailedProducts = append(detailedProducts, models.DetailedProduct{
			ProductID: dbProduct.ID,
			Quantity:  product.Quantity,
			Name:      dbProduct.Name,
			Price:     dbProduct.Price,
		})
		totalPrice += dbProduct.Price * float64(product.Quantity)
	}

	productEvent := models.ProductDetailsFetchedEvent{
		EventID:       uuid.New().String(),
		CorrelationID: inventoryEvent.EventID,
		OrderID:       inventoryEvent.OrderID,
		UserID:        inventoryEvent.UserID,
		Products:      detailedProducts,
		TotalPrice:    totalPrice,
		Timestamp:     time.Now(),
	}

	routingKey := p.cfg.OutgoingTopic
	if err := p.bus.PublishMessage(ctx, routingKey, productEvent); err != nil {
		log.Error().Err(err).Msg("Failed to publish ProductDetailsFetchedEvent. This is a transient error.")
		return err // Return a normal error to trigger a retry.
	}

	log.Info().Str("orderId", productEvent.OrderID).Msg("Successfully processed product details and published event")
	return nil
}
