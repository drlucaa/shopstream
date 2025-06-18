package models

import "time"

// --- Incoming Event ---

// OrderProduct represents a product within an order.
type OrderProduct struct {
	ProductID int64 `json:"productId"`
	Quantity  int   `json:"quantity"`
}

// OrderCreatedEvent is the event consumed by the inventory service.
type OrderCreatedEvent struct {
	EventID   string         `json:"eventId"`
	OrderID   string         `json:"orderId"`
	UserID    string         `json:"userId"`
	Products  []OrderProduct `json:"products"`
	Timestamp time.Time      `json:"timestamp"`
}

// --- Database Model ---

// ProductStock represents the product stock data in the database.
type ProductStock struct {
	ID        int64 `db:"id"`
	ProductID int64 `db:"product_id"`
	Quantity  int   `db:"quantity"`
}

// --- Outgoing Event ---

// CheckedProduct represents a product after inventory check.
type CheckedProduct struct {
	ProductID      int64 `json:"productId"`
	Quantity       int   `json:"quantity"`
	StockAvailable bool  `json:"stockAvailable"`
}

// InventoryCheckedEvent is the event published by the inventory service.
type InventoryCheckedEvent struct {
	EventID       string           `json:"eventId"`
	CorrelationID string           `json:"correlationId"` // ID of the original OrderCreatedEvent
	OrderID       string           `json:"orderId"`
	UserID        string           `json:"userId"`
	Products      []CheckedProduct `json:"products"`
	Timestamp     time.Time        `json:"timestamp"`
}
