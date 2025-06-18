package models

import "time"

// --- Incoming Event ---

// CheckedProduct represents a product from the inventory check event.
type CheckedProduct struct {
	ProductID      int64 `json:"productId"`
	Quantity       int   `json:"quantity"`
	StockAvailable bool  `json:"stockAvailable"`
}

// InventoryCheckedEvent is the event consumed by the product service.
type InventoryCheckedEvent struct {
	EventID       string           `json:"eventId"`
	CorrelationID string           `json:"correlationId"`
	OrderID       string           `json:"orderId"`
	UserID        string           `json:"userId"`
	Products      []CheckedProduct `json:"products"`
	Timestamp     time.Time        `json:"timestamp"`
}

// --- Database Model ---

// Product represents product data in the database.
type Product struct {
	ID    int64   `db:"id"`
	Name  string  `db:"name"`
	Price float64 `db:"price"`
}

// --- Outgoing Event ---

// DetailedProduct represents a product with full details.
type DetailedProduct struct {
	ProductID int64   `json:"productId"`
	Quantity  int     `json:"quantity"`
	Name      string  `json:"name"`
	Price     float64 `json:"price"`
}

// ProductDetailsFetchedEvent is the event published by the product service.
type ProductDetailsFetchedEvent struct {
	EventID       string            `json:"eventId"`
	CorrelationID string            `json:"correlationId"`
	OrderID       string            `json:"orderId"`
	UserID        string            `json:"userId"`
	Products      []DetailedProduct `json:"products"`
	TotalPrice    float64           `json:"totalPrice"`
	Timestamp     time.Time         `json:"timestamp"`
}
