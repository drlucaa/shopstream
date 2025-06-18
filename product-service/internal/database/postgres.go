package database

import (
	"context"
	"fmt"
	"time"

	"github.com/drluca/shopstream/productservice/config"
	"github.com/drluca/shopstream/productservice/internal/models"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // The blank import is for the PostgreSQL driver
	"github.com/rs/zerolog/log"
)

// DB represents the database connection pool.
type DB struct {
	SQL *sqlx.DB
}

// New creates a new database connection pool.
func New(cfg config.Config) (*DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName, cfg.DBSSLMode)

	log.Info().Msg("Connecting to product database...")
	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to product database: %w", err)
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	log.Info().Msg("Product database connection successful.")
	return &DB{SQL: db}, nil
}

// Close gracefully closes the database connection.
func (db *DB) Close() {
	log.Info().Msg("Closing product database connection.")
	db.SQL.Close()
}

// GetProductByID fetches a product from the database by its ID.
func (db *DB) GetProductByID(ctx context.Context, productID int64) (models.Product, error) {
	var product models.Product
	// Assuming your table is named 'products' and has columns 'id', 'name', and 'price'.
	query := `SELECT id, name, price FROM products WHERE id=$1`

	err := db.SQL.GetContext(ctx, &product, query, productID)
	if err != nil {
		return models.Product{}, fmt.Errorf("could not get product with ID %d: %w", productID, err)
	}

	return product, nil
}
