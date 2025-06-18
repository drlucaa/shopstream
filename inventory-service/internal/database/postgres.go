package database

import (
	"context"
	"fmt"

	"github.com/drluca/shopstream/inventoryservice/config"
	"github.com/drluca/shopstream/inventoryservice/internal/models"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type DB struct {
	SQL *sqlx.DB
}

func New(cfg config.Config) (*DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName, cfg.DBSSLMode)
	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	db.SetMaxOpenConns(10)
	return &DB{SQL: db}, nil
}

// GetStockByProductID fetches product stock from the database by product ID.
func (db *DB) GetStockByProductID(ctx context.Context, productID int64) (models.ProductStock, error) {
	var stock models.ProductStock
	query := `SELECT id, product_id, quantity FROM product_stock WHERE product_id=$1`
	err := db.SQL.GetContext(ctx, &stock, query, productID)
	return stock, err
}

// UpdateStock decreases the stock quantity for a given product ID.
func (db *DB) UpdateStock(ctx context.Context, productID int64, quantityToDecrement int) error {
	query := `UPDATE product_stock SET quantity = quantity - $1 WHERE product_id = $2 AND quantity >= $1`
	result, err := db.SQL.ExecContext(ctx, query, quantityToDecrement, productID)
	if err != nil {
		return fmt.Errorf("error updating stock for product %d: %w", productID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("error getting rows affected for product %d: %w", productID, err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("insufficient stock or product not found for ID %d", productID)
	}

	return nil
}

func (db *DB) Close() {
	db.SQL.Close()
}
