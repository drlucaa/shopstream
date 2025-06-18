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

	log.Info().Msg("Connecting to database...")
	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	log.Info().Msg("Database connection successful.")
	return &DB{SQL: db}, nil
}

// Close gracefully closes the database connection.
func (db *DB) Close() {
	log.Info().Msg("Closing database connection.")
	db.SQL.Close()
}

// GetUserByID fetches a user from the database by their ID.
func (db *DB) GetUserByID(ctx context.Context, userID int64) (models.User, error) {
	var user models.User
	query := `SELECT id, name, email FROM users WHERE id=$1` // Assuming your table is named 'users'

	err := db.SQL.GetContext(ctx, &user, query, userID)
	if err != nil {
		return models.User{}, fmt.Errorf("could not get user with ID %d: %w", userID, err)
	}

	return user, nil
}
