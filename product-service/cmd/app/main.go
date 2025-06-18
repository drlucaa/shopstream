package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/drluca/shopstream/productservice/config"
	"github.com/drluca/shopstream/productservice/internal/database"
	"github.com/drluca/shopstream/productservice/internal/eventbus"
	"github.com/drluca/shopstream/productservice/internal/processor"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// Setup structured logging
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// Load configuration
	cfg, err := config.LoadConfig(".")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// Set log level from config
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	log.Info().Str("appName", cfg.AppName).Msg("Application starting")

	// --- Initializations ---

	// Initialize Database
	db, err := database.New(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Database")
	}
	defer db.Close()

	// Initialize RabbitMQ Connection Manager
	rmqManager, err := eventbus.NewRabbitMQManager(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize RabbitMQ Manager")
	}
	defer rmqManager.Close()

	// Initialize the message processor
	msgProcessor := processor.New(db, rmqManager, cfg)

	// Start the consumer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rmqManager.StartConsuming(ctx, msgProcessor.MessageHandler); err != nil {
		log.Fatal().Err(err).Msg("Failed to start consumer")
	}

	log.Info().Msg("Application setup complete. Running and waiting for messages.")
	log.Info().Msg("Press Ctrl+C to exit.")

	// --- Wait for shutdown signal ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// --- Graceful Shutdown ---
	log.Info().Msg("Application shutting down...")
	cancel() // Signal context cancellation to long-running tasks
	// Deferred calls to db.Close() and rmqManager.Close() will execute here.
}
