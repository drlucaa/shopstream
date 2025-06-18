package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/drluca/shopstream/productservice/config"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// Setup structured logging
	// UNIX Time is faster and smaller than most timestamps
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	// Default level is info, unless debug flag is present
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	// Use pretty console logging
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// Load configuration
	cfg, err := config.LoadConfig(".") // Load app.env from the root project directory
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
	case "fatal":
		zerolog.SetGlobalLevel(zerolog.FatalLevel)
	case "panic":
		zerolog.SetGlobalLevel(zerolog.PanicLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	log.Info().Str("appName", cfg.AppName).Msg("Application starting")
	log.Info().Msgf("Log level set to: %s", cfg.LogLevel)

	// --- Placeholder for future initializations ---
	// Initialize Database
	// Initialize RabbitMQ Connections (Consumer & Producer)
	// Initialize Processor
	// Start Consumer

	log.Info().Msg("Application setup complete. (Placeholder for run)")

	// For now, just print some config values to test
	log.Debug().Msgf("DB Host: %s", cfg.DBHost)
	log.Debug().Msgf("RabbitMQ URL: %s", cfg.RabbitMQURL)
	log.Debug().Msgf("Incoming Queue: %s", cfg.IncomingQueueName)
	log.Debug().Msgf("Outgoing Topic: %s", cfg.OutgoingTopic)

	// Keep the main goroutine alive (until we have the actual consumer running)
	// select {} // or use a signal handler for graceful shutdown later
	fmt.Println("Application started. Press Ctrl+C to exit.")
	// Example: Wait for a signal to gracefully shut down
	// quit := make(chan os.Signal, 1)
	// signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	// <-quit
	// log.Info().Msg("Application shutting down...")
	// Add cleanup code here (e.g., close DB connections, RabbitMQ channels)
}
