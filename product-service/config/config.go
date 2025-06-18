package config

import (
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

// Config stores all configuration of the application.
// The values are read by viper from a config file or environment variable.
type Config struct {
	AppName string `mapstructure:"APP_NAME"`

	// PostgreSQL configuration
	DBHost     string `mapstructure:"DB_HOST"`
	DBPort     int    `mapstructure:"DB_PORT"`
	DBUser     string `mapstructure:"DB_USER"`
	DBPassword string `mapstructure:"DB_PASSWORD"`
	DBName     string `mapstructure:"DB_NAME"`
	DBSchema   string `mapstructure:"DB_SCHEMA"`   // e.g. "public"
	DBSSLMode  string `mapstructure:"DB_SSL_MODE"` // "disable", "require", "verify-full"

	// RabbitMQ configuration
	RabbitMQURL            string        `mapstructure:"RABBITMQ_URL"`
	IncomingExchangeName   string        `mapstructure:"INCOMING_EXCHANGE_NAME"`
	IncomingQueueName      string        `mapstructure:"INCOMING_QUEUE_NAME"`
	IncomingRoutingKey     string        `mapstructure:"INCOMING_ROUTING_KEY"`
	OutgoingExchangeName   string        `mapstructure:"OUTGOING_EXCHANGE_NAME"`
	OutgoingTopic          string        `mapstructure:"OUTGOING_TOPIC"` // This will be used as routing key
	ConsumerTag            string        `mapstructure:"CONSUMER_TAG"`
	ReconnectDelay         time.Duration `mapstructure:"RECONNECT_DELAY"`
	MaxReconnectAttempts   int           `mapstructure:"MAX_RECONNECT_ATTEMPTS"`
	RabbitMQPrefetchCount  int           `mapstructure:"RABBITMQ_PREFETCH_COUNT"` // How many messages to fetch at a time
	RabbitMQExchangeType   string        `mapstructure:"RABBITMQ_EXCHANGE_TYPE"`  // e.g., "direct", "topic", "fanout"
	OutgoingExchangeType   string        `mapstructure:"OUTGOING_EXCHANGE_TYPE"`  // e.g., "direct", "topic", "fanout"
	DLXName                string        `mapstructure:"DLX_NAME"`                // Dead Letter Exchange Name
	DLQRoutingKey          string        `mapstructure:"DLQ_ROUTING_KEY"`         // Dead Letter Queue Routing Key
	ParkingLotExchangeName string        `mapstructure:"PARKING_LOT_EXCHANGE_NAME"`
	ParkingLotQueueName    string        `mapstructure:"PARKING_LOT_QUEUE_NAME"`
	ParkingLotRoutingKey   string        `mapstructure:"PARKING_LOT_ROUTING_KEY"`
	MaxProcessingRetries   int           `mapstructure:"MAX_PROCESSING_RETRIES"` // Max retries for message processing before sending to DLQ/Parking Lot

	// Application settings
	LogLevel string `mapstructure:"LOG_LEVEL"` // e.g., "debug", "info", "warn", "error"
}

// LoadConfig reads configuration from file or environment variables.
func LoadConfig(path string) (config Config, err error) {
	viper.AddConfigPath(path)  // Path to look for the config file in
	viper.SetConfigName("app") // Name of config file (without extension)
	viper.SetConfigType("env") // TELL VIPER TO LOAD ENV VARS, CAN BE JSON, YAML etc.

	viper.AutomaticEnv() // Read in environment variables that match

	// Set default values
	viper.SetDefault("APP_NAME", "product-service")
	viper.SetDefault("LOG_LEVEL", "info")

	viper.SetDefault("DB_HOST", "localhost")
	viper.SetDefault("DB_PORT", 5432)
	viper.SetDefault("DB_USER", "postgres")
	viper.SetDefault("DB_PASSWORD", "password")
	viper.SetDefault("DB_NAME", "mydatabase")
	viper.SetDefault("DB_SCHEMA", "public")
	viper.SetDefault("DB_SSL_MODE", "disable")

	viper.SetDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	viper.SetDefault("INCOMING_EXCHANGE_NAME", "events.input")
	viper.SetDefault("INCOMING_QUEUE_NAME", "user_events_queue")
	viper.SetDefault("INCOMING_ROUTING_KEY", "user.created") // Example routing key
	viper.SetDefault("RABBITMQ_EXCHANGE_TYPE", "topic")      // Default to topic exchange
	viper.SetDefault("OUTGOING_EXCHANGE_NAME", "events.output")
	viper.SetDefault("OUTGOING_TOPIC", "user.processed") // Example output topic
	viper.SetDefault("OUTGOING_EXCHANGE_TYPE", "topic")  // Default to topic exchange for output
	viper.SetDefault("CONSUMER_TAG", "user-processor-consumer")
	viper.SetDefault("RECONNECT_DELAY", 5*time.Second)
	viper.SetDefault("MAX_RECONNECT_ATTEMPTS", 5)
	viper.SetDefault("RABBITMQ_PREFETCH_COUNT", 10)
	viper.SetDefault("DLX_NAME", "dlx.user_events")
	viper.SetDefault("DLQ_ROUTING_KEY", "dlq.user_events_queue")
	viper.SetDefault("PARKING_LOT_EXCHANGE_NAME", "parking_lot.user_events")
	viper.SetDefault("PARKING_LOT_QUEUE_NAME", "parking_lot_user_events_queue")
	viper.SetDefault("PARKING_LOT_ROUTING_KEY", "parking_lot.user_events_queue")
	viper.SetDefault("MAX_PROCESSING_RETRIES", 3)

	// If a config file is found, read it in.
	if err = viper.ReadInConfig(); err == nil {
		log.Info().Str("file", viper.ConfigFileUsed()).Msg("Using config file")
	} else if _, ok := err.(viper.ConfigFileNotFoundError); ok {
		log.Info().Msg("No config file found, using environment variables and defaults.")
	} else {
		// Config file was found but another error was produced
		log.Error().Err(err).Msg("Error reading config file")
		return
	}

	// Viper settings for environment variables
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	err = viper.Unmarshal(&config)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to decode into struct")
	}

	return
}
