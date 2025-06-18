package config

import (
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type Config struct {
	AppName string `mapstructure:"APP_NAME"`

	// PostgreSQL configuration
	DBHost     string `mapstructure:"DB_HOST"`
	DBPort     int    `mapstructure:"DB_PORT"`
	DBUser     string `mapstructure:"DB_USER"`
	DBPassword string `mapstructure:"DB_PASSWORD"`
	DBName     string `mapstructure:"DB_NAME"`
	DBSSLMode  string `mapstructure:"DB_SSL_MODE"`

	// RabbitMQ configuration
	RabbitMQURL          string `mapstructure:"RABBITMQ_URL"`
	IncomingExchangeName string `mapstructure:"INCOMING_EXCHANGE_NAME"`
	IncomingQueueName    string `mapstructure:"INCOMING_QUEUE_NAME"`
	IncomingRoutingKey   string `mapstructure:"INCOMING_ROUTING_KEY"`
	OutgoingExchangeName string `mapstructure:"OUTGOING_EXCHANGE_NAME"`
	OutgoingTopic        string `mapstructure:"OUTGOING_TOPIC"`
	ConsumerTag          string `mapstructure:"CONSUMER_TAG"`
	// ... (other rabbitmq fields)
}

func LoadConfig(path string) (config Config, err error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("app")
	viper.SetConfigType("env")

	viper.AutomaticEnv()

	// Set default values for inventory-service
	viper.SetDefault("APP_NAME", "inventory-service")
	viper.SetDefault("LOG_LEVEL", "info")

	viper.SetDefault("DB_HOST", "localhost")
	viper.SetDefault("DB_PORT", 54322)
	viper.SetDefault("DB_USER", "inventoryuser")
	viper.SetDefault("DB_PASSWORD", "inventorypassword")
	viper.SetDefault("DB_NAME", "inventory_db")
	viper.SetDefault("DB_SSL_MODE", "disable")

	viper.SetDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	viper.SetDefault("INCOMING_EXCHANGE_NAME", "events.inventory")
	viper.SetDefault("INCOMING_QUEUE_NAME", "inventory_events_queue")
	viper.SetDefault("INCOMING_ROUTING_KEY", "inventory.check")
	viper.SetDefault("OUTGOING_EXCHANGE_NAME", "events.inventory.processed")
	viper.SetDefault("OUTGOING_TOPIC", "inventory.processed")
	viper.SetDefault("CONSUMER_TAG", "inventory-processor-consumer")
	// ... (other defaults)

	if err = viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Info().Msg("No config file found, using environment variables and defaults.")
		} else {
			log.Error().Err(err).Msg("Error reading config file")
			return
		}
	}

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	err = viper.Unmarshal(&config)
	return
}
