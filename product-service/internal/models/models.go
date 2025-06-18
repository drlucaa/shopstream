package models

import "encoding/json"

// --- Incoming RabbitMQ Message Structures ---

// IncomingMessagePayload represents the "payload" part of the incoming message.
type IncomingMessagePayload struct {
	SomeKey string `json:"someKey"`
}

// IncomingMessage represents the structure of the message received from RabbitMQ.
type IncomingMessage struct {
	EventID string                 `json:"eventId"`
	UserID  int64                  `json:"userId"` // Changed to int64 for DB compatibility if ID is BIGINT
	Payload IncomingMessagePayload `json:"payload"`
}

// --- Database Model ---

// User represents the user data fetched from the database.
type User struct {
	ID    int64  `db:"id" json:"-"` // json:"-" to exclude from direct JSON marshaling if embedded
	Name  string `db:"name" json:"name"`
	Email string `db:"email" json:"email"`
}

// --- Outgoing RabbitMQ Message Structure ---

// EnrichedMessage represents the structure of the message to be published.
type EnrichedMessage struct {
	EventID         string                 `json:"eventId"`
	UserID          int64                  `json:"userId"`
	UserData        User                   `json:"userData"`
	OriginalPayload IncomingMessagePayload `json:"originalPayload"`
}

// --- Helper Methods (Optional but good practice) ---

// ToJSON converts an EnrichedMessage to its JSON representation.
func (em *EnrichedMessage) ToJSON() ([]byte, error) {
	return json.Marshal(em)
}

// FromJSON parses an IncomingMessage from its JSON representation.
func (im *IncomingMessage) FromJSON(data []byte) error {
	return json.Unmarshal(data, im)
}
