package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v4"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"time"
)

type MessageHandler func(context.Context, Message) error

type MessageOptions struct {
	Schedule *time.Time `json:"schedule,omitempty"`
}

// Message to be inserted to the outbox, Destination indicate what is the messaging system the message should be
// sent to, Route is optional Topic or Event stream to be used in the mentioned Destination
type Message struct {
	Destination Destination   `json:"destination"`
	Route       Route         `json:"route"`
	Payload     proto.Message `json:"payload"`
	Options     []byte        `json:"options"`
}

// WithOption places custom option on the message Options field
func WithOption[T any](m *Message, key string, value T) error {
	optionsJson := map[string]interface{}{}
	if m.Options != nil {
		if err := json.Unmarshal(m.Options, &optionsJson); err != nil {
			return fmt.Errorf("failed to unmarshal the message existing options: %w", err)
		}
	}

	optionsJson[key] = value

	updatedOptions, err := json.Marshal(optionsJson)
	if err != nil {
		return fmt.Errorf("failed to marshal updated message options: %w", err)
	}

	m.Options = updatedOptions
	return nil
}

// GetOption retrives custom option from the message Options field
func GetOption[T any](m *Message, key string) (T, bool) {
	var result T

	optionsMap := map[string]json.RawMessage{}
	if m.Options != nil {
		if err := json.Unmarshal(m.Options, &optionsMap); err != nil {
			return result, false
		}
	}

	raw, ok := optionsMap[key]
	if !ok {
		return result, false
	}

	if err := json.Unmarshal(raw, &result); err != nil {
		return result, false
	}

	return result, true
}

// message private type for marshal and unmarshalling to storage medium
type message struct {
	Message
	Id                 string     `json:"id"`
	PayloadType        string     `json:"@type"`
	DeliveriesAttempts int        `json:"deliveriesAttempts"`
	CreatedAt          time.Time  `json:"createdAt"`
	SentAt             time.Time  `json:"sentAt"`
	NextDelivery       *time.Time `json:"nextDelivery"`
}

func (e *message) UnmarshalJSON(raw []byte) error {
	type Alias message

	ex := struct {
		*Alias
		Payload []byte `json:"payload"`
	}{
		Alias: (*Alias)(e),
	}

	err := json.Unmarshal(raw, &ex)
	if err != nil {
		return err
	}

	pbType, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(ex.PayloadType))
	if err != nil {
		return fmt.Errorf("failed to find event message name by event type: %s", ex.PayloadType)
	}
	msg := pbType.New().Interface()
	err = protojson.Unmarshal([]byte(ex.Payload), msg)
	if err != nil {
		return err
	}

	e.Payload = msg
	return nil
}

func (e *message) MarshalJSON() ([]byte, error) {
	type Alias message

	payload, err := protojson.Marshal(e.Payload)
	if err != nil {
		return payload, err
	}

	return json.Marshal(&struct {
		*Alias
		PayloadType string `json:"@type"`
		Payload     []byte `json:"payload"`
	}{
		Payload:     payload,
		Alias:       (*Alias)(e),
		PayloadType: string(e.Payload.ProtoReflect().Descriptor().FullName()),
	})
}

type EnqueueOption func(*options)

type options struct {
	tx                   pgx.Tx
	messageMaxDeliveries uint32
	messageRetryMaxDelay time.Duration
}

func WithTransaction(tx pgx.Tx) EnqueueOption {
	return func(o *options) {
		o.tx = tx
	}
}

func WithMessageMaxDeliveries(messageMaxDeliveries uint32) EnqueueOption {
	return func(o *options) {
		o.messageMaxDeliveries = messageMaxDeliveries
	}
}

func WithMessageRetryMaxDelay(retryMaxDelay time.Duration) EnqueueOption {
	return func(o *options) {
		o.messageRetryMaxDelay = retryMaxDelay
	}
}
