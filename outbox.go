package outbox

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

var (
	meter                = otel.Meter("outbox")
	latencyMetric        metric.Int64Histogram
	deadLetterSizeMetric metric.Int64Counter
	schemaOutboxers      sync.Map
)

type Destination string
type Route string

const (
	EventBus     Destination = "event_bus"
	CommandQueue Destination = "command_queue"
	Mailman      Destination = "mailman"

	EventsRoute Route = "events"
)

func Enqueue(ctx context.Context, schema string, msg Message, enqueueOptions ...EnqueueOption) error {
	if value, ok := schemaOutboxers.Load(schema); ok {
		o := value.(*pgOutbox)
		return o.enqueue(ctx, msg, enqueueOptions...)
	}
	return fmt.Errorf("outboxer for schema %v not found", schema)
}

func Start(connPool *pgxpool.Pool, schema string, handlers map[Destination]MessageHandler, opts ...Option) error {
	pgob := newOutboxer(connPool, schema, handlers, opts...)
	if _, loaded := schemaOutboxers.LoadOrStore(schema, pgob); loaded {
		return fmt.Errorf("outboxer for schema %v already started", schema)
	}

	return pgob.start()
}

func init() {
	var err error
	latencyMetric, err = meter.Int64Histogram("message.delivery.latency.milliseconds",
		metric.WithDescription("Latency of message deliveries from receivedAt to sentAt"))
	if err != nil {
		latencyMetric, _ = noop.Meter{}.Int64Histogram("noOp")
		log.WithError(err).Warn("failed to init outbox latency metric")
	}

	deadLetterSizeMetric, err = meter.Int64Counter("message.dead_letter.size",
		metric.WithDescription("Size of dead letter queue"))
	if err != nil {
		deadLetterSizeMetric, _ = noop.Meter{}.Int64Counter("noOp")
		log.WithError(err).Warn("failed to init outbox dead letter size metric")
	}
}
