package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"math"
	"math/rand"
	"text/template"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	_ "embed"
)

//go:embed schema.sql
var schemaTemplate string

const outbox_msg_option_trace_key = "outbox_msg_trace_key"
const outbox_msg_option_max_delivery_key = "outbox_msg_max_delivery"
const outbox_msg_option_max_retry_delay_key = "outbox_msg_option_max_retry_delay"

func GetSchemaSQL(schemaName string) (string, error) {
	type TemplateData struct {
		Schema string
	}

	tmpl, err := template.New("outbox").Parse(schemaTemplate)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, TemplateData{Schema: schemaName})
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

type Option func(*pgOutbox)

func WithMaxDeliveriesAttempts(attempts int) Option {
	return func(o *pgOutbox) {
		o.maxDeliveriesAttempts = attempts
	}
}

func WithIntervalCheck(interval time.Duration) Option {
	return func(o *pgOutbox) {
		o.intervalCheck = interval
	}
}

type pgOutbox struct {
	connPool              *pgxpool.Pool
	schema                string
	handlers              map[Destination]MessageHandler
	signalChannel         chan struct{} // Channel to signal new messages
	intervalCheck         time.Duration // Interval time for regular checks
	maxDeliveriesAttempts int           // Interval time for regular checks
}

func newOutboxer(connPool *pgxpool.Pool, schema string, handlers map[Destination]MessageHandler, opts ...Option) *pgOutbox {
	outbox := &pgOutbox{
		connPool:              connPool,
		handlers:              handlers,
		schema:                schema,
		signalChannel:         make(chan struct{}, 1),
		intervalCheck:         time.Second * 3, // default
		maxDeliveriesAttempts: 10,              // default
	}

	for _, opt := range opts {
		opt(outbox)
	}

	return outbox
}

// Start outbox pgx processing
func (o *pgOutbox) start() error {
	err := o.checkOutboxTableExists(o.schema, o.connPool)
	if err != nil {
		return err
	}

	o.startCleanupRoutine(o.connPool)

	go func() {
		for {

			connection, err := o.connPool.Acquire(context.Background())
			if err != nil {
				log.WithError(err).Error("failed to acquire connection")
				<-time.After(time.Millisecond * 500)
				continue
			}

			_, err = connection.Exec(context.Background(), "LISTEN outbox_changes")
			if err != nil {
				connection.Release()
				log.WithError(err).Error("failed to listen on outbox changes")
				<-time.After(time.Millisecond * 500)
				continue
			}

			for {
				notif, err := connection.Conn().WaitForNotification(context.Background())
				if err != nil {
					log.WithError(err).Error("error waiting for outbox notification")
					<-time.After(time.Millisecond * 500)
					break
				}

				if notif != nil {
					o.signalChannel <- struct{}{}
				}
			}
			connection.Release()
		}

	}()

	// TODO: make processing goroutine bounded by pool or prefetch
	go func() {
		for {
			select {
			case <-o.signalChannel:
				o.processOutbox()
			case <-time.After(o.intervalCheck):
				o.processOutbox()
			}
		}
	}()

	return nil
}

func (o *pgOutbox) enqueue(ctx context.Context, msg Message, enqueueOptions ...EnqueueOption) error {
	op := &options{}
	for _, opt := range enqueueOptions {
		opt(op)
	}

	enrichMessageWithTrace(ctx, &msg)

	if op.messageMaxDeliveries != 0 {
		err := WithOption(&msg, outbox_msg_option_max_delivery_key, op.messageMaxDeliveries)
		if err != nil {
			return fmt.Errorf("failed to add message max_deliveries option: %w", err)
		}
	}

	if op.messageRetryMaxDelay != time.Duration(0) {
		err := WithOption(&msg, outbox_msg_option_max_retry_delay_key, op.messageRetryMaxDelay)
		if err != nil {
			return fmt.Errorf("failed to add message max_retry_delay option: %w", err)
		}
	}

	id, err := uuid.NewV4()
	if err != nil {
		return err
	}
	outboxMsg := message{Message: msg}
	outboxMsg.Id = id.String()
	now := time.Now().UTC()
	outboxMsg.CreatedAt = now
	outboxMsg.NextDelivery = &now
	payloadType := string(outboxMsg.Payload.ProtoReflect().Descriptor().FullName())

	payload, err := protojson.Marshal(outboxMsg.Payload)
	if err != nil {
		return err
	}

	if op.tx != nil {
		_, err = op.tx.Exec(ctx,
			fmt.Sprintf("INSERT INTO %s.pkg_outbox (id, type, destination, route, payload, delivery_attempts, created_at, next_delivery, options) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", o.schema),
			outboxMsg.Id, payloadType, outboxMsg.Destination, outboxMsg.Route, payload, outboxMsg.DeliveriesAttempts, time.Now().UTC(), time.Now().UTC(), outboxMsg.Options)
	} else {
		_, err = o.connPool.Exec(ctx,
			fmt.Sprintf("INSERT INTO %s.pkg_outbox (id, type, destination, route, payload, delivery_attempts, created_at, next_delivery, options) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", o.schema),
			outboxMsg.Id, payloadType, outboxMsg.Destination, outboxMsg.Route, payload, outboxMsg.DeliveriesAttempts, time.Now().UTC(), time.Now().UTC(), outboxMsg.Options)
	}

	return err
}

// enrichMessageWithTrace enrich message options with trace info, if provided in context ( best effort)
func enrichMessageWithTrace(ctx context.Context, msg *Message) {
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		_ = WithOption(msg, outbox_msg_option_trace_key, sc.TraceID().String())
	}
}

func (o *pgOutbox) processOutbox() {
	count, err := o.countNonLockedRows()
	if err != nil {
		log.WithError(err).Error("failed to process outbox")
		return
	}

	for i := 0; i < count; i++ {
		go o.processOutboxMessage()
	}
}

func (o *pgOutbox) processOutboxMessage() {
	err := o.connPool.BeginTxFunc(context.Background(), pgx.TxOptions{}, func(tx pgx.Tx) error {

		// read with lock (for update) limited amount of messages, order by delivery attempts (lowest number of deliveries first)
		// add random for the query to minimize lock contention on same rows if multiple processes exists
		rows, err := tx.Query(context.Background(),
			fmt.Sprintf("SELECT id, type, destination, route, delivery_attempts, payload, created_at, next_delivery, options "+
				"FROM %s.pkg_outbox "+
				"WHERE sent_at IS NULL AND next_delivery <= $1 "+
				"order by next_delivery, delivery_attempts asc "+
				"LIMIT 1 FOR UPDATE SKIP LOCKED", o.schema), time.Now().UTC())
		if err != nil {
			return err
		}
		defer rows.Close()

		messages := make([]message, 0, 10)
		for rows.Next() {
			var msg message
			var payload []byte
			if err := rows.Scan(&msg.Id, &msg.PayloadType, &msg.Destination, &msg.Route, &msg.DeliveriesAttempts, &payload, &msg.CreatedAt, &msg.NextDelivery, &msg.Options); err != nil {
				log.WithError(err).WithField("messageId", msg.Id).Error("failed to scan rows to outbox message")
				if msg.Id != "" {
					nextDelivery := calculateNextDeliveryWithJitter(time.Millisecond*500, &msg, msg.DeliveriesAttempts+1, 0.1)
					_, err = tx.Exec(context.Background(),
						fmt.Sprintf("UPDATE %s.pkg_outbox SET delivery_attempts = delivery_attempts+1, next_delivery = $1 where id = $2", o.schema), nextDelivery, msg.Id)
					if err != nil {
						log.WithError(err).WithFields(log.Fields{"messageId": msg.Id}).
							Errorf("failed to update message deliveires on outbox for failed scan")
					}
				}
				continue
			}

			pbType, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(msg.PayloadType))
			if err != nil {
				log.Errorf("failed to find event message name by event type: %s", msg.PayloadType)
				err = o.moveToDeadLetter(tx, msg.Id)
				if err != nil {
					log.WithError(err).WithField("messageId", msg.Id).Error("failed to move message to dead letter")
				}
				continue
			}
			protoPayload := pbType.New().Interface()
			err = protojson.Unmarshal([]byte(payload), protoPayload)
			if err != nil {
				log.WithError(err).Error(err)
				err = o.moveToDeadLetter(tx, msg.Id)
				if err != nil {
					log.WithError(err).WithField("messageId", msg.Id).Error("failed to move message to dead letter")
				}
				continue
			}

			msg.Payload = protoPayload

			messages = append(messages, msg)
		}

		for _, msg := range messages {
			maxDeliveries := o.maxDeliveriesAttempts
			if optionMaxDelivery, ok := GetOption[int](&msg.Message, outbox_msg_option_max_delivery_key); ok {
				maxDeliveries = optionMaxDelivery
			}

			if msg.DeliveriesAttempts > maxDeliveries {
				err = o.moveToDeadLetter(tx, msg.Id)
				if err != nil {
					log.WithError(err).WithField("messageId", msg.Id).Error("failed to move message to dead letter")
				}
				continue
			}

			handler, ok := o.handlers[msg.Destination]
			if !ok {
				log.Errorf("No handler for destination: %s", msg.Destination)
				err = o.moveToDeadLetter(tx, msg.Id)
				if err != nil {
					log.WithError(err).WithField("messageId", msg.Id).Error("failed to move missing handler message to dead letter")
				}
				continue
			}

			// extract trace information if exists
			ctx := extractTracingContext(msg.Message)

			err = handler(ctx, msg.Message)
			if err != nil {
				nextDelivery := calculateNextDeliveryWithJitter(time.Millisecond*500, &msg, msg.DeliveriesAttempts, 0.1)
				log.WithError(err).WithFields(log.Fields{"messageId": msg.Id, "nextDelivery": nextDelivery}).Warn("failed to send message")
				_, err = tx.Exec(context.Background(),
					fmt.Sprintf("UPDATE %s.pkg_outbox SET delivery_attempts = $1, next_delivery = $2 where id = $3", o.schema),
					msg.DeliveriesAttempts+1, nextDelivery, msg.Id)
				if err != nil {
					log.WithError(err).WithFields(log.Fields{"messageId": msg.Id}).
						Errorf("failed to update message deliveires on outbox")
				}
				continue
			}

			_, err = tx.Exec(context.Background(),
				fmt.Sprintf("UPDATE %s.pkg_outbox SET sent_at = $1, delivery_attempts = $2 WHERE id = $3", o.schema),
				time.Now().UTC(), msg.DeliveriesAttempts+1, msg.Id)
			if err != nil {
				log.WithError(err).WithFields(log.Fields{"messageId": msg.Id}).
					Errorf("failed to update message sent time on outbox")
			}

			latencyMetric.Record(context.Background(), int64(msg.SentAt.Sub(msg.CreatedAt)/time.Millisecond))
		}
		return nil
	})
	if err != nil {
		log.WithError(err).Error("failed processing outbox")
	}
}

func extractTracingContext(msg Message) context.Context {
	ctx := context.Background()
	if len(msg.Options) > 0 {
		optionsJson := map[string]interface{}{}
		err := json.Unmarshal(msg.Options, &optionsJson)
		if err == nil {
			traceIdStr, ok := GetOption[string](&msg, outbox_msg_option_trace_key)
			if ok {
				traceID, err := trace.TraceIDFromHex(traceIdStr)
				if err == nil {
					spanContext := trace.NewSpanContext(trace.SpanContextConfig{
						TraceID:    traceID,
						SpanID:     trace.SpanID{},
						TraceFlags: trace.FlagsSampled,
						Remote:     true,
					})
					ctx = trace.ContextWithSpanContext(context.Background(), spanContext)
				}
			}
		}
	}
	return ctx
}

func (o *pgOutbox) countNonLockedRows() (int, error) {
	tx, err := o.connPool.Begin(context.Background())
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())

	_, err = tx.Exec(context.Background(),
		fmt.Sprintf("CREATE TEMP TABLE temp_ids AS SELECT id FROM "+
			"%s.pkg_outbox WHERE sent_at IS NULL AND next_delivery <= $1 FOR SHARE SKIP LOCKED", o.schema), time.Now().UTC())
	if err != nil {
		return 0, err
	}

	var count int
	err = tx.QueryRow(context.Background(), "SELECT COUNT(*) FROM temp_ids").Scan(&count)
	if err != nil {
		return 0, err
	}

	_, err = tx.Exec(context.Background(), "DROP TABLE temp_ids")
	if err != nil {
		return 0, err
	}

	err = tx.Commit(context.Background())
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (o *pgOutbox) moveToDeadLetter(tx pgx.Tx, messageID string) error {
	// TODO replace context.Background() with context from caller (WithoutCancel) for tracing
	_, err := tx.Exec(context.Background(), "SAVEPOINT move_to_dead_letter01")
	if err != nil {
		return err
	}

	// Insert into dead_letter table from outbox table
	_, err = tx.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s.pkg_outbox_dead_letter SELECT * FROM %s.pkg_outbox WHERE id = $1`, o.schema, o.schema), messageID)
	if err != nil {
		return err
	}

	// Delete from outbox
	_, err = tx.Exec(context.Background(), fmt.Sprintf(`
		DELETE FROM %s.pkg_outbox WHERE id = $1`, o.schema), messageID)
	if err != nil {
		_, rollbackErr := tx.Exec(context.Background(), "ROLLBACK TO SAVEPOINT move_to_dead_letter01")
		if rollbackErr != nil {
			log.WithError(rollbackErr).Error("failed to rollback outbox dead letter move")
		}
		return err
	}

	_, err = tx.Exec(context.Background(), "RELEASE SAVEPOINT move_to_dead_letter01")
	if err != nil {
		return err
	}

	deadLetterSizeMetric.Add(context.Background(), 1, metric.WithAttributes(attribute.String("schema", o.schema)))

	return nil
}

func (o *pgOutbox) startCleanupRoutine(connPool *pgxpool.Pool) {
	ticker := time.NewTicker(24 * time.Hour)
	go func() {
		for {
			<-ticker.C
			_, err := connPool.Exec(context.Background(), fmt.Sprintf("DELETE FROM %s.pkg_outbox WHERE sent_at IS NOT NULL AND sent_at < NOW() - INTERVAL '24 hours'", o.schema))
			if err != nil {
				log.WithError(err).Error("error cleaning up outbox")
			}
		}
	}()
}

func (o *pgOutbox) checkOutboxTableExists(schemaName string, conn *pgxpool.Pool) error {
	const checkTableExistsQuery = `
	SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE table_schema = $1
		AND table_name = 'pkg_outbox'
	);
	`
	var exists bool
	err := conn.QueryRow(context.Background(), checkTableExistsQuery, schemaName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to execute query to check for outbox table existence: %w", err)
	}

	if !exists {
		return fmt.Errorf("outbox table does not exist")
	}

	return nil
}

func calculateNextDeliveryWithJitter(baseDelay time.Duration, msg *message, attempts int, jitterFraction float64) time.Time {
	if attempts <= 0 {
		return time.Now().UTC()
	}

	// Calculate the exponential backoff
	delay := time.Duration(math.Pow(2, float64(attempts-1))) * baseDelay

	delayUpperBoundary, ok := GetOption[time.Duration](&msg.Message, outbox_msg_option_max_retry_delay_key)
	if ok {
		if delay > delayUpperBoundary {
			delay = delayUpperBoundary
		}
	}

	// Apply jitter: randomize delay between [delay*jitterFraction, delay]
	jitteredDelay := time.Duration(rand.Float64()*float64((1-jitterFraction))*float64(delay)) + time.Duration(jitterFraction*float64(delay))

	return time.Now().UTC().Add(jitteredDelay)
}
