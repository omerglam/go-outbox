package outbox

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/gofrs/uuid"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/types/known/anypb"
)

var conn *pgxpool.Pool
var ctx = context.Background()
var handlerCounter = atomic.Int32{}
var failHandlerCounter = atomic.Int32{}
var mu = &sync.Mutex{}
var handlerCondition = sync.NewCond(mu)
var schema string = "public"
var failureHandlerMessageDeliveriesCount = map[string]int{}

func initTestHandlers() map[Destination]MessageHandler {
	return map[Destination]MessageHandler{
		EventBus: func(ctx context.Context, m Message) error {
			handlerCounter.Add(1)
			mu.Lock()
			handlerCondition.Broadcast()
			mu.Unlock()
			return nil
		},
		"failure_handler": func(ctx context.Context, m Message) error {
			var ok bool
			var messageDeliveryCounter int
			if messageDeliveryCounter, ok = failureHandlerMessageDeliveriesCount[string(m.Route)]; !ok {
				messageDeliveryCounter = 0
			}

			messageDeliveryCounter++
			failureHandlerMessageDeliveriesCount[string(m.Route)] = messageDeliveryCounter

			failHandlerCounter.Add(1)
			if messageDeliveryCounter >= 12 {
				mu.Lock()
				handlerCondition.Broadcast()
				mu.Unlock()
				return nil
			}
			return errors.New("something went wrong")
		},
	}
}

func TestMain(t *testing.M) {
	flag.Parse() // required in order to call testing.Short() in TestMain
	if testing.Short() {
		return
	}

	// dbName := "test-db"
	dbUsername := "test"
	dbPassword := "test"
	postgresContainer, err := testcontainers.GenericContainer(context.Background(),
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "postgres:12.4-alpine",
				ExposedPorts: []string{"5432/tcp"},
				Mounts:       testcontainers.ContainerMounts{},
				Env: map[string]string{
					"POSTGRES_USER":     dbUsername,
					"POSTGRES_PASSWORD": dbPassword,
				},
				WaitingFor: wait.ForAll(wait.ForExposedPort(), wait.ForLog("database system is ready to accept connections")),
			},
			Started: true,
		})
	if err != nil {
		log.Fatal("postgres test container creation: ", err)
	}

	defer func() {
		_ = postgresContainer.Terminate(context.Background())
	}()

	endpoint, err := postgresContainer.Endpoint(context.Background(), "")
	if err != nil {
		fmt.Printf("Got endpoint error: %v\n", err)
		panic(err)
	}

	dbConnStr := fmt.Sprintf("postgresql://%s:%s@%s", dbUsername, dbPassword, endpoint)
	connPoolCfg, err := pgxpool.ParseConfig(dbConnStr)
	if err != nil {
		log.Fatal("db connection string: ", err)
	}

	conn, err = pgxpool.ConnectConfig(context.Background(), connPoolCfg)
	if err != nil {
		log.Fatal("test db pool creation: ", err)
	}
	// create outbox tables
	schemaScript, _ := GetSchemaSQL(schema)
	_, err = conn.Exec(context.Background(), schemaScript)
	if err != nil {
		log.Fatal("failed test table creation: ", err)
	}

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS test_table (
		id SERIAL PRIMARY KEY
	);
	`

	_, err = conn.Exec(context.Background(), createTableSQL)
	if err != nil {
		log.Fatal("failed test table creation: ", err)
	}

	// use sync.Cond to randvouze between the outboxser event handler and the assertion of the event sending in the test
	// mu.Lock()
	err = Start(conn, schema, initTestHandlers())

	if err != nil {
		log.Fatal("outbox init: ", err)
	}

	t.Run()
}

func TestEnqueueWithTransaction_Rollback(t *testing.T) {
	tx, err := conn.Begin(context.Background())
	assert.NoError(t, err)

	// Insert a row into test_table
	_, err = tx.Exec(context.Background(), "INSERT INTO test_table DEFAULT VALUES")
	assert.NoError(t, err)

	err = Enqueue(ctx, schema, Message{Destination: EventBus, Payload: &anypb.Any{}}, WithTransaction(tx))
	assert.NoError(t, err)

	// Simulate a failure here and roll back the transaction
	err = tx.Rollback(context.Background())
	assert.NoError(t, err)

	// Verify that the table is empty
	count, err := getTableCount(t, "test_table")
	assert.NoError(t, err)
	assert.Equal(t, 0, count, "The table should be empty after rollback")

}

func TestEnqueueWithTransaction_RollbackTxFunc(t *testing.T) {
	truncateTestTable(t)

	err := conn.BeginTxFunc(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		_, err := tx.Exec(context.Background(), "INSERT INTO test_table DEFAULT VALUES")
		assert.NoError(t, err)

		err = Enqueue(ctx, schema, Message{Destination: EventBus, Payload: &anypb.Any{}}, WithTransaction(tx))
		assert.NoError(t, err)

		return errors.New("fake error")
	})
	assert.Error(t, err)

	// Verify that the table is empty
	count, err := getTableCount(t, "test_table")
	assert.NoError(t, err)
	assert.Equal(t, 0, count, "The table should be empty after rollback")
}

func TestEnqueueWithTransaction_Success(t *testing.T) {
	counter := handlerCounter.Load()
	var outboxCountPreTest int
	outboxCountPreTest, err := getTableCount(t, "pkg_outbox")
	assert.NoError(t, err)

	truncateTestTable(t)

	err = conn.BeginTxFunc(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		_, err := tx.Exec(context.Background(), "INSERT INTO test_table DEFAULT VALUES")
		assert.NoError(t, err)

		err = Enqueue(ctx, schema, Message{Destination: EventBus, Payload: &anypb.Any{}}, WithTransaction(tx))
		assert.NoError(t, err)
		return nil
	})

	count, err := getTableCount(t, "test_table")
	assert.NoError(t, err)
	assert.Equal(t, 1, count, "The table should be non empty after tx commit")

	outboxCountPostTest, err := getTableCount(t, "pkg_outbox")
	assert.NoError(t, err)
	assert.Equal(t, outboxCountPreTest+1, outboxCountPostTest, "outbox table should grow by 1")

	// wait for the message to be handled in the outbox handler
	mu.Lock()
	handlerCondition.Wait()
	mu.Unlock()
	assert.Equal(t, counter+1, handlerCounter.Load(), "expected one messages sent")
}

func TestEnqueueWithoutTransaction_Success(t *testing.T) {
	counter := handlerCounter.Load()
	outboxCountPreTest, err := getTableCount(t, "pkg_outbox")
	assert.NoError(t, err)

	err = Enqueue(ctx, schema, Message{Destination: EventBus, Payload: &anypb.Any{}})
	assert.NoError(t, err)

	outboxCountPostTest, err := getTableCount(t, "pkg_outbox")
	assert.NoError(t, err)
	assert.Equal(t, outboxCountPreTest+1, outboxCountPostTest, "outbox table should grow by 1")

	// wait for the message to be handled in the outbox handler
	mu.Lock()
	handlerCondition.Wait()
	mu.Unlock()
	assert.Equal(t, counter+1, handlerCounter.Load(), "expected one messages sent")

}

func truncateTestTable(t *testing.T) {
	truncateTable(t, "test_table")
}

func truncateTable(t *testing.T, tableName string) bool {
	t.Helper()
	_, err := conn.Exec(context.Background(), fmt.Sprintf("TRUNCATE TABLE %s", tableName))
	return assert.NoError(t, err)
}

func insertTestMessageToTable(t *testing.T, tableName string, messageID uuid.UUID, deliveryAttempts int) bool {
	t.Helper()
	_, err := conn.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (id, delivery_attempts) VALUES ($1, $2)`, tableName), messageID, deliveryAttempts)

	return assert.NoError(t, err, "failed to insert message into table", tableName)
}

func getTableCount(t *testing.T, tableName string) (int, error) {
	t.Helper()
	var count int
	err := conn.QueryRow(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	return count, err
}

func doesMessageExist(t *testing.T, tableName string, messageID uuid.UUID) (bool, error) {
	t.Helper()
	var exists bool
	err := conn.QueryRow(context.Background(),
		fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE id = $1)", tableName), messageID).
		Scan(&exists)
	return exists, err
}

func TestDeadLetter_Move(t *testing.T) {
	messageID, _ := uuid.NewV4()
	pgob := newOutboxer(conn, schema, initTestHandlers())

	if !insertTestMessageToTable(t, "pkg_outbox", messageID, 1) {
		return
	}
	defer truncateTable(t, "pkg_outbox")

	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Error("failed to begin transaction,", err)
		return
	}

	defer truncateTable(t, "pkg_outbox_dead_letter")
	if err := pgob.moveToDeadLetter(tx, messageID.String()); !assert.NoError(t, err, "failed to move message to dead letter") {
		_ = tx.Rollback(context.Background())
		return
	}

	err = tx.Commit(context.Background())
	assert.NoError(t, err, "failed to commit transaction")

	exists, err := doesMessageExist(t, "pkg_outbox_dead_letter", messageID)
	assert.NoError(t, err, "failed to query dead letter table")
	assert.True(t, exists, "message should exist in dead letter table")

	exists, err = doesMessageExist(t, "pkg_outbox", messageID)
	assert.NoError(t, err, "failed to query outbox table")
	assert.False(t, exists, "message should exist in outbox table")
}

func preventInsertOfMessage(t *testing.T, tableName string, messageID uuid.UUID) (success bool) {
	q := fmt.Sprintf(`
CREATE FUNCTION prevent_writes_on_specific_row() RETURNS trigger AS $$
BEGIN
    IF NEW.id = '%s' THEN
        RAISE EXCEPTION 'Writes to this row (id=%s) are not allowed!';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER block_writes_on_specific_row
BEFORE INSERT OR UPDATE OR DELETE ON %s
FOR EACH ROW EXECUTE FUNCTION prevent_writes_on_specific_row();
`, messageID, messageID, tableName)
	_, err := conn.Exec(context.Background(), q)
	return assert.NoError(t, err, "failed to create trigger")
}

func deletePreventInsertOfMessageTrigger(t *testing.T, tableName string) (success bool) {
	q := fmt.Sprintf(`
DROP TRIGGER block_writes_on_specific_row ON %s;
DROP FUNCTION prevent_writes_on_specific_row();
`, tableName)
	_, err := conn.Exec(context.Background(), q)
	return assert.NoError(t, err, "failed to drop trigger")
}

func TestDeadLetter_Move_Rollback(t *testing.T) {
	messageID, _ := uuid.NewV4()
	pgob := newOutboxer(conn, schema, initTestHandlers())

	if !insertTestMessageToTable(t, "pkg_outbox", messageID, 1) {
		return
	}
	defer truncateTable(t, "pkg_outbox")
	if !preventInsertOfMessage(t, "pkg_outbox_dead_letter", messageID) {
		return
	}
	defer deletePreventInsertOfMessageTrigger(t, "pkg_outbox_dead_letter")

	tx, err := conn.Begin(context.Background())
	if !assert.NoError(t, err, "failed to begin transaction,", err) {
		return
	}

	if err := pgob.moveToDeadLetter(tx, messageID.String()); !assert.Error(t, err, "should fail moving message to dead letter") {
		_ = tx.Rollback(context.Background())
	}

	err = tx.Commit(context.Background())
	assert.Error(t, err, "should fail to commit transaction")

	exists, err := doesMessageExist(t, "pkg_outbox_dead_letter", messageID)
	assert.NoError(t, err, "failed to query dead letter table")
	assert.False(t, exists, "message should exist in dead letter table")

	exists, err = doesMessageExist(t, "pkg_outbox", messageID)
	assert.NoError(t, err, "failed to query outbox table")
	assert.True(t, exists, "message should exist in outbox table")
}

func TestEnqueueWithMaxDeliveries_Success(t *testing.T) {
	deliveries := 12

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)

	// Set this as the global tracer provider for the test
	otel.SetTracerProvider(tp)

	sctx, span := otel.Tracer("test-tracer").Start(context.Background(), "my-test-span")
	span.End()

	err := Enqueue(sctx, schema, Message{Destination: "failure_handler", Payload: &anypb.Any{}},
		WithMessageRetryMaxDelay(30*time.Second),
		WithMessageMaxDeliveries(uint32(deliveries)))
	if err != nil {
		t.Error(err)
	}

	done := make(chan struct{})

	go func() {
		select {
		case <-time.After(time.Duration(int64(deliveries)) * (32 * time.Second)):
			t.Error("test timed out waiting for handlerCondition")
		case <-done:
		}
	}()

	// wait for the message to be handled in the outbox handler
	mu.Lock()
	handlerCondition.Wait()
	close(done)
	mu.Unlock()

	assert.Equal(t, int32(deliveries), failHandlerCounter.Load(), "expected one messages sent")
}
