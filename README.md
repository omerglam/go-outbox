## go-outbox

![go_outbox](https://github.com/user-attachments/assets/a2094665-2069-41f2-965d-876c7137c53e)


This package provides facilities for implementing the [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html) for reliable message passing 

### Database schema setup

Services which want to utilize the package needs to integrate the schema creation process provided from the package into their own process
for example creating a database migration script to create the required tables:  

```go
func init() {
	goose.AddMigration(upOutbox, downOutbox)
}
func upOutbox(tx *sql.Tx) error {
	schema, err := outbox.GetSchemaSQL("{SCHEMA NAME}")
	if err != nil {
		return err
	}
	log.Println(schema)
	_, err = tx.Exec(schema)
	return err
}

func downOutbox(tx *sql.Tx) error {
	return nil
}

```

This will set up the `pkg_outbox`, `pkg_outbox_dead_letter` tables and function, triggers required for the package to function. 

### Bootstrap

Once database tables are configured the service should bootstrap the Outbox package to initiate the outbox:  

```go

	// Start the outbox, provide the schema name where the Outbox tables where defined 
    // Set up the handlers for each outbox message based on the destination defined on the outbox message, 
	// each outbox message will be routed to delivery based on the configured handlers
    err := outbox.Start(conn, schema, map[outbox.Destination]func(message outbox.Message) error{
	                outbox.EventBus: func(message outbox.Message) error {
	                    return bus.Publish(context.Background(), message.Payload)
	            },
	                outbox.CommandQueue: func(message outbox.Message) error {
	                    return queue.Push(context.Background(), string(message.Route), message.Payload)
	            },
            })
	 
	if err != nil {
		return err
	}
```

### Usage

To utilize the Outbox, use the `Enqueue` function in a database transaction while providing the transaction object to the `Enqueue` function: 


```go

     conn.BeginTxFunc(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		// database operations..
		tx.DbOperation1(...)
		 
		tx.DbOperation2(...)
		 ...
         ...
         ...
		 
		 // enqueue all provided message to the outbox under the same database transaction
		for _, msg := range messages {
			err = outbox.Enqueue(ctx, schema, tx, msg)
			if err != nil {
				return err
			}
		}

		return nil
	})

```



