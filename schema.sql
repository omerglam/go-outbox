CREATE SCHEMA IF NOT EXISTS {{.Schema}};

CREATE TABLE IF NOT EXISTS {{.Schema}}.pkg_outbox (
    id VARCHAR(255) PRIMARY KEY,
    destination VARCHAR(255),
    type VARCHAR(255),
    payload JSONB,
    status VARCHAR(50),
    route VARCHAR,
    delivery_attempts INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    sent_at TIMESTAMPTZ,
    next_delivery TIMESTAMPTZ,
    options jsonb
    );

CREATE TABLE IF NOT EXISTS {{.Schema}}.pkg_outbox_dead_letter (
    id VARCHAR(255) PRIMARY KEY,
    destination VARCHAR(255),
    type VARCHAR(255),
    payload JSONB,
    status VARCHAR(50),
    route VARCHAR,
    delivery_attempts INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    sent_at TIMESTAMPTZ,
    next_delivery TIMESTAMPTZ,
    options jsonb
    );

-- Create a function to notify of new inserts into the outbox table
CREATE OR REPLACE FUNCTION {{.Schema}}.notify_outbox_change()
RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('outbox_changes', NEW.id::text);
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a trigger that will invoke the notify_outbox_change function
-- after a new row is inserted into the outbox table
DO $$
BEGIN
    -- Check if the trigger already exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE NOT tgisinternal -- Exclude internally created triggers
          AND tgname = 'outbox_insert_trigger'
          AND tgrelid = '{{.Schema}}.pkg_outbox'::regclass
    ) THEN
-- Trigger does not exist, so create it
CREATE TRIGGER outbox_insert_trigger
    AFTER INSERT ON {{.Schema}}.pkg_outbox
    FOR EACH ROW EXECUTE FUNCTION {{.Schema}}.notify_outbox_change();
ELSE
        -- Optional: output a notice if the trigger already exists
        RAISE NOTICE 'Trigger already exists';
END IF;
END$$;



-- Add route column
DO $$
BEGIN
    -- Check if the column does not exist in the table
    IF NOT EXISTS (
        SELECT FROM information_schema.columns
        WHERE table_schema = '{{.Schema}}'
        AND table_name   = 'pkg_outbox'
        AND column_name  = 'route'
    ) THEN
        -- Perform the ALTER TABLE operation
ALTER TABLE {{.Schema}}.pkg_outbox ADD COLUMN route VARCHAR;
ELSE
        RAISE NOTICE 'Column already exists';
END IF;
END$$;

DO $$
BEGIN
    -- Check if the column does not exist in the table
    IF NOT EXISTS (
        SELECT FROM information_schema.columns
        WHERE table_schema = '{{.Schema}}'
        AND table_name   = 'pkg_outbox_dead_letter'
        AND column_name  = 'route'
    ) THEN
        -- Perform the ALTER TABLE operation
ALTER TABLE {{.Schema}}.pkg_outbox_dead_letter ADD COLUMN route VARCHAR;
ELSE
        RAISE NOTICE 'Column already exists dead letter';
END IF;
END$$;


-- Add next delivery column
DO $$
BEGIN
    -- Check if the column does not exist in the table
    IF NOT EXISTS (
        SELECT FROM information_schema.columns
        WHERE table_schema = '{{.Schema}}'
        AND table_name   = 'pkg_outbox'
        AND column_name  = 'next_delivery'
    ) THEN
        -- Perform the ALTER TABLE operation
ALTER TABLE {{.Schema}}.pkg_outbox ADD COLUMN next_delivery TIMESTAMPTZ;
ELSE
        RAISE NOTICE 'Column next_delivery already exists';
END IF;
END$$;

DO $$
BEGIN
    -- Check if the column does not exist in the table
    IF NOT EXISTS (
        SELECT FROM information_schema.columns
        WHERE table_schema = '{{.Schema}}'
        AND table_name   = 'pkg_outbox_dead_letter'
        AND column_name  = 'next_delivery'
    ) THEN
        -- Perform the ALTER TABLE operation
ALTER TABLE {{.Schema}}.pkg_outbox_dead_letter ADD COLUMN next_delivery TIMESTAMPTZ;
ELSE
        RAISE NOTICE 'Column next_delivery already exists dead letter';
END IF;
END$$;

-- Add options column
DO $$
BEGIN
    -- Check if the column does not exist in the table
    IF NOT EXISTS (
        SELECT FROM information_schema.columns
        WHERE table_schema = '{{.Schema}}'
        AND table_name   = 'pkg_outbox'
        AND column_name  = 'options'
    ) THEN
        -- Perform the ALTER TABLE operation
ALTER TABLE {{.Schema}}.pkg_outbox ADD COLUMN options VARCHAR;
ELSE
        RAISE NOTICE 'options column already exists';
END IF;
END$$;

DO $$
BEGIN
    -- Check if the column does not exist in the table
    IF NOT EXISTS (
        SELECT FROM information_schema.columns
        WHERE table_schema = '{{.Schema}}'
        AND table_name   = 'pkg_outbox_dead_letter'
        AND column_name  = 'options'
    ) THEN
        -- Perform the ALTER TABLE operation
ALTER TABLE {{.Schema}}.pkg_outbox_dead_letter ADD COLUMN options VARCHAR;
ELSE
        RAISE NOTICE 'options column already exists dead letter';
END IF;
END$$;





