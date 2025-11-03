-- Create database if it doesn't exist
SELECT 'CREATE DATABASE binance_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'binance_db')\gexec

\c binance_db;

-- Drop and recreate debezium user to ensure correct password
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'debezium') THEN
        REVOKE ALL PRIVILEGES ON DATABASE binance_db FROM debezium;
        DROP ROLE debezium;
    END IF;
    CREATE USER debezium WITH REPLICATION LOGIN PASSWORD 'debezium';
END $$;

-- Grant necessary permissions to debezium user
GRANT CONNECT ON DATABASE binance_db TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;

-- Create crypto_24h_stats table if it doesn't exist
CREATE TABLE IF NOT EXISTS crypto_24h_stats (
    symbol VARCHAR(20) PRIMARY KEY,
    price_change_percent DOUBLE PRECISION,
    last_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    quote_volume DOUBLE PRECISION,
    open_time TIMESTAMP,
    close_time TIMESTAMP,
    updated_at TIMESTAMP
);

-- Grant permissions on tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO debezium;

-- Future privileges for new tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

-- Enable logical replication
ALTER SYSTEM SET wal_level = logical;
SELECT pg_reload_conf();
