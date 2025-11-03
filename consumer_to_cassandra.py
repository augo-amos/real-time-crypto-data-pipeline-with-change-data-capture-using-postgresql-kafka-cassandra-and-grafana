import os
import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider
from cassandra import OperationTimedOut

load_dotenv()

# Cassandra Configuration
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", 9042))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "binance_keyspace")
CASSANDRA_USERNAME = os.getenv("CASSANDRA_USERNAME", "cassandra")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD", "cassandra")

# Kafka Configuration
KAFKA_BROKER_LOCAL = os.getenv("KAFKA_BROKER_LOCAL", "localhost:9093")
TOPICS = [
    "binance.public.crypto_24h_stats",
    "binance.public.latest_prices",
    "binance.public.order_book",
    "binance.public.recent_trades",
    "binance.public.klines"
]

# Connect to Cassandra (Retry)
print("Connecting to Cassandra...")
for attempt in range(10):
    try:
        auth_provider = PlainTextAuthProvider(
            username=CASSANDRA_USERNAME, password=CASSANDRA_PASSWORD
        )
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)
        session = cluster.connect()
        print("Connected to Cassandra.")
        break
    except (NoHostAvailable, ConnectionRefusedError, OperationTimedOut) as e:
        print(f"Connection attempt {attempt + 1}/10 failed: {e}")
        time.sleep(5)
else:
    raise Exception("Failed to connect to Cassandra after multiple attempts.")

# Create Keyspace & Use
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
""")
session.set_keyspace(CASSANDRA_KEYSPACE)
print(f"Using keyspace: {CASSANDRA_KEYSPACE}")

# Create Tables if Not Exist
TABLES = {
    "crypto_24h_stats": """
        CREATE TABLE IF NOT EXISTS crypto_24h_stats (
            symbol TEXT PRIMARY KEY,
            price_change_percent DOUBLE,
            last_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            volume DOUBLE,
            quote_volume DOUBLE,
            open_time TIMESTAMP,
            close_time TIMESTAMP,
            updated_at TIMESTAMP
        );
    """,
    "latest_prices": """
        CREATE TABLE IF NOT EXISTS latest_prices (
            symbol TEXT PRIMARY KEY,
            price DOUBLE,
            updated_at TIMESTAMP
        );
    """,
    "order_book": """
        CREATE TABLE IF NOT EXISTS order_book (
            symbol TEXT,
            last_update_id BIGINT,
            bids TEXT,
            asks TEXT,
            updated_at TIMESTAMP,
            PRIMARY KEY (symbol, last_update_id)
        );
    """,
    "recent_trades": """
        CREATE TABLE IF NOT EXISTS recent_trades (
            id BIGINT PRIMARY KEY,
            symbol TEXT,
            price DOUBLE,
            qty DOUBLE,
            quote_qty DOUBLE,
            time TIMESTAMP,
            is_buyer_maker BOOLEAN,
            updated_at TIMESTAMP
        );
    """,
    "klines": """
        CREATE TABLE IF NOT EXISTS klines (
            symbol TEXT,
            open_time TIMESTAMP,
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            close_price DOUBLE,
            volume DOUBLE,
            close_time TIMESTAMP,
            quote_asset_volume DOUBLE,
            number_of_trades BIGINT,
            PRIMARY KEY (symbol, open_time)
        );
    """
}

for table_name, query in TABLES.items():
    session.execute(query)
print("All tables ready in Cassandra.")

# Kafka Consumer Setup
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[KAFKA_BROKER_LOCAL],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="cassandra-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
)
print(f"Listening to Kafka topics: {', '.join(TOPICS)}")

# --- Timestamp Normalization ---
def normalize_timestamp(value):
    """Convert timestamp-like values to datetime."""
    if value is None:
        return None
    try:
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        if isinstance(value, (int, float)):
            # Detect microseconds/nanoseconds/milliseconds
            if value > 1e18:   # nanoseconds
                value = value / 1e9
            elif value > 1e15: # microseconds
                value = value / 1e6
            elif value > 1e12: # milliseconds
                value = value / 1e3
            return datetime.utcfromtimestamp(value)
    except Exception:
        pass
    # fallback: use current UTC time
    return datetime.utcnow()

def normalize_timestamps_in_dict(data):
    """Iterate through all dict items and normalize any timestamp fields."""
    for key, val in data.items():
        # Detect likely timestamp keys or values by pattern
        if isinstance(val, (int, float, str)):
            if any(kw in key.lower() for kw in ["time", "updated", "open", "close"]):
                data[key] = normalize_timestamp(val)
    return data

# Insert Function Per Table
def insert_to_cassandra(topic, message):
    table = topic.split(".")[-1]
    after_data = message.get("payload", {}).get("after", {})

    if not after_data:
        return  # Skip tombstones or delete events

    # Normalize timestamps globally
    after_data = normalize_timestamps_in_dict(after_data)

    try:
        placeholders = ", ".join(f"%({k})s" for k in after_data.keys())
        columns = ", ".join(after_data.keys())
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders});"
        session.execute(query, after_data)
        print(f"Inserted into {table}: {list(after_data.values())[:3]} ...")
    except Exception as e:
        print(f"Error inserting into {table}: {e}")

# Consume and Insert
try:
    for msg in consumer:
        if msg.value:
            insert_to_cassandra(msg.topic, msg.value)
except KeyboardInterrupt:
    print("Stopped by user.")
finally:
    consumer.close()
    cluster.shutdown()
