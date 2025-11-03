import requests, json, time, os
from dotenv import load_dotenv

load_dotenv()

DEBEZIUM_CONNECT_URL = os.getenv("DEBEZIUM_CONNECT_URL", "http://localhost:8083/connectors")

TABLES = [
    "public.crypto_24h_stats",
    "public.latest_prices",
    "public.order_book",
    "public.recent_trades",
    "public.klines"
]

def register_connector(table):
    # Replace hyphens with underscores and ensure lowercase
    table_name = table.split('.')[-1].lower()
    name = f"binance_{table_name}"
    slot_name = f"{name}_slot"
    publication_name = f"{name}_pub"

    config = {
        "name": name,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "plugin.name": "pgoutput",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "debezium",
            "database.password": "debezium",
            "database.dbname": "binance_db",
            "database.server.name": "binance",
            "slot.name": slot_name,
            "publication.name": publication_name,
            "table.include.list": table,
            "topic.prefix": "binance",
            "snapshot.mode": "initial",
            "include.schema.changes": "false",
            "decimal.handling.mode": "double"
        }
    }

    print(f"Registering connector: {name}")
    response = requests.post(
        DEBEZIUM_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(config)
    )
    if response.status_code in [200, 201]:
        print(f"Connector {name} registered.")
    else:
        print(f"Failed for {name}: {response.text}")

if __name__ == "__main__":
    for t in TABLES:
        register_connector(t)
        time.sleep(2)
