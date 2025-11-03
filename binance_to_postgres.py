import requests
from datetime import datetime
from dotenv import load_dotenv
import os
import time
from sqlalchemy import create_engine, text
import json

load_dotenv()

# Database config
DB_USER = os.getenv("POSTGRES_ADMIN_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_ADMIN_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "binance_db")
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL", 60))

# Base Binance URL
BASE_URL = "https://api.binance.com"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL, future=True)

# CREATE TABLES
table_schemas = {
    "crypto_24h_stats": """
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
    """,
    "latest_prices": """
        CREATE TABLE IF NOT EXISTS latest_prices (
            symbol VARCHAR(20) PRIMARY KEY,
            price DOUBLE PRECISION,
            updated_at TIMESTAMP
        );
    """,
    "order_book": """
        CREATE TABLE IF NOT EXISTS order_book (
            symbol VARCHAR(20),
            last_update_id BIGINT,
            bids JSONB,
            asks JSONB,
            updated_at TIMESTAMP,
            PRIMARY KEY (symbol, last_update_id)
        );
    """,
    "recent_trades": """
        CREATE TABLE IF NOT EXISTS recent_trades (
            id BIGINT PRIMARY KEY,
            symbol VARCHAR(20),
            price DOUBLE PRECISION,
            qty DOUBLE PRECISION,
            quote_qty DOUBLE PRECISION,
            time TIMESTAMP,
            is_buyer_maker BOOLEAN,
            updated_at TIMESTAMP
        );
    """,
    "klines": """
        CREATE TABLE IF NOT EXISTS klines (
            symbol VARCHAR(20),
            open_time TIMESTAMP,
            open_price DOUBLE PRECISION,
            high_price DOUBLE PRECISION,
            low_price DOUBLE PRECISION,
            close_price DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            close_time TIMESTAMP,
            quote_asset_volume DOUBLE PRECISION,
            number_of_trades BIGINT,
            PRIMARY KEY (symbol, open_time)
        );
    """
}

with engine.begin() as conn:
    for name, query in table_schemas.items():
        conn.execute(text(query))
print("PostgreSQL tables ready.")

# UPSERT QUERIES
UPSERTS = {
    "crypto_24h_stats": text("""
        INSERT INTO crypto_24h_stats (symbol, price_change_percent, last_price, high_price, low_price, volume, quote_volume, open_time, close_time, updated_at)
        VALUES (:symbol, :price_change_percent, :last_price, :high_price, :low_price, :volume, :quote_volume, :open_time, :close_time, :updated_at)
        ON CONFLICT (symbol) DO UPDATE SET
            price_change_percent = EXCLUDED.price_change_percent,
            last_price = EXCLUDED.last_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            volume = EXCLUDED.volume,
            quote_volume = EXCLUDED.quote_volume,
            open_time = EXCLUDED.open_time,
            close_time = EXCLUDED.close_time,
            updated_at = EXCLUDED.updated_at;
    """),
    "latest_prices": text("""
        INSERT INTO latest_prices (symbol, price, updated_at)
        VALUES (:symbol, :price, :updated_at)
        ON CONFLICT (symbol) DO UPDATE SET
            price = EXCLUDED.price,
            updated_at = EXCLUDED.updated_at;
    """),
    "order_book": text("""
        INSERT INTO order_book (symbol, last_update_id, bids, asks, updated_at)
        VALUES (:symbol, :last_update_id, :bids, :asks, :updated_at)
        ON CONFLICT (symbol, last_update_id) DO NOTHING;
    """),
    "recent_trades": text("""
        INSERT INTO recent_trades (id, symbol, price, qty, quote_qty, time, is_buyer_maker, updated_at)
        VALUES (:id, :symbol, :price, :qty, :quote_qty, :time, :is_buyer_maker, :updated_at)
        ON CONFLICT (id) DO NOTHING;
    """),
    "klines": text("""
        INSERT INTO klines (symbol, open_time, open_price, high_price, low_price, close_price, volume, close_time, quote_asset_volume, number_of_trades)
        VALUES (:symbol, :open_time, :open_price, :high_price, :low_price, :close_price, :volume, :close_time, :quote_asset_volume, :number_of_trades)
        ON CONFLICT (symbol, open_time) DO NOTHING;
    """),
}

# FETCH AND STORE DATA
def fetch_and_store_data():
    now = datetime.utcnow()

    try:
        # 24H Ticker
        resp = requests.get(f"{BASE_URL}/api/v3/ticker/24hr", timeout=20)
        resp.raise_for_status()
        data = resp.json()
        with engine.begin() as conn:
            for d in data:
                if d["symbol"].endswith("USDT"):
                    conn.execute(UPSERTS["crypto_24h_stats"], {
                        "symbol": d["symbol"],
                        "price_change_percent": float(d.get("priceChangePercent", 0.0)),
                        "last_price": float(d.get("lastPrice", 0.0)),
                        "high_price": float(d.get("highPrice", 0.0)),
                        "low_price": float(d.get("lowPrice", 0.0)),
                        "volume": float(d.get("volume", 0.0)),
                        "quote_volume": float(d.get("quoteVolume", 0.0)),
                        "open_time": datetime.fromtimestamp(d["openTime"] / 1000) if d.get("openTime") else None,
                        "close_time": datetime.fromtimestamp(d["closeTime"] / 1000) if d.get("closeTime") else None,
                        "updated_at": now
                    })

        # Latest Prices
        resp = requests.get(f"{BASE_URL}/api/v3/ticker/price", timeout=20)
        resp.raise_for_status()
        prices = resp.json()
        with engine.begin() as conn:
            for p in prices:
                if p["symbol"].endswith("USDT"):
                    conn.execute(UPSERTS["latest_prices"], {
                        "symbol": p["symbol"],
                        "price": float(p["price"]),
                        "updated_at": now
                    })

        # Order Book (limited to a few for performance)
        for sym in ["BTCUSDT", "ETHUSDT", "BNBUSDT"]:
            resp = requests.get(f"{BASE_URL}/api/v3/depth", params={"symbol": sym, "limit": 20}, timeout=20)
            resp.raise_for_status()
            ob = resp.json()
            with engine.begin() as conn:
                conn.execute(UPSERTS["order_book"], {
                    "symbol": sym,
                    "last_update_id": ob.get("lastUpdateId"),
                    "bids": json.dumps(ob.get("bids", [])),
                    "asks": json.dumps(ob.get("asks", [])),
                    "updated_at": now
                })

        # Recent Trades (limited)
        for sym in ["BTCUSDT", "ETHUSDT", "BNBUSDT"]:
            resp = requests.get(f"{BASE_URL}/api/v3/trades", params={"symbol": sym, "limit": 20}, timeout=20)
            resp.raise_for_status()
            trades = resp.json()
            with engine.begin() as conn:
                for t in trades:
                    conn.execute(UPSERTS["recent_trades"], {
                        "id": t["id"],
                        "symbol": sym,
                        "price": float(t["price"]),
                        "qty": float(t["qty"]),
                        "quote_qty": float(t["quoteQty"]),
                        "time": datetime.fromtimestamp(t["time"] / 1000),
                        "is_buyer_maker": t["isBuyerMaker"],
                        "updated_at": now
                    })

        # Klines (candlesticks)
        for sym in ["BTCUSDT", "ETHUSDT", "BNBUSDT"]:
            resp = requests.get(f"{BASE_URL}/api/v3/klines", params={"symbol": sym, "interval": "1h", "limit": 10}, timeout=20)
            resp.raise_for_status()
            klines = resp.json()
            with engine.begin() as conn:
                for k in klines:
                    conn.execute(UPSERTS["klines"], {
                        "symbol": sym,
                        "open_time": datetime.fromtimestamp(k[0] / 1000),
                        "open_price": float(k[1]),
                        "high_price": float(k[2]),
                        "low_price": float(k[3]),
                        "close_price": float(k[4]),
                        "volume": float(k[5]),
                        "close_time": datetime.fromtimestamp(k[6] / 1000),
                        "quote_asset_volume": float(k[7]),
                        "number_of_trades": int(k[8])
                    })

        print(f"Updated all endpoints at {now} UTC")

    except Exception as e:
        print("Error fetching/storing data:", e)

if __name__ == "__main__":
    print("Binance → PostgreSQL multi-endpoint loader running...")
    while True:
        fetch_and_store_data()
        time.sleep(REFRESH_INTERVAL)
