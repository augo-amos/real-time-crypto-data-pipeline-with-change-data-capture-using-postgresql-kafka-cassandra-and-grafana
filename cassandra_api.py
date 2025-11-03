from flask import Flask, jsonify
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

app = Flask(__name__)

# Connect to Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect('binance_keyspace')
session.row_factory = dict_factory  # returns rows as dictionaries

# Routes

@app.route("/")
def index():
    return jsonify({
        "message": "Binance → Cassandra API is running",
        "endpoints": [
            "/crypto_24h_stats",
            "/latest_prices",
            "/order_book",
            "/recent_trades",
            "/klines"
        ]
    })

# Crypto 24h Stats
@app.route("/crypto_24h_stats")
def crypto_24h_stats():
    query = "SELECT symbol, price_change_percent, last_price, high_price, low_price, volume, quote_volume, updated_at FROM crypto_24h_stats LIMIT 100;"
    rows = session.execute(query)
    return jsonify(list(rows))

# Latest Prices
@app.route("/latest_prices")
def latest_prices():
    query = "SELECT symbol, price, updated_at FROM latest_prices LIMIT 100;"
    rows = session.execute(query)
    return jsonify(list(rows))

# Order Book
@app.route("/order_book")
def order_book():
    query = "SELECT symbol, bids, asks, updated_at FROM order_book LIMIT 100;"
    rows = session.execute(query)
    return jsonify(list(rows))

# Recent Trades
@app.route("/recent_trades")
def recent_trades():
    query = "SELECT symbol, trade_id, price, qty, quote_qty, is_buyer_maker, time FROM recent_trades LIMIT 100;"
    rows = session.execute(query)
    return jsonify(list(rows))

# Klines (Candlestick Data)
@app.route("/klines")
def klines():
    query = "SELECT symbol, open_time, open_price, high_price, low_price, close_price, volume, close_time, quote_asset_volume, number_of_trades FROM klines LIMIT 100;"
    rows = session.execute(query)
    return jsonify(list(rows))

# Main

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
