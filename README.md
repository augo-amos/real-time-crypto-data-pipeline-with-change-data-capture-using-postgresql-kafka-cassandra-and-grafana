# Real-Time Crypto Data Pipeline with Change Data Capture (CDC) Using PostgreSQL, Kafka, Cassandra, and Grafana

This project demonstrates a **real-time data streaming pipeline** that captures cryptocurrency trading data changes using **Debezium CDC (Change Data Capture)** from **PostgreSQL**, streams them through **Apache Kafka**, stores them in **Cassandra**, and visualizes insights in **Grafana** — all in near real-time.

---

## **Architecture Overview**

**Flow:**  
**Binance API → PostgreSQL → Debezium (CDC) → Kafka → Cassandra → Flask API → Grafana**

Instead of using a **Kafka sink connector**, this setup implements a **custom Python consumer** that listens to Kafka topics and writes to Cassandra.  
This approach allows for **greater control**, **flexibility**, and **transformation logic** before persistence — unlike connectors which have limited transformation capabilities.

---

## **Key Components**

| Component | Description |
|------------|-------------|
| **PostgreSQL** | Stores raw crypto market data (24h stats, trades, order book, etc.) before streaming |
| **Debezium** | Monitors PostgreSQL changes via CDC and publishes them to Kafka topics |
| **Apache Kafka** | Acts as the message broker between Debezium and the downstream Cassandra consumer |
| **Cassandra** | Stores processed data for efficient querying and real-time analytics |
| **Flask API** | Lightweight REST API to expose Cassandra data to Grafana |
| **Grafana** | Visualization layer for live crypto dashboards |

---

## **Database Schema Overview**

### **PostgreSQL Source Tables**
- `crypto_24h_stats`
- `latest_prices`
- `order_book`
- `recent_trades`
- `klines`

### **Cassandra Target Tables**
Mirrors the PostgreSQL schema but optimized for time-series queries.

Example:
```sql
CREATE TABLE crypto_24h_stats (
    symbol text,
    price_change_percent double,
    last_price double,
    high_price double,
    low_price double,
    volume double,
    quote_volume double,
    updated_at timestamp,
    PRIMARY KEY ((symbol), updated_at)
);
````

---

## **How CDC Works**

1. **Debezium** monitors PostgreSQL’s `WAL (Write-Ahead Log)` for changes.
2. When a new crypto record is inserted or updated, Debezium publishes a message to a **Kafka topic**.
3. A **Python Kafka Consumer** (`consumer_to_cassandra.py`) receives the message and writes it to Cassandra.
4. **Flask API** (`cassandra_api.py`) exposes endpoints for visualization.
5. **Grafana** fetches data via the API or directly from Cassandra to create live dashboards.

---

## **Flask API Overview**

Example endpoint:

```python
@app.route("/latest_prices")
def latest_prices():
    query = "SELECT symbol, price, updated_at FROM latest_prices LIMIT 100;"
    rows = session.execute(query)
    return jsonify(list(rows))
```

Available routes:

* `/crypto_24h_stats`
* `/latest_prices`
* `/order_book`
* `/recent_trades`
* `/klines`

---

## **Grafana Visualizations**

### **1. ETH Price Over Time (Line Chart)**

A time-series visualization tracking ETH price changes.

```sql
SELECT 
    updated_at AS time,
    price AS value
FROM latest_prices
WHERE symbol = 'ETHUSDT'
ALLOW FILTERING;
```

### **2. Top 10 Most Traded Cryptocurrencies (Bar Chart)**

Displays highest-volume coins over a recent time window.

```sql
SELECT symbol, volume FROM crypto_24h_stats LIMIT 10;
```

### **3. Trade Count Overview (Histogram / Time Bar)**

Shows trading activity trends.

```sql
SELECT open_time AS time, number_of_trades FROM klines WHERE symbol = 'ETHUSDT' ALLOW FILTERING;

---

## **Running the Project**

### **1. Start Dependencies**

Ensure Docker containers for PostgreSQL, Kafka, Zookeeper, and Debezium are running.

```bash
docker-compose up -d
```

### **2. Start the Consumer**

Run the custom Kafka → Cassandra consumer:

```bash
python3 consumer_to_cassandra.py
```

### **3. Start the Flask API**

```bash
python3 cassandra_api.py
```

### **4. Connect Grafana**

1. Install the **Infinity Plugin** or **Cassandra Plugin** in Grafana.
2. Add a new data source (HTTP API or Cassandra).
3. Use endpoints such as:

   ```
   http://host.docker.internal:5000/latest_prices
   ```
4. Create dashboards and select visualization types.

---

## **Advantages of the Custom Approach**

**Fine-grained control** over transformations before insertion.
**Flexible schema evolution** — Python logic handles minor structure changes gracefully.
**Avoids dependency** on heavy Kafka sink connectors.
**Scalable** — can process multiple tables or topics independently.
**Simple to extend** — e.g., add APIs, models, or analytics layers easily.

---

## **Possible Extensions**

* Add **Redis** for caching real-time price queries.
* Enable **Prometheus metrics** for API performance monitoring.
* Stream live updates directly to Grafana using WebSocket.
* Introduce **machine learning models** for predictive analytics.

---

## **Project Flow Diagram**

![Flow Diagram](A_flowchart_diagram_illustrates_the_architecture_o.png)

---

## **Author**

**Augo Amos**
*Data Engineer | Python Developer | Data Analyst*

---

## **License**

MIT License © 2025 

