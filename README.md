# Real-Time Cryptocurrency Data Pipeline

A scalable end-to-end data engineering pipeline that consumes real-time cryptocurrency prices from Binance, processes streams with stateful logic, and performs daily batch aggregations for analytics.


### 🛠 Tech Stack

  * **Ingestion:** Python (WebSockets), Apache Kafka (Confluent)
  * **Stream Processing:** Apache Spark (PySpark), Structured Streaming
  * **State Management:** Redis (for calculating inter-batch price changes)
  * **Storage:** PostgreSQL (Staging & Data Warehousing schemas)
  * **Orchestration:** Apache Airflow
  * **Infrastructure:** Docker, Docker Compose

-----

### 🚀 Key Features

#### 1\. Real-Time Ingestion & State Management

  * Connects to the Binance WebSocket API to fetch live ticker data for BTC, ETH, and BNB.
  * **Stateful Processing:** Utilizes **Redis** as a low-latency cache to store the last seen price of each symbol.
  * **Logic:** The Spark consumer compares the current incoming price against the Redis state to calculate the instantaneous `exchange_rate` (volatility) before persisting the data.

#### 2\. Hybrid Lambda Architecture

  * **Speed Layer:** Live data is ingested, enriched, and written to the `staging.raw_prices` table immediately.
  * **Batch Layer:** An Airflow DAG triggers a daily SQL Stored Procedure (`get_daily_summary`).
  * **Complex Aggregation:** The SQL logic utilizes Window Functions (`FIRST_VALUE`) to correctly identify the Open and Close prices for the day, alongside Min/Max/Avg aggregations.

#### 3\. Robustness & Idempotency

  * **Upsert Logic:** The PostgreSQL stored procedure uses `ON CONFLICT` to handle data re-runs without duplication.
  * **Data Retention:** Automatic cleanup logic removes raw staging data older than 7 days to manage storage costs.

-----

### 📂 Project Structure

```bash
├── airflow/                # Airflow DAGs and configuration
│   └── dags/
│       └── get_daily_summary_dag.py
├── consumer_app/           # Spark Streaming logic
│   └── consumer.py
├── producer_app/           # Kafka Producer logic
│   └── producer.py
├── init.sql                # Database schema and Stored Procedures
├── docker-compose.yaml     # Infrastructure definition
└── README.md
```

-----

### ⚡ How to Run

**Prerequisites:** Docker and Docker Compose installed.

1.  **Clone the repository**

2.  **Create Environment File and set(AIRFLOW_UID,POSTGRES_USER,POSTGRES_PASSWORD,POSTGRES_DB,POSTGRES_PORT)**
  
3.  **Spin up Infrastructure**

4.  **Verify Components**

5.  **Check Data Flow**
