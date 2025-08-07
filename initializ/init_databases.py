from cassandra.cluster import Cluster
from cassandra.io.asyncio import AsyncioConnection  # Required for Python 3.12+
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time

def init_cassandra():
    max_retries = 5
    retry_delay = 5

    for _ in range(max_retries):
        try:
            # Use AsyncioConnection for Python 3.12+ compatibility
            cluster = Cluster(
                ['cassandra'],
                connection_class=AsyncioConnection,  # Critical for 3.12+
                protocol_version=4
            )
            session = cluster.connect()

            session.execute("""
                CREATE KEYSPACE IF NOT EXISTS market_data 
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """)

            session.execute("""
                CREATE TABLE IF NOT EXISTS market_data.stock_prices (
                    symbol text,
                    timestamp timestamp,
                    price double,
                    volume double,
                    PRIMARY KEY ((symbol), timestamp)
                ) WITH CLUSTERING ORDER BY (timestamp DESC)
            """)

            session.execute("""
                CREATE TABLE IF NOT EXISTS market_data.reports (
                    symbol text,
                    report_date timestamp,
                    report_image blob,
                    forecast_json text,
                    metrics text,
                    PRIMARY KEY ((symbol), report_date)
                ) WITH CLUSTERING ORDER BY (report_date DESC)
            """)

            print("Cassandra initialized successfully")
            return
        except Exception as e:
            print(f"Cassandra init failed: {e}")
            time.sleep(retry_delay)

    raise Exception("Failed to initialize Cassandra after multiple attempts")

def init_postgres():
    conn = connect(
        user="airflow",
        password="airflow",
        host="postgres",
        database="airflow"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10),
            price DECIMAL(15, 2),
            volume BIGINT,
            timestamp TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_symbol ON stock_prices(symbol)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_timestamp ON stock_prices(timestamp)
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS model_metrics (
            symbol VARCHAR(10),
            model_type VARCHAR(20),
            mse FLOAT,
            mae FLOAT,
            r2 FLOAT,
            training_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, model_type, training_date)
        )
    """)

    print("PostgreSQL initialized successfully")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    init_cassandra()
    init_postgres()