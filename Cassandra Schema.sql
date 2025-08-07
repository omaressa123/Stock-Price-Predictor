-- The following schema is valid for Apache Cassandra CQL. 
-- If you are running this in a different database (like SQL Server, MySQL, or PostgreSQL), it will not work.
-- Make sure you are using a Cassandra CQL shell (cqlsh) or a compatible Cassandra client.

CREATE KEYSPACE IF NOT EXISTS market_data 
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS market_data.stock_prices (
    symbol text,
    timestamp timestamp,
    price double,
    volume double,
    PRIMARY KEY ((symbol), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE IF NOT EXISTS market_data.reports (
    symbol text,
    report_date timestamp,
    report_image blob,
    PRIMARY KEY ((symbol), report_date)
) WITH CLUSTERING ORDER BY (report_date DESC);