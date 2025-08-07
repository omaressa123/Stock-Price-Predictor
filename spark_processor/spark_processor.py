import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from config.config_manager import ConfigManager
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spark_processor")

def save_to_cassandra(df, epoch_id):
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode('append') \
            .options(
                table="stock_prices", 
                keyspace=ConfigManager().get("database.cassandra.keyspace")
            ) \
            .save()
        logger.info(f"Saved {df.count()} records to Cassandra")
    except Exception as e:
        logger.error(f"Failed to save to Cassandra: {str(e)}")

def save_to_postgres(df, epoch_id):
    try:
        db_config = ConfigManager().get("database.postgres")
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['db']}") \
            .option("dbtable", "stock_prices") \
            .option("user", db_config["user"]) \
            .option("password", db_config["password"]) \
            .mode('append') \
            .save()
        logger.info(f"Saved {df.count()} records to PostgreSQL")
    except Exception as e:
        logger.error(f"Failed to save to PostgreSQL: {str(e)}")

def process_stream(df, epoch_id):
    logger.info(f"Processing batch {epoch_id}")
    save_to_cassandra(df, epoch_id)
    save_to_postgres(df, epoch_id)

def main():
    config = ConfigManager()
    
    # Schema for stock data
    schema = StructType([
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("timestamp", StringType()),
        StructField("volume", DoubleType())
    ])

    try:
        spark = SparkSession.builder \
            .appName("StockStreamProcessor") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                   "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                   "org.postgresql:postgresql:42.6.0") \
            .config("spark.cassandra.connection.host", config.get("database.cassandra.hosts[0]")) \
            .getOrCreate()

        # Read from Kafka
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.get("kafka.bootstrap_servers")) \
            .option("subscribe", config.get("kafka.topic")) \
            .option("startingOffsets", "latest") \
            .load()

        # Parse JSON data
        parsed_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json("value", schema).alias("data")) \
            .select("data.*") \
            .withColumn("timestamp", to_timestamp(col("timestamp")))

        # Write to both databases
        query = parsed_df.writeStream \
            .foreachBatch(process_stream) \
            .outputMode("append") \
            .start()

        logger.info("Stream processing started")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Spark processing failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()