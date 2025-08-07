import requests
from cassandra.cluster import Cluster
from psycopg2 import connect
from kafka import KafkaConsumer
import logging

logger = logging.getLogger(__name__)

class HealthChecker:
    @staticmethod
    def check_kafka():
        try:
            consumer = KafkaConsumer(
                bootstrap_servers='kafka:9092',
                consumer_timeout_ms=1000
            )
            consumer.topics()
            return True, "Kafka is healthy"
        except Exception as e:
            return False, f"Kafka check failed: {str(e)}"

    @staticmethod
    def check_cassandra():
        try:
            cluster = Cluster(['cassandra'])
            session = cluster.connect()
            session.execute("SELECT release_version FROM system.local")
            return True, "Cassandra is healthy"
        except Exception as e:
            return False, f"Cassandra check failed: {str(e)}"

    @staticmethod
    def check_postgres():
        try:
            conn = connect(
                user="airflow",
                password="airflow",
                host="postgres",
                database="airflow"
            )
            cur = conn.cursor()
            cur.execute("SELECT 1")
            return True, "PostgreSQL is healthy"
        except Exception as e:
            return False, f"PostgreSQL check failed: {str(e)}"
        finally:
            if 'conn' in locals():
                conn.close()

    @staticmethod
    def check_spark():
        try:
            response = requests.get("http://spark:8080")
            return response.status_code == 200, "Spark is healthy"
        except Exception as e:
            return False, f"Spark check failed: {str(e)}"

    @staticmethod
    def full_health_check():
        results = []
        services = {
            'Kafka': HealthChecker.check_kafka,
            'Cassandra': HealthChecker.check_cassandra,
            'PostgreSQL': HealthChecker.check_postgres,
            'Spark': HealthChecker.check_spark
        }
        
        for name, check in services.items():
            status, message = check()
            results.append({
                'service': name,
                'status': 'UP' if status else 'DOWN',
                'message': message
            })
            if not status:
                logger.error(f"Health check failed for {name}: {message}")
        
        return results