import json
import logging
import os
import time

import psycopg2
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def wait_for_kafka(
    bootstrap_servers: str,
    retries: int = 40,
    delay: int = 3,
) -> None:
    """
    Ждёт доступности Kafka.
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
            )
            producer.close()

            logger.info("Kafka is available")
            return

        except Exception as error:
            logger.warning(
                "Kafka is not available yet. Attempt %s/%s. Error: %s",
                attempt,
                retries,
                error,
            )
            time.sleep(delay)

    raise RuntimeError("Kafka is not available")


def wait_for_postgres(
    retries: int = 40,
    delay: int = 3,
):
    """
    Ждёт доступности PostgreSQL и возвращает connection.
    """
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB", "fraud_db")
    user = os.getenv("POSTGRES_USER", "fraud_user")
    password = os.getenv("POSTGRES_PASSWORD", "fraud_pass")

    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password,
            )

            logger.info("PostgreSQL is available")
            return conn

        except Exception as error:
            logger.warning(
                "PostgreSQL is not available yet. Attempt %s/%s. Error: %s",
                attempt,
                retries,
                error,
            )
            time.sleep(delay)

    raise RuntimeError("PostgreSQL is not available")


def create_scores_consumer(
    topic: str,
    bootstrap_servers: str,
) -> KafkaConsumer:
    """
    Создаёт consumer для topic scoring.
    """
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="scoring-writer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def ensure_scores_table_exists(conn) -> None:
    """
    Создаёт таблицу scores, если init.sql по какой-то причине не отработал.
    """
    query = """
        CREATE TABLE IF NOT EXISTS scores (
            id SERIAL PRIMARY KEY,
            transaction_id TEXT NOT NULL,
            score DOUBLE PRECISION NOT NULL,
            fraud_flag INTEGER NOT NULL,
            us_state TEXT,
            merch TEXT,
            cat_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_scores_created_at ON scores(created_at);
        CREATE INDEX IF NOT EXISTS idx_scores_fraud_flag ON scores(fraud_flag);
        CREATE INDEX IF NOT EXISTS idx_scores_us_state ON scores(us_state);
        CREATE INDEX IF NOT EXISTS idx_scores_merch ON scores(merch);
        CREATE INDEX IF NOT EXISTS idx_scores_cat_id ON scores(cat_id);
    """

    with conn.cursor() as cursor:
        cursor.execute(query)

    conn.commit()

    logger.info("scores table is ready")


def insert_score(conn, row: dict) -> None:
    """
    Записывает один результат скоринга в PostgreSQL.
    """
    query = """
        INSERT INTO scores (
            transaction_id,
            score,
            fraud_flag,
            us_state,
            merch,
            cat_id
        )
        VALUES (%s, %s, %s, %s, %s, %s);
    """

    transaction_id = str(row.get("transaction_id"))
    score = float(row.get("score"))
    fraud_flag = int(row.get("fraud_flag"))
    us_state = row.get("us_state")
    merch = row.get("merch")
    cat_id = row.get("cat_id")

    with conn.cursor() as cursor:
        cursor.execute(
            query,
            (
                transaction_id,
                score,
                fraud_flag,
                us_state,
                merch,
                cat_id,
            ),
        )

    conn.commit()


def main() -> None:
    kafka_bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092",
    )

    scoring_topic = os.getenv(
        "KAFKA_SCORING_TOPIC",
        "scores",
    )

    logger.info("Starting scoring writer service")
    logger.info("Reading Kafka topic: %s", scoring_topic)

    wait_for_kafka(kafka_bootstrap_servers)

    conn = wait_for_postgres()
    ensure_scores_table_exists(conn)

    consumer = create_scores_consumer(
        topic=scoring_topic,
        bootstrap_servers=kafka_bootstrap_servers,
    )

    logger.info("Scoring writer started. Waiting for scored messages...")

    for message in consumer:
        try:
            row = message.value

            insert_score(
                conn=conn,
                row=row,
            )

            logger.info(
                "Inserted score for transaction_id=%s score=%s fraud_flag=%s",
                row.get("transaction_id"),
                row.get("score"),
                row.get("fraud_flag"),
            )

        except Exception as error:
            logger.exception(
                "Failed to insert score message into PostgreSQL: %s",
                error,
            )

            try:
                conn.rollback()
            except Exception:
                pass


if __name__ == "__main__":
    main()
