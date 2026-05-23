import logging
import os
import sys
import time

import pandas as pd
from prometheus_client import Counter, Gauge, Histogram, start_http_server

sys.path.append(os.path.abspath("./src"))

from kafka_io import create_consumer, create_producer, wait_for_kafka
from preprocessing import load_train_data, run_preproc
from scorer import load_model, make_prediction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


# Prometheus metrics
TRANSACTIONS_TOTAL = Counter(
    "transactions",
    "Total number of processed transactions",
)

FRAUD_DETECTED_TOTAL = Counter(
    "fraud_detected",
    "Total number of transactions predicted as fraud",
)

PROCESSING_ERRORS_TOTAL = Counter(
    "processing_errors",
    "Total number of transaction processing errors",
)

FRAUD_RATIO = Gauge(
    "fraud_ratio",
    "Share of fraud transactions among processed transactions",
)

FRAUD_SCORE = Histogram(
    "fraud_score",
    "Distribution of fraud prediction scores",
    buckets=[
        0.0,
        0.1,
        0.2,
        0.3,
        0.4,
        0.5,
        0.6,
        0.7,
        0.8,
        0.9,
        1.0,
    ],
)

TRANSACTION_PROCESSING_SECONDS = Histogram(
    "transaction_processing_seconds",
    "Transaction processing time in seconds",
)


def prepare_input_dataframe(transaction: dict) -> pd.DataFrame:
    """
    Преобразует одну транзакцию из Kafka в DataFrame для preprocessing.
    """
    input_df = pd.DataFrame([transaction])

    input_df = input_df.drop(
        columns=["transaction_id"],
        errors="ignore",
    )

    input_df = input_df.drop(
        columns=["name_1", "name_2", "street", "post_code"],
        errors="ignore",
    )

    return input_df


def build_score_message(
    transaction: dict,
    transaction_id: str,
    score: float,
    fraud_flag: int,
) -> dict:
    """
    Формирует сообщение для выходного Kafka topic scores.
    """
    return {
        "transaction_id": transaction_id,
        "score": score,
        "fraud_flag": fraud_flag,
        "us_state": transaction.get("us_state"),
        "merch": transaction.get("merch"),
        "cat_id": transaction.get("cat_id"),
    }


def main() -> None:
    kafka_bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092",
    )

    transactions_topic = os.getenv(
        "KAFKA_TRANSACTIONS_TOPIC",
        "transactions",
    )

    scoring_topic = os.getenv(
        "KAFKA_SCORING_TOPIC",
        "scores",
    )

    logger.info("Starting fraud detector service")
    logger.info("Input topic: %s", transactions_topic)
    logger.info("Output topic: %s", scoring_topic)

    start_http_server(8000)
    logger.info("Prometheus metrics server started on port 8000")

    processed_count = 0
    fraud_count = 0

    wait_for_kafka(kafka_bootstrap_servers)

    logger.info("Loading train reference data")
    train = load_train_data()

    logger.info("Loading ML model")
    model = load_model()

    consumer = create_consumer(
        topic=transactions_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        group_id="fraud-detector",
    )

    producer = create_producer(
        bootstrap_servers=kafka_bootstrap_servers,
    )

    logger.info("Fraud detector started. Waiting for Kafka messages...")

    for message in consumer:
        try:
            started_at = time.perf_counter()

            transaction = message.value

            transaction_id = str(
                transaction.get(
                    "transaction_id",
                    message.offset,
                )
            )

            logger.info("Received transaction_id=%s", transaction_id)

            input_df = prepare_input_dataframe(transaction)

            processed_df = run_preproc(
                train=train,
                input_df=input_df,
            )

            score, fraud_flag = make_prediction(
                model=model,
                processed_df=processed_df,
            )

            processing_time = time.perf_counter() - started_at

            TRANSACTIONS_TOTAL.inc()
            FRAUD_SCORE.observe(score)
            TRANSACTION_PROCESSING_SECONDS.observe(processing_time)

            processed_count += 1
            fraud_count += fraud_flag

            if fraud_flag == 1:
                FRAUD_DETECTED_TOTAL.inc()

            FRAUD_RATIO.set(fraud_count / processed_count)

            score_message = build_score_message(
                transaction=transaction,
                transaction_id=transaction_id,
                score=score,
                fraud_flag=fraud_flag,
            )

            producer.send(
                scoring_topic,
                value=score_message,
            )
            producer.flush()

            logger.info(
                "Sent score to topic=%s transaction_id=%s score=%.6f fraud_flag=%s",
                scoring_topic,
                transaction_id,
                score,
                fraud_flag,
            )

        except Exception as error:
            PROCESSING_ERRORS_TOTAL.inc()

            logger.exception(
                "Error while processing Kafka message: %s",
                error,
            )


if __name__ == "__main__":
    main()
