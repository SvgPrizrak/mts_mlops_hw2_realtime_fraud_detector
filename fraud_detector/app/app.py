import logging
import os
import sys

import pandas as pd

sys.path.append(os.path.abspath("./src"))

from kafka_io import create_consumer, create_producer, wait_for_kafka
from preprocessing import load_train_data, run_preproc
from scorer import load_model, make_prediction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


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
    Формирует сообщение для выходного Kafka topic scoring.
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
            logger.exception(
                "Error while processing Kafka message: %s",
                error,
            )


if __name__ == "__main__":
    main()
