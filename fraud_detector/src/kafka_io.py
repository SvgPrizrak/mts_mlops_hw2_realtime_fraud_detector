import json
import logging
import time

from kafka import KafkaConsumer, KafkaProducer

logger = logging.getLogger(__name__)


def wait_for_kafka(
    bootstrap_servers: str,
    retries: int = 40,
    delay: int = 3,
) -> None:
    """
    Ждёт, пока Kafka станет доступна.
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


def create_consumer(
    topic: str,
    bootstrap_servers: str,
    group_id: str,
) -> KafkaConsumer:
    """
    Создаёт Kafka consumer.
    """
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def create_producer(
    bootstrap_servers: str,
) -> KafkaProducer:
    """
    Создаёт Kafka producer.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
        ).encode("utf-8"),
    )
