from datetime import datetime, timedelta
import json
import time

from kafka import KafkaProducer


def __create_kafka_producer() -> KafkaProducer:
    retries = 5
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=["kafka:9092"],
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Connected to Kafka!")
            return producer
        except Exception as e:
            print(f"Kafka connection attempt {i + 1} failed: {e}")
            time.sleep(5)

    raise Exception("Failed to connect to Kafka after multiple attempts")


_producer = None


def get_kafka_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = __create_kafka_producer()
    return _producer


def get_date_one_month_ago() -> str:
    today = datetime.today()
    one_month_ago = today - timedelta(days=30)
    return one_month_ago.strftime("%Y-%m-%d")


def subtract_n_days(date_string: str, days: int) -> str:
    date_object = datetime.strptime(date_string, "%Y-%m-%d")
    new_date = date_object - timedelta(days=days)
    return new_date.strftime("%Y-%m-%d")


def add_n_days(date_string: str, days: int) -> str:
    date_object = datetime.strptime(date_string, "%Y-%m-%d")
    new_date = date_object + timedelta(days=days)
    return new_date.strftime("%Y-%m-%d")
