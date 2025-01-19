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


def get_date_one_month_ago(date_format: str = "%Y-%m-%d") -> str:
    today = datetime.today()
    one_month_ago = today - timedelta(days=30)
    return one_month_ago.strftime(date_format)


def subtract_n_days(
        date_string: str,
        days: int,
        date_format: str = "%Y-%m-%d"
        ) -> str:
    date_object = datetime.strptime(date_string, date_format)
    new_date = date_object - timedelta(days=days)
    return new_date.strftime(date_format)


def add_n_days(
        date_string: str,
        days: int,
        date_format: str = "%Y-%m-%d"
        ) -> str:
    date_object = datetime.strptime(date_string, date_format)
    new_date = date_object + timedelta(days=days)
    return new_date.strftime(date_format)


def add_n_minutes(
        datetime_string: str,
        minutes: int,
        date_format: str = "%Y-%m-%dT%H:%M:%S"
        ) -> str:
    datetime_object = datetime.strptime(datetime_string, date_format)
    new_datetime = datetime_object + timedelta(minutes=minutes)
    return new_datetime.strftime(date_format)


def current_date(date_format: str = "%Y-%m-%d") -> str:
    return datetime.now().strftime(date_format)


def compare_dates(
        date1: str,
        date2: str,
        date_format: str = "%Y-%m-%d",
        comparation: str = "=="
        ) -> bool:
    """
    Check if date1 is less than or equal to date2.

    :param date1: The first date as a string.
    :param date2: The second date as a string.
    :param date_format: The format of the input dates (default: "%Y-%m-%d").
    :param comparation: ==, <, >, <=, >=
    :return: True if date1 <= date2, False otherwise.
    """
    d1 = datetime.strptime(date1, date_format)
    d2 = datetime.strptime(date2, date_format)
    if comparation == "==":
        return d1 == d2
    elif comparation == "<":
        return d1 < d2
    elif comparation == ">":
        return d1 > d2
    elif comparation == "<=":
        return d1 <= d2
    elif comparation == ">=":
        return d1 >= d2


def too_far_in_the_past_newsapi(json_response: dict) -> bool:
    if json_response["status"] == "error":
        return (
            json_response["message"][:54] ==
            "You are trying to request results too far in the past."
        )
    return False


def too_far_in_the_past_worldnewsapi(json_response: dict) -> bool:
    if "status" in json_response and json_response["status"] == "failure":
        return (
            json_response["message"][:65] ==
            "On the free and starter plan, you cannot look back further than 1"
        )
    return False
