import json
import time
import os

from kafka import KafkaProducer
from dotenv import load_dotenv
from xtb import XTB

from data_processing.scraping.news_xtb import xtb_parse_news


def create_kafka_producer() -> KafkaProducer:
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
            time.sleep(3)

    raise Exception("Failed to connect to Kafka after multiple attempts")


if __name__ == "__main__":
    load_dotenv()

    api_key = os.getenv("NEWSAPI_API_KEY")
    key_words = ["kakao"]

    producer = create_kafka_producer()

    while True:
        xtb = XTB(
            os.getenv("XTB_USER_ID"),
            os.getenv("XTB_PASSWORD")
        )
        xtb.login()

        date_start = "2024-10-01"  # TODO: automatyczna data
        date_end = "2024-10-25"  # TODO: automatyczna data
        news = xtb.get_news(date_start, date_end)["returnData"]

        parsed_articles = xtb_parse_news(key_words, news)

        the_time = time.strftime("%Y-%d-%m %I:%M:%S")
        data = {
            "source": "scraper_news_xtb",
            "news": parsed_articles,
            "time": the_time
        }

        producer.send("scraped_data", value=data)
        print("New data:", the_time)

        time.sleep(15*60)
