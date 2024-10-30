import json
import time
import os

from kafka import KafkaProducer
from dotenv import load_dotenv
from xtb import XTB

from data_processing.scrapping.news_xtb import xtb_parse_news


if __name__ == "__main__":
    load_dotenv()

    api_key = os.getenv("NEWSAPI_API_KEY")
    key_words = ["kakao"]

    producer = KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

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

        data = {
            "source": "scraper_news_xtb",
            "news": parsed_articles,
            "time": time.strftime("%Y-%d-%m %I:%M:%S")
        }

        producer.send("scraped_data", value=data)

        time.sleep(15*60)
