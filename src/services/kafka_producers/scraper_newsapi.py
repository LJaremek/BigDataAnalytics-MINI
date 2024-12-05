import json
import time
import os

from kafka import KafkaProducer
from dotenv import load_dotenv
import requests

from data_processing.scraping.news_newsapi import (
    newsapi_generate_url, newsapi_parse_articles
)
from tools import get_kafka_producer


if __name__ == "__main__":
    load_dotenv()

    api_key = os.getenv("NEWSAPI_API_KEY")
    key_words = ["cocoa", "Ivory Coast"]

    producer = get_kafka_producer()

    while True:
        # TODO: dodać wybór daty do generowania url
        url = newsapi_generate_url(key_words, api_key)

        # response = requests.get(url)
        # articles = json.loads(response.text)["articles"]
        # parsed_articles = newsapi_parse_articles(articles)
        parsed_articles = [{"data": "SOME DATA", "date": "2024-11-24"}]

        data = {
            "source": "scraper_news_newsapi",
            "news": parsed_articles,
            "time": time.strftime("%Y-%d-%m %I:%M:%S")
        }

        producer.send("scraped_data", value=data)

        # time.sleep(15*60)
        time.sleep(10)
