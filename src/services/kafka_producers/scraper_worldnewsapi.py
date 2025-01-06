import json
import time
import os

from dotenv import load_dotenv
import requests

from data_processing.scraping.worldnewsapi_news import (
    worldnewsapi_generate_url, worldnewsapi_parse_news
)
from tools import get_kafka_producer


if __name__ == "__main__":
    load_dotenv()

    api_key = os.getenv("WORLDNEWSAPI_API_KEY")
    key_word = "cocoa"

    producer = get_kafka_producer()

    while True:
        # INFO: max 30 days old
        start_date = "2024-11-15"  # TODO: automatyczna data
        end_date = "2024-11-18"  # TODO: automatyczna data

        # url = worldnewsapi_generate_url(
        #     key_word, api_key,
        #     start_date, end_date,
        #     5.46, 6.36, 100
        #     )

        # response = requests.get(url)
        # articles = json.loads(response.text)["news"]
        # parsed_articles = worldnewsapi_parse_news(articles)

        # data = {
        #     "source": "scraper_news_worldnewsapi",
        #     "news": parsed_articles,
        #     "time": time.strftime("%Y-%d-%m %I:%M:%S")
        # }

        with open("worldnewsapi.json", "r") as file:
            content = file.read().replace("\\", "")
        data = json.loads(rf"{content}")
        data["time"] = time.strftime("%Y-%d-%m %I:%M:%S")

        producer.send("scraped_data", value=data)

        # time.sleep(15*60)
        time.sleep(10)
