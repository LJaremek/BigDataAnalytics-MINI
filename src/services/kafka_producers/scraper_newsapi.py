import json
import time
import os

from kafka import KafkaProducer
from dotenv import load_dotenv
import requests

from tools import get_kafka_producer, get_date_one_month_ago, add_n_minutes
from mongodb_logging import get_last_scraper_end_date, add_new_scraper_log
from data_processing.scraping.news_newsapi import (
    newsapi_generate_url, newsapi_parse_articles
)

SCRAPER_NAME = "scraper_news_newsapi"
MINUTES = 15

if __name__ == "__main__":
    load_dotenv()

    api_key = os.getenv("NEWSAPI_API_KEY")
    key_words = ["cocoa", "Ivory Coast"]

    producer = get_kafka_producer()

    last_end_date = get_last_scraper_end_date(SCRAPER_NAME)

    if last_end_date is None:
        date_start = get_date_one_month_ago("%Y-%m-%dT%H:%M:%S")
    else:
        date_start = last_end_date
    date_end = add_n_minutes(date_start, MINUTES)

    # TODO: na produkcji usunąć 'while True' i zrobić cron-job
    while True:
        # url = newsapi_generate_url(key_words, date_start, date_end, api_key)

        # response = requests.get(url)
        # articles = json.loads(response.text)["articles"]
        # parsed_articles = newsapi_parse_articles(articles)

        # data = {
        #     "source": SCRAPER_NAME,
        #     "news": parsed_articles,
        #     "time": time.strftime("%Y-%d-%m %I:%M:%S")
        # }

        with open("newsapi.json", "r") as file:
            content = file.read().replace("\\", "")
        data = json.loads(rf"{content}")

        producer.send("scraped_data", value=data)

        count = len(data["news"])
        add_new_scraper_log(SCRAPER_NAME, date_start, date_end, count)

        date_end = date_start
        date_start = add_n_minutes(date_start, 15)

        # time.sleep(60*MINUTES)
        time.sleep(10)
