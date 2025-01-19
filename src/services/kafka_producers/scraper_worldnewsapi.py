import json
import time
import os

from dotenv import load_dotenv
import requests

from data_processing.scraping.worldnewsapi_news import (
    worldnewsapi_generate_url, worldnewsapi_parse_news
)
from tools import (
    get_kafka_producer, get_date_one_month_ago, add_n_minutes, add_n_days,
    too_far_in_the_past_worldnewsapi, compare_dates, current_date
)
from mongodb_logging import get_last_scraper_end_date, add_new_scraper_log

SCRAPER_NAME = "scraper_news_worldnewsapi"
DATE_FORMAT = "%Y-%m-%d %I:%M:%S"
MINUTES = 5


if __name__ == "__main__":
    load_dotenv()

    DEBUG_MODE = bool(int(os.getenv("DEBUG_MODE")))
    print("[START] Mode:", DEBUG_MODE)

    if not DEBUG_MODE:
        api_key = os.getenv("WORLDNEWSAPI_API_KEY")
        key_word = "Africa"

    producer = get_kafka_producer()

    last_end_date = get_last_scraper_end_date(SCRAPER_NAME)

    if last_end_date is None:
        date_start = get_date_one_month_ago(DATE_FORMAT)
    else:
        date_start = last_end_date
    print("[START]", date_start)
    date_end = add_n_minutes(date_start, MINUTES, DATE_FORMAT)

    running = compare_dates(
        current_date(DATE_FORMAT), date_start, DATE_FORMAT, ">="
        )

    if not running:
        print(f"Everything is up to date. Last date: {date_start}.", end=" ")
        print(f"Current date: {current_date(DATE_FORMAT)}")

    while running:
        print("Time:", date_start, date_end)
        if not DEBUG_MODE:
            url = worldnewsapi_generate_url(
                key_word, api_key,
                date_start, date_end,
                5.46, 6.36, 100
                )

            response = requests.get(url)
            json_response = json.loads(response.text)
            print(json_response)

            if too_far_in_the_past_worldnewsapi(json_response):
                date_start = add_n_days(date_start, 1, DATE_FORMAT)
                print("[i] Too far in the past. Add 1 day")
                continue

            articles = json_response["news"]
            parsed_articles = worldnewsapi_parse_news(articles)

            data = {
                "source": "scraper_news_worldnewsapi",
                "news": parsed_articles,
                "time": time.strftime(DATE_FORMAT),
                "date_start": date_start,
                "date_end": date_end,
                "date_format": DATE_FORMAT
            }
        else:
            print("MOCK DATA WARNING")
            with open("worldnewsapi.json", "r") as file:
                content = file.read().replace("\\", "")
            data = json.loads(rf"{content}")

        print(data.keys())
        producer.send("scraped_data", value=data)

        count = len(data["news"])
        add_new_scraper_log(SCRAPER_NAME, date_start, date_end, count)

        date_start = date_end
        date_end = add_n_minutes(date_start, MINUTES, DATE_FORMAT)

        if compare_dates(
                current_date(DATE_FORMAT), date_start, DATE_FORMAT, "<="
                ):
            running = False
            print("[INFO] running = False")

        time.sleep(60*MINUTES)
