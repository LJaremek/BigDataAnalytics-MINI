import json
import time
import os

from dotenv import load_dotenv
from xtb import XTB

from tools import get_kafka_producer, get_date_one_month_ago, add_n_days
from mongodb_logging import get_last_scraper_end_date, add_new_scraper_log
from data_processing.scraping.news_xtb import xtb_parse_news
from tools import current_date, compare_dates


SCRAPER_NAME = "scraper_news_xtb"
DATE_FORMAT = "%Y-%m-%d"
MINUTES = 5
DAYS = 1


if __name__ == "__main__":
    load_dotenv()

    DEBUG_MODE = bool(int(os.getenv("DEBUG_MODE")))
    print("[START] Mode:", DEBUG_MODE)

    api_key = os.getenv("NEWSAPI_API_KEY")
    key_words = ["Afryka"]

    producer = get_kafka_producer()

    last_end_date = get_last_scraper_end_date(SCRAPER_NAME)

    if last_end_date is None:
        date_start = get_date_one_month_ago(DATE_FORMAT)
    else:
        date_start = last_end_date
    date_end = add_n_days(date_start, DAYS, DATE_FORMAT)

    running = compare_dates(
        current_date(DATE_FORMAT), date_start, DATE_FORMAT, ">="
        )

    if not running:
        print(f"Everything is up to date. Last date: {date_start}.", end=" ")
        print(f"Current date: {current_date(DATE_FORMAT)}")

    while running:
        print("Time:", date_start, date_end)
        if not DEBUG_MODE:
            xtb = XTB(
                os.getenv("XTB_USER_ID"),
                os.getenv("XTB_PASSWORD")
            )
            xtb.login()

            news = xtb.get_news(date_start, date_end)["returnData"]

            parsed_articles = xtb_parse_news(key_words, news)

            the_time = time.strftime(DATE_FORMAT)
            data = {
                "source": SCRAPER_NAME,
                "news": parsed_articles,
                "time": the_time,
                "date_start": date_start,
                "date_end": date_end,
                "date_format": DATE_FORMAT
            }
        else:
            with open("xtb.json", "r") as file:
                data = json.loads(file.read())

        producer.send("scraped_data", value=data)

        count = len(data["news"])
        add_new_scraper_log(SCRAPER_NAME, date_start, date_end, count)

        date_start = date_end
        date_end = add_n_days(date_start, DAYS, DATE_FORMAT)

        if compare_dates(
                current_date(DATE_FORMAT), date_start, DATE_FORMAT, "<="
                ):
            running = False
            print("[INFO] running = False")

        time.sleep(60*MINUTES)
