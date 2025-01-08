import time
import os

from dotenv import load_dotenv

from mongodb_logging import get_last_scraper_end_date, add_new_scraper_log
from tools import get_kafka_producer, get_date_one_month_ago, add_n_days
from data_processing.scraping.weather_openmeteo import get_weather
from tools import current_date, compare_dates


SCRAPER_NAME = "scraper_weather_openmeteo"
DATE_FORMAT = "%Y-%m-%d"
MINUTES = 5
DAYS = 1


if __name__ == "__main__":  # CRON JOB
    load_dotenv()

    DEBUG_MODE = bool(int(os.getenv("DEBUG_MODE")))
    print("[START] Mode:", DEBUG_MODE)

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

    while running:
        weather = get_weather(date_start, date_end, "cocoa")
        the_time = time.strftime(DATE_FORMAT)
        data = {
            "source": SCRAPER_NAME,
            "weather": [weather],
            "time": the_time,
            "date_start": date_start,
            "date_end": date_end,
            "date_format": DATE_FORMAT
        }
        print(data.keys())

        producer.send("scraped_data", value=data)

        add_new_scraper_log(SCRAPER_NAME, date_start, date_end, 1)

        date_start = date_end
        date_end = add_n_days(date_start, DAYS, DATE_FORMAT)

        if compare_dates(
                current_date(DATE_FORMAT), date_start, DATE_FORMAT, "<="
                ):
            running = False
            print("[INFO] running = False")

        time.sleep(60*MINUTES)
