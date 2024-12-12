import time

from mongodb_logging import get_last_scraper_end_date, add_new_scraper_log
from tools import get_kafka_producer, get_date_one_month_ago, add_n_days
from data_processing.scraping.weather_openmeteo import get_weather


SCRAPER_NAME = "scraper_weather_openmeteo"

if __name__ == "__main__":
    producer = get_kafka_producer()

    last_end_date = get_last_scraper_end_date(SCRAPER_NAME)

    if last_end_date is None:
        date_start = get_date_one_month_ago()
    else:
        date_start = last_end_date
    date_end = add_n_days(date_start, 1)

    weather = get_weather(date_start, date_end, "cocoa")
    the_time = time.strftime("%Y-%d-%m %I:%M:%S")
    data = {
        "source": SCRAPER_NAME,
        "weather": [weather],
        "time": the_time
    }

    producer.send("scraped_data", value=data)

    add_new_scraper_log(SCRAPER_NAME, date_start, date_end, 1)
