import json
import time

from data_processing.scraping.weather_openmeteo import get_cocoa_weather
from tools import get_kafka_producer


if __name__ == "__main__":
    producer = get_kafka_producer()

    while True:
        date_start = "2024-11-01"  # TODO: automatyczna data
        date_end = "2024-11-25"  # TODO: automatyczna data
        
        # parsed_articles = xtb_parse_news(key_words, news)

        # the_time = time.strftime("%Y-%d-%m %I:%M:%S")
        # data = {
        #     "source": "scraper_news_xtb",
        #     "news": parsed_articles,
        #     "time": the_time
        # }

        with open("xtb.json", "r") as file:
            data = json.loads(file.read())

        producer.send("scraped_data", value=data)

        # time.sleep(15*60)
        time.sleep(10)
