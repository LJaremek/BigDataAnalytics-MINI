import json
import time
import os

from dotenv import load_dotenv
from xtb import XTB

from data_processing.scraping.news_xtb import xtb_parse_news
from tools import get_kafka_producer


if __name__ == "__main__":
    load_dotenv()

    api_key = os.getenv("NEWSAPI_API_KEY")
    key_words = ["kakao"]

    producer = get_kafka_producer()

    while True:
        # xtb = XTB(
        #     os.getenv("XTB_USER_ID"),
        #     os.getenv("XTB_PASSWORD")
        # )
        # xtb.login()

        date_start = "2024-11-01"  # TODO: automatyczna data
        date_end = "2024-11-25"  # TODO: automatyczna data
        # news = xtb.get_news(date_start, date_end)["returnData"]

        # parsed_articles = xtb_parse_news(key_words, news)

        # the_time = time.strftime("%Y-%d-%m %I:%M:%S")
        # data = {
        #     "source": "scraper_news_xtb",
        #     "news": parsed_articles,
        #     "time": the_time
        # }

        with open("xtb.json", "r") as file:
            data = json.loads(file.read())
            
        data["time"] = time.strftime("%Y-%d-%m %I:%M:%S")
        producer.send("scraped_data", value=data)

        # time.sleep(15*60)
        time.sleep(10)
