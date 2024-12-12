from pymongo import MongoClient

SCRAPER_NAMES = (
    "scraper_weather_openmeteo",
    "scraper_news_newsapi"
)


def initialize_collections() -> None:
    client = MongoClient("mongodb://admin:admin123@mongodb:27017/")
    db = client["logs"]  # Specify the database

    for scraper_name in SCRAPER_NAMES:
        scraper_log_name = f"{scraper_name}_logs"

        if scraper_log_name not in db.list_collection_names():
            db.create_collection(scraper_log_name, capped=False)
            collection = db[scraper_log_name]

            collection.create_index("start_date", unique=False)
            collection.create_index("end_date", unique=False)
            collection.create_index("record_count", unique=False)
            print(f"Create new log table for: '{scraper_log_name}'")
        else:
            print(f"'{scraper_log_name}' already has the log table")

    print("Collections and indexes have been initialized.")
    client.close()


if __name__ == "__main__":
    initialize_collections()
