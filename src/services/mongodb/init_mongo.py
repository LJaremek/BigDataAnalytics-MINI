from pymongo import MongoClient

SCRAPER_NAMES = (
    "scraper_weather_openmeteo",
    "scraper_news_newsapi",
    "scraper_news_xtb",
    "scraper_stock_xtb",
    "scraper_news_worldnewsapi"
)

MODEL_NAMES = (
    "lstm_stock_prediction",
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

    for model_name in MODEL_NAMES:
        model_log_name = f"{model_name}_logs"

        if model_log_name not in db.list_collection_names():
            db.create_collection(model_log_name, capped=False)
            collection = db[model_log_name]

            collection.create_index("start_datetime", unique=False)
            collection.create_index("end_datetime", unique=False)
            collection.create_index("param_name", unique=False)
            collection.create_index("param_value", unique=False)
            print(f"Create new log table for: '{model_log_name}'")
        else:
            print(f"'{model_log_name}' already has the log table")

    print("Collections and indexes have been initialized.")
    client.close()


if __name__ == "__main__":
    initialize_collections()
