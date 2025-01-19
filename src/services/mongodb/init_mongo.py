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


def initialize_database_logs(client: MongoClient) -> None:
    db = client["logs"]

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

            collection.create_index("the_date", unique=False)
            collection.create_index("loss", unique=False)
            print(f"Create new log table for: '{model_log_name}'")
        else:
            print(f"'{model_log_name}' already has the log table")


def initialize_database_data(client: MongoClient) -> None:
    db = client["data"]

    if "means" not in db.list_collection_names():
        db.create_collection("means", capped=False)
        collection = db["means"]

        collection.create_index("open", unique=False)
        collection.create_index("close", unique=False)
        collection.create_index("high", unique=False)
        collection.create_index("low", unique=False)
        collection.create_index("vol", unique=False)
        collection.create_index("temperature", unique=False)
        collection.create_index("rain", unique=False)
        collection.create_index("sun", unique=False)
        collection.create_index("sentiment", unique=False)
        collection.create_index("language", unique=False)
        print("Create new data table: 'means'")
    else:
        print("'means' table exists")

    if "real_open" not in db.list_collection_names():
        db.create_collection("real_open", capped=False)
        collection = db["real_open"]

        collection.create_index("record_date", unique=False)
        collection.create_index("open", unique=False)
        print("Create new data table: 'real_open'")
    else:
        print("'real_open' table exists")

    if "predicted_open" not in db.list_collection_names():
        db.create_collection("predicted_open", capped=False)
        collection = db["predicted_open"]

        collection.create_index("the_date", unique=False)
        collection.create_index("open", unique=False)
        print("Create new data table: 'predicted_open'")
    else:
        print("'predicted_open' table exists")

    if "weather" not in db.list_collection_names():
        db.create_collection("weather", capped=False)
        collection = db["weather"]

        collection.create_index("the_date", unique=False)
        collection.create_index("temperature", unique=False)
        collection.create_index("rain", unique=False)
        collection.create_index("sun", unique=False)
        print("Create new data table: 'weather'")
    else:
        print("'weather' table exists")

    if "news" not in db.list_collection_names():
        db.create_collection("news", capped=False)
        collection = db["news"]

        collection.create_index("record_date", unique=False)
        collection.create_index("source", unique=False)
        collection.create_index("count", unique=False)
        print("Create new data table: 'news'")
    else:
        print("'news' table exists")


def initialize_collections() -> None:
    client = MongoClient("mongodb://admin:admin123@mongodb:27017/")

    initialize_database_logs(client)
    initialize_database_data(client)

    print("Collections and indexes have been initialized.")
    client.close()


if __name__ == "__main__":
    initialize_collections()
