from pymongo import MongoClient


def initialize_collections() -> None:
    client = MongoClient("mongodb://admin:admin123@mongodb:27017/")
    db = client["logs"]  # Specify the database

    if "scraper_weather_openmeteo_logs" not in db.list_collection_names():
        db.create_collection("scraper_weather_openmeteo_logs", capped=False)
        collection = db["scraper_weather_openmeteo_logs"]

        collection.create_index("start_date", unique=False)
        collection.create_index("end_date", unique=False)
        collection.create_index("record_count", unique=False)

    print("Collections and indexes have been initialized.")
    client.close()


if __name__ == "__main__":
    initialize_collections()
