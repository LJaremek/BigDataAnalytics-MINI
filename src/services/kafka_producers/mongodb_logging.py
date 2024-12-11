import os

from pymongo import MongoClient
from dotenv import load_dotenv


def get_last_scraper_end_date(scraper_name: str) -> str | None:
    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")

    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["logs"]
    collection = db[f"{scraper_name}_logs"]

    latest_record = collection.find_one(sort=[("end_date", -1)])
    client.close()
    
    if latest_record is not None:
        return str(latest_record["end_date"])
    return None


def add_new_scraper_log(
        scraper_name: str,
        start_date: str,
        end_date: str,
        record_count: int
        ) -> None:

    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")

    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["logs"]
    collection = db[f"{scraper_name}_logs"]

    collection.insert_one({
        "start_date": start_date,
        "end_date": end_date,
        "record_count": record_count
    })

    client.close()
