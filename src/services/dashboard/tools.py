import os
from pymongo import MongoClient
from dotenv import load_dotenv


def get_data_means():
    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")

    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["data"]
    collection = db["means"]

    record = collection.find_one({"_id": "means"})

    if record is None:
        return {}

    record.pop("_id", None)
    client.close()
    return record
