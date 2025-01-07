import os

from pymongo import MongoClient
from dotenv import load_dotenv


def get_last_model_log(model_name: str) -> str | None:
    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")

    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["logs"]
    collection = db[f"{model_name}_logs"]

    latest_record = collection.find_one(sort=[("end_datetime", -1)])
    client.close()

    if latest_record is not None:
        return str(latest_record["end_datetime"])
    return None


def add_new_model_log(
        model_name: str,
        start_datetime: str,
        end_datetime: str,
        param_name: str,
        param_value: float
        ) -> None:

    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")

    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["logs"]
    collection = db[f"{model_name}_logs"]

    collection.insert_one({
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "param_name": param_name,
        "param_value": param_value
    })

    client.close()
