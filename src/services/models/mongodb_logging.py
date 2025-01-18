import os

from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd


def get_last_model_log(model_name: str) -> str | None:
    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")

    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["logs"]
    collection = db[f"{model_name}_logs"]

    latest_record = collection.find_one(sort=[("the_date", -1)])
    client.close()

    if latest_record is not None:
        return str(latest_record["the_date"])
    return None


def add_new_model_log(
        model_name: str,
        the_date: str,
        loss: float
        ) -> None:

    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")

    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["logs"]
    collection = db[f"{model_name}_logs"]

    collection.insert_one({
        "the_date": the_date,
        "loss": loss
    })

    client.close()


def set_data_means(dataframe: pd.DataFrame) -> None:

    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")
    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["data"]
    collection = db["means"]

    mean_values = {
        "open": dataframe["open"].iloc[-7:].mean(),
        "close": dataframe["close"].iloc[-7:].mean(),
        "high": dataframe["high"].iloc[-7:].mean(),
        "low": dataframe["low"].iloc[-7:].mean(),
        "vol": dataframe["vol"].iloc[-7:].mean(),
        "temperature": dataframe["temperature"].iloc[-7:].mean(),
        "rain": dataframe["rain"].iloc[-7:].mean(),
        "sun": dataframe["sun"].iloc[-7:].mean(),
        "sentiment": dataframe["sentiment"].iloc[-7:].median(),
        "language": dataframe["language"].iloc[-7:].median()
    }

    collection.replace_one(
        {"_id": "means"},
        {"_id": "means", **mean_values},
        upsert=True
        )

    client.close()


def get_data_means() -> dict[str, float]:
    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")

    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["data"]
    collection = db["means"]

    record = collection.find_one({"_id": "means"})

    if record is None:
        means = {}
    else:
        record.pop("_id", None)
        means = record

    client.close()

    return means
