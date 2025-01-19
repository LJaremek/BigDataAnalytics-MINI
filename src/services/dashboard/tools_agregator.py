import os

from pymongo import MongoClient
from hdfs import InsecureClient
from dotenv import load_dotenv
import pandas as pd
import fastavro


def log_real_open(date: str, value: float) -> None:
    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")
    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["data"]
    collection = db["real_open"]

    collection.insert_one({
        "the_date": date,
        "open": value
    })


def log_weather(
        date: str, temperature: float, rain: float, sun: float
        ) -> None:
    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")
    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["data"]
    collection = db["weather"]

    collection.insert_one({
        "the_date": date,
        "temperature": temperature,
        "rain": rain,
        "sun": sun
    })


def add_news_to_mongo(dataframe: pd.DataFrame) -> None:
    """
    Adds records from a DataFrame to MongoDB.
    Each row in the DataFrame must have:
        'the_date', 'source', and 'count' columns.

    :param dataframe: pd.DataFrame with columns ['the_date', 'source', 'count']
    """
    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")

    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["data"]
    collection = db["news"]

    records = dataframe.to_dict(orient="records")
    if records:
        collection.insert_many(records)

    client.close()


def add_weather_to_mongo(dataframe: pd.DataFrame) -> None:
    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")

    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["data"]
    collection = db["weather"]

    records = dataframe.to_dict(orient="records")
    if records:
        collection.insert_many(records)

    client.close()


def add_stock_to_mongo(dataframe: pd.DataFrame) -> None:
    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")

    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["data"]
    collection = db["real_open"]

    records = dataframe.to_dict(orient="records")
    if records:
        collection.insert_many(records)

    client.close()


def get_mongo_news() -> pd.DataFrame:
    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")

    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["data"]
    collection = db["news"]

    records = list(collection.find(
        {},
        {"_id": 0, "record_date": 1, "source": 1, "count": 1}
        ))
    client.close()

    return pd.DataFrame(records)


def get_mongo_weather() -> pd.DataFrame:
    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")

    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["data"]
    collection = db["weather"]

    records = list(collection.find(
        {},
        {"_id": 0, "record_date": 1, "temperature": 1, "rain": 1, "sun": 1}
        ))
    client.close()

    return pd.DataFrame(records)


def get_mongo_stock() -> pd.DataFrame:
    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")

    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["data"]
    collection = db["real_open"]

    records = list(collection.find({}, {
        "_id": 0, "record_date": 1, "open": 1
        }))
    client.close()

    return pd.DataFrame(records)


def collect_avro_files_to_dataframe(
        hdfs_client: InsecureClient,
        base_folder: str
        ) -> pd.DataFrame:
    """
    Zbiera wszystkie pliki Avro z podfolderów danego folderu na HDFS
        i buduje ramkę danych.

    :param hdfs_client: Klient HDFS (InsecureClient).
    :param base_folder: Ścieżka do folderu na HDFS.
    :return: DataFrame zawierający dane z wszystkich plików Avro.
    """
    all_dataframes = []

    try:
        for root, dirs, files in hdfs_client.walk(base_folder):
            for file in files:
                if file.endswith(".avro"):
                    avro_path = os.path.join(root, file)
                    try:
                        with hdfs_client.read(avro_path) as avro_reader:
                            records = list(fastavro.reader(avro_reader))
                            if records:
                                df = pd.DataFrame(records)
                                all_dataframes.append(df)
                    except Exception as e:
                        print(f"Nie udało się odczytać pliku {avro_path}: {e}")
    except Exception as e:
        print(f"Nie udało się odczytać folderu {base_folder}: {e}")

    if all_dataframes:
        full_dataframe = pd.concat(all_dataframes, ignore_index=True)
    else:
        full_dataframe = pd.DataFrame()

    return full_dataframe


def add_record_date(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
    df['record_date'] = df[date_column].apply(lambda x: str(x)[:10])

    df['record_date'] = pd.to_datetime(
        df["record_date"]
    ).dt.strftime('%Y-%m-%d')

    return df


def find_records_to_add(
        hdfs_df: pd.DataFrame,
        mongo_df: pd.DataFrame,
        merge_on: list[str]
        ) -> pd.DataFrame:

    if mongo_df.empty:
        return hdfs_df

    merged_df = hdfs_df.merge(
        mongo_df,
        on=merge_on,
        how="left",
        indicator=True
        )

    records_to_add = merged_df[
        merged_df["_merge"] == "left_only"
    ].drop(columns="_merge")

    return records_to_add


def get_predicted_and_real_open() -> pd.DataFrame:
    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")

    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["data"]

    predicted_data = pd.DataFrame(list(db["predicted_open"].find(
        {}, {"_id": 0, "record_date": 1, "open": 1}))
        )
    predicted_data.rename(columns={"open": "predicted_open"}, inplace=True)

    real_data = pd.DataFrame(list(db["real_open"].find(
        {}, {"_id": 0, "record_date": 1, "open": 1}))
        )
    real_data.rename(columns={"open": "real_open"}, inplace=True)

    if predicted_data.empty and real_data.empty:
        return pd.DataFrame(
            columns=["record_date", "predicted_open", "real_open"]
            )

    if predicted_data.empty:
        real_data["predicted_open"] = None
        return real_data[["record_date", "predicted_open", "real_open"]]

    if real_data.empty:
        predicted_data["real_open"] = None
        return predicted_data[["record_date", "predicted_open", "real_open"]]

    combined_data = pd.merge(
        predicted_data, real_data, on="record_date", how="outer"
        )

    combined_data["record_date"] = pd.to_datetime(combined_data["record_date"])

    combined_data.sort_values(by="record_date", inplace=True)

    return combined_data


def get_weather_from_logs() -> pd.DataFrame:
    load_dotenv()
    root_name = os.getenv("ROOT_NAME")
    root_pswd = os.getenv("ROOT_PSWD")

    client = MongoClient(f"mongodb://{root_name}:{root_pswd}@mongodb:27017/")
    db = client["logs"]

    newsapi = pd.DataFrame(list(db["scraper_news_newsapi_logs"].find(
        {}, {"_id": 0, "start_date": 1, "record_count": 1})))
    newsapi["source"] = "newsapi"
    if not newsapi.empty:
        newsapi = newsapi.sort_values(by="start_date")

    worldnewsapi = pd.DataFrame(list(db["scraper_news_worldnewsapi_logs"].find(
        {}, {"_id": 0, "start_date": 1, "record_count": 1})))
    worldnewsapi["source"] = "worldnewsapi"
    if not worldnewsapi.empty:
        worldnewsapi = worldnewsapi.sort_values(by="start_date")

    xtb = pd.DataFrame(list(db["scraper_news_xtb_logs"].find(
        {}, {"_id": 0, "start_date": 1, "record_count": 1})))
    xtb["source"] = "xtb"
    if not xtb.empty:
        xtb = xtb.sort_values(by="start_date")

    return pd.concat([newsapi, worldnewsapi, xtb])
