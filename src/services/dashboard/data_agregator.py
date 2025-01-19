import requests
from time import sleep

from hdfs import InsecureClient
import pandas as pd

from tools_agregator import (
    collect_avro_files_to_dataframe, add_record_date, find_records_to_add,
    get_mongo_news, add_news_to_mongo,
    get_mongo_weather, add_weather_to_mongo,
    get_mongo_stock, add_stock_to_mongo,
)


MINUTES = 1


def is_safemode_on() -> bool:
    url = "http://namenode:50070/safemode"  # WebHDFS NameNode adress
    response = requests.get(url)
    return response.text.lower().strip() == "on"


def feed_news(hdfs_client: InsecureClient) -> int:
    df_news_newsapi = collect_avro_files_to_dataframe(
        hdfs_client, "/data/batch_scraper_news_newsapi"
    )
    df_news_newsapi["source"] = "newsapi"

    df_news_worldnewsapi = collect_avro_files_to_dataframe(
        hdfs_client, "/data/batch_scraper_news_worldnewsapi"
    )
    df_news_worldnewsapi["source"] = "worldnewsapi"

    df_news_xtb = collect_avro_files_to_dataframe(
        hdfs_client, "/data/batch_scraper_news_xtb"
    )
    df_news_xtb["source"] = "xtb"
    df_news_hdfs = add_record_date(
        pd.concat([df_news_newsapi, df_news_worldnewsapi, df_news_xtb]),
        "date"
    )

    df_news_hdfs = (
        df_news_hdfs.groupby(["record_date", "source"])
        .size()
        .reset_index(name="count")
    )

    df_news_mongo = get_mongo_news()
    new_records_news = find_records_to_add(
        df_news_hdfs, df_news_mongo, ["record_date", "source"]
        )
    add_news_to_mongo(new_records_news)

    return new_records_news.shape[0]


def feed_weather(hdfs_client: InsecureClient) -> int:
    df_weather_hdfs = add_record_date(
        collect_avro_files_to_dataframe(
            hdfs_client, "/data/batch_scraper_weather_openmeteo"
        ),
        "date_start"
    )
    df_weather_hdfs = (
        df_weather_hdfs.groupby(["record_date"])
        .agg({"temperature": "mean", "rain": "sum", "sun": "sum"})
        .reset_index()
    )

    df_weather_mongo = get_mongo_weather()
    new_records_weather = find_records_to_add(
        df_weather_hdfs, df_weather_mongo, ["record_date"]
        )
    add_weather_to_mongo(new_records_weather)

    return new_records_weather.shape[0]


def feed_stock(hdfs_client: InsecureClient) -> int:
    df_stock_hdfs = add_record_date(
        collect_avro_files_to_dataframe(
            hdfs_client, "/data/batch_scraper_stock_xtb"
        ),
        "date_start",
    )

    df_stock_mongo = get_mongo_stock()

    new_records_stock = find_records_to_add(
        df_stock_hdfs, df_stock_mongo, ["record_date"]
        )[["record_date", "open"]]
    print(df_stock_hdfs.shape, df_stock_hdfs.columns)
    print(df_stock_mongo.shape, df_stock_mongo.columns)
    print(new_records_stock.shape, new_records_stock.columns)
    add_stock_to_mongo(new_records_stock)

    return new_records_stock.shape[0]


if __name__ == "__main__":
    hdfs_client = InsecureClient("http://namenode:50070", user="root")

    running = True
    while running:
        if is_safemode_on():
            print("SAFE MODE IS ON")
            sleep(10)

        print("[News] New records:", feed_news(hdfs_client))
        print("[Weather] New records:", feed_weather(hdfs_client))
        print("[Stock] New records:", feed_stock(hdfs_client))

        sleep(MINUTES*60)
