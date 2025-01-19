from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
    LongType,
    DecimalType,
    DateType,
)
import requests


WATERMARK_TIME = "2 seconds"

spark = SparkSession.builder.appName("KafkaSpeedLayerIngestion").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kafka_brokers = "kafka:9092"
kafka_topic_in = "scraped_data"
kafka_topic_out = "prediction_data"

single_news_schema = StructType(
    [StructField("data", StringType(), False), StructField("date", DateType(), True)]
)
single_stock_schema = StructType(
    [
        StructField("ctm", LongType(), False),
        StructField("ctmString", StringType(), True),
        StructField("open", DecimalType(10, 2), True),
        StructField("close", DecimalType(10, 2), True),
        StructField("high", DecimalType(10, 2), True),
        StructField("low", DecimalType(10, 2), True),
        StructField("vol", DecimalType(10, 2), True),
    ]
)
json_schema = StructType(
    [
        StructField("source", StringType(), False),
        StructField("news", ArrayType(single_news_schema), True),
        StructField("candlesticks", ArrayType(single_stock_schema), True),
        StructField("time", StringType(), False),
    ]
)
raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_brokers)
    .option("subscribe", kafka_topic_in)
    .load()
)
string_df = raw_df.selectExpr("CAST(value AS STRING) as json_string")

parsed_df = string_df.select(
    F.from_json(F.col("json_string"), json_schema).alias("data"),
    F.to_timestamp("data.time").alias("watermark"),
).select("data.*", "watermark")

news_df = (
    parsed_df.filter(F.col("source") != F.lit("scraper_stock_xtb"))
    .select(F.explode("news").alias("single_news"), "watermark")
    .select(
        "single_news", F.to_date("single_news.date").alias("parsed_date"), "watermark"
    )
).withWatermark("watermark", WATERMARK_TIME)
stock_df = (
    parsed_df.filter(F.col("source") == F.lit("scraper_stock_xtb"))
    .select(F.explode("candlesticks").alias("single_candle"), "watermark")
    .select(
        "single_candle",
        F.to_timestamp("single_candle.ctmString").alias("parsed_date"),
        "watermark",
    )
).withWatermark("watermark", WATERMARK_TIME)

news_w_df = news_df.groupBy(
    F.window("watermark", "10 seconds", "3 seconds").alias("window")
).agg(F.collect_list("single_news").alias("news_list"))
stock_w_df = stock_df.groupBy(
    F.window("watermark", "10 seconds", "3 seconds").alias("window")
).agg(F.collect_list("single_candle").alias("stock_list"))

# spark doesn't like imports
import json
import time

from kafka import KafkaProducer


def __create_kafka_producer() -> KafkaProducer:
    retries = 5
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=["kafka:9092"],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("Connected to Kafka!")
            return producer
        except Exception as e:
            print(f"Kafka connection attempt {i + 1} failed: {e}")
            time.sleep(3)

    raise Exception("Failed to connect to Kafka after multiple attempts")


_producer = None


def get_kafka_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = __create_kafka_producer()
    return _producer


def send_to_kafka(data):
    producer = get_kafka_producer()
    producer.send(topic=kafka_topic_out, value=data.value)


joined_w_df = news_w_df.join(
    stock_w_df, news_w_df.window.start == stock_w_df.window.start, "inner"
).select("news_list", "stock_list")

query = (
    joined_w_df.select(
        F.struct(["news_list", "stock_list"]).cast("string").alias("value")
    )
    .writeStream.foreach(send_to_kafka)
    .option("checkpointLocation", "./checkpoint")
    # .start()
)

def send_http_request(row):
    try:
        response = requests.get(f"http://model_api:8000/predict/?open={row['single_candle']['open']}&close=11450.0&high={row['single_candle']['close']}&low={row['single_candle']['low']}&vol={row['single_candle']['vol']}")
        print(f"Response: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"Error: {e}")


query_predict = stock_df.writeStream.foreach(send_http_request).option("checkpointLocation", "./checkpoint").start()

query_predict.awaitTermination()
