from fastavro.schema import load_schema
from kafka import KafkaConsumer

BATCH_LIMITS = {
    "news": 5,
    "stock": 10,
    "weather": 1
}

AVRO_SCHEMAS = {
    "scraper_news_worldnewsapi":
        load_schema("avro_schemas/news_worldnewsapi.avsc"),

    "scraper_news_newsapi":
        load_schema("avro_schemas/news_newsapi.avsc"),

    "scraper_news_xtb":
        load_schema("avro_schemas/news_xtb.avsc"),

    "scraper_stock_xtb":
        load_schema("avro_schemas/stock_xtb.avsc"),

    "scraper_weather_openmeteo":
        load_schema("avro_schemas/weather_openmeteo.avsc"),
}


def add_dates_to_records(
        records: list[dict],
        date_start: str,
        date_end: str
        ) -> list[dict]:

    for record in records:
        record["date_start"] = date_start
        record["date_end"] = date_end

    return records


class Batch:
    def __init__(self, size: int = 0, records: list = None) -> None:
        self.size = size
        if records is None:
            self.records = []
        else:
            self.records = records

    def append(self, new_record: dict, date_start: str, date_end: str) -> None:
        if "news" in new_record:
            self.records += add_dates_to_records(
                new_record["news"], date_start, date_end
                )
        elif "candlesticks" in new_record:
            self.records += add_dates_to_records(
                new_record["candlesticks"], date_start, date_end
                )
        elif "weather" in new_record:
            self.records += add_dates_to_records(
                new_record["weather"], date_start, date_end
                )
        else:
            msg = "Uknown record data. Available: news, candlesticks, weather"
            raise Exception(msg)

        self.size += 1

    def reset(self) -> None:
        self.records = []
        self.size = 0


def get_kafka_consumer(group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        "scraped_data",
        bootstrap_servers="kafka:9092",
        group_id=group_id,
        auto_offset_reset="earliest",
        api_version=(3, 5, 0)
    )
