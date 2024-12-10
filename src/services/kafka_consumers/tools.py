from kafka import KafkaConsumer


class Batch:
    def __init__(self, size: int = 0, records: list = None) -> None:
        self.size = size
        if records is None:
            self.records = []
        else:
            self.records = records
    
    def append(self, new_record: dict) -> None:
        if "news" in new_record:
            self.records += new_record["news"]
        elif "candlesticks" in new_record:
            self.records += new_record["candlesticks"]
        else:
            msg = "Uknown record data. Available: news, candlesticks"
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
