from kafka import KafkaConsumer


def get_kafka_consumer(group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        "scraped_data",
        bootstrap_servers="kafka:9092",
        group_id=group_id,
        auto_offset_reset="earliest",
        api_version=(3, 5, 0)
    )
