import json
import time

from hdfs import InsecureClient
from dotenv import load_dotenv

from tools import get_kafka_consumer


def create_dir_if_not_exists(
        hdfs_client: InsecureClient,
        hdfs_directory: str
        ) -> None:

    print("Checking...")
    if not hdfs_client.status(hdfs_directory, strict=False):
        hdfs_client.makedirs(hdfs_directory)
        print("Create")


if __name__ == "__main__":
    load_dotenv()

    batch_size = 10

    hdfs_client = InsecureClient("http://namenode:9870", user="root")
    create_dir_if_not_exists(hdfs_client, "/data")

    kafka_consumer = get_kafka_consumer("batch")

    batch = []
    for message in kafka_consumer:
        batch.append(json.loads(message.value.decode("utf-8")))

        if len(batch) >= batch_size:
            the_time = time.strftime("%Y_%d_%m-%I_%M")
            hdfs_path = f"/data/batch_{the_time}.json"

            with hdfs_client.write(hdfs_path, encoding="utf-8") as writer:
                json.dump(batch, writer)

            batch = []
