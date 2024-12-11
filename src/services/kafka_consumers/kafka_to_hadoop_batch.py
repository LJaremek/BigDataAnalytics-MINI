import json
import time

from hdfs import InsecureClient
from dotenv import load_dotenv
from fastavro import writer

from tools import BATCH_LIMITS, AVRO_SCHEMAS
from tools import get_kafka_consumer, Batch


def create_dir_if_not_exists(
        hdfs_client: InsecureClient,
        hdfs_directory: str
        ) -> None:

    res = hdfs_client.status(hdfs_directory, strict=False)
    if not res:
        hdfs_client.makedirs(hdfs_directory)


if __name__ == "__main__":
    load_dotenv()

    batch_size = 10

    hdfs_client = InsecureClient("http://namenode:50070", user="root")
    create_dir_if_not_exists(hdfs_client, "/data")

    kafka_consumer = get_kafka_consumer("batch")

    batches: dict[str, Batch] = {}
    while True:
        for message in kafka_consumer:
            new_record = json.loads(message.value.decode("utf-8"))
            source = new_record["source"]
            del new_record["source"]
            print("Soruce:", source)

            if source not in batches:
                batches[source] = Batch()
            batches[source].append(new_record)

            batch_size = batches[source].size
            batch_limit = BATCH_LIMITS[source.split("_")[1]]

            if batch_size >= batch_limit:
                print(f"New '{source}' batch! Records: {batches[source].size}")
                the_time = time.strftime("%Y_%m_%d-%I_%M_%S")
                hdfs_path = f"/data/batch_{source}/{the_time}.avro"

                with hdfs_client.write(hdfs_path, encoding=None) as w_output:
                    schema = AVRO_SCHEMAS[source]
                    avro_data = batches[source].records
                    writer(w_output, schema, avro_data)

                batches[source].reset()
