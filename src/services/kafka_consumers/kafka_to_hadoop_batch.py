import requests
import json
import time

from hdfs.util import HdfsError
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


def is_safemode_on() -> bool:
    url = "http://namenode:50070/safemode"  # WebHDFS NameNode adress
    response = requests.get(url)
    return response.text.lower().strip() == "on"


if __name__ == "__main__":
    load_dotenv()

    batch_size = 10
    ATTEMPTS = 5

    hdfs_client = InsecureClient("http://namenode:50070", user="root")
    create_dir_if_not_exists(hdfs_client, "/data")
    create_dir_if_not_exists(hdfs_client, "/eda_tmp")

    kafka_consumer = get_kafka_consumer("batch")

    print("SAFE MODE:", is_safemode_on())

    batches: dict[str, Batch] = {}
    while True:
        for message in kafka_consumer:
            new_records = json.loads(message.value.decode("utf-8"))
            source = new_records["source"]
            date_start = new_records["date_start"]
            date_end = new_records["date_end"]
            print("Soruce:", source)
            del new_records["source"]
            del new_records["date_format"]

            if source not in batches:
                batches[source] = Batch()
            batches[source].append(new_records, date_start, date_end)

            batch_size = batches[source].size
            batch_limit = BATCH_LIMITS[source.split("_")[1]]

            if batch_size >= batch_limit:
                print(f"New '{source}' batch! Records: {batches[source].size}")
                the_time = time.strftime("%Y_%m_%d-%I_%M_%S")
                month = the_time[:7]
                hdfs_path = f"/data/batch_{source}/{month}/{the_time}.avro"

                if is_safemode_on():
                    print("SAFE MODE IS ON")
                    time.sleep(10)

                for _ in range(ATTEMPTS):
                    try:
                        with (
                            hdfs_client.write(hdfs_path, encoding=None)
                            as w_output
                        ):
                            schema = AVRO_SCHEMAS[source]
                            avro_data = batches[source].records
                            print(avro_data)
                            writer(w_output, schema, avro_data)
                        batches[source].reset()
                        break
                    except HdfsError as e:
                        print("[ERROR]", str(e))
                        time.sleep(5)
