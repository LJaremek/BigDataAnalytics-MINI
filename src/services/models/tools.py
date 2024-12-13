from hdfs import InsecureClient
import pandas as pd
import fastavro


def load_avro_from_hdfs(
        hdfs_client: InsecureClient,
        hdfs_path: str
        ) -> pd.DataFrame:
    with hdfs_client.read(hdfs_path) as reader:
        records = list(fastavro.reader(reader))
    return pd.DataFrame(records)


def prepare_dataframe_with_datetime(
        hdfs_client: InsecureClient,
        hdfs_folder: str
        ) -> pd.DataFrame:

    file_statuses = hdfs_client.list(hdfs_folder, status=True)
    avro_files = [
        f"{hdfs_folder}/{file_status['pathSuffix']}"
        for file_status in file_statuses
        if file_status["type"] == "FILE"
    ]

    dataframes = [
        load_avro_from_hdfs(hdfs_client, file)
        for file in avro_files
        ]
    full_data = pd.concat(dataframes, ignore_index=True)

    full_data["ctmString"] = pd.to_datetime(
        full_data["ctmString"],
        format="%b %d, %Y, %I:%M:%S %p"
        )

    return full_data
