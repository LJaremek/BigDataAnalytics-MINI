from datetime import datetime, timedelta

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
        f"{hdfs_folder}/{file_status[1]['pathSuffix']}"
        for file_status in file_statuses
        if file_status[1]["type"] == "FILE"
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


def get_date_one_month_ago(date_format: str = "%Y-%m-%d") -> str:
    today = datetime.today()
    one_month_ago = today - timedelta(days=30)
    return one_month_ago.strftime(date_format)


def add_n_days(
        date_string: str,
        days: int,
        date_format: str = "%Y-%m-%d"
        ) -> str:
    date_object = datetime.strptime(date_string, date_format)
    new_date = date_object + timedelta(days=days)
    return new_date.strftime(date_format)
