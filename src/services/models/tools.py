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
