from random import randint
import pickle

from hdfs import InsecureClient


class ModelMock:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def fit(self, *args, **kwargs) -> None:
        ...

    def predict(self, *args, **kwargs) -> float:
        return [
            randint(0, 10)/10
            for _ in range(5)
        ]


def load_model_from_hdfs(
        hdfs_client: InsecureClient,
        hdfs_path: str
        ) -> ModelMock:

    try:
        with hdfs_client.read(hdfs_path) as reader:
            model = pickle.load(reader)
        print("Model loaded successfully from HDFS.")
        return model
    except Exception as e:
        print(f"Failed to load model from HDFS: {e}")
        return ModelMock()


def save_model_to_hdfs(
        hdfs_client: InsecureClient,
        hdfs_path: str,
        model: ModelMock
        ) -> ModelMock:

    try:
        with hdfs_client.write(hdfs_path, encoding=None) as writer:
            pickle.dump(model, writer)
        print("Model saved successfully to HDFS.")
    except Exception as e:
        print(f"Failed to save model to HDFS: {e}")
