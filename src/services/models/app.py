from hdfs import InsecureClient
from fastapi import FastAPI
import pandas as pd

from model_tools import load_model_from_hdfs, save_model_to_hdfs
from tools import load_avro_from_hdfs

app = FastAPI()

hdfs_client = InsecureClient("http://namenode:50070", user="root")
model = load_model_from_hdfs(hdfs_client, "/models/model.pkl")


@app.post("/train-avro/")
async def train_avro(hdfs_folder: str):
    file_statuses = hdfs_client.list(hdfs_folder, status=True)
    avro_files = [
        f"{hdfs_folder}/{file_status['pathSuffix']}"
        for file_status in file_statuses
        if file_status['type'] == 'FILE'
        ]

    dataframes = [
        load_avro_from_hdfs(hdfs_client, file)
        for file in avro_files
        ]
    full_data = pd.concat(dataframes, ignore_index=True)

    X = full_data.iloc[:, :-1].values
    y = full_data.iloc[:, -1].values

    model.fit(X, y)
    save_model_to_hdfs(hdfs_client, hdfs_folder, model)

    return {"message": "Model trained successfully on Avro data."}


@app.post("/predict-avro/")
async def predict_avro(hdfs_file: str):
    data = load_avro_from_hdfs(hdfs_file)

    predictions = model.predict(data.values)

    return {"predictions": predictions}
