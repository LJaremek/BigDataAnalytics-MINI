from hdfs import InsecureClient
from fastapi import FastAPI
import pandas as pd
import torch

from tools import prepare_dataframe_with_datetime
from tools import get_date_one_month_ago, add_n_days
from model_tools import load_model_from_hdfs, save_model_to_hdfs, train_model
from mongodb_logging import get_last_model_log, add_new_model_log

app = FastAPI()

hdfs_client = InsecureClient("http://namenode:50070", user="root")

model_path = "/models/lstm_model.pkl"
hdfs_stock_folder = "/data/batch_scraper_stock_xtb"

model = load_model_from_hdfs(hdfs_client, model_path)
dataframe = prepare_dataframe_with_datetime(hdfs_client, hdfs_stock_folder)


def prepare_dataframe(hdfs_client, start_time, end_time):
    ...


# Model giełdy - pogoda + giełda + wynik_modelu_sentyment


@app.post("/train/")
async def train(start_time: str, end_time: str,) -> dict:
    """
    Trains the LSTM model on Avro data from the given date range.
    :param start_time: Start date in the format `YYYY-MM-DD HH:MM:SS`.
    :param end_time: End date in the format `YYYY-MM-DD HH:MM:SS`.
    """
    global hdfs_client

    filtered_df = prepare_dataframe(hdfs_client, start_time, end_time)

    feature_columns = ["open", 'close', 'high', 'low', 'vol']
    target_column = 'open'

    X = torch.tensor(filtered_df[feature_columns].values, dtype=torch.float32)
    y = torch.tensor(filtered_df[target_column].values, dtype=torch.float32)

    train_size = int(0.8 * len(X))
    X_train, X_test = X[:train_size], X[train_size:]
    y_train, y_test = y[:train_size], y[train_size:]

    criterion = torch.nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    epochs = 50
    train_model(
        model, criterion, optimizer,
        X_train, y_train, epochs,
        X_test, y_test, 5
        )

    save_model_to_hdfs(hdfs_client, model_path, model)

    return {
        "message": "Model trained successfully on filtered Avro data."
    }


@app.post("/predict/")
async def predict(
        hdfs_file: str,
        sequence_length: int = 10,
        num_predictions: int = 1
        ) -> dict:
    """
    Generuje prognozy "open" na podstawie całej historii dla kolejnych dni.
    :param hdfs_file: Ścieżka do pliku Avro na HDFS.
    :param sequence_length: Długość sekwencji historycznej używanej
        do przewidywania.
    :param num_predictions: Liczba kolejnych prognoz do wygenerowania.
    """
    data = prepare_dataframe_with_datetime(hdfs_client, hdfs_file)

    feature_columns = ['open', 'close', 'high', 'low', 'vol']
    features = data[feature_columns].values
    X = torch.tensor(features, dtype=torch.float32)

    if len(X) < sequence_length:
        return {"message": "Not enough data to form a sequence."}

    # [1, sequence_length, num_features]
    sequence = X[-sequence_length:].unsqueeze(0)

    predictions = []

    model.eval()
    with torch.no_grad():
        for _ in range(num_predictions):
            prediction = model(sequence).squeeze().item()
            predictions.append(prediction)

            next_step = torch.tensor(
                [[prediction] + sequence[0, -1, 1:].tolist()],
                dtype=torch.float32
                )
            sequence = torch.cat(
                (sequence[:, 1:, :], next_step.unsqueeze(0)), dim=1
                )

    return {"next_open_predictions": predictions}
