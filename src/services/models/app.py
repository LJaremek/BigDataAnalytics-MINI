from torch.utils.data import DataLoader, TensorDataset
from hdfs import InsecureClient
from datetime import timedelta
from fastapi import FastAPI
import pandas as pd
import torch

from tools import merge_dataframes_on_date, collect_avro_files_to_dataframe
from model_tools import LSTMModel, load_model_from_hdfs, save_model_to_hdfs
from mongodb_logging import get_last_model_log, add_new_model_log

app = FastAPI()

hdfs_client = InsecureClient("http://namenode:50070", user="root")
MODEL_NAME = "lstm_stock_prediction"


def model_path(date: str) -> str:
    return f"/models/lstm_model_{str(date)}.pkl"


@app.get("/train/")
async def train() -> dict:
    """
    Trains the LSTM model on Avro data from the given date range.
    :param start_time: Start date in the format `YYYY-MM-DD HH:MM:SS`.
    :param end_time: End date in the format `YYYY-MM-DD HH:MM:SS`.
    """
    global hdfs_client

    df_news = pd.concat([
        collect_avro_files_to_dataframe(
            hdfs_client, "/data/batch_scraper_news_newsapi"
            ),
        collect_avro_files_to_dataframe(
            hdfs_client, "/data/batch_scraper_news_worldnewsapi"
            ),
        collect_avro_files_to_dataframe(
            hdfs_client, "/data/batch_scraper_news_xtb"
            )
    ], ignore_index=True)

    df_weather = \
        collect_avro_files_to_dataframe(
            hdfs_client, "/data/batch_scraper_weather_openmeteo"
            )

    df_stock = \
        collect_avro_files_to_dataframe(
            hdfs_client, "/data/batch_scraper_stock_xtb"
            )

    df = merge_dataframes_on_date(df_news, df_weather, df_stock)

    last_date = None  # get_last_model_log(MODEL_NAME)
    if last_date is None:
        last_date = df['date_start'].min().strftime("%Y-%m-%d")
        model = load_model_from_hdfs(hdfs_client, model_path(last_date))

    df["date_start"] = pd.to_datetime(df["date_start"])
    df["date_end"] = pd.to_datetime(df["date_end"])

    criterion = torch.nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    start_date = pd.to_datetime(last_date)
    end_date = df['date_end'].max()

    while start_date < end_date:
        batch_start = start_date
        batch_end = batch_start + timedelta(days=5)

        batch_data = df[
            (df['date_start'] >= batch_start) & (df['date_end'] < batch_end)
            ]

        if batch_data.empty:
            start_date = batch_end
            continue

        X = batch_data[[
            "open", "close", "high", "low", "vol", "temperature",
            "rain", "sun", "sentiment", "language"
            ]]
        y = batch_data[["open"]]

        X = torch.tensor(X.values, dtype=torch.float32)
        y = torch.tensor(y.values, dtype=torch.float32)

        dataset = TensorDataset(X, y)
        dataloader = DataLoader(dataset, batch_size=32, shuffle=True)

        for epoch in range(10):
            epoch_loss = 0
            batch_count = 0
            for inputs, targets in dataloader:
                inputs = inputs.unsqueeze(1)
                optimizer.zero_grad()
                outputs = model(inputs)
                loss = criterion(outputs, targets)
                loss.backward()
                optimizer.step()
                epoch_loss += loss.item()
                batch_count += 1
            avg_loss = epoch_loss / batch_count if batch_count > 0 else 0
            print(f"Epoch: {epoch+1}, Date Start: {batch_start}, End Date: {batch_end}, Average Loss: {avg_loss:.4f}")

        # save_model_to_hdfs(hdfs_client, model_path(start_date), model)
        start_date = batch_end

    return {"message": "Model trained successfully over 5-day intervals."}
