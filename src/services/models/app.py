from datetime import datetime, timedelta
from typing import AsyncGenerator

from torch.utils.data import DataLoader, TensorDataset
from fastapi.responses import StreamingResponse
from hdfs import InsecureClient
from fastapi import FastAPI
import pandas as pd
import torch

from mongodb_logging import set_data_means, get_data_means, log_predicted_open
from tools import merge_dataframes_on_date, collect_avro_files_to_dataframe
from model_tools import load_model_from_hdfs, save_model_to_hdfs
from mongodb_logging import get_last_model_log, add_new_model_log
from tools import merge_dataframes_with_nulls

app = FastAPI()

hdfs_client = InsecureClient("http://namenode:50070", user="root")
MODEL_NAME = "lstm_stock_prediction"


def model_path(date: str) -> str:
    return f"/models/lstm_model_{str(date)}.pkl"


@app.get("/train/")
async def train(
        epochs: int = 10
        ) -> dict:
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
    set_data_means(df)

    last_date = get_last_model_log(MODEL_NAME)
    if last_date is None:
        last_date = df['date_start'].min().strftime("%Y-%m-%d")
    model = load_model_from_hdfs(hdfs_client, model_path(last_date))

    df["date_start"] = pd.to_datetime(df["date_start"])
    df["date_end"] = pd.to_datetime(df["date_end"])

    criterion = torch.nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    start_date = pd.to_datetime(last_date)
    end_date = df["date_end"].max()

    while start_date < end_date:
        batch_start = start_date
        batch_end = batch_start + timedelta(days=5)

        batch_data = df[
            (df["date_start"] >= batch_start) & (df["date_end"] < batch_end)
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

        batch_loss = 0
        model.train()
        for epoch in range(epochs):
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
            batch_loss += avg_loss

            msg = f"Epoch: {epoch+1}, Date Start: {batch_start}, "
            msg += f"End Date: {batch_end}, Average Loss: {avg_loss:.4f}"
            print(msg)

        batch_loss /= epochs
        model_save_time = batch_end.strftime("%Y-%m-%d")
        save_model_to_hdfs(hdfs_client, model_path(model_save_time), model)
        add_new_model_log(MODEL_NAME, model_save_time, batch_loss)

        start_date = batch_end

    return {"message": "Model trained successfully over 5-day intervals."}


async def train_generator(
        df: pd.DataFrame,
        model,
        criterion,
        optimizer,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        epochs: int
        ) -> AsyncGenerator[str, None]:
    while start_date < end_date:
        batch_start = start_date
        batch_end = batch_start + timedelta(days=5)

        batch_data = df[
            (df["date_start"] >= batch_start) & (df["date_end"] < batch_end)
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

        batch_loss = 0
        for epoch in range(epochs):
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
            batch_loss += avg_loss

        batch_loss /= epochs
        yield (f"Batch completed: Date Start: {batch_start}, "
               f"End Date: {batch_end}, Batch Loss: {batch_loss:.4f}\n")

        model_save_time = batch_end.strftime("%Y-%m-%d")
        save_model_to_hdfs(hdfs_client, model_path(model_save_time), model)
        add_new_model_log(MODEL_NAME, model_save_time, batch_loss)

        start_date = batch_end

    yield "Training completed successfully.\n"


@app.get("/train_stream/")
async def train_stream(epochs: int = 10) -> StreamingResponse:
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
    set_data_means(df)

    last_date = get_last_model_log(MODEL_NAME)
    if last_date is None:
        last_date = df['date_start'].min().strftime("%Y-%m-%d")
    model = load_model_from_hdfs(hdfs_client, model_path(last_date))

    df["date_start"] = pd.to_datetime(df["date_start"])
    df["date_end"] = pd.to_datetime(df["date_end"])

    criterion = torch.nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    start_date = pd.to_datetime(last_date)
    end_date = df["date_end"].max()

    return StreamingResponse(
        train_generator(
            df=df,
            model=model,
            criterion=criterion,
            optimizer=optimizer,
            start_date=start_date,
            end_date=end_date,
            epochs=epochs
        ),
        media_type="text/plain"
    )


@app.get("/predict/")
async def predict(
        open: float = None,
        close: float = None,
        high: float = None,
        low: float = None,
        vol: float = None,
        temperature: float = None,
        rain: float = None,
        sun: float = None,
        sentiment: float = None,
        language: float = None,
        prediction_date: str = None
        ) -> dict:
    """
    Predicts the 'open' value based on input data passed as query parameters.
    Handles missing inputs by filling with average values from the database.
    """
    global hdfs_client

    means = get_data_means()

    input_data = {
        "open": open if open is not None else means.get("open", 0.0),
        "close": close if close is not None else means.get("close", 0.0),
        "high": high if high is not None else means.get("high", 0.0),
        "low": low if low is not None else means.get("low", 0.0),
        "vol": vol if vol is not None else means.get("vol", 0.0),
        "temperature": (
            temperature
            if temperature is not None
            else means.get("temperature", 0.0)
        ),
        "rain": rain if rain is not None else means.get("rain", 0.0),
        "sun": sun if sun is not None else means.get("sun", 0.0),
        "sentiment": (
            sentiment
            if sentiment is not None
            else means.get("sentiment", 0.0)
        ),
        "language": (
            language
            if language is not None
            else means.get("language", 0.0)
        )
    }

    X = torch.tensor(
        [list(input_data.values())],
        dtype=torch.float32
    ).unsqueeze(1)

    last_date = get_last_model_log(MODEL_NAME)
    if last_date is None:
        return {"predicted_open": 0.0, "message": "Model does not exist"}

    model = load_model_from_hdfs(hdfs_client, model_path(last_date))

    model.eval()
    with torch.no_grad():
        predicted_open = model(X).item()

    if prediction_date is None:
        prediction_date = datetime.today().strftime("%Y-%m-%d")
    log_predicted_open(prediction_date, predicted_open)

    return {"predicted_open": predicted_open}


@app.get("/predict_history/")
async def predict_history(
        start_date: str,
        end_date: str
        ) -> dict:
    """
    Predicts 'open' values for a given date range based on historical data.
    :param start_date: Start date in the format `YYYY-MM-DD`.
    :param end_date: End date in the format `YYYY-MM-DD`.
    :return: Predicted values for the given range.
    """
    global hdfs_client

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

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

    df_news['date_start'] = pd.to_datetime(
        df_news["date_start"].apply(lambda x: str(x)[:10])
        )
    df_news['date_end'] = pd.to_datetime(
        df_news["date_end"].apply(lambda x: str(x)[:10])
        )
    df_weather['date_start'] = pd.to_datetime(
        df_weather["date_start"].apply(lambda x: str(x)[:10])
        )
    df_weather['date_end'] = pd.to_datetime(
        df_weather["date_end"].apply(lambda x: str(x)[:10])
        )
    df_stock['date_start'] = pd.to_datetime(
        df_stock["date_start"].apply(lambda x: str(x)[:10])
        )
    df_stock['date_end'] = pd.to_datetime(
        df_stock["date_end"].apply(lambda x: str(x)[:10])
        )

    df = merge_dataframes_with_nulls(df_news, df_weather, df_stock)

    df = df[(df["date_start"] >= start_date) & (df["date_end"] <= end_date)]

    if df.empty:
        return {"message": "No data available for the given date range."}

    unique_dates = sorted(df["date_start"].dt.strftime("%Y-%m-%d").unique())

    X = df[[
        "open", "close", "high", "low", "vol", "temperature",
        "rain", "sun", "sentiment", "language"
    ]]
    X = torch.tensor(
        X.fillna(X.mean(numeric_only=True)).values,
        dtype=torch.float32
    ).unsqueeze(1)

    last_date = get_last_model_log(MODEL_NAME)
    if last_date is None:
        return {"message": "Model does not exist."}

    model = load_model_from_hdfs(hdfs_client, model_path(last_date))

    model.eval()
    with torch.no_grad():
        predictions = model(X).squeeze().tolist()

    result = []
    for row, pred in zip(df.to_dict(orient="records"), predictions):
        prediction_date = row["date_start"].strftime("%Y-%m-%d")
        log_predicted_open(prediction_date, pred)
        result.append({"date": prediction_date, "predicted_open": pred})

    return {
        "predictions": result,
        "unique_start_dates": unique_dates
    }


def train_and_predict_in_intervals(
        df: pd.DataFrame,
        model,
        criterion,
        optimizer,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        epochs: int
        ) -> list[dict]:
    """
    Trains the model on 5-day intervals and generates predictions.

    :param df: The DataFrame containing the data.
    :param model: The model to be trained and used for predictions.
    :param criterion: Loss function for training.
    :param optimizer: Optimizer for training.
    :param start_date: Start date for training and prediction.
    :param end_date: End date for training and prediction.
    :param epochs: Number of epochs to train for each interval.
    :return: A list of predictions for each interval.
    """
    predictions = []

    while start_date < end_date:
        batch_start = start_date
        batch_end = batch_start + timedelta(days=5)

        batch_data = df[
            (df["date_start"] >= batch_start) & (df["date_end"] < batch_end)
        ]

        if batch_data.empty:
            start_date = batch_end
            continue

        X = batch_data[[
            "open", "close", "high", "low", "vol", "temperature",
            "rain", "sun", "sentiment", "language"
        ]].fillna(0)
        y = batch_data[["open"]].fillna(0)

        X_tensor = torch.tensor(X.values, dtype=torch.float32)
        y_tensor = torch.tensor(y.values, dtype=torch.float32)

        dataset = TensorDataset(X_tensor, y_tensor)
        dataloader = DataLoader(dataset, batch_size=32, shuffle=True)

        model.train()
        loss_ = 0
        for epoch in range(epochs):
            for inputs, targets in dataloader:
                inputs = inputs.unsqueeze(1)
                optimizer.zero_grad()
                outputs = model(inputs)
                loss = criterion(outputs, targets)
                loss.backward()
                optimizer.step()
                loss_ += loss
        loss_ /= epochs

        model.eval()
        with torch.no_grad():
            predictions_tensor = model(X_tensor.unsqueeze(1)).squeeze()
            predictions_tensor[torch.isnan(predictions_tensor)] = 0
            batch_predictions = predictions_tensor.tolist()

        for index, (row, pred) in enumerate(zip(batch_data.to_dict(orient="records"), batch_predictions)):
            index += 1
            prediction_date = row["date_start"].strftime("%Y-%m-%d")
            pred = float(pred) if pred is not None else 0.0
            log_predicted_open(prediction_date, pred)
            predictions.append(
                {"date": prediction_date, "predicted_open": pred}
                )

        model_save_time = batch_end.strftime("%Y-%m-%d")
        save_model_to_hdfs(hdfs_client, model_path(model_save_time), model)
        add_new_model_log(MODEL_NAME, model_save_time, float(loss_))

        start_date = batch_end

    return predictions


@app.get("/train_and_predict/")
async def train_and_predict(
        start_date: str,
        end_date: str,
        epochs: int = 10
        ) -> dict:
    global hdfs_client

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

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

    df_weather = collect_avro_files_to_dataframe(
        hdfs_client, "/data/batch_scraper_weather_openmeteo"
    )
    df_stock = collect_avro_files_to_dataframe(
        hdfs_client, "/data/batch_scraper_stock_xtb"
    )

    df_news['date_start'] = pd.to_datetime(
        df_news["date_start"].apply(lambda x: str(x)[:10])
        )
    df_news['date_end'] = pd.to_datetime(
        df_news["date_end"].apply(lambda x: str(x)[:10])
        )
    df_weather['date_start'] = pd.to_datetime(
        df_weather["date_start"].apply(lambda x: str(x)[:10])
        )
    df_weather['date_end'] = pd.to_datetime(
        df_weather["date_end"].apply(lambda x: str(x)[:10])
        )
    df_stock['date_start'] = pd.to_datetime(
        df_stock["date_start"].apply(lambda x: str(x)[:10])
        )
    df_stock['date_end'] = pd.to_datetime(
        df_stock["date_end"].apply(lambda x: str(x)[:10])
        )

    df = merge_dataframes_with_nulls(df_news, df_weather, df_stock)
    df = df[(df["date_start"] >= start_date) & (df["date_end"] <= end_date)]

    if df.empty:
        return {"message": "No data available for the given date range."}

    # Load the model
    last_date = get_last_model_log(MODEL_NAME)
    if last_date is None:
        last_date = df["date_start"].min().strftime("%Y-%m-%d")
    model = load_model_from_hdfs(hdfs_client, model_path(last_date))

    if model is None:
        return {"message": "Model does not exist."}

    # Prepare training parameters
    criterion = torch.nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    # Train and predict
    predictions = train_and_predict_in_intervals(
        df=df,
        model=model,
        criterion=criterion,
        optimizer=optimizer,
        start_date=start_date,
        end_date=end_date,
        epochs=epochs
    )

    return {"predictions": predictions}
