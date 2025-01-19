from datetime import datetime, timedelta
import os
import re

from sklearn.preprocessing import LabelEncoder
from hdfs import InsecureClient
import pandas as pd
import fastavro

from model_tools import get_sentiment, get_language


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


def collect_avro_files_to_dataframe(
        hdfs_client: InsecureClient,
        base_folder: str
        ) -> pd.DataFrame:
    """
    Zbiera wszystkie pliki Avro z podfolderów danego folderu na HDFS
    i buduje ramkę danych.

    :param hdfs_client: Klient HDFS (InsecureClient).
    :param base_folder: Ścieżka do folderu na HDFS.
    :return: DataFrame zawierający dane z wszystkich plików Avro.
    """
    all_dataframes = []

    try:
        for root, dirs, files in hdfs_client.walk(base_folder):
            for file in files:
                if file.endswith('.avro'):
                    avro_path = os.path.join(root, file)
                    try:
                        with hdfs_client.read(avro_path) as avro_reader:
                            records = list(fastavro.reader(avro_reader))
                            if records:
                                df = pd.DataFrame(records)
                                all_dataframes.append(df)
                    except Exception as e:
                        print(f"Nie udało się odczytać pliku {avro_path}: {e}")
    except Exception as e:
        print(f"Nie udało się odczytać folderu {base_folder}: {e}")

    if all_dataframes:
        full_dataframe = pd.concat(all_dataframes, ignore_index=True)
    else:
        full_dataframe = pd.DataFrame()

    return full_dataframe


def clean_text(text):
    """
    Oczyszcza tekst, usuwając obiekty HTML, dziwne znaki,
    adresy email, adresy URL i inne niechciane elementy
    oraz zmieniając tekst na małe litery.

    :param text: Tekst do oczyszczenia.
    :return: Oczyszczony tekst.
    """
    if not isinstance(text, str):
        return ""
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\b', '', text
        )
    text = re.sub(r'http[s]?://\\S+|www\\.\\S+', '', text)
    text = re.sub(r'\\b\\d{10,}\\b', '', text)
    text = re.sub(r'\\s+', ' ', text).strip()
    text = re.sub(r'[^a-zA-Z0-9\\s]', '', text)
    text = text.lower()
    return text


def merge_dataframes_on_date(df1, df2, df3):
    if "data" in df1.columns:
        df1["data"] = df1["data"].apply(clean_text)
        df1["sentiment"] = df1["data"].apply(get_sentiment)
        df1["language"] = df1["data"].apply(get_language)

    merged_df = pd.merge(df1, df2, on=["date_start", "date_end"], how="inner")

    merged_df = pd.merge(
        merged_df, df3, on=["date_start", "date_end"], how="inner"
    )

    merged_df["date_start"] = pd.to_datetime(
        merged_df["date_start"], errors="coerce"
        )
    merged_df["date_end"] = pd.to_datetime(
        merged_df["date_end"], errors="coerce"
        )

    merged_df["ctm"] = merged_df["ctm"].astype(float, errors="ignore")
    merged_df["open"] = merged_df["open"].astype(float, errors="ignore")
    merged_df["close"] = merged_df["close"].astype(float, errors="ignore")
    merged_df["high"] = merged_df["high"].astype(float, errors="ignore")
    merged_df["low"] = merged_df["low"].astype(float, errors="ignore")
    merged_df["vol"] = merged_df["vol"].astype(float, errors="ignore")
    merged_df["temperature"] = merged_df["temperature"].astype(
        float, errors="ignore"
        )
    merged_df["rain"] = merged_df["rain"].astype(float, errors="ignore")
    merged_df["sun"] = merged_df["sun"].astype(float, errors="ignore")

    sentiment_mapping = {"pos": 1, "neu": 0, "neg": -1}
    merged_df['sentiment'] = merged_df['sentiment'].map(sentiment_mapping)

    label_encoder = LabelEncoder()
    merged_df['language'] = label_encoder.fit_transform(merged_df['language'])

    final_columns = ["ctm", "ctmString", "open", "close", "high", "low", "vol",
                     "date_start", "date_end", "temperature", "rain", "sun",
                     "date", "data", "sentiment", "language"]

    return merged_df[final_columns]


def merge_dataframes_with_nulls(
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        df3: pd.DataFrame
        ) -> pd.DataFrame:
    """
    Merges three DataFrames on `date_start` and `date_end` columns,
    ensuring the result contains all columns from all DataFrames.
    Maps sentiment, encodes language, and ensures numeric types.

    :param df1: First DataFrame with `date_start` and `date_end`.
    :param df2: Second DataFrame with `date_start` and `date_end`.
    :param df3: Third DataFrame with `date_start` and `date_end`.
    :return: Processed and merged DataFrame.
    """
    df1["data"] = df1["data"].apply(clean_text)
    df1["sentiment"] = df1["data"].apply(get_sentiment)
    df1["language"] = df1["data"].apply(get_language)

    merged_df = pd.merge(
        df1, df2, on=["date_start", "date_end"], how="outer"
        )
    merged_df = pd.merge(
        merged_df, df3, on=["date_start", "date_end"], how="outer"
        )

    numeric_columns = ["ctm", "open", "close", "high", "low", "vol",
                       "temperature", "rain", "sun"]
    for col in numeric_columns:
        if col in merged_df.columns:
            merged_df[col] = merged_df[col].astype(float, errors="ignore")

    sentiment_mapping = {"pos": 1, "neu": 0, "neg": -1}
    if 'sentiment' in merged_df.columns:
        merged_df['sentiment'] = merged_df['sentiment'].map(sentiment_mapping)

    if 'language' in merged_df.columns:
        label_encoder = LabelEncoder()
        merged_df['language'] = label_encoder.fit_transform(
            merged_df['language'].astype(str)
        )

    final_columns = ["ctm", "ctmString", "open", "close", "high", "low", "vol",
                     "date_start", "date_end", "temperature", "rain", "sun",
                     "date", "data", "sentiment", "language"]
    for col in numeric_columns:
        if col in merged_df.columns:
            merged_df[col] = merged_df[col].fillna(merged_df[col].mean())

    return merged_df[final_columns]
