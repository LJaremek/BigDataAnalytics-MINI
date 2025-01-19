import pickle

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from hdfs import InsecureClient
from langdetect import detect
import torch.nn as nn


class LSTMModel(nn.Module):
    def __init__(
            self,
            input_size=10, hidden_size=50,
            num_layers=2, output_size=1, dropout=0.2
            ) -> None:
        """
        input_size (int): Number of input features. Default:
            open, close, high, low, vol, temperature, rain, sun,
            date (as numerical feature), sentiment, language.
        hidden_size (int): Number of hidden units in the LSTM layers.
        num_layers (int): Number of LSTM layers.
        output_size (int): Number of output features (e.g., predicted price).
        dropout (float): Dropout rate for regularization.
        """
        super(LSTMModel, self).__init__()
        self.lstm = nn.LSTM(
            input_size, hidden_size, num_layers,
            batch_first=True, dropout=dropout
            )
        self.dropout = nn.Dropout(dropout)
        self.fc = nn.Linear(hidden_size, output_size)
        self.relu = nn.ReLU()

    def forward(self, x):
        """
        Forward pass:
        x: Input tensor of shape (batch_size, sequence_length, input_size)
        Returns:
            out: Output tensor of shape (batch_size, output_size)
        """
        out, _ = self.lstm(x)
        out = self.dropout(out[:, -1, :])  # Take the output of the last time s
        out = self.fc(out)
        out = self.relu(out)
        return out


def get_language(text: str) -> str:
    """
    Available language codes:
        af, ar, bg, bn, ca, cs, cy, da, de, el, en, es, et, fa, fi, fr, gu, he,
        hi, hr, hu, id, it, ja, kn, ko, lt, lv, mk, ml, mr, ne, nl, no, pa, pl,
        pt, ro, ru, sk, sl, so, sq, sv, sw, ta, te, th, tl, tr, uk, ur, vi,
        zh-cn, zh-tw
    """
    return detect(text)


def get_sentiment(
        text: str,
        analyzer: SentimentIntensityAnalyzer | None = None
        ) -> str:
    """
    Available answers:
        neg, neu, pos
    """
    if analyzer is None:
        analyzer = SentimentIntensityAnalyzer()

    scores = analyzer.polarity_scores(text)
    del scores["compound"]

    return max(scores, key=scores.get)


def load_model_from_hdfs(
        hdfs_client: InsecureClient,
        hdfs_path: str
        ) -> nn.Module:

    try:
        with hdfs_client.read(hdfs_path) as reader:
            model = pickle.load(reader)
        print("Model loaded successfully from HDFS.")
        return model
    except Exception as e:
        print(f"Failed to load model from HDFS: {e}")
        return LSTMModel(
            input_size=10, hidden_size=50,
            num_layers=2, output_size=1, dropout=0.2
            )


def save_model_to_hdfs(
        hdfs_client: InsecureClient,
        hdfs_path: str,
        model: nn.Module
        ) -> None:

    try:
        with hdfs_client.write(
            hdfs_path, encoding=None, overwrite=True
        ) as writer:
            pickle.dump(model, writer)
        print("Model saved successfully to HDFS.")
    except Exception as e:
        print(f"Failed to save model to HDFS: {e}")
