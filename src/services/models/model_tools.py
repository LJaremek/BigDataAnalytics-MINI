import pickle

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from hdfs import InsecureClient
from langdetect import detect
import torch.nn as nn
import numpy as np
import torch


class LSTMModel(nn.Module):
    def __init__(self, input_size=12, hidden_size=50, num_layers=2, output_size=1, dropout=0.2):
        """
        input_size (int): Number of input features. Default:
            Hour, Day of Week, Open, Close, High, Low, Volume, 
            Average news sentiment, Temperature min 2m (°C), 
            Temperature max 2m (°C), Rain sum (mm), Sunshine duration (s).
        hidden_size (int): Number of hidden units in the LSTM layers.
        num_layers (int): Number of LSTM layers.
        output_size (int): Number of output features (e.g., predicted price).
        dropout (float): Dropout rate for regularization.
        """
        super(LSTMModel, self).__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True, dropout=dropout)
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
        out = self.dropout(out[:, -1, :])  # Take the output of the last time step
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
        analyzer: SentimentIntensityAnalyzer | None
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
        return LSTMModel()


def save_model_to_hdfs(
        hdfs_client: InsecureClient,
        hdfs_path: str,
        model: nn.Module
        ) -> None:

    try:
        with hdfs_client.write(hdfs_path, encoding=None) as writer:
            pickle.dump(model, writer)
        print("Model saved successfully to HDFS.")
    except Exception as e:
        print(f"Failed to save model to HDFS: {e}")


def train_model(
        model: nn.Module,
        criterion,
        optimizer,
        X_train: torch.Tensor,
        y_train: torch.Tensor,
        epochs: int,
        X_test: torch.Tensor | None = None,
        y_test: torch.Tensor | None = None,
        eval_interval: int | None = None
        ) -> None:
    """
    Trenuje model LSTM na danych treningowych.

    :param model: Model LSTM.
    :param criterion: Kryterium strat.
    :param optimizer: Optymalizator.
    :param X_train: Dane wejściowe treningowe.
    :param y_train: Dane docelowe treningowe.
    :param epochs: Liczba epok treningowych.
    :param X_test: Dane wejściowe testowe (opcjonalne).
    :param y_test: Dane docelowe testowe (opcjonalne).
    :param eval_interval: Liczba epok między ewaluacjami (None = brak ewaluacji).
    """
    if eval_interval is not None:
        if X_test is None or y_test is None:
            raise Exception("Please provide test data for evaluation")

    for epoch in range(epochs):
        model.train()
        outputs = model(X_train)
        loss = criterion(outputs.squeeze(), y_train)

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if (epoch + 1) % 10 == 0:
            print(f'Epoch [{epoch + 1}/{epochs}], Loss: {loss.item():.4f}')

        if eval_interval and (epoch + 1) % eval_interval == 0:
            test_loss = evaluate_model(model, criterion, X_test, y_test)
            print(f"Evaluation after epoch {epoch + 1}: Test Loss = {test_loss:.4f}")


def evaluate_model(model, criterion, X_test, y_test):
    """
    Ewaluacja modelu na danych testowych.
    """
    model.eval()
    with torch.no_grad():
        test_outputs = model(X_test)
        test_loss = criterion(test_outputs.squeeze(), y_test)
    return test_loss.item()


def forecast_model(model, scaler, sample_input):
    """
    Prognoza wartości wyjściowej dla pojedynczego przykładu.
    """
    model.eval()
    with torch.no_grad():
        predicted = model(sample_input.unsqueeze(0)).item()
        predicted_open = scaler.inverse_transform(
            np.array([[predicted, 0, 0, 0, 0]])
        )[0, 0]
        return predicted_open


def train_sentiment_model(
        model, dataloader, optimizer, criterion, device='cpu'
        ) -> float:
    """
    Trains the model for one epoch.
    :param model:      The SentimentLSTM model.
    :param dataloader: DataLoader with (text_batch, labels).
    :param optimizer:  Optimizer (e.g., Adam).
    :param criterion:  Loss function (e.g., CrossEntropyLoss).
    :param device:     'cpu' or 'cuda'.
    """
    model.train()
    total_loss = 0

    for text_batch, labels in dataloader:
        # Move data to device
        text_batch, labels = text_batch.to(device), labels.to(device)

        # Forward pass
        logits = model(text_batch)

        # Calculate loss
        loss = criterion(logits, labels)

        # Backward and optimize
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        total_loss += loss.item()

    return total_loss / len(dataloader)


def evaluate_sentiment_model(model, dataloader, criterion, device='cpu'):
    """
    Evaluates the model on a validation or test set.

    :param model:      The SentimentLSTM model.
    :param dataloader: DataLoader with (text_batch, labels).
    :param criterion:  Loss function (e.g., CrossEntropyLoss).
    :param device:     'cpu' or 'cuda'.
    """
    model.eval()
    total_loss = 0
    correct = 0
    total = 0

    with torch.no_grad():
        for text_batch, labels in dataloader:
            text_batch, labels = text_batch.to(device), labels.to(device)

            logits = model(text_batch)
            loss = criterion(logits, labels)
            total_loss += loss.item()

            # Predictions
            predictions = torch.argmax(logits, dim=1)
            correct += (predictions == labels).sum().item()
            total += labels.size(0)

    avg_loss = total_loss / len(dataloader)
    accuracy = correct / total
    return avg_loss, accuracy
