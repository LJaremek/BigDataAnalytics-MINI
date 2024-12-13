import pickle

from hdfs import InsecureClient
import torch.nn as nn
import numpy as np
import torch


class LSTMModel(nn.Module):
    def __init__(self, input_size=7, hidden_size=50, num_layers=2, output_size=1, dropout=0.2):
        """
        input_size (int): Number of input features. Default:
            open, close, high, low, vol, hour, day_of_week
        """
        super(LSTMModel, self).__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True, dropout=dropout)
        self.dropout = nn.Dropout(dropout)
        self.fc = nn.Linear(hidden_size, output_size)
        self.relu = nn.ReLU()

    def forward(self, x):
        out, _ = self.lstm(x)
        out = self.dropout(out[:, -1, :])
        out = self.fc(out)
        out = self.relu(out)
        return out


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
