from statsmodels.tsa.arima.model import ARIMA
import pickle

def train_and_pickle_prediction_model(data: pd.DataFrame, model_path: str, target_column: str):
    """
    Train the prediction model (ARIMA) and save it as a pickle file.
    """
    if target_column not in data.columns:
        raise ValueError(f"Target column {target_column} not found in data")

    # Extract time-series data
    series = data[target_column].astype(float)

    # Train ARIMA model
    model = ARIMA(series, order=(1, 1, 1))
    fit_model = model.fit()

    # Save the trained model
    with open(model_path, "wb") as f:
        pickle.dump(fit_model, f)
    print(f"Prediction model saved to {model_path}")
