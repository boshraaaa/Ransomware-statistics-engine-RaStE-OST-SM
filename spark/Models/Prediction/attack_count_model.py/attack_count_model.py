import pickle
import pandas as pd
from river import stream

class AttackCountModel:
    def __init__(self):
        self.model = pickle.load(open('spark/Models/Prediction/attack_count_model.py/rf_attack_count_model.pkl', 'rb'))
    
    def predict(self, df):
        predictions = []
        for x, _ in stream.iter_pandas(df):
            predictions.append(self.model.predict_one(x))
        return pd.DataFrame(predictions, columns=["Attack Count Predictions"])
