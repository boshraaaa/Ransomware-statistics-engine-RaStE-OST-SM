import pandas as pd
from sklearn.preprocessing import LabelEncoder
import joblib

class RandomForestModel:
    def __init__(self, model_path):
        """
        Initialize the class and load the trained model.
        """
        self.model = joblib.load(model_path)
        self.label_encoders = {
            'target_country': LabelEncoder(),
            'source_country': LabelEncoder(),
            'malware_family': LabelEncoder()
        }

    def preprocess(self, data):
        """
        Preprocess the incoming Pandas DataFrame to match the training data structure.
        Includes:
        - Label encoding categorical columns.
        - Adding temporal features and lag features.
        - Handling missing data.
        """
        # Ensure required columns are present
        required_columns = ['created_indicator', 'target_country', 'source_country', 'malware_family']
        for col in required_columns:
            if col not in data.columns:
                data[col] = 'unknown' if col in ['target_country', 'source_country', 'malware_family'] else pd.NaT

        # Convert created_indicator to datetime
        data['created_indicator'] = pd.to_datetime(data['created_indicator'], errors='coerce')

        # Drop rows with missing created_indicator
        data.dropna(subset=['created_indicator'], inplace=True)

        # Encode categorical columns
        for col in ['target_country', 'source_country', 'malware_family']:
            if col in data.columns:
                data[col] = self.label_encoders[col].fit_transform(data[col].astype(str))

        # Extract temporal features
        data['year'] = data['created_indicator'].dt.year
        data['month'] = data['created_indicator'].dt.month
        data['day'] = data['created_indicator'].dt.day
        data['day_of_week'] = data['created_indicator'].dt.dayofweek
        data['is_weekend'] = data['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)

        # Group by target_country and date to aggregate num_attacks
        if 'num_attacks' not in data.columns:
            data['num_attacks'] = 0  # Default value for missing column
        aggregated_data = data.groupby(['target_country', 'created_indicator'])['num_attacks'].sum().reset_index()

        # Ensure no missing dates for each target_country
        complete_data = pd.DataFrame()
        for country in aggregated_data['target_country'].unique():
            country_data = aggregated_data[aggregated_data['target_country'] == country]
            country_data.set_index('created_indicator', inplace=True)
            country_data = country_data.reindex(
                pd.date_range(country_data.index.min(), country_data.index.max(), freq='D'),
                fill_value=0
            ).reset_index()
            country_data.columns = ['created_indicator', 'num_attacks']
            country_data['target_country'] = country
            complete_data = pd.concat([complete_data, country_data], ignore_index=True)

        # Add lag features
        for lag in [1, 7, 30]:
            complete_data[f'lag_{lag}'] = complete_data.groupby('target_country')['num_attacks'].shift(lag)

        # Drop rows with NaN values (from lag features)
        complete_data.dropna(inplace=True)

        # Add additional temporal features
        complete_data['year'] = complete_data['created_indicator'].dt.year
        complete_data['month'] = complete_data['created_indicator'].dt.month
        complete_data['day'] = complete_data['created_indicator'].dt.day
        complete_data['day_of_week'] = complete_data['created_indicator'].dt.dayofweek
        complete_data['is_weekend'] = complete_data['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)

        return complete_data

    def predict(self, df, forecast_days=730):
        """
        Predict the number of attacks using the pre-trained RandomForest model.
        Adds future predictions for the specified number of days.
        """
        processed_df = self.preprocess(df)
        if processed_df.empty:
            raise ValueError("Processed DataFrame is empty after preprocessing.")

        forecast_data = processed_df.copy()
        feature_columns = [col for col in processed_df.columns if col not in ['created_indicator', 'num_attacks']]

        # Generate future predictions
        last_date = forecast_data['created_indicator'].max()
        for day in range(1, forecast_days + 1):
            future_date = last_date + pd.Timedelta(days=day)
            last_row = forecast_data.iloc[-1]

            # Prepare new row with lagged features
            new_row = {
                'created_indicator': future_date,
                'year': future_date.year,
                'month': future_date.month,
                'day': future_date.day,
                'day_of_week': future_date.dayofweek,
                'is_weekend': 1 if future_date.dayofweek >= 5 else 0,
                'target_country': last_row['target_country']
            }

            for lag in [1, 7, 30]:
                lag_date = future_date - pd.Timedelta(days=lag)
                lag_value = forecast_data.loc[forecast_data['created_indicator'] == lag_date, 'num_attacks']
                new_row[f'lag_{lag}'] = lag_value.values[0] if not lag_value.empty else 0

            # Prepare for prediction
            new_row_df = pd.DataFrame([new_row]).reindex(columns=feature_columns, fill_value=0)
            new_row['num_attacks'] = self.model.predict(new_row_df)[0]

            # Append to the forecast data
            forecast_data = pd.concat([forecast_data, pd.DataFrame([new_row])], ignore_index=True)

        return forecast_data[['created_indicator', 'num_attacks']]
