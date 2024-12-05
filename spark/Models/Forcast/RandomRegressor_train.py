# Import Libraries
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib

# Load Data
data = pd.read_csv(
    'C:/Users/I745988/Ransomware-attack/data/enriched_data.csv',
    low_memory=False
)

# Convert created_indicator to datetime
data['created_indicator'] = pd.to_datetime(data['created_indicator'])

# Label Encode Categorical Columns
label_encoder_target_country = LabelEncoder()
label_encoder_source_country = LabelEncoder()
label_encoder_malware_family = LabelEncoder()

data['target_country'] = label_encoder_target_country.fit_transform(data['target_country'])
data['source_country'] = label_encoder_source_country.fit_transform(data['source_country'])
data['malware_family'] = label_encoder_malware_family.fit_transform(data['malware_family'])

# Extract temporal features
data['year'] = data['created_indicator'].dt.year
data['month'] = data['created_indicator'].dt.month
data['day'] = data['created_indicator'].dt.day
data['day_of_week'] = data['created_indicator'].dt.dayofweek
data['is_weekend'] = data['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)

# Aggregate data to daily attacks per country
daily_data = data.groupby(['target_country', 'created_indicator']).size().reset_index(name='num_attacks')

# Ensure no missing dates for each country
countries = daily_data['target_country'].unique()
full_data = pd.DataFrame()

for country in countries:
    country_data = daily_data[daily_data['target_country'] == country]
    country_data = country_data.set_index('created_indicator')
    country_data = country_data.reindex(
        pd.date_range(start=country_data.index.min(), end=country_data.index.max(), freq='D'),
        fill_value=0
    )
    country_data['target_country'] = country
    full_data = pd.concat([full_data, country_data])

# Reset index and rename columns
full_data.reset_index(inplace=True)
full_data.columns = ['created_indicator', 'num_attacks', 'target_country']

# Add lag features
for lag in [1, 7, 30]:
    full_data[f'lag_{lag}'] = full_data.groupby('target_country')['num_attacks'].shift(lag)

# Extract additional date features
full_data['year'] = full_data['created_indicator'].dt.year
full_data['month'] = full_data['created_indicator'].dt.month
full_data['day'] = full_data['created_indicator'].dt.day
full_data['day_of_week'] = full_data['created_indicator'].dt.dayofweek
full_data['is_weekend'] = full_data['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)

# Drop rows with NaN values (from lag features)
full_data = full_data.dropna()

# Define features (X) and target (y)
X = full_data.drop(['num_attacks', 'created_indicator'], axis=1)
y = full_data['num_attacks']

# Split into train and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

# Train Random Forest Model
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Evaluate the Model
predictions = model.predict(X_test)
mae = mean_absolute_error(y_test, predictions)
rmse = mean_squared_error(y_test, predictions, squared=False)

print(f"MAE: {mae}")
print(f"RMSE: {rmse}")

# Forecast for the Next 2 Years
forecast_data = full_data.copy()
future_dates = pd.date_range(start=forecast_data['created_indicator'].max() + pd.Timedelta(days=1), periods=730)

# Ensure alignment of features
feature_columns = X_train.columns  # Use the same feature columns as in training

for date in future_dates:
    last_row = forecast_data.iloc[-1]
    
    # Create new row with lagged features
    new_row = {
        'created_indicator': date,
        'num_attacks': None,  # Placeholder for prediction
        'year': date.year,
        'month': date.month,
        'day': date.day,
        'day_of_week': date.dayofweek,
        'is_weekend': 1 if date.dayofweek >= 5 else 0,
        'target_country': last_row['target_country']
    }
    
    # Add lagged features
    for lag in [1, 7, 30]:
        lag_date = date - pd.Timedelta(days=lag)
        lag_value = forecast_data[forecast_data['created_indicator'] == lag_date]['num_attacks'].values
        new_row[f'lag_{lag}'] = lag_value[0] if len(lag_value) > 0 else 0

    # Align new_row to feature_columns
    new_row_df = pd.DataFrame([new_row]).drop(['num_attacks', 'created_indicator'], axis=1)
    new_row_df = new_row_df.reindex(columns=feature_columns, fill_value=0)  # Reorder and fill missing features

    # Predict and append the row
    new_row['num_attacks'] = model.predict(new_row_df)[0]
    forecast_data = pd.concat([forecast_data, pd.DataFrame([new_row])], ignore_index=True)


# Save Results
forecast_data[['created_indicator', 'num_attacks']].to_csv('forecast_results.csv', index=False)


# Save the trained model to a file
joblib.dump(model, 'C:/Users/I745988/Ransomware-attack/spark/Models/Forcastrandom_forest_model.pkl')
print("Model saved successfully.")

