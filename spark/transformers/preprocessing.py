import pandas as pd
from sklearn.preprocessing import LabelEncoder, StandardScaler
from datetime import datetime

def ip_to_int(ip):
    """Convert IP address to integer representation"""
    import socket
    import struct
    try:
        return struct.unpack("!I", socket.inet_aton(ip))[0]
    except socket.error:
        return None

def preprocess_data(df):
    """Preprocess data: drop unnecessary columns, encode categorical variables, etc."""
    columns_to_drop = [
        'content', 'title', 'description_indicator', 'expiration', 
        'is_active', 'in_group', 'is_subscribing', 
        'description_pulse', 'author_name', 'modified', 'public', 
        'adversary'
    ]
    df.drop(columns=columns_to_drop, inplace=True)
    
    # Label Encoding for categorical columns
    label_encoder = LabelEncoder()
    categorical_columns = [
        'source_city', 'source_country', 'target_country', 'malware_family'
    ]
    
    for column in categorical_columns:
        if column in df.columns:
            df[column] = label_encoder.fit_transform(df[column].astype(str))

    # Encode IP addresses to integer representation
    if 'ip' in df.columns:
        df['ip_encoded'] = df['ip'].apply(ip_to_int)

    # Feature Engineering - Convert 'created_indicator' to timestamp
    df['created_indicator_timestamp'] = pd.to_datetime(df['created_indicator'], errors='coerce').astype('int64') / 10**9

    # Normalize latitude and longitude
    latitude_longitude_columns = [
        ('source_latitude', 'source_longitude'),
        ('target_latitude', 'target_longitude')
    ]
    scaler = StandardScaler()
    for lat_col, lon_col in latitude_longitude_columns:
        if lat_col in df.columns and lon_col in df.columns:
            df[[lat_col, lon_col]] = scaler.fit_transform(df[[lat_col, lon_col]])

    return df
