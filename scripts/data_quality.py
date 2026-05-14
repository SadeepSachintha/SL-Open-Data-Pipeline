import pandas as pd

def validate_weather_data(df):
    """Basic quality checks for weather data."""
    # 1. No null values in temperature
    if df['temperature'].isnull().any():
        raise ValueError("Data Quality Check Failed: Found null values in temperature!")
    
    # 2. Temperature range check (Sri Lanka range approx 15C to 45C)
    if not df['temperature'].between(10, 50).all():
        print("Warning: Temperature outside expected range for Sri Lanka detected.")
    
    # 3. Check for duplicates
    if df.duplicated().any():
        print("Warning: Duplicate records found in weather data.")
    
    return True

def validate_market_data(df):
    """Basic quality checks for market price data."""
    # 1. No negative prices
    if (df['price'] < 0).any():
        raise ValueError("Data Quality Check Failed: Found negative prices!")
    
    # 2. Check for missing markets
    if df['market'].isnull().any():
        raise ValueError("Data Quality Check Failed: Market names are missing!")
    
    return True
