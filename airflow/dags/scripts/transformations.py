import pandas as pd

def transform_flight_data(df: pd.DataFrame):
    """
    Silver Layer: Handles data type conversion and mandatory fare calculations.
    """
    # 1. Ensure numeric types for calculation
    df['Base Fare (BDT)'] = pd.to_numeric(df['Base Fare (BDT)'], errors='coerce').fillna(0)
    df['Tax & Surcharge (BDT)'] = pd.to_numeric(df['Tax & Surcharge (BDT)'], errors='coerce').fillna(0)
    
    # 2. Requirement: Calculate Total Fare = Base Fare + Tax & Surcharge
    df['Total Fare (BDT)'] = df['Base Fare (BDT)'] + df['Tax & Surcharge (BDT)']
    
    # 3. Cleaning for Spark
    # Standardize column names for easier Spark SQL handling later
    df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').lower() for c in df.columns]
    
    return df