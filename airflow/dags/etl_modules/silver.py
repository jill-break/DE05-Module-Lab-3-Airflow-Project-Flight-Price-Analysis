import pandas as pd

def clean_flight_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pure transformation function to validate and clean flight data.
    This logic is now testable without a database connection.
    """
    # 1. Handle Nulls
    if df.isnull().values.any():
        df = df.dropna(subset=['Airline', 'Total Fare (BDT)'])
    
    # 2. Validate Fares (Non-negative)
    # Ensure column exists before filtering
    if 'Total Fare (BDT)' in df.columns:
        df = df[df['Total Fare (BDT)'] > 0]
        # 3. Correct data types and Apply Business Rule
        df = validate_fare_logic(df)
    
    return df

def validate_fare_logic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies business rule: Total Fare = Base Fare + Tax & Surcharge
    """
    required_cols = ['Base Fare (BDT)', 'Tax & Surcharge (BDT)', 'Total Fare (BDT)']
    
    # Check if we have the necessary columns
    if not all(col in df.columns for col in required_cols):
        return df

    # Convert to numeric, forcing errors to NaN
    for col in required_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Calculate Expected Total
    expected_total = df['Base Fare (BDT)'].fillna(0) + df['Tax & Surcharge (BDT)'].fillna(0)
    
    # Update Total Fare where necessary (or just overwrite to be safe and consistent)
    # This ensures "Transformation: If not already present, calculate Total Fare" requirement is met.
    df['Total Fare (BDT)'] = expected_total
    
    # Re-map to the snake_case column expected by downstream Spark
    df['total_fare_bdt'] = df['Total Fare (BDT)']
    
    return df
