import pandas as pd

def run_flight_validation(df: pd.DataFrame):
    """
    Silver Layer: Validates data quality and performs basic cleaning.
    """
    # Requirement: All required columns exist
    required_cols = [
        'Airline', 'Source', 'Destination', 'Base Fare (BDT)', 
        'Tax & Surcharge (BDT)', 'Total Fare (BDT)'
    ]
    
    # Handle missing or null values
    clean_df = df.dropna(subset=['Airline', 'Total Fare (BDT)']).copy()

    # Validate data types and handle negative fares
    clean_df['Total Fare (BDT)'] = pd.to_numeric(clean_df['Total Fare (BDT)'], errors='coerce')
    clean_df = clean_df[clean_df['Total Fare (BDT)'] > 0]

    # Transformation: Calculate Total Fare = Base Fare + Tax & Surcharge
    clean_df['Total Fare (BDT)'] = clean_df['Base Fare (BDT)'] + clean_df['Tax & Surcharge (BDT)']

    return clean_df