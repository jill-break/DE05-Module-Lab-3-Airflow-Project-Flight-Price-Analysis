import pytest
import pandas as pd
from etl_modules.silver import clean_flight_data, validate_fare_logic

def test_clean_flight_data_nulls():
    """Test that null values in critical columns are dropped."""
    data = {
        'Airline': ['A', None, 'C'],
        'Total Fare (BDT)': [1000, 2000, None]
    }
    df = pd.DataFrame(data)
    
    cleaned_df = clean_flight_data(df)
    
    assert len(cleaned_df) == 1
    assert cleaned_df.iloc[0]['Airline'] == 'A'

def test_clean_flight_data_negative_fare():
    """Test that negative fares are removed."""
    data = {
        'Airline': ['A', 'B'],
        'Total Fare (BDT)': [1000, -500]
    }
    df = pd.DataFrame(data)
    
    cleaned_df = clean_flight_data(df)
    
    assert len(cleaned_df) == 1
    assert cleaned_df.iloc[0]['Airline'] == 'A'

def test_fare_math_correction():
    """Test logic for Base + Tax = Total."""
    # Placeholder for when validate_fare_logic is implemented
    pass