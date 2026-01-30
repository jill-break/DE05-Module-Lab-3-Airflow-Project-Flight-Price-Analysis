"""
Gold Layer - KPI Computation Module
Pure Python/Pandas implementation for computing flight analytics KPIs.
"""
import pandas as pd
from sqlalchemy.engine import Engine


def compute_airline_performance(df: pd.DataFrame) -> pd.DataFrame:
    """
    KPI 1: Average Fare and Booking Count by Airline
    
    Args:
        df: Cleaned flight data from Silver layer
        
    Returns:
        DataFrame with airline, avg_fare, booking_count
    """
    # Ensure we have the required column
    fare_col = 'total_fare_bdt' if 'total_fare_bdt' in df.columns else 'Total Fare (BDT)'
    airline_col = 'airline' if 'airline' in df.columns else 'Airline'
    
    result = df.groupby(airline_col).agg(
        avg_fare=(fare_col, 'mean'),
        booking_count=(fare_col, 'count')
    ).reset_index()
    
    # Normalize column names for output
    result.columns = ['airline', 'avg_fare', 'booking_count']
    return result


def compute_seasonal_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    KPI 2: Seasonal Fare Variation
    
    Categorizes flights as Peak (Winter, Eid, Holiday) or Non-Peak
    and computes average fare for each category.
    
    Args:
        df: Cleaned flight data from Silver layer
        
    Returns:
        DataFrame with season_category, avg_seasonal_fare
    """
    fare_col = 'total_fare_bdt' if 'total_fare_bdt' in df.columns else 'Total Fare (BDT)'
    season_col = 'seasonality' if 'seasonality' in df.columns else 'Seasonality'
    
    # Define peak season pattern
    peak_pattern = r'Winter|Eid|Holiday'
    
    # Create a copy to avoid SettingWithCopyWarning
    df_seasonal = df.copy()
    df_seasonal['season_category'] = df_seasonal[season_col].str.contains(
        peak_pattern, case=False, na=False, regex=True
    ).map({True: 'Peak', False: 'Non-Peak'})
    
    result = df_seasonal.groupby('season_category').agg(
        avg_seasonal_fare=(fare_col, 'mean')
    ).reset_index()
    
    return result


def compute_popular_routes(df: pd.DataFrame) -> pd.DataFrame:
    """
    KPI 3: Most Popular Routes
    
    Identifies top source-destination pairs by booking count.
    
    Args:
        df: Cleaned flight data from Silver layer
        
    Returns:
        DataFrame with source, destination, route_booking_count (sorted desc)
    """
    source_col = 'source' if 'source' in df.columns else 'Source'
    dest_col = 'destination' if 'destination' in df.columns else 'Destination'
    
    result = df.groupby([source_col, dest_col]).size().reset_index(name='route_booking_count')
    result.columns = ['source', 'destination', 'route_booking_count']
    result = result.sort_values('route_booking_count', ascending=False)
    
    return result


def load_kpis_to_postgres(engine: Engine, airline_df: pd.DataFrame, 
                          seasonal_df: pd.DataFrame, routes_df: pd.DataFrame):
    """
    Load all KPI DataFrames to PostgreSQL Analytics database.
    
    Args:
        engine: SQLAlchemy engine for PostgreSQL
        airline_df: Airline performance KPI
        seasonal_df: Seasonal analysis KPI
        routes_df: Popular routes KPI
    """
    # Overwrite mode ensures idempotency for daily runs
    airline_df.to_sql('gold_airline_performance', con=engine, if_exists='replace', index=False)
    seasonal_df.to_sql('gold_seasonal_analysis', con=engine, if_exists='replace', index=False)
    routes_df.to_sql('gold_popular_routes', con=engine, if_exists='replace', index=False)
    
    print("Gold Layer KPIs successfully loaded to PostgreSQL.")
