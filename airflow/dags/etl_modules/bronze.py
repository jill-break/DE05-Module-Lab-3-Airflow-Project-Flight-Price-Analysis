import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

def ingest_raw_data(file_path: str, engine: Engine):
    """
    Job 1: Bronze Layer - Raw Ingestion
    Ingests raw CSV data into MySQL staging table.
    Decoupled from Airflow Hooks for testability.
    """
    print(f"Reading data from {file_path}")
    df = pd.read_csv(file_path)
    
    print("Loading data into flight_prices_raw_stg (Staging)")
    
    # Add ingestion timestamp to match the destination table schema
    df['ingestion_timestamp'] = pd.Timestamp.now()
    
    # Load to staging table first (replace allows fresh staging every time)
    df.to_sql('flight_prices_raw_stg', con=engine, if_exists='replace', index=False)
    
    print("Performing Atomic Swap to flight_prices_raw")
    with engine.begin() as conn:
        # Atomic Transaction
        conn.execute(text("TRUNCATE TABLE flight_prices_raw"))
        conn.execute(text("INSERT INTO flight_prices_raw SELECT * FROM flight_prices_raw_stg"))
    
    print("Ingestion complete.")
