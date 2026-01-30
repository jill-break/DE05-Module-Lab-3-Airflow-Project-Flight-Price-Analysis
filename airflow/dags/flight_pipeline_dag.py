from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'Courage',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# import ETL module functions
from etl_modules.bronze import ingest_raw_data
from etl_modules.silver import clean_flight_data
from etl_modules.gold import (
    compute_airline_performance,
    compute_seasonal_analysis,
    compute_popular_routes,
    load_kpis_to_postgres
)

#  Task Functions 

def run_ingest_csv_to_mysql():
    """
    Wrapper for Bronze Layer ingestion.
    """
    mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
    engine = mysql_hook.get_sqlalchemy_engine()
    ingest_raw_data('/opt/data/Flight_Price_Dataset_of_Bangladesh.csv', engine)

def run_validate_data_quality():
    """
    Wrapper for Silver Layer validation.
    """
    mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
    engine = mysql_hook.get_sqlalchemy_engine()
    
    # Read
    df = mysql_hook.get_pandas_df("SELECT * FROM flight_prices_raw")
    
    # Transform (Pure Logic)
    cleaned_df = clean_flight_data(df)
    
    # Write to Staging
    cleaned_df.to_sql('flight_prices_clean_stg', con=engine, if_exists='replace', index=False)
    
    # Atomic Swap
    with engine.begin() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS flight_prices_clean LIKE flight_prices_clean_stg")
        conn.execute("TRUNCATE TABLE flight_prices_clean")
        conn.execute("INSERT INTO flight_prices_clean SELECT * FROM flight_prices_clean_stg")

def run_compute_kpis():
    """
    Wrapper for Gold Layer KPI computation.
    Reads from MySQL Silver layer, computes KPIs, loads to PostgreSQL.
    """
    # Read cleaned data from MySQL
    mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
    df = mysql_hook.get_pandas_df("SELECT * FROM flight_prices_clean")
    
    print(f"Loaded {len(df)} records from flight_prices_clean")
    
    # Compute KPIs
    airline_performance = compute_airline_performance(df)
    seasonal_analysis = compute_seasonal_analysis(df)
    popular_routes = compute_popular_routes(df)
    
    print(f"Computed KPIs: {len(airline_performance)} airlines, "
          f"{len(seasonal_analysis)} seasons, {len(popular_routes)} routes")
    
    # Load to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_analytics')
    pg_engine = pg_hook.get_sqlalchemy_engine()
    
    load_kpis_to_postgres(pg_engine, airline_performance, seasonal_analysis, popular_routes)

#  DAG Definition 

with DAG(
    'flight_price_analysis_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    template_searchpath='/opt/airflow/sql'
) as dag:
    # Job 1: Bronze Layer - Wait for data file
    wait_for_file = FileSensor(
        task_id='wait_for_csv',
        fs_conn_id='fs_default',
        filepath='Flight_Price_Dataset_of_Bangladesh.csv',
        poke_interval=30 # 
    )
    
    # Job 2: Bronze Layer - Raw Ingestion
    ingest_raw = PythonOperator(
        task_id='ingest_raw_to_mysql',
        python_callable=run_ingest_csv_to_mysql
    )
    
    # Job 3: Silver Layer - Data Validation
    validate_data = PythonOperator(
        task_id='validate_and_clean_data',
        python_callable=run_validate_data_quality
    )

    # Job 4: Gold Layer - KPI Computation (Pure Python)
    compute_kpis = PythonOperator(
        task_id='compute_kpis_and_load_postgres',
        python_callable=run_compute_kpis
    )

    # Define Dependency Flow
    wait_for_file >> ingest_raw >> validate_data >> compute_kpis