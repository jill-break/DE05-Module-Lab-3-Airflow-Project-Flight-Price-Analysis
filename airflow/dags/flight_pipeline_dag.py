from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'Courage',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#  Task Functions 

def ingest_csv_to_mysql():
    """
    Job 1: Bronze Layer - Raw Ingestion
    Ingests raw CSV data into MySQL staging table."""
    mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
    df = pd.read_csv('/opt/data/Flight_Price_Dataset_of_Bangladesh.csv')
    engine = mysql_hook.get_sqlalchemy_engine()
    # Truncate before load to ensure idempotency for this specific project
    with engine.connect() as conn:
        conn.execute("TRUNCATE TABLE flight_prices_raw")
    df.to_sql('flight_prices_raw', con=engine, if_exists='append', index=False)

def validate_data_quality():
    """
    Job 2: Silver Layer - Data Validation
    Ensures data quality checks and basic cleaning.
    """
    mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
    df = mysql_hook.get_pandas_df("SELECT * FROM flight_prices_raw")
    
    # Requirements Check
    # 1. Handle Nulls
    if df.isnull().values.any():
        df = df.dropna(subset=['Airline', 'Total Fare (BDT)'])
    
    # 2. Validate Fares (Non-negative)
    df = df[df['Total Fare (BDT)'] > 0]
    
    # 3. Correct data types for specific columns
    df['total_fare_bdt'] = pd.to_numeric(df['Total Fare (BDT)'], errors='coerce')
    
    # Save cleaned data to a temporary location or XCom
    mysql_hook.get_sqlalchemy_engine().connect().execute("CREATE TABLE IF NOT EXISTS flight_prices_clean AS SELECT * FROM flight_prices_raw WHERE 1=0")
    df.to_sql('flight_prices_clean', con=mysql_hook.get_sqlalchemy_engine(), if_exists='replace', index=False)

def load_to_postgres_analytics():
    """
    Job 3: Gold Layer - KPI Computation & Final Load
     Computes KPIs and loads them into PostgreSQL Analytics DB.
    """
    mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
    pg_hook = PostgresHook(postgres_conn_id='postgres_analytics')
    
    # Pull the cleaned data
    clean_df = mysql_hook.get_pandas_df("SELECT * FROM flight_prices_clean")
    
    # Compute KPIs: Average Fare by Airline
    kpi_df = clean_df.groupby('Airline')['Total Fare (BDT)'].mean().reset_index()
    kpi_df.columns = ['airline', 'avg_fare']
    
    # Load into Postgres
    pg_engine = pg_hook.get_sqlalchemy_engine()
    kpi_df.to_sql('kpi_airline_avg_fare', con=pg_engine, if_exists='replace', index=False)

#  DAG Definition 

with DAG(
    'flight_price_analysis_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    template_searchpath='/opt/airflow/sql'
) as dag:
    # Job 1: Bronze Layer - Raw Ingestion
    wait_for_file = FileSensor(
        task_id='wait_for_csv',
        fs_conn_id='fs_default',
        filepath='Flight_Price_Dataset_of_Bangladesh.csv',
        poke_interval=30
    )
    # Job 2.1: Silver Layer - Data Validation
    ingest_raw = PythonOperator(
        task_id='ingest_raw_to_mysql',
        python_callable=ingest_csv_to_mysql
    )
    # Job 2.2: Silver Layer - Data Validation
    validate_data = PythonOperator(
        task_id='validate_and_clean_data',
        python_callable=validate_data_quality
    )

    # load_analytics = PythonOperator(
    #     task_id='compute_kpis_and_load_postgres',
    #     python_callable=load_to_postgres_analytics
    # )

   # Job 3: Gold Layer using Spark
    compute_kpis_spark = SparkSubmitOperator(
        task_id='compute_kpis_with_spark',
        application='/opt/jobs/transformation/transform_flight_data.py',  # Mapped to your ./spark folder
        conn_id='spark_default',
        # conf={
        #     "spark.master": "local[*]",
        #     "spark.driver.extraClassPath": "/opt/jars/mysql-connector-j-9.5.0.jar,/opt/jars/postgresql-42.7.6.jar"
        #     },

        name="arrow-spark",
        # executor_memory='1g',
        # driver_memory='1g',
        # We need to include the MySQL and Postgres JDBC drivers here
        jars='/opt/jars/mysql-connector-j-9.5.0.jar,/opt/jars/postgresql-42.7.6.jar',
        dag=dag
    )

    # Define Dependency Flow with Python Jobs
    #wait_for_file >> ingest_raw >> validate_data >> load_analytics

    # Define Dependency Flow with Spark Job
    wait_for_file >> ingest_raw >> validate_data >> compute_kpis_spark