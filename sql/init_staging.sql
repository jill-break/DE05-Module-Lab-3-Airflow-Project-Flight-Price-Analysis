CREATE TABLE IF NOT EXISTS flight_prices_raw (
    airline VARCHAR(100),
    source_city VARCHAR(100),
    destination_city VARCHAR(100),
    base_fare DECIMAL(10, 2),
    tax_surcharge DECIMAL(10, 2),
    total_fare DECIMAL(10, 2),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);