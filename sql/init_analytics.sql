-- 1. Airline Performance: Stores Average Fares and Booking Counts
CREATE TABLE IF NOT EXISTS gold_airline_performance (
    airline VARCHAR(255) PRIMARY KEY,
    avg_fare DECIMAL(15, 2),
    booking_count INT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Seasonal Analysis: Compares Peak vs Non-Peak pricing
CREATE TABLE IF NOT EXISTS gold_seasonal_analysis (
    season_category VARCHAR(50) PRIMARY KEY, -- e.g., 'Peak', 'Non-Peak'
    avg_seasonal_fare DECIMAL(15, 2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Popular Routes: Identifies the busiest source-destination pairs
CREATE TABLE IF NOT EXISTS gold_popular_routes (
    source VARCHAR(100),
    destination VARCHAR(100),
    route_booking_count INT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source, destination)
);