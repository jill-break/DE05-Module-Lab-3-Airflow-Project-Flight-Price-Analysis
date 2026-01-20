DROP TABLE IF EXISTS flight_prices_raw;
CREATE TABLE flight_prices_raw (
    `Airline` VARCHAR(255),
    `Source` VARCHAR(100),
    `Source Name` VARCHAR(255),
    `Destination` VARCHAR(100),
    `Destination Name` VARCHAR(255),
    `Departure Date & Time` VARCHAR(100),
    `Arrival Date & Time` VARCHAR(100),
    `Duration (hrs)` DECIMAL(15, 6),
    `Stopovers` VARCHAR(50),
    `Aircraft Type` VARCHAR(100),
    `Class` VARCHAR(50),
    `Booking Source` VARCHAR(100),
    `Base Fare (BDT)` DECIMAL(15, 4),
    `Tax & Surcharge (BDT)` DECIMAL(15, 4),
    `Total Fare (BDT)` DECIMAL(15, 4),
    `Seasonality` VARCHAR(50),
    `Days Before Departure` INT,
    `ingestion_timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);