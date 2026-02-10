-- Enable UUID extension if needed, though SERIAL is fine for IDs here.

-- 1. Dimension Tables

CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_id INT PRIMARY KEY,
    vendor_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_rate_code (
    rate_code_id INT PRIMARY KEY,
    rate_code_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type_id INT PRIMARY KEY,
    payment_type_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    quarter INT,
    day_of_week INT,
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id TIME PRIMARY KEY,
    hour INT,
    minute INT,
    second INT,
    time_of_day VARCHAR(50) -- Morning, Afternoon, Evening, Night
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id INT PRIMARY KEY,
    borough VARCHAR(255),
    zone VARCHAR(255),
    service_zone VARCHAR(255)
);

-- 2. Fact Table

CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id SERIAL PRIMARY KEY,
    vendor_id INT,
    pickup_date_id DATE,
    pickup_time_id TIME,
    dropoff_date_id DATE,
    dropoff_time_id TIME,
    rate_code_id INT,
    payment_type_id INT,
    pickup_location_id INT,
    dropoff_location_id INT,
    
    passenger_count FLOAT, -- Sometimes parquet has float for counts with NaNs
    trip_distance FLOAT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    
    FOREIGN KEY (vendor_id) REFERENCES dim_vendor(vendor_id),
    FOREIGN KEY (rate_code_id) REFERENCES dim_rate_code(rate_code_id),
    FOREIGN KEY (payment_type_id) REFERENCES dim_payment_type(payment_type_id),
    FOREIGN KEY (pickup_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (pickup_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (dropoff_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (dropoff_time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (pickup_location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (dropoff_location_id) REFERENCES dim_location(location_id)
);

-- Optional: Indexes for performance
CREATE INDEX idx_fact_trips_pickup_date ON fact_trips(pickup_date_id);
CREATE INDEX idx_fact_trips_vendor ON fact_trips(vendor_id);
