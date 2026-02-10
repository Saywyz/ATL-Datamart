-- 0. Setup Connection to Data Warehouse using Foreign Data Wrapper (FDW)
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Create server object
DROP SERVER IF EXISTS nyc_warehouse_server CASCADE;
CREATE SERVER nyc_warehouse_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'data-warehouse', dbname 'nyc_warehouse', port '5432');

-- Create user mapping
CREATE USER MAPPING IF NOT EXISTS FOR admin
SERVER nyc_warehouse_server
OPTIONS (user 'admin', password 'admin');

-- Import the raw table as a foreign table
IMPORT FOREIGN SCHEMA public
LIMIT TO (nyc_raw)
FROM SERVER nyc_warehouse_server
INTO public;

-- 1. Populate Static Dimensions (Vendor, RateCode, PaymentType) with standard NYC Taxi values
INSERT INTO dim_vendor (vendor_id, vendor_name) VALUES
(1, 'Creative Mobile Technologies, LLC'),
(2, 'VeriFone Inc.')
ON CONFLICT (vendor_id) DO NOTHING;

INSERT INTO dim_rate_code (rate_code_id, rate_code_name) VALUES
(1, 'Standard rate'),
(2, 'JFK'),
(3, 'Newark'),
(4, 'Nassau or Westchester'),
(5, 'Negotiated fare'),
(6, 'Group ride'),
(99, 'Unknown')
ON CONFLICT (rate_code_id) DO NOTHING;

INSERT INTO dim_payment_type (payment_type_id, payment_type_name) VALUES
(0, 'Unknown'),
(1, 'Credit card'),
(2, 'Cash'),
(3, 'No charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided trip')
ON CONFLICT (payment_type_id) DO NOTHING;

-- 2. Populate Date Dimension (Dynamic based on data range + buffer)
INSERT INTO dim_date (date_id, year, month, day, quarter, day_of_week, is_weekend)
SELECT DISTINCT
    d::DATE,
    EXTRACT(YEAR FROM d::DATE),
    EXTRACT(MONTH FROM d::DATE),
    EXTRACT(DAY FROM d::DATE),
    EXTRACT(QUARTER FROM d::DATE),
    EXTRACT(ISODOW FROM d::DATE),
    CASE WHEN EXTRACT(ISODOW FROM d::DATE) IN (6, 7) THEN TRUE ELSE FALSE END
FROM generate_series('2023-01-01'::DATE, '2024-12-31'::DATE, '1 day'::INTERVAL) d
ON CONFLICT (date_id) DO NOTHING;

-- 3. Populate Time Dimension (All seconds in a day)
INSERT INTO dim_time (time_id, hour, minute, second, time_of_day)
SELECT
    t::TIME,
    EXTRACT(HOUR FROM t::TIME),
    EXTRACT(MINUTE FROM t::TIME),
    EXTRACT(SECOND FROM t::TIME),
    CASE
        WHEN EXTRACT(HOUR FROM t::TIME) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM t::TIME) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM t::TIME) BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END
FROM generate_series('2023-01-01 00:00:00'::TIMESTAMP, '2023-01-01 23:59:59'::TIMESTAMP, '1 second'::INTERVAL) t
ON CONFLICT (time_id) DO NOTHING;

-- 4. Populate Location Dimension (From raw data distinct values, as lookup is missing)
INSERT INTO dim_location (location_id, borough, zone, service_zone)
SELECT DISTINCT pulocationid, 'Unknown', 'Unknown', 'Unknown' FROM nyc_raw WHERE pulocationid IS NOT NULL
UNION
SELECT DISTINCT dolocationid, 'Unknown', 'Unknown', 'Unknown' FROM nyc_raw WHERE dolocationid IS NOT NULL
ON CONFLICT (location_id) DO NOTHING;

-- 5. Populate Fact Table
INSERT INTO fact_trips (
    vendor_id,
    pickup_date_id,
    pickup_time_id,
    dropoff_date_id,
    dropoff_time_id,
    rate_code_id,
    payment_type_id,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee
)
SELECT
    vendorid,
    tpep_pickup_datetime::DATE,
    tpep_pickup_datetime::TIME,
    tpep_dropoff_datetime::DATE,
    tpep_dropoff_datetime::TIME,
    ratecodeid,
    payment_type,
    pulocationid,
    dolocationid,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee
FROM nyc_raw
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL;
