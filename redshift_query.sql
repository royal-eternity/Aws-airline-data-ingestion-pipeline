-- =====================================================
-- AWS Airline Data Engineering Pipeline
-- Amazon Redshift SQL
-- =====================================================


-- 1. Create Schema

CREATE SCHEMA IF NOT EXISTS airlines;


-- 2. Create Airport Dimension Table

CREATE TABLE IF NOT EXISTS airlines.airports_dim (
    airport_id BIGINT,
    city VARCHAR(100),
    state VARCHAR(100),
    name VARCHAR(200)
);


-- 3. Load Airport Dimension Data from S3

-- Run this COPY command only when initially loading
-- airport data. Avoid rerunning it unnecessarily.

COPY airlines.airports_dim
FROM 's3://airline-dataset-gds77/dims/airports.csv'
IAM_ROLE 'arn:aws:iam::836548370628:role/RedshiftAirlineS3Role'
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1';


-- 4. Create Daily Flights Fact Table

CREATE TABLE IF NOT EXISTS airlines.daily_flights_fact (
    carrier VARCHAR(10),
    dep_airport VARCHAR(200),
    arr_airport VARCHAR(200),
    dep_city VARCHAR(100),
    arr_city VARCHAR(100),
    dep_state VARCHAR(100),
    arr_state VARCHAR(100),
    dep_delay BIGINT,
    arr_delay BIGINT
);


-- 5. Verify Airport Dimension Records

SELECT COUNT(*) AS total_airports
FROM airlines.airports_dim;




-- 6. Verify Fact Table Structure

SELECT
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'airlines'
  AND table_name = 'daily_flights_fact'
ORDER BY ordinal_position;




-- 7. Preview Flight Fact Data

SELECT *
FROM airlines.daily_flights_fact
LIMIT 10;