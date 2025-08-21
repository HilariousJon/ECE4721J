-- table for stations
CREATE TABLE dfs.h4.`station` AS
SELECT 
    *
FROM (
    SELECT 
        columns[0] AS s_id,
        columns[1] AS s_latitude,
        columns[2] AS s_longtitude,
        columns[3] AS s_altitude,
        columns[4] AS s_state,
        columns[5] AS s_name
    FROM dfs.`/home/hadoopuser/ece4721-submission/hw/h4/src/data/ghcnd-stations.csv`
);

-- table for countries
CREATE TABLE dfs.h4.`country` AS 
SELECT 
    *
FROM (
    SELECT
        columns[0] as c_name,
        columns[1] as c_continent,
        columns[2] as c_fips
        from dfs.`/home/hadoopuser/ece4721-submission/hw/h4/src/data/country_continent.csv`
);

-- table for 2021 2022 2023 2024 2025 data
CREATE TABLE dfs.h4.`weather_2021` AS
SELECT 
    *
FROM (
    SELECT 
        columns[0] as w_station,
        columns[1] as w_date,
        columns[2] as w_type,
        columns[3] as w_val
        FROM dfs.`/home/hadoopuser/ece4721-submission/hw/h4/src/data/2021.csv.gz`
);

CREATE TABLE dfs.h4.`weather_2022` AS
SELECT 
    *
FROM (
    SELECT 
        columns[0] as w_station,
        columns[1] as w_date,
        columns[2] as w_type,
        columns[3] as w_val
        FROM dfs.`/home/hadoopuser/ece4721-submission/hw/h4/src/data/2022.csv.gz`
);

CREATE TABLE dfs.h4.`weather_2022` AS
SELECT 
    *
FROM (
    SELECT 
        columns[0] as w_station,
        columns[1] as w_date,
        columns[2] as w_type,
        columns[3] as w_val
        FROM dfs.`/home/hadoopuser/ece4721-submission/hw/h4/src/data/2022.csv.gz`
);

CREATE TABLE dfs.h4.`weather_2023` AS
SELECT 
    *
FROM (
    SELECT 
        columns[0] as w_station,
        columns[1] as w_date,
        columns[2] as w_type,
        columns[3] as w_val
        FROM dfs.`/home/hadoopuser/ece4721-submission/hw/h4/src/data/2023.csv.gz`
);

CREATE TABLE dfs.h4.`weather_2024` AS
SELECT 
    *
FROM (
    SELECT 
        columns[0] as w_station,
        columns[1] as w_date,
        columns[2] as w_type,
        columns[3] as w_val
        FROM dfs.`/home/hadoopuser/ece4721-submission/hw/h4/src/data/2024.csv.gz`
);

CREATE TABLE dfs.h4.`weather_2025` AS
SELECT 
    *
FROM (
    SELECT 
        columns[0] as w_station,
        columns[1] as w_date,
        columns[2] as w_type,
        columns[3] as w_val
        FROM dfs.`/home/hadoopuser/ece4721-submission/hw/h4/src/data/2025.csv.gz`
);

-- 2025 weather data as default table
CREATE TABLE dfs.h4.`weather` AS
SELECT 
    *
FROM (
    SELECT 
        columns[0] as w_station,
        columns[1] as w_date,
        columns[2] as w_type,
        columns[3] as w_val
        FROM dfs.`/home/hadoopuser/ece4721-submission/hw/h4/src/data/2025.csv.gz`
);

USE dfs.h4;
