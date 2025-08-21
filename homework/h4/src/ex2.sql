-- 2021
SELECT 
    '2021' AS year,
    c.c_continent AS continent,
    c.c_fips AS fips_code,
    c.c_name AS country_name,
    s.s_name AS station_name
FROM 
    station s
JOIN country c ON SUBSTR(s.s_id, 1, 2) = c.c_fips
WHERE
    EXISTS (
        SELECT 1 FROM weather_2022 w 
        WHERE w.w_station = s.s_id AND w.w_type = 'PRCP'
          AND CAST(w.w_val AS FLOAT) BETWEEN 20 AND 30
          AND w.w_date BETWEEN 20210701 AND 20210831
    )
    AND EXISTS (
        SELECT 1 FROM weather_2022 w 
        WHERE w.w_station = s.s_id AND w.w_type = 'TMAX'
          AND CAST(w.w_val AS FLOAT) < 30
          AND w.w_date BETWEEN 20210701 AND 20210831
    )
    AND EXISTS (
        SELECT 1 FROM weather_2022 w 
        WHERE w.w_station = s.s_id AND w.w_type = 'TMIN'
          AND CAST(w.w_val AS FLOAT) > 15
          AND w.w_date BETWEEN 20210701 AND 20210831
    )
    AND EXISTS (
        SELECT 1 FROM weather_2022 w 
        WHERE w.w_station = s.s_id AND w.w_type = 'TAVG'
          AND CAST(w.w_val AS FLOAT) BETWEEN 20 AND 25
          AND w.w_date BETWEEN 20210701 AND 20210831
    )

UNION ALL
-- 2022
SELECT 
    '2022' AS year,
    c.c_continent AS continent,
    c.c_fips AS fips_code,
    c.c_name AS country_name,
    s.s_name AS station_name
FROM 
    station s
JOIN country c ON SUBSTR(s.s_id, 1, 2) = c.c_fips
WHERE
    EXISTS (
        SELECT 1 FROM weather_2022 w 
        WHERE w.w_station = s.s_id AND w.w_type = 'PRCP'
          AND CAST(w.w_val AS FLOAT) BETWEEN 20 AND 30
          AND w.w_date BETWEEN 20220701 AND 20220831
    )
    AND EXISTS (
        SELECT 1 FROM weather_2022 w 
        WHERE w.w_station = s.s_id AND w.w_type = 'TMAX'
          AND CAST(w.w_val AS FLOAT) < 30
          AND w.w_date BETWEEN 20220701 AND 20220831
    )
    AND EXISTS (
        SELECT 1 FROM weather_2022 w 
        WHERE w.w_station = s.s_id AND w.w_type = 'TMIN'
          AND CAST(w.w_val AS FLOAT) > 15
          AND w.w_date BETWEEN 20220701 AND 20220831
    )
    AND EXISTS (
        SELECT 1 FROM weather_2022 w 
        WHERE w.w_station = s.s_id AND w.w_type = 'TAVG'
          AND CAST(w.w_val AS FLOAT) BETWEEN 20 AND 25
          AND w.w_date BETWEEN 20220701 AND 20220831
    )

UNION ALL

-- 2023
SELECT 
    '2023' AS year,
    c.c_continent AS continent,
    c.c_fips AS fips_code,
    c.c_name AS country_name,
    s.s_name AS station_name
FROM 
    station s
JOIN country c ON SUBSTR(s.s_id, 1, 2) = c.c_fips
WHERE
    EXISTS (
        SELECT 1 FROM weather_2023 w 
        WHERE w.w_station = s.s_id AND w.w_type = 'PRCP'
          AND CAST(w.w_val AS FLOAT) BETWEEN 20 AND 30
          AND w.w_date BETWEEN 20230701 AND 20230831
    )
    AND EXISTS (
        SELECT 1 FROM weather_2023 w 
        WHERE w.w_station = s.s_id AND w.w_type = 'TMAX'
          AND CAST(w.w_val AS FLOAT) < 30
          AND w.w_date BETWEEN 20230701 AND 20230831
    )
    AND EXISTS (
        SELECT 1 FROM weather_2023 w 
        WHERE w.w_station = s.s_id AND w.w_type = 'TMIN'
          AND CAST(w.w_val AS FLOAT) > 15
          AND w.w_date BETWEEN 20230701 AND 20230831
    )
    AND EXISTS (
        SELECT 1 FROM weather_2023 w 
        WHERE w.w_station = s.s_id AND w.w_type = 'TAVG'
          AND CAST(w.w_val AS FLOAT) BETWEEN 20 AND 25
          AND w.w_date BETWEEN 20230701 AND 20230831
    )

UNION ALL

-- 2024
SELECT 
    '2024' AS year,
    c.c_continent AS continent,
    c.c_fips AS fips_code,
    c.c_name AS country_name,
    s.s_name AS station_name
FROM 
    station s
JOIN country c ON SUBSTR(s.s_id, 1, 2) = c.c_fips
WHERE
    EXISTS (
        SELECT 1 FROM weather_2024 w 
        WHERE w.w_station = s.s_id AND w.w_type = 'PRCP'
          AND CAST(w.w_val AS FLOAT) BETWEEN 20 AND 30
          AND w.w_date BETWEEN 20240701 AND 20240831
    )
    AND EXISTS (
        SELECT 1 FROM weather_2024 w 
        WHERE w.w_station = s.s_id AND w.w_type = 'TMAX'
          AND CAST(w.w_val AS FLOAT) < 30
          AND w.w_date BETWEEN 20240701 AND 20240831
    )
    AND EXISTS (
        SELECT 1 FROM weather_2024 w 
        WHERE w.w_station = s.s_id AND w.w_type = 'TMIN'
          AND CAST(w.w_val AS FLOAT) > 15
          AND w.w_date BETWEEN 20240701 AND 20240831
    )
    AND EXISTS (
        SELECT 1 FROM weather_2024 w 
        WHERE w.w_station = s.s_id AND w.w_type = 'TAVG'
          AND CAST(w.w_val AS FLOAT) BETWEEN 20 AND 25
          AND w.w_date BETWEEN 20240701 AND 20240831
    );
