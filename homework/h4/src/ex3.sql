SET `store.format` = 'csv';

CREATE TABLE dfs.h4.`2024_temperature_data` AS 
SELECT
    country.c_continent AS continent,
    SUBSTR(weather_2024.w_date, 5, 2) AS month,
    AVG(CAST(weather_2024.w_val AS FLOAT)) AS avg_temperature
FROM 
    weather_2024
INNER JOIN 
    country ON SUBSTR(weather_2024.w_station, 1, 2) = country.c_fips
WHERE
    weather_2024.w_type = 'TAVG' -- we only consider the average temperature per day
GROUP BY
    country.c_continent, month
ORDER BY
    country.c_continent, month;