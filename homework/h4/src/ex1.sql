-- Top 10 stations with the highest percipitation in 2024
SELECT
    station.s_name AS station,
    station.s_id AS station_id,
    weather_2024.w_val AS percipitation
FROM
    weather_2024
INNER JOIN
    station ON weather_2024.w_station = station.s_id
WHERE
    weather_2024.w_type = 'PRCP'
    AND weather_2024.w_val IS NOT NULL
    AND weather_2024.w_val <> -999
    AND weather_2024.w_val > 0
    AND station.s_name is not NULL
ORDER BY
    weather_2024.w_val DESC
LIMIT 10;

-- Top 10 stations with the highest daily average temperatures in 2022
SELECT
    station.s_name AS station,
    station.s_id AS station_id,
    weather_2022.w_date AS specific_date,
    weather_2022.w_val AS avg_temp
FROM 
    weather_2022
INNER JOIN
    station ON weather_2022.w_station = station.s_id
WHERE 
    weather_2022.w_type = 'TAVG'
    AND weather_2022.w_val IS NOT NULL
    AND weather_2022.w_val <> -999
    AND weather_2022.w_val > 0
    AND station.s_name is not NULL
ORDER BY
    weather_2022.w_val DESC
LIMIT 10;

-- Top 10 stations with the snowfall length larger than 10 in 2023
SELECT 
    station.s_name AS station,
    station.s_id AS station_id,
    weather_2023.w_date AS specific_date,
    weather_2023.w_val AS snowfall_length_inches
FROM 
    weather_2023
INNER JOIN 
    station ON weather_2023.w_station = station.s_id
WHERE
    weather_2023.w_type = 'SNWD'
    AND weather_2023.w_val IS NOT NULL
    AND weather_2023.w_val <> -999
    AND weather_2023.w_val > 20
    AND station.s_name is not NULL
ORDER BY 
    weather_2023.w_val DESC
LIMIT 10;


-- Top 10 date with the lowest average temperature in 2021, Feburary 17th
SELECT 
    station.s_name AS station,
    station.s_id AS station_id,
    weather_2021.w_date AS specific_date,
    weather_2021.w_val AS avg_temp
FROM 
    weather_2021
INNER JOIN
    station ON weather_2021.w_station = station.s_id
WHERE 
    weather_2021.w_type = 'TAVG'
    AND weather_2021.w_val IS NOT NULL
    AND weather_2021.w_val <> -999
    AND weather_2021.w_val > 0
    AND weather_2021.w_date = '20210217'
ORDER BY
    weather_2021.w_val ASC
LIMIT 10;
