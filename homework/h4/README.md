# ECE4721 homework 4

- before hand, setup your drill workspace as `dfs.h4` and allow for read and write for the data

## Ex.1

1; Explain what is a Join operation, and describe its most common types

- Basics: a join option in database is to concatenate row data from two or more tables based on their related column between them.
- **Common Types:**
  - Inner Join: returns only rows that have matching values in both tables
  - Left Join: returns all rows from the left tables, the matched row from the right table, those without matches from right tables filled with NULL
  - Right Join: returns all rows from the right tables, the matched row from the left table, those without matches from left tables filled with NULL
  - Full Join: returns all rows from the left and right tables, those who matched shared same rows, those who not match is filled with NULL
  - Cross Join: returns the cartesian products of both tables, every row from A combined with every row from B
  - Self Join: a table is jointed with itself, it is used when rows in the same table are related to each other

2; What is an aggregate operation?

- An aggregate operation in databases is computations that summarize multiple rows into one single value, such as sum, avg, count, variance etc. This can give us overview on how the data behave in general and perform data analysis.

3; Write at least three advanced nested queries on the weather database.

- Top 10 stations with the highest percipitation in 2024

```sql
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
```

```shell
+--------------------------------+-------------+---------------+
|            station             | station_id  | percipitation |
+--------------------------------+-------------+---------------+
| GANCA                          | AJ000037735 | 998           |
| NEWTOWN 5.3 S                  | US1CTFR0096 | 998           |
| MASRIK                         | AM000037815 | 998           |
| SISIAN                         | AM000037897 | 998           |
| ARMAVIR                        | AM000037787 | 998           |
| CLEMSON OCONEE CO AP           | USW00053850 | 998           |
| CHORNOMORSKE                   | UPM00033924 | 998           |
| CHERAW                         | USC00381588 | 998           |
| COOKEVILLE 3.9 E               | US1TNPM0021 | 998           |
| BRISTOL 2.8 NNE                | US1CTHR0128 | 998           |
+--------------------------------+-------------+---------------+
10 rows selected (1.602 seconds)
```

- Top 10 stations with the highest daily average temperatures in 2022

```sql
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
```

```shell
+--------------------------------+-------------+---------------+----------+
|            station             | station_id  | specific_date | avg_temp |
+--------------------------------+-------------+---------------+----------+
| Rocky Point                    | USS0063P01S | 20220803      | 994      |
| Quemazon                       | USS0006P01S | 20220911      | 99       |
| FAREWELL ALASKA                | USR0000AFAR | 20220911      | 99       |
| Molas Lake                     | USS0007M12S | 20220911      | 99       |
| Little Snake River             | USS0006H25S | 20220911      | 99       |
| Mountain Meadows               | USS0015D06S | 20220911      | 99       |
| South Brush Creek              | USS0006H19S | 20220911      | 99       |
| Columbus Basin                 | USS0008M10S | 20220911      | 99       |
| Togwotee Pass                  | USS0010F09S | 20220911      | 99       |
| South Colony                   | USS0005M13S | 20220911      | 99       |
+--------------------------------+-------------+---------------+----------+
10 rows selected (1.488 seconds)
```

- Top 10 stations with the snowfall length larger than 10 in 2023

```sql
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
```

```shell
+--------------------------------+-------------+---------------+------------------------+
|            station             | station_id  | specific_date | snowfall_length_inches |
+--------------------------------+-------------+---------------+------------------------+
| HAILEY 1.0 WNW                 | US1IDBN0006 | 20230306      | 998                    |
| Spirit Lk                      | USS0010J55S | 20230101      | 991                    |
| Roach                          | USS0006J12S | 20230101      | 991                    |
| EF Blacks Fork GS              | USS0010J21S | 20230101      | 991                    |
| Lily Lake                      | USS0010J35S | 20230101      | 991                    |
| SNAKE RVR                      | USC00488315 | 20230101      | 991                    |
| Grizzly Peak                   | USS0005K09S | 20230101      | 991                    |
| Loveland Basin                 | USS0005K05S | 20230101      | 991                    |
| Townsend Creek                 | USS0008G07S | 20230101      | 991                    |
| Wild Basin                     | USS0005J05S | 20230101      | 991                    |
+--------------------------------+-------------+---------------+------------------------+
```

- Top 10 date with the lowest average temperature in 20210217

```sql
-- Top 10 date with the lowest average temperature in 20210217
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
```

```shell
+--------------------------------+-------------+---------------+----------+
|            station             | station_id  | specific_date | avg_temp |
+--------------------------------+-------------+---------------+----------+
| Toketee Airstrip               | USS0022F45S | 20210217      | 1        |
| KIRBYVILLE TEXAS               | USR0000TKIR | 20210217      | 1        |
| PROVIDENCE                     | USW00014765 | 20210217      | 1        |
| Jump Off Joe                   | USS0022E07S | 20210217      | 1        |
| GREENSBORO AP                  | USW00013723 | 20210217      | 1        |
| MARAZA                         | AJ000037756 | 20210217      | 1        |
| TWIN BUTTES IDAHO              | USR0000ITWI | 20210217      | 1        |
| OWENS CAMP LOC CALIFORNIA      | USR0000COWE | 20210217      | 1        |
| TRUXTON CANYON ARIZONA         | USR0000ATRU | 20210217      | 10       |
| BUFFALO CREEK NEVADA           | USR0000NBUF | 20210217      | 10       |
+--------------------------------+-------------+---------------+----------+
10 rows selected (0.691 seconds)
```

## Ex.2

### Definition of a perfect whether

- The percipitation level is between 20-30mm
- The maximum temperature is less than 30
- The minimum temperature is greater than 15
- The average temperature is between 20 to 25
- The date is between `0701` to `0831`
- **NOTE:** we shall use the data of 2022, 2023, 2024

### src and outputs

```sql
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
```

```shell
+------+-----------+-----------+--------------------------+--------------------------------+
| year | continent | fips_code |       country_name       |          station_name          |
+------+-----------+-----------+--------------------------+--------------------------------+
| 2022 | SA        | AR        | Argentina                | PERITO MORENO ARPT             |
| 2022 | SA        | AR        | Argentina                | MAQUINCHAO                     |
| 2022 | SA        | AR        | Argentina                | RIO GALLEGOS AERO              |
| 2022 | NA        | US        | United States of America | Atigun Pass                    |
| 2022 | SA        | AR        | Argentina                | EL CALAFATE AERO               |
| 2022 | SA        | AR        | Argentina                | ESQUEL AERO                    |
| 2022 | SA        | AR        | Argentina                | SAN CARLOS DE BARILOCHE        |
| 2022 | NA        | US        | United States of America | Imnaviat Creek                 |
| 2022 | OC        | AS        | Australia                | MACQUARIE ISLAND               |
| 2022 | NA        | US        | United States of America | BARROW AP                      |
| 2022 | SA        | AR        | Argentina                | USHUAIA MALVINAS ARGENTINAS    |
| 2022 | SA        | AR        | Argentina                | RIO GRANDE                     |
| 2023 | OC        | AS        | Australia                | MACQUARIE ISLAND               |
| 2023 | EU        | SZ        | Switzerland              | COL DU GRAND ST-BERNARD        |
| 2023 | SA        | AR        | Argentina                | MAQUINCHAO                     |
| 2023 | SA        | AR        | Argentina                | PUERTO DESEADO                 |
| 2023 | EU        | SI        | Slovenia                 | KREDARICA                      |
| 2023 | SA        | AR        | Argentina                | ESQUEL AERO                    |
| 2023 | SA        | AR        | Argentina                | RIO GALLEGOS AERO              |
| 2023 | SA        | AR        | Argentina                | SAN JULIAN                     |
| 2023 | NA        | US        | United States of America | Atigun Pass                    |
| 2023 | EU        | SZ        | Switzerland              | SAENTIS                        |
| 2023 | SA        | AR        | Argentina                | PERITO MORENO ARPT             |
| 2023 | SA        | AR        | Argentina                | RIO GRANDE                     |
| 2023 | SA        | AR        | Argentina                | USHUAIA MALVINAS ARGENTINAS    |
| 2023 | NA        | US        | United States of America | Imnaviat Creek                 |
| 2024 | NA        | US        | United States of America | Fisher Creek                   |
| 2024 | SA        | AR        | Argentina                | USHUAIA MALVINAS ARGENTINAS    |
| 2024 | NA        | US        | United States of America | Eagle Summit                   |
| 2024 | NA        | US        | United States of America | Prudhoe Bay                    |
| 2024 | NA        | US        | United States of America | Atigun Pass                    |
| 2024 | NA        | US        | United States of America | Imnaviat Creek                 |
| 2024 | SA        | AR        | Argentina                | RIO GRANDE                     |
| 2024 | OC        | AS        | Australia                | MACQUARIE ISLAND               |
| 2024 | NA        | US        | United States of America | BARROW AP                      |
| 2024 | SA        | AR        | Argentina                | PUERTO DESEADO                 |
| 2024 | SA        | AR        | Argentina                | EL CALAFATE AERO               |
+------+-----------+-----------+--------------------------+--------------------------------+
37 rows selected (9.835 seconds)
```

- further polish can be done by output the exact value, but my computer have limited memory to run it.

## Ex.3

- **NOTE:** All of the following plot are based on the data of average temperature of 2024.
- the graph of the temperature variation for each continent.

![graph](img/monthly_temp_variation.jpg)

- the graph of the average temperature for each continent.

![graph](img/avg_temp_by_continent.jpg)