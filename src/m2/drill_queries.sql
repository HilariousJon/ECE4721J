

-- Query to find the earliest and latest year in the dataset

SELECT
  MIN(year) AS earliest_year,
  MAX(year) AS latest_year
FROM dfs.`__PROJECT_PATH__data/aggregate.avro`

WHERE year > 0;

-- Query to find the shortest song with the highest energy and lowest tempo

SELECT
  song_id,
  track_id,
  title,
  artist_name,
  duration,
  energy,
  tempo
FROM dfs.`__PROJECT_PATH__data/aggregate.avro`
WHERE year > 0
ORDER BY
  duration    ASC,   -- shortest first
  energy      DESC,  -- among those, highest energy
  tempo       ASC    -- among ties, lowest tempo
LIMIT 1;

-- Query to find the album with the most songs
SELECT
  release,
  COUNT(*) AS song_per_album
FROM dfs.`__PROJECT_PATH__data/aggregate.avro`
GROUP BY
  release
ORDER BY
  song_per_album DESC
LIMIT 1;


-- Query to find the longest song in the dataset
SELECT
  duration,
  artist_name
FROM dfs.`__PROJECT_PATH__data/aggregate.avro`
ORDER BY
  duration DESC
LIMIT 1;