.mode column
.headers on

-- find the oldest movie
SELECT primaryTitle, startYear
FROM title
WHERE titleType = 'movie' 
ORDER BY startYear ASC
LIMIT 1;

-- find the longest movie in 2009
SELECT primaryTitle, runtimeMinutes
FROM title
WHERE titleType = 'movie' AND 
        startYear = '2009' AND 
        runtimeMinutes <> '\N'
ORDER BY runtimeMinutes DESC 
LIMIT 1;

-- find the year with the most movies
SELECT startYear, COUNT(*) AS movieCount
FROM title 
WHERE titleType = 'movie' AND 
        startYear <> '\N'
GROUP BY startYear  
ORDER BY movieCount DESC
LIMIT 1;

-- find the name of person who contains in the most movies
SELECT n.primaryName, COUNT(*) AS moviesCount
FROM name n
JOIN principal p ON n.nconst = p.nconst 
JOIN title t ON p.tconst = t.tconst 
WHERE t.titleType = 'movie'
GROUP BY n.nconst
ORDER BY moviesCount DESC 
LIMIT 1;

-- find the principle crew of 
-- the movie with highest average ratings
-- and more than 500 votes
WITH top_movie AS (
  SELECT tconst
  FROM rating
  WHERE numVotes > 500
  ORDER BY averageRating DESC
  LIMIT 1
)
SELECT p.category, n.primaryName
FROM principal p
JOIN name n ON p.nconst = n.nconst
WHERE p.tconst = (SELECT tconst FROM top_movie);


-- (Advanced) The count of each 
-- Pair<BirthYear, DeathYear> of the people
SELECT n.birthYear, n.deathYear, COUNT(*) AS count
FROM name n 
WHERE n.birthYear IS NOT NULL AND n.deathYear IS NOT NULL
GROUP BY n.birthYear, n.deathYear 
ORDER BY count DESC;
