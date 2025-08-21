-- The top 3 most common professions 
-- among these people 
-- and also the average life span 
-- of these three professions
SELECT p.profession,
    COUNT(*) AS counts,
    AVG(n.deathYear - n.birthYear) AS avgLifespan
FROM name_profession p,
    name n
WHERE p.nconst = n.nconst
    AND n.birthYear <> '\N'
    AND n.deathYear <> '\N'
GROUP BY p.profession
ORDER BY counts DESC 
LIMIT 3;

-- The top 3 most popular (received most votes) genres
SELECT g.genre,
    SUM(r.numVotes) AS totalVotes
FROM title_genre g,
    rating r
WHERE g.tconst = r.tconst 
GROUP BY g.genre 
ORDER BY totalVotes DESC
LIMIT 3;

-- The average time span (endYear - startYear) 
-- of the titles for each person
SELECT n.primaryName,
    AVG(t.endYear - t.startYear) AS avgTimeSpan
FROM name n,
    principal p,
    title t 
WHERE n.nconst = p.nconst AND
    p.tconst = t.tconst 
    AND t.startYear IS NOT NULL
    AND t.endYear IS NOT NULL
GROUP BY p.nconst
ORDER BY avgTimeSpan DESC;
