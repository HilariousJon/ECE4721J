import sqlite3

conn = sqlite3.connect("./var/imdb.sqlite3")

c = conn.cursor()

# process genres
genres = []
rows = c.execute("SELECT * FROM title")
for row in rows:
    if row[8] != "\\N" and row[8] is not None:
        for genre in row[8].split(","):
            genres.append((row[0], genre.strip()))

c.executemany("INSERT INTO title_genre (tconst, genre) VALUES (?, ?)", genres)

# process professions
professions = []
rows = c.execute("SELECT * FROM name")
for row in rows:
    if row[4] != "\\N" and row[4] is not None:
        for profession in row[4].split(","):
            professions.append((row[0], profession.strip()))

c.executemany(
    "INSERT INTO name_profession (nconst, profession) VALUES (?, ?)", professions
)

conn.commit()
conn.close()
