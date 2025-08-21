-- import all the necessary data for the labs

-- set the separator as tab, shouldn't written as '\t'
.separator "\t"

-- import all the data files into the corresponding tables
.import data/name.basics.tsv name 
.import data/title.basics.tsv title
-- .import data/title.akas.tsv akas
-- .import data/title.crew.tsv crew
-- .import data/title.episode.tsv episode
.import data/title.ratings.tsv rating
.import data/title.principals.tsv principal
