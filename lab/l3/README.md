# ECE4721 lab3

## data initialization

- put the data `imdb.tar` in the folder `data` at the code directory.
- run

```shell
cd data
tar -xf imdb.tar
gunzip *.gz
```

- Then we find out all the data being extracted out.

## Create a SQLite databases

- run the following commands

```shell
mkdir var 
sqlite3 var/imdb.sqlite3
```

1; import thew sqlite schema

```shell
sqlite3 var/imdb.sqlite3 < schema.sql
```

2; import the `*.tsv` files into our db to create tables

```shell
sqlite3 var/imdb.sqlite3 < import.sql
```

3; execute our sql scripts

```shell
sqlite3 var/imdb.sqlite3 < ex3.sql > ex3.out
sqlite3 var/imdb.sqlite3 < ex5.sql > ex5.out
```

## Outputs

- outputs for Ex.3, note the output of the last one is too long, so not included

```shell
primaryTitle  startYear
------------  ---------
Birmingham    1896     
primaryTitle       runtimeMinutes
-----------------  --------------
Native of Owhyhee  390           
startYear  movieCount
---------  ----------
2019       14193     
primaryName  moviesCount
-----------  -----------
Ilaiyaraaja  951        
category             primaryName     
-------------------  ----------------
production_designer  Kiril Spaseski  
actor                Ivan Zaric      
director             Slobodan Skerlic
producer             Ivana Mikovic   
composer             Mate Matisic    
cinematographer      Maja Radosevic  
```

- Outputs for Ex.5, note the output of the last one is too long, so not inclueded

```shell
actor|120320|70.0459940159574
writer|61828|71.8133531733195
actress|52496|73.4032688204816
Drama|932565840
Action|604898012
Comedy|572779942
```
