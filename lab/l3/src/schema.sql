-- create schema for labs 

create table name (
    nconst varchar(10) not null primary key,
    primaryName text not null,
    birthYear varchar(4) not null,
    deathYear varchar(4) not null,
    primaryProfession text not null,
    knownForTitles text not null
);

create table title (
    tconst varchar(10) not null primary key,
    titleType varchar(64) not null,
    primaryTitle text not null,
    originalTitle text not null,
    isAdult boolean not null,
    startYear varchar(4) not null,
    endYear varchar(4) not null,
    runtimeMinutes integer not null,
    genres text not null
);

create table rating (
    tconst varchar(10) not null,
    averageRating double not null,
    numVotes integer not null
);

create table principal (
    tconst varchar(10) not null,
    ordering integer not null,
    nconst varchar(10) not null,
    category text not null,
    job text not null,
    characters text not null
);

create table name_profession (
    nconst varchar(10) not null,
    profession text not null,
    foreign key(nconst) references name(nconst)
);

create table title_genre (
    tconst varchar(10) not null,
    genre text not null,
    foreign key(tconst) references title(tconst)
);

create table akas (
    titleId varchar(10) not null,
    ordering int not null,
    title text not null,
    region varchar(2) not null,
    language varchar(2) not null,
    types text not null,
    attributes text not null,
    isOriginalTitle boolean not null
);

create table episode (
    tconst varchar(10) not null,
    parentTconst varchar(10) not null,
    seasonNumber int not null,
    episodeNumber int not null
);

create table crew (
    tconst varchar(10) not null,
    directors text not null,
    writers text not null
);