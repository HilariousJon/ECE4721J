# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.1] - 2025-06-28

### Added

- Avro schema
- Compact large amount of small `h5` files to Avro files. 
- Extract Avro files to many `h5` files.
- README contains team members' information and p1m1 features' summary.

## [0.1.1] - 2025-07-07

### Added

- Add more commands in Makefile.
- Add usage in README.
- Use `pyproject.toml` to manage python dependencies.
- Import functions from external repos.

### Changed

- Modify the file structure.  

## [0.1.2] - 2025-07-12

### Added

- Add drill query module.
- Add PCA to judge the distance between artists.
- Add the graph based on the principle components.
- Add query of the distance between two artists.
- Add PCA + linear regression to predict year.
- Add poster & slides think flow.

### Changed

- Refactor drill query 

## [0.1.3] - 2025-07-19

### Added

- Add PCA to reduce 1 dimension and crossjoin + Eucidean distance.
- Add LSH to artist distance.
- Add poster & slides images.

### Fixed

- Fix drill query search for the earliest song year and the latest song year.
- Fix drill query find the name of the band who record the longest song.

## [0.1.4] - 2025-07-21

### Added

- Add random forest to predict year.
- Add gradient boosted tree.

### Fixed 

- Fix drill query most popular song.
- Fix artist distance bug.

### Changed

- Change threshold to "close distance" and the adjacency is more reasonable from top-k.
- Change linear regression 

## [0.1.5] - 2025-07-25

### Added

- Add makefile.
- Add workflow.
- Add song recommendation bfs artist graph
- Add song recommendation bfs song similarity graph
- Add song recommendation ANN algorithms, graph based or tree based

### Fixed

- Add an option to remove songs from same artist while doing song prediction.

## [0.1.6] - 2025-07-31

### Added

- Add poster & slides
- Add MlLib spark year prediction
- Add experiments results
