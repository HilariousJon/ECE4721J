# ECE4721J SU25 Project

Big data analysis on [Million Song Dataset(MSD)](http://millionsongdataset.com/) and music recommendation system.

Team Members:

- Aghamatlab Akbarzade
- Li Yanzhi
- Zhang Lingwei
- Zuo Tianyou

## Milestone 1

- Define Avro schema.
- Compact many small `h5` files into a large file by Avro.
- Extract an Avro file to many `h5` files.
- Reference: [MSongsDB](https://github.com/tbertinmahieux/MSongsDB)

## Milestone 2

### Framework

- Project configuration
- Research documentation
- Makefile automation

### Avro

- Schema refactoring

### Drill

- Song-specific queries
- Code refactoring

### Song Recommendation

- Artist distance analysis
- 3 approaches tested:
    - Artist graph BFS
    - Song similarity BFS
    - ANN algorithms

### Year Prediction

- Regression methods
- Classification approaches
- Spark MLlib implementation
