# ECE4721J SU25 Project

## Preparation

- Make sure you run the program on Linux and use Python3.  
- Clone this repository:
```bash
git clone ssh://git@focs.ji.sjtu.edu.cn:2222/ece472-25su/p1team02.git
```
- Run the program under `p1team02/` rather than `p1team02/src/`.
- Create a virtual environment of python and install dependencies:
```bash
make init_env
```

## Usage

### 1. Data Management

#### Mount dataset (run after computer reset)

```bash
make mount_data_init
```

#### Unmount when finished
```bash
make unmount_data
```

### 2. Data Conversion

#### Convert HDF5 to Avro (Spark version)

```bash
make aggregate_avro
```

#### Alternative non-Spark conversion

```bash
make agg_avro
```

### Extract HDF5 from Avro

```bash
make extract AVRO_FILE=<path_to_avro_file> OUTPUT_DIR=<output_dir>
```

### 3. Graph Analysis

#### Build artist similarity graph

```bash
make build_artists_graph
```

#### Query distance between artists

```bash
make query_artists_distance
```

#### Build song similarity graph

```bash
make build_songs_graph
```

### 4. Recommendation Systems

#### Graph-based recommendations

```bash
make song_recommend
```

#### ANN-based recommendations (HNSW)

```bash
make run_ann_HNSW_build
make query_ann_HNSW
```

#### ANN-based recommendations (LSH)

```bash
make run_ann_LSH_build
make query_ann_LSH
```

### 5. Year Prediction

#### Train models

```bash
make train_ridge  # Ridge Regression
make train_gbt    # Gradient Boosted Trees
make train_lr     # Linear Regression
make train_xgboost # XGBoost
```

### 6. MapReduce BFS

#### Local mode

```bash
make run_mapreduce_bfs_local
```

#### Cluster mode

```bash
make run_mapreduce_bfs_cluster
```

### 7. Drill Queries

```bash
make run_drill
```

### 8. Maintenance

#### Format AVSC schema files

```bash
make fmt_json
```

#### Auto-commit changes

```bash
make commit
```
