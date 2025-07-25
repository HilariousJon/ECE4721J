#!/bin/bash

set -e

echo "--- Song Recommender Hadoop Workflow ---"

# parameters, win?
SONG_ID="TRMUOZE12903CDF721"
BFS_DEPTH=2
ARTIST_DB="./data/artist_similarity.db"
META_DB="./data/track_metadata.db"
AVRO_DATA="./year-data/aggregate_year_prediction.avro"
PYTHON_EXEC="poetry run python3"

ARTIST_LIST_FILE="artist_list_input.txt"
FEATURE_FILE="song_features.json"
CACHE_DIR="./.feature_cache"

PREPARE_SCRIPT="./src/m2/bfs/prepare_inputs.py"
WORKER_SCRIPT="./src/m2/bfs/hadoop_worker.py"
UTILS_SCRIPT="./src/m2/bfs/utils.py"

# local environment setup
echo "STEP 1: Preparing local inputs (BFS & Feature Extraction)..."
$PYTHON_EXEC $PREPARE_SCRIPT \
    --song-id $SONG_ID \
    --bfs-depth $BFS_DEPTH \
    --meta-db $META_DB \
    --artist-db $ARTIST_DB \
    --avro-path $AVRO_DATA \
    --out-artists $ARTIST_LIST_FILE \
    --out-features $FEATURE_FILE \
    --cache-dir $CACHE_DIR

echo "STEP 2: Setting up HDFS environment..."
export INPUT_TRACK_ID=$(jq -r '.track_id' $FEATURE_FILE)
export INPUT_FEATURES_JSON=$(jq -r '.features_json' $FEATURE_FILE)

JOB_DIR="hdfs:///user/hadoopuser/song_recommender_$(date +%s)"
INPUT_DIR_STEP1="${JOB_DIR}/input"
OUTPUT_DIR_STEP1="${JOB_DIR}/output_step1"
OUTPUT_DIR_STEP2="${JOB_DIR}/output_final"

hdfs dfs -mkdir -p $INPUT_DIR_STEP1
hdfs dfs -put -f $ARTIST_LIST_FILE $INPUT_DIR_STEP1/

# run hadoop job
HADOOP_STREAMING_JAR="/home/hadoopuser/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar"
FILES_TO_UPLOAD="$WORKER_SCRIPT,$UTILS_SCRIPT,$META_DB,$AVRO_DATA"
MAPRED_ENV="-D mapred.child.env=META_DB_PATH=$(basename $META_DB),AVRO_PATH=$(basename $AVRO_DATA),INPUT_TRACK_ID=$INPUT_TRACK_ID,INPUT_FEATURES_JSON='$INPUT_FEATURES_JSON'"

echo "STEP 3: Running MapReduce Job 1 (Top 200 Songs)..."
hadoop jar $HADOOP_STREAMING_JAR $MAPRED_ENV \
    -D mapreduce.job.name="SongRec - Step 1" -D mapreduce.job.reduces=1 \
    -files $FILES_TO_UPLOAD \
    -input $INPUT_DIR_STEP1 \
    -output $OUTPUT_DIR_STEP1 \
    -mapper "python3 $(basename $WORKER_SCRIPT) --phase mapper1" \
    -reducer "python3 $(basename $WORKER_SCRIPT) --phase reducer1"

echo "STEP 4: Running MapReduce Job 2 (Calculate Similarity)..."
hadoop jar $HADOOP_STREAMING_JAR $MAPRED_ENV \
    -D mapreduce.job.name="SongRec - Step 2" -D mapreduce.job.reduces=1 \
    -files $FILES_TO_UPLOAD \
    -input $OUTPUT_DIR_STEP1 \
    -output $OUTPUT_DIR_STEP2 \
    -mapper "python3 $(basename $WORKER_SCRIPT) --phase mapper2" \
    -reducer "python3 $(basename $WORKER_SCRIPT) --phase reducer2"

# fetch the results
echo "STEP 5: Fetching and displaying results..."
RESULT=$(hdfs dfs -cat $OUTPUT_DIR_STEP2/part-00000)
SCORE=$(echo "$RESULT" | cut -f1)
TITLE=$(echo "$RESULT" | cut -f2)
ARTIST=$(echo "$RESULT" | cut -f3)
TID=$(echo "$RESULT" | cut -f4)

echo "----------------------------------------"
echo ">>> Most Similar Song Found <<<"
echo "  Title:    $TITLE"
echo "  Artist:   $ARTIST"
echo "  Track ID: $TID"
echo "  Score:    $SCORE"
echo "----------------------------------------"

# clear env
echo "STEP 6: Cleaning up HDFS and local temp files..."
hdfs dfs -rm -r $JOB_DIR
rm $ARTIST_LIST_FILE
rm $FEATURE_FILE

echo "--- Workflow Finished ---"