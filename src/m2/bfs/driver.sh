#!/bin/bash

set -e 

STREAMING_JAR="/home/hadoopuser/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar"

# Define local directories for better organization.
PYTHON_SCRIPTS_DIR="./src/m2/bfs"
DATA_DIR="./data"
OUTPUT_DATA_DIR="./year-data"

ARTIST_DB="${DATA_DIR}/artist_similarity.db"
META_DB="${DATA_DIR}/track_metadata.db"

# Job parameters.
INPUT_SONG_ID="TRMUOZE12903CDF721" # The track_id to find recommendations for.
BFS_DEPTH=2 # How many layers of artist similarity to explore.

HDFS_WORKDIR="/user/$(whoami)/song_similarity_$(date +%s)"
HDFS_INPUT_SONG_DATA="${HDFS_WORKDIR}/input/song_data.jsonl"
HDFS_BFS_BASE="${HDFS_WORKDIR}/bfs"
HDFS_SONGS_OUTPUT="${HDFS_WORKDIR}/songs_output"
HDFS_TOP_SONGS_OUTPUT="${HDFS_WORKDIR}/top_songs_output"
HDFS_FINAL_OUTPUT="${HDFS_WORKDIR}/final_output"

echo "--- Starting Song Similarity Workflow ---"
echo "HDFS Working Directory: ${HDFS_WORKDIR}"

echo ">>> STEP 0: Cleaning up and preparing HDFS directory..."
# Check if the HDFS working directory already exists.
if hdfs dfs -test -d "${HDFS_WORKDIR}"; then
    echo "HDFS directory ${HDFS_WORKDIR} exists. Removing it for a fresh start."
    # Remove the old directory. '-skipTrash' permanently deletes it.
    hdfs dfs -rm -r -skipTrash "${HDFS_WORKDIR}"
fi
echo "Creating new HDFS directory structure at ${HDFS_WORKDIR}"
hdfs dfs -mkdir -p "${HDFS_WORKDIR}/input"

# --- LOCAL SETUP AND UPLOAD ---
echo ">>> STEP 1: Preparing local files and uploading to HDFS..."
# Run the local python script to generate the initial artist list.
python3 "${PYTHON_SCRIPTS_DIR}/mapreduce_setup.py" "$INPUT_SONG_ID" "$META_DB"

hdfs dfs -put "${OUTPUT_DATA_DIR}/song_data.jsonl" "${HDFS_INPUT_SONG_DATA}"
hdfs dfs -put "${OUTPUT_DATA_DIR}/input_song_features.json" "${HDFS_WORKDIR}/"
hdfs dfs -put ./initial_artists.txt "${HDFS_WORKDIR}/input/"
echo "$INPUT_SONG_ID" > ./input_song_id.txt 
hdfs dfs -put ./input_song_id.txt "${HDFS_WORKDIR}/"
rm ./input_song_id.txt 
echo ">>> STEP 1: COMPLETE"

# --- JOB 1: ITERATIVE BFS ---
echo ">>> JOB 2: Running Iterative BFS for depth ${BFS_DEPTH}..."
BFS_INPUT="${HDFS_WORKDIR}/input/initial_artists.txt"
for i in $(seq 1 $BFS_DEPTH); do
    echo "  -> BFS Depth ${i}"
    BFS_OUTPUT="${HDFS_BFS_BASE}/depth_${i}"
    hadoop jar "$STREAMING_JAR" \
        -D mapreduce.job.name="SongSim_BFS_Depth_${i}" \
        -files "${PYTHON_SCRIPTS_DIR}/utils.py,${PYTHON_SCRIPTS_DIR}/mapper_bfs.py,${PYTHON_SCRIPTS_DIR}/reducer_bfs.py,${ARTIST_DB}" \
        -input "$BFS_INPUT" \
        -output "$BFS_OUTPUT" \
        -mapper "python3 mapper_bfs.py" \
        -reducer "python3 reducer_bfs.py"
    BFS_INPUT="$BFS_OUTPUT" 
done
BFS_FINAL_OUTPUT=$BFS_INPUT
echo ">>> JOB 2: COMPLETE. Final artist list is in ${BFS_FINAL_OUTPUT}"

# --- JOB 3: GET SONGS FROM ARTISTS ---
echo ">>> JOB 3: Fetching all songs for discovered artists..."
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="SongSim_GetSongs" \
    -files "${PYTHON_SCRIPTS_DIR}/utils.py,${PYTHON_SCRIPTS_DIR}/mapper_get_songs.py,${PYTHON_SCRIPTS_DIR}/reducer_get_songs.py,${META_DB}" \
    -input "$BFS_FINAL_OUTPUT" \
    -output "$HDFS_SONGS_OUTPUT" \
    -mapper "python3 mapper_get_songs.py" \
    -reducer "python3 reducer_get_songs.py"
[ -f candidate_song_ids.txt ] && rm candidate_song_ids.txt
hdfs dfs -cat "${HDFS_SONGS_OUTPUT}/part-*" > candidate_song_ids.txt
echo ">>> JOB 3: COMPLETE. Found $(wc -l < candidate_song_ids.txt) candidate songs."

# --- JOB 4: FILTER TOP 200 HOTTEST SONGS ---
echo ">>> JOB 4: Filtering for Top 200 Hottest Songs..."
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="SongSim_TopSongs" \
    -files "${PYTHON_SCRIPTS_DIR}/mapper_top_songs.py,${PYTHON_SCRIPTS_DIR}/reducer_top_songs.py,./candidate_song_ids.txt,${HDFS_WORKDIR}/input_song_id.txt" \
    -input "$HDFS_INPUT_SONG_DATA" \
    -output "$HDFS_TOP_SONGS_OUTPUT" \
    -mapper "python3 mapper_top_songs.py" \
    -reducer "python3 reducer_top_songs.py" \
    -numReduceTasks 1
echo ">>> JOB 4: COMPLETE."

# --- JOB 5: CALCULATE SIMILARITY ---
echo ">>> JOB 5: Calculating similarity for top songs..."
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="SongSim_FindSimilar" \
    -files "${PYTHON_SCRIPTS_DIR}/utils.py,${PYTHON_SCRIPTS_DIR}/mapper_similarity.py,${PYTHON_SCRIPTS_DIR}/reducer_similarity.py,${OUTPUT_DATA_DIR}/input_song_features.json" \
    -input "$HDFS_TOP_SONGS_OUTPUT" \
    -output "$HDFS_FINAL_OUTPUT" \
    -mapper "python3 mapper_similarity.py" \
    -reducer "python3 reducer_similarity.py" \
    -numReduceTasks 1
echo ">>> JOB 5: COMPLETE."

echo ""
echo "--- FINAL RESULT ---"
hdfs dfs -cat "${HDFS_FINAL_OUTPUT}/part-00000"
echo "--------------------"
echo ">>> Cleaning up local temporary files..."
rm ./candidate_song_ids.txt ./initial_artists.txt
echo ">>> Workflow Finished ---"
echo "You can manually clean up the HDFS directory with: hdfs dfs -rm -r -skipTrash ${HDFS_WORKDIR}"