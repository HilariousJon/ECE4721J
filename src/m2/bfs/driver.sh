#!/bin/bash
set -e 

# --- CONFIGURATION ---
STREAMING_JAR="/home/hadoopuser/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar"
HDFS_WORKDIR="/user/$(whoami)/song_similarity_$(date +%s)"
ARTIST_DB="./data/artist_similarity.db"
META_DB="./data/track_metadata.db"
INPUT_SONG_ID="TRMUOZE12903CDF721" 
BFS_DEPTH=2

# --- HDFS Paths ---
HDFS_INPUT_SONG_DATA="${HDFS_WORKDIR}/input/song_data.jsonl"
HDFS_BFS_BASE="${HDFS_WORKDIR}/bfs"
HDFS_SONGS_OUTPUT="${HDFS_WORKDIR}/songs_output"
HDFS_TOP_SONGS_OUTPUT="${HDFS_WORKDIR}/top_songs_output"
HDFS_FINAL_OUTPUT="${HDFS_WORKDIR}/final_output"

echo "--- Starting Song Similarity Workflow ---"
echo "HDFS Working Directory: ${HDFS_WORKDIR}"

# --- JOB 0: SETUP AND UPLOAD ---
echo ">>> JOB 0: Preparing local files and uploading to HDFS..."
python3 ./src/m2/bfs/mapreduce_setup.py "$INPUT_SONG_ID" "$META_DB"
hdfs dfs -mkdir -p "${HDFS_WORKDIR}/input"
hdfs dfs -put ./year-data/song_data.jsonl "${HDFS_INPUT_SONG_DATA}"
hdfs dfs -put ./year-data/input_song_features.json "${HDFS_WORKDIR}/"
hdfs dfs -put initial_artists.txt "${HDFS_WORKDIR}/input/"
echo "$INPUT_SONG_ID" > input_song_id.txt 
hdfs dfs -put input_song_id.txt "${HDFS_WORKDIR}/"
rm input_song_id.txt 
echo ">>> JOB 0: COMPLETE"

# --- JOB 1: ITERATIVE BFS ---
echo ">>> JOB 1: Running Iterative BFS for depth ${BFS_DEPTH}..."
BFS_INPUT="${HDFS_WORKDIR}/input/initial_artists.txt"
for i in $(seq 1 $BFS_DEPTH); do
    echo "  -> BFS Depth ${i}"
    BFS_OUTPUT="${HDFS_BFS_BASE}/depth_${i}"
    hadoop jar "$STREAMING_JAR" \
        -D mapreduce.job.name="SongSim_1_BFS_Depth_${i}" \
        -files "./src/m2/bfs/mapper_bfs.py,./src/m2/bfs/reducer_bfs.py,./src/m2/bfs/utils.py,${ARTIST_DB}" \
        -input "$BFS_INPUT" \
        -output "$BFS_OUTPUT" \
        -mapper "python3 mapper_bfs.py" \
        -reducer "python3 reducer_bfs.py"
    BFS_INPUT="$BFS_OUTPUT" 
done
BFS_FINAL_OUTPUT=$BFS_INPUT
echo ">>> JOB 1: COMPLETE. Final artist list is in ${BFS_FINAL_OUTPUT}"

# --- JOB 2: GET SONGS FROM ARTISTS ---
echo ">>> JOB 2: Fetching all songs for discovered artists..."
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="SongSim_2_GetSongs" \
    -files "mapper_get_songs.py,reducer_get_songs.py,utils.py,${META_DB}" \
    -input "$BFS_FINAL_OUTPUT" \
    -output "$HDFS_SONGS_OUTPUT" \
    -mapper "python3 mapper_get_songs.py" \
    -reducer "python3 reducer_get_songs.py"
[ -f candidate_song_ids.txt ] && rm candidate_song_ids.txt
hdfs dfs -cat "${HDFS_SONGS_OUTPUT}/part-*" > candidate_song_ids.txt
echo ">>> JOB 2: COMPLETE. Found $(wc -l < candidate_song_ids.txt) candidate songs."

# --- JOB 3: FILTER TOP 200 HOTTEST SONGS ---
echo ">>> JOB 3: Filtering for Top 200 Hottest Songs..."
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="SongSim_3_TopSongs" \
    -files "mapper_top_songs.py,reducer_top_songs.py,candidate_song_ids.txt,${HDFS_WORKDIR}/input_song_id.txt" \
    -input "$HDFS_INPUT_SONG_DATA" \
    -output "$HDFS_TOP_SONGS_OUTPUT" \
    -mapper "python3 mapper_top_songs.py" \
    -reducer "python3 reducer_top_songs.py" \
    -numReduceTasks 1
echo ">>> JOB 3: COMPLETE."

# --- JOB 4: CALCULATE SIMILARITY ---
echo ">>> JOB 4: Calculating similarity for top songs..."
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="SongSim_4_FindSimilar" \
    -files "mapper_similarity.py,reducer_similarity.py,utils.py,${HDFS_WORKDIR}/input_song_features.json" \
    -input "$HDFS_TOP_SONGS_OUTPUT" \
    -output "$HDFS_FINAL_OUTPUT" \
    -mapper "python3 mapper_similarity.py" \
    -reducer "python3 reducer_similarity.py" \
    -numReduceTasks 1
echo ">>> JOB 4: COMPLETE."

# --- FINAL STEP: DISPLAY RESULTS AND CLEANUP ---
echo ""
echo "--- FINAL RESULT ---"
hdfs dfs -cat "${HDFS_FINAL_OUTPUT}/part-00000"
echo "--------------------"
echo ">>> Cleaning up HDFS directory: ${HDFS_WORKDIR}"
hdfs dfs -rm -r -skipTrash "${HDFS_WORKDIR}"
echo ">>> Cleaning up local files..."
rm candidate_song_ids.txt initial_artists.txt
echo "--- Workflow Finished ---"