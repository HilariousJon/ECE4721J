#!/bin/bash
set -e

# --- Configuration ---
echo "--- Configuring Local Simulation ---"
PYTHON_SCRIPTS_DIR="./src/m2/bfs"
DATA_DIR="./data"
OUTPUT_DATA_DIR="./year-data"

ARTIST_DB="${DATA_DIR}/artist_similarity.db"
META_DB="${DATA_DIR}/track_metadata.db"
INPUT_FEATURES_JSON="${OUTPUT_DATA_DIR}/input_song_features.json"
SONG_DATA_JSONL="${OUTPUT_DATA_DIR}/song_data.jsonl"

INPUT_SONG_ID="TRMUOZE12903CDF721"
BFS_DEPTH=2

# Create a temporary directory for all local work.
LOCAL_WORK_DIR="local_run"

# --- Workflow ---

# --- STEP 0: Cleanup and Setup ---
echo ">>> STEP 0: Preparing local environment..."
# Clean up any previous runs.
rm -rf "${LOCAL_WORK_DIR}"
# Create a fresh working directory.
mkdir -p "${LOCAL_WORK_DIR}"
echo "Local working directory created at ./${LOCAL_WORK_DIR}/"

# Run the setup script to generate initial_artists.txt.
python3 "${PYTHON_SCRIPTS_DIR}/mapreduce_setup.py" "$INPUT_SONG_ID" "$(realpath ${META_DB})"
mv ./initial_artists.txt "${LOCAL_WORK_DIR}/"
echo ">>> STEP 0: COMPLETE"


# --- JOB 1: Iterative BFS ---
echo ">>> JOB 1: Simulating Iterative BFS for depth ${BFS_DEPTH}..."
BFS_INPUT="${LOCAL_WORK_DIR}/initial_artists.txt"
for i in $(seq 1 $BFS_DEPTH); do
    echo "  -> BFS Depth ${i}"
    # Define output for this iteration.
    BFS_OUTPUT_TEMP="${LOCAL_WORK_DIR}/bfs_output_temp.txt"
    
    # Simulate the Hadoop environment: copy necessary files and 'cd' into the work dir.
    # This ensures the mapper can find 'artist_similarity.db' in its CWD.
    (
        cd "${LOCAL_WORK_DIR}" && \
        cp ../${ARTIST_DB} . && \
        cat "$(basename ${BFS_INPUT})" | \
        python3 ../${PYTHON_SCRIPTS_DIR}/mapper_bfs.py | \
        sort -k1,1 | \
        python3 ../${PYTHON_SCRIPTS_DIR}/reducer_bfs.py > "$(basename ${BFS_OUTPUT_TEMP})"
    )
    
    # The output of this iteration becomes the input for the next.
    mv "${BFS_OUTPUT_TEMP}" "${BFS_INPUT}"
done
BFS_FINAL_OUTPUT=$BFS_INPUT
echo ">>> JOB 1: COMPLETE. Final artist list is in ${BFS_FINAL_OUTPUT}"


# --- JOB 2: GET SONGS FROM ARTISTS ---
echo ">>> JOB 2: Simulating Get Songs..."
SONGS_OUTPUT="${LOCAL_WORK_DIR}/candidate_songs.txt"
(
    cd "${LOCAL_WORK_DIR}" && \
    cp ../${META_DB} . && \
    cat "$(basename ${BFS_FINAL_OUTPUT})" | \
    python3 ../${PYTHON_SCRIPTS_DIR}/mapper_get_songs.py | \
    sort -u -k1,1 | \
    python3 ../${PYTHON_SCRIPTS_DIR}/reducer_get_songs.py > "$(basename ${SONGS_OUTPUT})"
)
echo ">>> JOB 2: COMPLETE. Candidate songs are in ${SONGS_OUTPUT}"


# --- JOB 3: FILTER TOP 200 HOTTEST SONGS ---
echo ">>> JOB 3: Simulating Top Songs Filter..."
TOP_SONGS_OUTPUT="${LOCAL_WORK_DIR}/top_songs.txt"
(
    cd "${LOCAL_WORK_DIR}" && \
    # The mapper needs candidate_song_ids.txt and input_song_id.txt in its CWD
    cp ./"$(basename ${SONGS_OUTPUT})" ./candidate_song_ids.txt && \
    echo "${INPUT_SONG_ID}" > ./input_song_id.txt && \
    cat ../${SONG_DATA_JSONL} | \
    python3 ../${PYTHON_SCRIPTS_DIR}/mapper_top_songs.py | \
    sort -k1,1 | \
    python3 ../${PYTHON_SCRIPTS_DIR}/reducer_top_songs.py > "$(basename ${TOP_SONGS_OUTPUT})"
)
echo ">>> JOB 3: COMPLETE. Top songs are in ${TOP_SONGS_OUTPUT}"


# --- JOB 4: CALCULATE SIMILARITY ---
echo ">>> JOB 4: Simulating Similarity Calculation..."
FINAL_OUTPUT="${LOCAL_WORK_DIR}/final_result.txt"
(
    cd "${LOCAL_WORK_DIR}" && \
    # The mapper needs input_song_features.json in its CWD
    cp ../${INPUT_FEATURES_JSON} ./input_song_features.json && \
    cat "$(basename ${TOP_SONGS_OUTPUT})" | \
    python3 ../${PYTHON_SCRIPTS_DIR}/mapper_similarity.py | \
    sort -k1,1 | \
    python3 ../${PYTHON_SCRIPTS_DIR}/reducer_similarity.py > "$(basename ${FINAL_OUTPUT})"
)
echo ">>> JOB 4: COMPLETE. Final result is in ${FINAL_OUTPUT}"


# --- FINAL STEP: DISPLAY RESULTS AND CLEANUP ---
echo ""
echo "--- LOCAL SIMULATION FINAL RESULT ---"
cat "${FINAL_OUTPUT}"
echo "-------------------------------------"
echo ">>> Cleaning up local simulation directory..."
rm -r "${LOCAL_WORK_DIR}"
echo "--- Workflow Finished Successfully on Local Machine ---"
