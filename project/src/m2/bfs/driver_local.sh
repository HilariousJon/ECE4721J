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

# Define job parameters.
INPUT_SONG_ID="TRMUOZE12903CDF721"
BFS_DEPTH=2

# Define a temporary directory for all intermediate files.
LOCAL_WORK_DIR="local_run"

echo ""
echo "--- Starting Timed Execution of MapReduce Simulation ---"
time (
    # --- STEP 0: Cleanup and Setup ---
    echo ">>> STEP 0: Preparing local environment..."
    # Clean up any previous runs to ensure a fresh start.
    rm -rf "${LOCAL_WORK_DIR}"
    # Create a fresh working directory.
    mkdir -p "${LOCAL_WORK_DIR}"
    echo "Local working directory created at ./${LOCAL_WORK_DIR}/"

    # Run the setup script to generate the initial artist file.
    # We pass the absolute path to the DB to ensure the setup script can find it.
    python3 "${PYTHON_SCRIPTS_DIR}/mapreduce_setup.py" "$INPUT_SONG_ID" "$(realpath ${META_DB})"
    # Move the output into our working directory.
    mv ./initial_artists.txt "${LOCAL_WORK_DIR}/"
    echo ">>> STEP 0: COMPLETE"


    # --- JOB 1: Iterative BFS ---
    echo ">>> JOB 1: Simulating Iterative BFS for depth ${BFS_DEPTH}..."
    BFS_INPUT="${LOCAL_WORK_DIR}/initial_artists.txt"
    for i in $(seq 1 $BFS_DEPTH); do
        echo "  -> BFS Depth ${i}"
        BFS_OUTPUT_TEMP="${LOCAL_WORK_DIR}/bfs_output_temp.txt"
        
        # This subshell simulates the Hadoop task environment.
        (
            # 'cd' into the working directory.
            cd "${LOCAL_WORK_DIR}" && \
            # Copy the database file into the CWD, just like Hadoop's -files.
            cp ../${ARTIST_DB} . && \
            # The MapReduce simulation pipeline for one BFS iteration.
            cat "$(basename ${BFS_INPUT})" | \
            python3 ../${PYTHON_SCRIPTS_DIR}/mapper_bfs.py | \
            # 'sort -u' correctly deduplicates while preserving F/V tags.
            sort -u > "$(basename ${BFS_OUTPUT_TEMP})"
        )
        
        # The output of this iteration becomes the input for the next.
        mv "${BFS_OUTPUT_TEMP}" "${BFS_INPUT}"
        
        # Log the progress.
        NUM_ARTISTS=$(cut -f1 ${BFS_INPUT} | sort -u | wc -l)
        echo "  -> Depth ${i} finished. Found ${NUM_ARTISTS} unique artists."
    done

    # After the loop, we use the original reducer to produce a final, clean list.
    BFS_FINAL_OUTPUT_WITH_TAGS=$BFS_INPUT
    BFS_FINAL_OUTPUT="${LOCAL_WORK_DIR}/bfs_final_artists.txt"

    # The output file will now contain lines like "ARTIST_ID \t V".
    cat "${BFS_FINAL_OUTPUT_WITH_TAGS}" | \
        python3 "${PYTHON_SCRIPTS_DIR}/reducer_bfs.py" > "${BFS_FINAL_OUTPUT}"

    echo ">>> JOB 1: COMPLETE. Final artist list is in ${BFS_FINAL_OUTPUT}"


    # --- JOB 2: GET SONGS FROM ARTISTS ---
    echo ">>> JOB 2: Simulating Get Songs..."
    SONGS_OUTPUT="${LOCAL_WORK_DIR}/candidate_songs.txt"
    (
        cd "${LOCAL_WORK_DIR}" && \
        cp ../${META_DB} . && \
        # This mapper now receives the correct "key \t value" format.
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
        # The mapper needs these files in its CWD to simulate the map-side join.
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
        cp ../${INPUT_FEATURES_JSON} ./input_song_features.json && \
        cat "$(basename ${TOP_SONGS_OUTPUT})" | \
        python3 ../${PYTHON_SCRIPTS_DIR}/mapper_similarity.py | \
        sort -k1,1 | \
        python3 ../${PYTHON_SCRIPTS_DIR}/reducer_similarity.py > "$(basename ${FINAL_OUTPUT})"
    )
    echo ">>> JOB 4: COMPLETE. Final result is in ${FINAL_OUTPUT}"


    # --- FINAL STEP: DISPLAY RESULTS ---
    echo ""
    echo "--- LOCAL SIMULATION FINAL RESULT ---"
    if [ -s "${FINAL_OUTPUT}" ]; then
        cat "${FINAL_OUTPUT}"
    else
        echo "!!! No similar song found. The final result is empty."
    fi
    echo "-------------------------------------"
)
echo "--- Timed Execution Finished ---"
echo ""

# --- Cleanup is outside the timed block ---
echo ">>> Cleaning up local simulation directory..."
rm -r "${LOCAL_WORK_DIR}"
echo "--- Workflow Finished Successfully on Local Machine ---"
