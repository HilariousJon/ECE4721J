#!/usr/bin/env bash
set -euo pipefail

MODE=${1:-1}  # 1=single, 2=cluster

BASE_DIR=".."
SRC_DIR="$BASE_DIR/src"
HDFS_INPUT="input/"
HDFS_OUTPUT="output"
LOG_DIR="$BASE_DIR/log"
METRICS_CSV="$LOG_DIR/metrics.csv"
LOCAL_OUT="$BASE_DIR/output"

mkdir -p "$LOG_DIR" "$LOCAL_OUT"

if [ ! -f "$METRICS_CSV" ]; then
  echo "num,size,time,status" > "$METRICS_CSV"
fi

for exp in {3..4}; do # modify to 8 if you have larger resources
  num=$((10 ** exp))
  echo "=== Generating $num students ==="

  bash "$SRC_DIR/generator.sh" "$num"
done

mv "$BASE_DIR/outputs" "$BASE_DIR/input_csv"
hdfs dfs -mkdir -p "$HDFS_INPUT"
hdfs dfs -put "$BASE_DIR/input_csv" "$HDFS_INPUT"
mv "$BASE_DIR/input_csv" "$BASE_DIR/outputs"

for exp in {3..4}; do # modify to 8 if you have larger resources
  num=$((10 ** exp))
  echo "=== Running for $num students ==="

  bash "$SRC_DIR/generator.sh" "$num"
  filesize=$(stat -c%s "../outputs/students_$num.csv")

  hdfs dfs -rm -r -f "$HDFS_OUTPUT"

  start=$(date +%s.%N)
  hadoop jar /home/hadoopuser/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar \
    -input "$HDFS_INPUT/input_csv/students_$num.csv" \
    -output "$HDFS_OUTPUT" \
    -mapper "$SRC_DIR/mapper.sh" \
    -reducer "$SRC_DIR/reducer.sh" \
    -file "$SRC_DIR/mapper.sh" \
    -file "$SRC_DIR/reducer.sh"
  end=$(date +%s.%N)

  elapsed=$(echo "$end - $start" | bc)

  hdfs dfs -cat "$HDFS_OUTPUT/part-00000" "$BASE_DIR/"

  echo "$num,$filesize,$elapsed,$MODE" >> "$METRICS_CSV"
  echo "Finished $num students, ${filesize}B, time ${elapsed}s"
done

echo "All benchmarks complete. See $METRICS_CSV"
