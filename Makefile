PYTHON=python3

AVRO_FILE ?= src/m1/songs.avro
OUTPUT_DIR ?= src/m1/output_h5

main:
	$(PYTHON) src/m1/compress.py 

init_env:
	# make sure you are in a virtual environment
	pip install poetry
	poetry install && poetry update

aggregate_avro:
	mkdir -p ./data
	poetry run spark-submit \
		--master local[12] \
		--conf spark.pyspark.driver.python=$(PYTHON) \
		--conf spark.pyspark.python=$(PYTHON) \
		--driver-cores 2 \
		--driver-memory 8g \
		--executor-cores 4 \
		--num-executors 10 \
		--executor-memory 4g \
		--py-files src/m1/hdf5_getters.py \
		src/m1/h5_to_avro.py \
		-s src/m1/msd.avsc \
		-o ./data/ \
		-i /mnt/msd_data/data

mount_data_init:
	# run it every time you reset your computer
	sudo mkdir -p /mnt/msd_data /home/hadoopuser/ece472
	sudo sshfs /home/hadoopuser/ece472 -o allow_other \
		-o Port=2223 ece472@focs.ji.sjtu.edu.cn: \
		-o IdentityFile=/home/hadoopuser/.ssh/id_ed25519
	sudo mount -o loop /home/hadoopuser/ece472/millionsong.iso /mnt/msd_data
	# check success by running ls /mnt/msd_data

unmount_data:
	sudo umount /mnt/msd_data
	sudo umount /home/hadoopuser/ece472

extract:
	# use API from display_song later
	$(PYTHON) src/m1/extract.py $(AVRO_FILE) $(OUTPUT_DIR)

build_artists_graph_spark:
	# run the spark job to build the artists graph
	poetry run spark-submit \
		--master local[4] \
		--conf spark.pyspark.driver.python=python3 \
		--conf spark.pyspark.python=python3 \
		--driver-cores 2 \
		--driver-memory 3g \
		--executor-cores 1 \
		--num-executors 2 \
		--executor-memory 2g \
		--packages org.apache.spark:spark-avro_2.12:3.2.4 \
		src/m2/artistsDis/spark/build_artists_graph_spark.py \
		--input ./data/aggregate.avro \
		--output ./data/artists_graph \
		--threshold 0.5

convert_avro_to_json:
	# convert the avro file to jsonl format
	poetry run $(PYTHON) src/m2/artistsDis/mapreduce/convert_avro_to_json.py \
		--input ./data/aggregate.avro \
		--output ./data/artists.jsonl

# build_artists_graph_mr:
# 	# run the mapreduce job to build the artists graph
# 	ulimit -v 6000000; \
# 	cpulimit --limit=400 -- \
# 	poetry run python3 src/m2/artistsDis/mapreduce/build_artists_graph_mr.py \
# 	--input ./data/artists.jsonl \
# 	--output ./data/artists_graph_mr.jsonl \
# 	--topk 50

build_artists_graph_mr:
	# Stage 1: LSH hashing + clean JSON format
	mkdir -p data/tmp
	poetry run $(PYTHON) src/m2/artistsDis/mapreduce/build_artists_graph_stage1.py \
	--num-hash 20 \
	--seed 42 \
	< data/artists.jsonl | \
	sed 's/\\//g' > data/tmp/lsh_buckets.jsonl

	# Stage 2: Top-K in each bucket
	poetry run $(PYTHON) src/m2/artistsDis/mapreduce/build_artists_graph_stage2.py \
	--topk 50 \
	< data/tmp/lsh_buckets.jsonl > data/tmp/raw_neighbors.jsonl

	# Final merge
	poetry run $(PYTHON) src/m2/artistsDis/mapreduce/merge_neighbors.py \
	--input data/tmp/raw_neighbors.jsonl \
	--output data/artists_graph_mr.jsonl \
	--topk 50

query_artists_distance:
	# run the spark job to query the distance between two artists in the graph
	poetry run spark-submit \
		--master local[2] \
		--conf spark.pyspark.driver.python=python3 \
		--conf spark.pyspark.python=python3 \
		src/m2/artistsDis/spark/query_artist_distance.py \
		--graph ./data/artists_graph \
		--start b\'AR00JIO1187B9A5A15\' \
		--end b\'AR8KJG41187B9AF8EC\'

build_songs_graph:
	# run the spark job to build the songs graph
	poetry run spark-submit \
		--master local[4] \
		--conf spark.pyspark.driver.python=python3 \
		--conf spark.pyspark.python=python3 \
		--driver-cores 2 \
		--driver-memory 3g \
		--executor-cores 1 \
		--num-executors 2 \
		--executor-memory 2g \
		--packages org.apache.spark:spark-avro_2.12:3.2.4 \
		src/m2/songRec/build_songs_graph.py \
		--input ./year-data/aggregate_year_prediction.avro \
		--output ./data/songs_graph \
		--threshold 2.2

song_recommend:
	# run the spark job to recommend songs based on the graph and features
	poetry run spark-submit \
		--master local[2] \
		--conf spark.pyspark.driver.python=python3 \
		--conf spark.pyspark.python=python3 \
		--packages org.apache.spark:spark-avro_2.12:3.2.4 \
		src/m2/songRec/song_recommend.py \
		--graph ./data/songs_graph \
		--features ./year-data/aggregate_year_prediction.avro \
		--seeds SOAAAQN12AB01856D3 SOAADAD12A8C13D5B0 SOAAGJG12A8C141F3F \
		--topk 10 \
		--w_sim 0.5 \
		--w_hot 0.3 \
		--w_bfs 0.2

commit:
	git add -A; \
	git commit -m "chore(p1m2): auto backup [build joj]" --allow-empty && git push

fmt_json:
	cat src/m1/msd.avsc | jq '.' > tmp.avsc && mv tmp.avsc src/m1/msd.avsc

.PHONY: commit main extract mount_data_init fmt_json init_env
