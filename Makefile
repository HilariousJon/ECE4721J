PYTHON=python3

MAKEFILE_PATH := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

AVRO_FILE ?= src/m1/songs.avro
OUTPUT_DIR ?= src/m1/output_h5


init_env:
	# make sure you are in a virtual environment
	pip install poetry
	poetry install && poetry update

aggregate_avro:
	mkdir -p ./data
	poetry run spark-submit \
		--master local[4] \
		--conf spark.pyspark.driver.python=$(PYTHON) \
		--conf spark.pyspark.python=$(PYTHON) \
		--driver-cores 2 \
		--driver-memory 3g \
		--executor-cores 1 \
		--num-executors 2 \
		--executor-memory 2g \
		--conf spark.default.parallelism=2 \
		--conf spark.local.dir=/tmp/spark_tmp \
		--py-files src/m1/hdf5_getters.py \
		src/m1/h5_to_avro.py \
		-s src/m1/msd_meta.avsc \
		-o ./data/ \
		-i /mnt/msd_data/data
	python src/m1/merge_avro.py \
		data/ \
		aggregate.avro

agg_avro:
	python src/m1/h5_to_avro_nonspark.py \
		-s src/m1/msd_meta.avsc \
		-o ./data/ \
		-i /mnt/msd_data/data
	python src/m1/merge_avro.py \
		data/ \
		aggregate.avro

year_avro:
	python src/m1/h5_to_avro_nonspark.py \
		-s src/m1/msd_year_prediction.avsc \
		-o ./year-data/ \
		-i /mnt/msd_data/data 
	python src/m1/merge_avro.py \
		year-data/ \
		aggregate_year_prediction.avro

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

build_artists_graph:
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
		src/m2/artistsDis/build_artists_graph.py \
		--input ./data/aggregate.avro \
		--output ./data/artists_graph \
		--threshold 0.5

query_artists_distance:
	# run the spark job to query the distance between two artists in the graph
	poetry run spark-submit \
		--master local[2] \
		--conf spark.pyspark.driver.python=python3 \
		--conf spark.pyspark.python=python3 \
		src/m2/artistsDis/query_artist_distance.py \
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
		--w_sim 0.6 \
		--w_hot 0.2 \
		--w_bfs 0.2

commit:
	git add -A; \
	git commit -m "chore(p1m2): auto backup [build joj]" --allow-empty && git push

fmt_json:
	cat src/m1/msd_meta.avsc | jq '.' > tmp.avsc && mv tmp.avsc src/m1/msd_meta.avsc
	cat src/m1/msd_year_prediction.avsc | jq '.' > tmp.avsc && mv tmp.avsc src/m1/msd_year_prediction.avsc

# TODO: remove absolute path in the makefile
# remain bugs in the mini-batch-gd model

train_%:
	poetry run spark-submit \
		--master local[1] \
		--deploy-mode client \
		--conf spark.pyspark.driver.python=$(PYTHON) \
		--conf spark.pyspark.python=$(PYTHON) \
		src/year_prediction/ml_models.py \
		--model $(subst train_,,$@) \
		--filepath $(CSV_PATH) \
		--model_output $(MODEL_DIR)/$(subst train_,,$@)_model \
		--output $(RESULTS_DIR) \
		--tolerance 5.0

run_drill:
	sed 's|__PROJECT_PATH__|$(MAKEFILE_PATH)|g' src/m2/drill_queries.sql \
	| $(DRILL_HOME)/bin/drill-embedded -f /dev/stdin

run_spark_bfs_local:
	poetry run spark-submit \
		--master local[*] \
		--deploy-mode client \
		--packages org.apache.spark:spark-avro_2.12:3.2.4 \
		--conf spark.pyspark.driver.python=$(PYTHON) \
		--conf spark.pyspark.python=$(PYTHON) \
		src/m2/bfs/spark_driver.py \
		-m spark \
		-a ./data/artist_similarity.db \
		-c local \
		-i ./year-data/aggregate_year_prediction.avro \
		-M ./data/track_metadata.db \
		-D 2 \
		--exclude_current_artist true \
		--num_of_recommends 20 \
		-s TRMUOZE12903CDF721

run_spark_bfs_cluster:
	poetry run spark-submit \
		--master yarn \
		--deploy-mode cluster \
		--packages org.apache.spark:spark-avro_2.12:3.2.4 \
		--conf spark.pyspark.driver.python=$(PYTHON) \
		--conf spark.pyspark.python=$(PYTHON) \
		src/m2/bfs/spark_driver.py \
		-m spark \
		-a ./data/artist_similarity.db \
		-c cluster \
		-i ./year-data/aggregate_year_prediction.avro \
		-M ./data/track_metadata.db \
		-D 2 \
		--exclude_current_artist true \
		--num_of_recommends 20 \
		-s TRMUOZE12903CDF721

run_mapreduce_setup:
	poetry run spark-submit \
		--master local[*] \
		--packages org.apache.spark:spark-avro_2.12:3.2.4 \
		src/m2/bfs/create_song_data.py \
		./year-data/aggregate_year_prediction.avro \
		./year-data/tmp
	mv year-data/tmp/part-00000* year-data/song_data.jsonl
	rm -rf year-data/tmp
	poetry run spark-submit \
		--master local[*] \
		--packages org.apache.spark:spark-avro_2.12:3.2.4 \
		src/m2/bfs/create_input_features.py \
		./year-data/aggregate_year_prediction.avro \
		TRMUOZE12903CDF721 \
		./year-data/input_song_features.json

run_mapreduce_bfs_local:
	bash src/m2/bfs/driver_local.sh

run_mapreduce_bfs_cluster:
	bash src/m2/bfs/driver.sh

run_ann_HNSW_build:
	poetry run spark-submit \
		--master local[*] \
		--packages org.apache.spark:spark-avro_2.12:3.2.4 \
		src/m2/ann/build_ann_HNSW_index.py \
		-i ./year-data/aggregate_year_prediction.avro \
		-o ./year-data/index_HNSW

query_ann_HNSW:
	poetry run $(PYTHON) src/m2/ann/query_HNSW_recommendation.py \
		-i ./year-data/index_HNSW \
		-k 100 \
		--track "TRMMMYQ128F932D901:0.6" \
		--track "TRMMMWA128F426B589:0.2" \
		--track "TRMMMRX128F93187D9:0.2"
	# song are:
	# first: Faster pussycat - Silent Night
	# second: Der Mystic - Tangle of Aspens
	# third: Hudson Mohawke - No One Could Ever

run_ann_LSH_build:
	poetry run spark-submit \
		--packages org.apache.spark:spark-avro_2.12:3.2.4 \
		src/m2/ann/build_ann_LSH_index.py \
		-i ./year-data/aggregate_year_prediction.avro \
		-o ./year-data/index_LSH

query_ann_LSH:
	poetry run spark-submit \
		--packages org.apache.spark:spark-avro_2.12:3.2.4 \
		src/m2/ann/query_LSH_recommendation.py \
		--avro ./year-data/aggregate_year_prediction.avro \
		--model ./year-data/index_LSH \
		-k 100 \
		--track "TRMMMYQ128F932D901:0.6" \
		--track "TRMMMWA128F426B589:0.2" \
		--track "TRMMMRX128F93187D9:0.2"
	# song are:
	# first: Faster pussycat - Silent Night
	# second: Der Mystic - Tangle of Aspens
	# third: Hudson Mohawke - No One Could Ever

train_ridge:
	poetry run spark-submit \
		--master local[*] \
		src/m2/year_prediction/ml_models.py \
		--mode train \
		--filepath ./year-data/YearPredictionMSD.csv \
		--model 1 \
		--output ./experiment_results.csv \
		--model-output-path ./model/ridge_model \
		--tolerance 5.0

train_rf:
	poetry run spark-submit \
		--master local[*] \
		src/m2/year_prediction/ml_models.py \
		--mode train \
		--filepath ./year-data/YearPredictionMSD.csv \
		--model 2 \
		--output ./experiment_results.csv \
		--model-output-path ./model/rf_model \
		--tolerance 5.0

train_gbt:
	poetry run spark-submit \
		--master local[*] \
		src/m2/year_prediction/ml_models.py \
		--mode train \
		--filepath ./year-data/YearPredictionMSD.csv \
		--model 3 \
		--output ./experiment_results.csv \
		--model-output-path ./model/gbt_model \
		--tolerance 5.0

train_sgd:
	poetry run spark-submit \
		--master local[*] \
		src/m2/year_prediction/ml_models.py \
		--mode train \
		--filepath ./year-data/YearPredictionMSD.csv \
		--model 4 \
		--output ./experiment_results.csv \
		--model-output-path ./model/sgd_model \
		--tolerance 5.0

train_xgboost:
	poetry run spark-submit \
		--master local[*] \
		src/m2/year_prediction/ml_models.py \
		--mode train \
		--filepath ./year-data/YearPredictionMSD.csv \
		--model 5 \
		--output ./experiment_results.csv \
		--model-output-path ./model/xgb_model \
		--tolerance 5.0

.PHONY: commit extract mount_data_init fmt_json init_env
