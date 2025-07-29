PYTHON=python3

PROJECT_ROOT := $(shell pwd)
AVRO_FILE ?= src/m1/songs.avro
OUTPUT_DIR ?= src/m1/output_h5
MODEL_DIR=$(PROJECT_ROOT)/models
RESULTS_DIR=$(PROJECT_ROOT)/results
AVRO_PATH=$(PROJECT_ROOT)/year-data/aggregate_year_prediction.avro
OUTPUT_PATH=$(PROJECT_ROOT)/outputs/prediction_results.csv
CSV_PATH=$(PROJECT_ROOT)/yearpredictionmsd/YearPredictionMSD.csv

main:
	$(PYTHON) src/m1/compress.py 

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

predict:
	poetry run spark-submit \
		--master local \
		--deploy-mode client \
		--packages org.apache.spark:spark-avro_2.12:3.2.4 \
		--conf spark.pyspark.driver.python=$(PYTHON) \
		--conf spark.pyspark.python=$(PYTHON) \
		src/year_prediction/prediction.py \
		--model_path $(MODEL_DIR)/$(MODEL_NAME)_model \
		--avro_path $(AVRO_PATH) \
		--output_path $(OUTPUT_PATH)

run_%: train_% predict_%
	@echo "Completed training and prediction for model: $(subst run_,,$@)"

predict_%:
	$(MAKE) predict MODEL_NAME=$(subst predict_,,$@)

.PHONY: commit main extract mount_data_init fmt_json init_env run_random_forest \
run_ridge_regression run_GBT run_mini_batch_gd aggregate_avro agg_avro year_avro mount_data unmount_data run_xgboost
