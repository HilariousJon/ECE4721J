PYTHON=python3

MAKEFILE_PATH := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

AVRO_FILE ?= src/m1/songs.avro
OUTPUT_DIR ?= src/m1/output_h5
DRILL_PATH ?= ~/mnt/drill

DATA_PATH = ${MAKEFILE_PATH}data/aggregate.avro


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

commit:
	git add -A; \
	git commit -m "chore(p1m2): auto backup [build joj]" --allow-empty && git push

fmt_json:
	cat src/m1/msd.avsc | jq '.' > tmp.avsc && mv tmp.avsc src/m1/msd.avsc

run_drill:
	@echo "Substuting Data Path..."
	@sed 's|__DATA_PATH__|$(DATA_PATH)|g' ./src/m2/drill_queries_template.sql > ./src/m2/drill_queries.sql 
	@echo "Running Drill Queries..."
	@$(DRILL_PATH)/bin/drill-embedded -f ./src/m2/drill_queries.sql
	@echo "Removing temporary drill queries file..."
	@rm ./src/m2/drill_queries.sql 

.PHONY: commit main extract mount_data_init fmt_json init_env
