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
		-s src/m1/msd.avsc \
		-o ./data/ \
		-i /mnt/msd_data/data

agg_avro:
	python src/m1/h5_to_avro_nonspark.py \
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

.PHONY: commit main extract mount_data_init fmt_json init_env
