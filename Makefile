PYTHON=python3

AVRO_FILE ?= src/m1/songs.avro
OUTPUT_DIR ?= src/m1/output_h5

main:
	$(PYTHON) src/m1/compress.py 

mount_data_init:
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
	$(PYTHON) src/m1/extract.py $(AVRO_FILE) $(OUTPUT_DIR)

commit:
	git add -A; \
	git commit -m "chore(p1m2): auto backup [build joj]" --allow-empty && git push

fmt_json:
	cat src/m1/msd.avsc | jq '.' > tmp.avsc && mv tmp.avsc src/m1/msd.avsc

.PHONY: commit main extract mount_data_init fmt_json
