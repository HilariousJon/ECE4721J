PYTHON=python3

AVRO_FILE ?= src/m1/songs.avro
OUTPUT_DIR ?= src/m1/output_h5

main:
	$(PYTHON) src/m1/compress.py 

mount_data_init:
	sudo -S sshfs /home/hadoopuser/ece472 -o allow_other -o Port=2223 \
	ece472@focs.ji.sjtu.edu.cn: -o IdentityFile=/home/hadoopuser/.ssh/id_ed25519
	sudo -S mount /home/hadoopuser/ece472/millionsong.iso /home/hadoopuser/ece472

extract:
	$(PYTHON) src/m1/extract.py $(AVRO_FILE) $(OUTPUT_DIR)

commit:
	git add -A; \
	git commit -m "chore(p1m1): auto backup [build joj]" --allow-empty && git push

fmt_json:
	cat src/m1/msd.avsc | jq '.' > tmp.avsc && mv tmp.avsc src/m1/msd.avsc

.PHONY: commit main extract mount_data_init fmt_json
