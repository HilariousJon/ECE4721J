run_batch_without_broadcast_gd:
	spark-submit gd.py \
		--data_path ../data/PBMC_16k_RNA.csv \
		--label_path ../data/PBMC_16k_RNA_label.csv \
		--method batch \
		--use_standardization \
		--num_iterations 150 \
		--learning_rate 0.05 \
		--log_file training_log.csv


run_batch_broadcast_gd:
	spark-submit gd.py \
		--data_path ../data/PBMC_16k_RNA.csv \
		--label_path ../data/PBMC_16k_RNA_label.csv \
		--method broadcast \
		--use_standardization \
		--num_iterations 150 \
		--learning_rate 0.05 \
		--log_file training_log.csv

run_batch_steepest_gd:
	spark-submit gd.py \
		--data_path ../data/PBMC_16k_RNA.csv \
		--label_path ../data/PBMC_16k_RNA_label.csv \
		--method steepest \
		--use_standardization \
		--num_iterations 150 \
		--learning_rate 0.05 \
		--log_file training_log.csv

run_sgd_without_hogwild:
	spark-submit gd.py \
		--data_path ../data/PBMC_16k_RNA.csv \
		--label_path ../data/PBMC_16k_RNA_label.csv \
		--method sgd \
		--use_standardization \
		--num_iterations 150 \
		--learning_rate 0.05 \
		--log_file training_log.csv

run_sgd_with_hogwild:
	spark-submit gd.py \
		--data_path ../data/PBMC_16k_RNA.csv \
		--label_path ../data/PBMC_16k_RNA_label.csv \
		--method hogwild \
		--use_standardization \
		--num_iterations 150 \
		--learning_rate 0.05 \
		--log_file training_log.csv
		--n_threads 8

commit:
	git add -A
	git commit -m "docs(h6): update README answers [build joj]" && git push

.PHONY: commit