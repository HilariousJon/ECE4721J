run_cholesky:
	spark-submit \
	--master local[*] \
	--driver-memory 4g \
	Cholesky.py

run_LU:
	Rscript --vanilla --slave \
	--no-save --no-restore \
	LU.r

commit:
	git add -A
	git commit -m "chore(l6): auto backup and update [build joj]" && git push

.PHONY: commit run_cholesky run_LU
