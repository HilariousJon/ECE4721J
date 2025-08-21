stream:
	make clean
	hadoop jar /home/hadoopuser/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar \
  	-input /user/hadoopuser/input/students.csv \
  	-output /user/hadoopuser/output \
  	-mapper mapper.sh \
  	-reducer reducer.sh \
  	-file mapper.sh \
	-file reducer.sh

benchmark-single:
	cd scripts 
	bash run_benchmark.sh 1
	cd -

benchmark-cluster:
	cd scripts
	bash run_benchmark.sh 2
	cd -

plot:
	python3 scripts/benchmark.py

commit:
	git add -A 
	git commit -m "feat(l2): regular commit [build joj]" && git push

clean:
	rm -rf students.csv
	rm -rf input_csv
	rm -rf log
	rm -rf outputs/*
	rm -rf output/*
	hadoop fs -rm -r /user/hadoopuser/input/*
	hadoop fs -rm -r /user/hadoopuser/output

PHONY: stream clean commit benchmark-single benchmark-cluster plot