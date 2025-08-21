main:
	echo "Generating data..."; \
	python scripts/generate.py; \
	echo "Running the application..."; \
	mvn package; \
	hdfs dfs -rm -r /user/hadoopuser/data; \
	hdfs dfs -mkdir -p /user/hadoopuser/data/input; \
	hadoop jar target/ex3-1.0-SNAPSHOT.jar; \
	rm -r data/output; \
	mkdir -p data/output; \
	hdfs dfs -get /user/hadoopuser/data/output/part-r-00000 ./data/output/part-r-00000; \
	cat data/small_extracted.csv

commit:
	git add -A; \
	git commit -m "feat(h3): auto backup [build joj]" && git push

compile_avro:
	mvn package; \
	mkdir -p src/main/java/avro; \
	mkdir -p src/main/java; \
	wget https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.8.2/avro-tools-1.8.2.jar -O target/avro-tools-1.8.2.jar; \
	java -jar target/avro-tools-1.8.2.jar compile schema ./schema.json src/main/java
	
clean:
	mvn clean; \
	rm -rf data

.PHONY: main clean