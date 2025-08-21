main:
	echo "Generating data..."; \
	python scripts/generate.py; \
	\
	echo "Compiling Avro schema..."; \
	mvn package; \
	mvn compile; \
	\
	echo "Running Java application..."; \
	java -jar target/ex4-1.0-SNAPSHOT.jar; \
	echo "Data generation and processing complete."; \
	\
	echo "Checking difference the output content..."; \
	if diff -qr data/small_extracted_csv data/small_generated_csv > /dev/null; then \
	  echo "All files the same!"; \
	else \
	  echo "Found differences between the input folder and the output folder!"; \
	fi

compile_avro:
	mvn package \
	wget https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.8.2/avro-tools-1.8.2.jar -O target/avro-tools-1.8.2.jar \
	mkdir -p src/main/java \
	java -jar target/avro-tools-1.8.2.jar compile schema ./schema.json src/main/java

cleanCached:
	rm -rf data 

clean:
	mvn clean

fmt:
	mvn spotless:apply

.PHONY: main clean fmt cleanCached compile_avro
