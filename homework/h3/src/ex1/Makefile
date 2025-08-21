main:
	@echo "Running job..."
	@bash -c '\
		mvn clean && \
		mvn package && \
		start=$$(date +%s.%N); \
		hadoop jar target/ex1-1.0-SNAPSHOT.jar; \
		end=$$(date +%s.%N); \
		elapsed=$$(echo "$$end - $$start" | bc); \
		echo "Execution time: $$elapsed seconds"; \
		hdfs dfs -rm -r /user/hadoopuser/data/output \
	'

clean:
	mvn clean; \
	rm -rf target; \
	rm -rf dependency-reduced-pom.xml

commit:
	git add -A; \
	git commit -m "feat(h3): auto backup [build joj]" && git push

.PHONY: main clean commit