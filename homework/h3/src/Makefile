create:
	mvn archetype:generate -DgroupId=com.ve472.h3 \
                       -DartifactId=ex1 \
                       -DarchetypeArtifactId=maven-archetype-quickstart \
                       -DinteractiveMode=false
	mvn archetype:generate -DgroupId=com.ve472.h3 \
                       -DartifactId=ex2 \
                       -DarchetypeArtifactId=maven-archetype-quickstart \
                       -DinteractiveMode=false
	mvn archetype:generate -DgroupId=com.ve472.h3 \
                       -DartifactId=ex3 \
                       -DarchetypeArtifactId=maven-archetype-quickstart \
                       -DinteractiveMode=false

commit:
	git add -A 
	git commit -m "feat(h3): auto backup [build joj]" && git push

.PHONY: commit create