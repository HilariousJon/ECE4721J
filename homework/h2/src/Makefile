all:
	cd ex4 && make main
	cd -

cleanAll:
	cd ex4 && make clean
	cd - 

create:
	mvn archetype:generate -DgroupId=com.ve472.h2 \
                       -DartifactId=ex4 \
                       -DarchetypeArtifactId=maven-archetype-quickstart \
                       -DinteractiveMode=false

.PHONY: all cleanAll create
