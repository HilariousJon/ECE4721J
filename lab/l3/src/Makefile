main:
	# @bash -c "make clean"
	# @echo "Building the projects"
	# @echo "Importing the data"
	# @bash -c "make import"
	@echo "Running the queries"
	@bash -c "sqlite3 var/imdb.sqlite3 < ex3.sql > ex3.out 2>error.log"
	@bash -c "cat ex3.out"
	@bash -c "python3 interaction_insert.py"
	@bash -c "sqlite3 var/imdb.sqlite3 < ex5.sql > ex5.out 2>error.log"
	@bash -c "cat ex5.out"

create_db:
	# @bash -c "bash init.sh"
	@bash -c "mkdir -p var"
	@bash -c "sqlite3 var/imdb.sqlite3"


import:
	@bash -c "sqlite3 var/imdb.sqlite3 < schema.sql"

commit:
	@git add -A
	@git commit -m "feat(l3): auto backup [build joj]" && git push

clean:
	rm -rf var
	rm *.out
	rm *.log

debug:
	rm -rf var
	mkdir -p var
	sqlite3 var/imdb.sqlite3
	sqlite3 var/imdb.sqlite3 < schema.sql
	sqlite3 var/imdb.sqlite3 < import.sql
	sqlite3 var/imdb.sqlite3 < ex3.sql > ex3.out 2>error.log
	python3 interaction_insert.py
	sqlite3 var/imdb.sqlite3 < ex5.sql > ex5.out 2>error.log

.PHONY: commit main clean create_db import debug