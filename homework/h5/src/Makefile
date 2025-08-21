ex1:
	octave --no-gui --eval "run('ex1_2.m')"
	# octave --no-gui --eval "run('ex1_3.m')"

ex3:
	spark-submit ex3.py

extract_data:
	tar xf ../data/cinema_sensors.tar.gz --directory=../data

commit:
	git add -A 
	git commit -m "feat(h5): auto backup [build joj]" && git push

.PHONY: commit ex1 extract_data