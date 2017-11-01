.PHONY: test

test:
	python3 -m doctest -v extent_writers.py
