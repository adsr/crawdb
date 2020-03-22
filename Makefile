all: crawdb

crawdb: crawdb.c
	gcc -Wall -pedantic -g crawdb.c -o crawdb

test: crawdb
	./test.sh

clean:
	rm -f crawdb

.PHONY: all test clean
