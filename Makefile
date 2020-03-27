all: crawdb libcrawdb.so

crawdb: crawdb.c
	gcc -Wall -pedantic -g crawdb.c -o crawdb -D CRAWDB_MAIN

libcrawdb.so: crawdb.c
	gcc -Wall -pedantic -g crawdb.c -o libcrawdb.so -shared -fPIC -Wl,-soname,libcrawdb.so.1 -fvisibility=hidden

test: crawdb libcrawdb.so
	./test.sh
	CRAWDB_PHP_TEST=1 php crawdb.php

clean:
	rm -f crawdb libcrawdb.so

.PHONY: all test clean
