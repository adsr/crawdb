#!/bin/bash
set -eux

test_dir=$(mktemp -d)
pass=0

cleanup() { rm -rf $test_dir; [ $pass -ne 1 ] && { echo FAIL; exit 1; } || echo PASS; }
trap cleanup EXIT

# Create db
./crawdb -i $test_dir/idx -d $test_dir/dat -N -n8
[ -f $test_dir/idx ]
[ -f $test_dir/dat ]

# Write and read key
./crawdb -i $test_dir/idx -d $test_dir/dat -S -k key1 -v val42
[ "$(./crawdb -i $test_dir/idx -d $test_dir/dat -G -k key1)" = "val42" ]

# Write and read another key
./crawdb -i $test_dir/idx -d $test_dir/dat -S -k key2 -v hi
[ "$(./crawdb -i $test_dir/idx -d $test_dir/dat -G -k key2)" = "hi" ]

# Index and read keys
./crawdb -i $test_dir/idx -d $test_dir/dat -I
[ "$(./crawdb -i $test_dir/idx -d $test_dir/dat -G -k key1)" = "val42" ]
[ "$(./crawdb -i $test_dir/idx -d $test_dir/dat -G -k key2)" = "hi" ]

# Ensure dup key fails
ok=0
./crawdb -i $test_dir/idx -d $test_dir/dat -S -k key2 -v again || ok=1
[ "$ok" -eq 1 ]

pass=1
