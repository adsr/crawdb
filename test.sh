#!/bin/bash
set -eux

test_dir=$(mktemp -d)
pass=0

cleanup() { rm -rf $test_dir; [ $pass -eq 1 ] && echo PASS || echo FAIL; }
trap cleanup EXIT

./crawdb -i $test_dir/idx -d $test_dir/dat -N -n8
[ -f $test_dir/idx ]
[ -f $test_dir/dat ]

./crawdb -i $test_dir/idx -d $test_dir/dat -S -k key1 -v val42
[ "$(./crawdb -i $test_dir/idx -d $test_dir/dat -G -k key1)" = "val42" ]

./crawdb -i $test_dir/idx -d $test_dir/dat -S -k key2 -v hi
[ "$(./crawdb -i $test_dir/idx -d $test_dir/dat -G -k key2)" = "hi" ]

./crawdb -i $test_dir/idx -d $test_dir/dat -I
[ "$(./crawdb -i $test_dir/idx -d $test_dir/dat -G -k key1)" = "val42" ]
[ "$(./crawdb -i $test_dir/idx -d $test_dir/dat -G -k key2)" = "hi" ]

pass=1
