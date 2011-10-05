#!/bin/bash

cd $(dirname $0)/../..
pwd

cat test/nightly_workload | sed 's:^:test/scripts/query_test.rb :' | sh