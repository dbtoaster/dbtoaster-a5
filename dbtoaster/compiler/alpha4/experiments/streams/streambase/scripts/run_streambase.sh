#!/bin/bash

/damsl/software/streambase/bin/sbd $@ &
pid=$!
echo $pid
wait $pid