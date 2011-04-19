#!/bin/bash
if [ ! -d protocol ] ; then
  mkdir protocol
fi
thrift -o protocol --gen java profiler.thrift
thrift -o protocol --gen cpp  profiler.thrift
