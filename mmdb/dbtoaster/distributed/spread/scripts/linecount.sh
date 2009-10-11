#!/bin/bash

for i in src/bin src/node src/common; do 
  echo $(basename $i)":" $(find $i -type f | grep -v svn | xargs cat | grep -v "^ *#" |grep -v '^ *$' | wc -l)
done
echo "--------------"
echo "comments: " $(find src -type f | grep -v svn | grep -v gen-rb | grep -v Makefile | xargs cat | grep "^ *#" | wc -l)
echo "whitespace: " $(find src -type f | grep -v svn | grep -v gen-rb | grep -v Makefile | xargs cat | grep '^ *$' | wc -l)
echo "--------------"
echo "total: " $(find src -type f | grep -v svn | grep -v gen-rb | grep -v Makefile | xargs cat | wc -l)
