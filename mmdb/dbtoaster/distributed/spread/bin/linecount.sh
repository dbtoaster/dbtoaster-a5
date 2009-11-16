#!/bin/bash

for i in $(find src -type d | grep -v svn | grep '/' | grep -v "data" | grep -v "gen-"); do 
  echo $(basename $i)":" $(find $i -type f | grep -v svn | xargs cat | grep -v "^ *#" |grep -v '^ *$' | wc -l)
done
echo "--------------"
echo "comments: " $(find src -type f | grep -v svn | grep -v "data" | grep -v gen- | grep -v Makefile | xargs cat | grep "^ *#" | wc -l)
echo "whitespace: " $(find src -type f | grep -v svn | grep -v "data" | grep -v gen- | grep -v Makefile | xargs cat | grep '^ *$' | wc -l)
echo "--------------"
echo "total: " $(find src -type f | grep -v svn | grep -v "data" | grep -v gen- | grep -v Makefile | xargs cat | wc -l)
