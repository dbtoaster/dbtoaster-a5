#!/bin/bash
#Convertes eps to pdf

#Generates FILENAME-generated.pdf, but do not work recursively
for f in ./graphics/*.eps; do epstopdf $f -o ${f%.eps}-generated.pdf; done

#Generates FILENAME.eps-generated.pdf, but works recursively
#find -iname "*.eps" -execdir epstopdf '{}' -o '{}'-generated.pdf \;
