#!/bin/bash
find . -name "*.ml" -exec grep -l $'\t' {} ";"
find . -name "*.mly" -exec grep -l $'\t' {} ";"
find . -name "*.cpp" -exec grep -l $'\t' {} ";"
find . -name "*.hpp" -exec grep -l $'\t' {} ";"
