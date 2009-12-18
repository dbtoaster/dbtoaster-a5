#!/bin/sh

for i in customer lineitem nation orders part partsupp region supplier; do
    sed "s/xxxx/$1/g" < loadctl/$i.ctl.in > loadctl/$i.ctl;
done
