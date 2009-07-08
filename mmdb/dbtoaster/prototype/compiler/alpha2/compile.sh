#!/bin/sh

ocamlbuild -cflags -dtypes,-g -lflag -g test.byte
