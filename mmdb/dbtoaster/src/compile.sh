#!/bin/sh

ocamlbuild -cflags -dtypes,-g -lflag -g -Is common backend/compile.byte
