
include ../makefile.local

all: dependencies

dependencies:

runtimelibs: 
	@make -C dbt_ocaml
	@if [ "$(COMPILE_SCALA)" =  "1" ] ; then make -C dbt_scala; fi

clean:
	make -C dbt_ocaml clean
	make -C dbt_scala clean

.PHONY: all ocaml clean dependencies
