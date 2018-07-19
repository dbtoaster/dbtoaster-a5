
include ../makefile.local

all: dependencies

dependencies:

runtimelibs: 
	@make -C dbt_ocaml
	#@make -C dbt_c++
	#@make -C dbt_scala 
	
clean:
	make -C dbt_ocaml clean
	make -C dbt_c++ clean
	make -C dbt_scala clean

.PHONY: all ocaml clean dependencies runtimelibs_force
