
all: dependencies

dependencies:

runtimelibs: 
	@make -C dbt_ocaml

clean:
	make -C dbt_ocaml clean
	make -C xml-light clean

.PHONY: all ocaml clean dependencies