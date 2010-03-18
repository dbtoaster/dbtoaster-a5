
SOURCE = \
	Util.ml \
	Ring.ml \
	Calculus.mli \
	Calculus.ml \
	M3.ml \
	M3Common.ml \
	CalcToM3.mli \
	CalcToM3.ml \
	Compiler.ml \
	M3Codegen.mli \
	M3Compiler.ml \
	codegen/ocaml/SliceableMap.mli \
	codegen/ocaml/SliceableMap.ml \
	codegen/ocaml/M3OCaml.ml \
	codegen/ocaml/M3Interpreter.ml

all: top

bytecode:
	ocamlc -c -I codegen/ocaml/ $(SOURCE)

native:
	ocamlopt -ccopt -O3 -c -I codegen/ocaml/ $(SOURCE)

top: bytecode
	ocamlmktop -o dbtoaster_top unix.cma $(patsubst %.ml,%.cmo,$(filter %.ml,$(SOURCE))) 

benchmark: native
	ocamlopt -I codegen/ocaml/ -o m3_ex unix.cmxa $(patsubst %.ml,%.cmx,$(filter %.ml,$(SOURCE))) test/m3_ex.ml

vwap: native
	ocamlopt -I codegen/ocaml/ -ccopt -O3 -o vwap str.cmxa unix.cmxa $(patsubst %.ml,%.cmx,$(filter %.ml,$(SOURCE))) test/vwap_m3_ex.ml

clean:
	rm -f *.cmi *.cmo *.cmx *.o test/*.cmi test/*.cmo test/*.cmx test/*.o dbtoaster_top m3_ex vwap

test: top
	./dbtoaster_top test/m3_ex.ml