
SOURCE = \
	Util.ml \
	Ring.ml \
	SliceableMap.mli \
	SliceableMap.ml \
	Calculus.mli \
	Calculus.ml \
	M3.ml \
	CalcToM3.mli \
	CalcToM3.ml \
	Compiler.ml \
	M3Compiler.ml

all: top

bytecode:
	ocamlc -g -c $(SOURCE)

native:
	ocamlopt -ccopt -O3 -c $(SOURCE)

top: bytecode
	ocamlmktop -o dbtoaster_top unix.cma $(patsubst %.ml,%.cmo,$(filter %.ml,$(SOURCE))) 

benchmark: bytecode
	ocamlc -g -o m3_ex unix.cma $(patsubst %.ml,%.cmo,$(filter %.ml,$(SOURCE))) test/m3_ex.ml

vwap: native
	ocamlopt -ccopt -O3 -o vwap str.cmxa unix.cmxa $(patsubst %.ml,%.cmx,$(filter %.ml,$(SOURCE))) test/vwap_m3_ex.ml

clean:
	rm -f *.cmi *.cmo *.cmx *.o test/*.cmi test/*.cmo test/*.cmx test/*.o dbtoaster_top m3_ex vwap
