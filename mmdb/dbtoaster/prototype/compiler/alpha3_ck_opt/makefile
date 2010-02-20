
SOURCE = Util.ml \
	Ring.ml \
	SuperMap.ml \
	Calculus.mli \
	Calculus.ml \
	M3.ml \
	Compiler.ml \
	M3Eval.ml

all: top

bytecode:
	ocamlc -c $(SOURCE)

native:
	ocamlopt -c $(SOURCE)

top: bytecode
	ocamlmktop -o dbtoaster_top unix.cma Util.cmo Ring.cmo SuperMap.cmo \
                                Calculus.cmo M3.cmo M3Eval.cmo Compiler.cmo

benchmark: native
	ocamlopt -o m3_ex unix.cmxa Util.cmx Ring.cmx SuperMap.cmx \
                                Calculus.cmx M3.cmx M3Eval.cmx Compiler.cmx test/m3_ex.ml

vwap: native
	ocamlopt -o vwap unix.cmxa Util.cmx Ring.cmx SuperMap.cmx \
                                Calculus.cmx M3.cmx M3Eval.cmx Compiler.cmx test/vwap_m3_ex.ml

clean:
	rm -f *.cmi *.cmo *.cmx *.o test/*.cmi test/*.cmo test/*.cmx test/*.o dbtoaster_top m3_ex vwap
