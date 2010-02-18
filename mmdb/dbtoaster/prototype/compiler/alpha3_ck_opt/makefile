
all:
	ocamlc -c Util.ml
	ocamlc -c Ring.ml
	ocamlc -c SuperMap.ml
	ocamlc -c Calculus.mli
	ocamlc -c Calculus.ml
	ocamlc -c M3.ml
	ocamlc -c Compiler.ml
	ocamlc -c M3Eval.ml
	ocamlmktop -o alpha3_ck unix.cma Util.cmo Ring.cmo SuperMap.cmo \
                                Calculus.cmo M3.cmo M3Eval.cmo Compiler.cmo

benchmark:
	ocamlopt -c Util.ml
	ocamlopt -c Ring.ml
	ocamlopt -c SuperMap.ml
	ocamlopt -c Calculus.mli
	ocamlopt -c Calculus.ml
	ocamlopt -c M3.ml
	ocamlopt -c Compiler.ml
	ocamlopt -c M3Eval.ml
	ocamlopt -o m3_ex unix.cmxa Util.cmx Ring.cmx SuperMap.cmx \
                                Calculus.cmx M3.cmx M3Eval.cmx Compiler.cmx test/m3_ex.ml

clean:
	rm -f *.cmi *.cmo *.cmx *.o alpha3_ck m3_ex
