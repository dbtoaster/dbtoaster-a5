
all:
	ocamlc -c Util.ml
	ocamlc -c Ring.ml
	ocamlc -c SuperMap.ml
	ocamlc -c Calculus.mli
	ocamlc -c Calculus.ml
	ocamlc -c M3.ml
	ocamlc -c Compiler.ml
	ocamlc -c M3Eval.ml
	ocamlmktop -o alpha3_ck Util.cmo Ring.cmo SuperMap.cmo \
                                Calculus.cmo M3.cmo M3Eval.cmo Compiler.cmo

clean:
	rm -f *.cmi *.cmo alpha3_ck
