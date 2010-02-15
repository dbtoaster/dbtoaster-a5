
m3:
	ocamlc -c SuperMap.ml
	ocamlc -c M3.ml
	ocamlc -c M3Eval.ml
	ocamlmktop -o M3interp SuperMap.cmo M3.cmo M3Eval.cmo

all:
	ocamlc -c Util.ml
	ocamlc -c SemiRing.ml
	ocamlc -c Ring.ml
	ocamlc -c Calculus.mli
	ocamlc -c Calculus.ml
	ocamlc -c Compiler.ml
	ocamlmktop -o alpha3_ck Util.cmo SemiRing.cmo Calculus.cmo Compiler.ml Ring.ml

clean:
	rm -f *.cmi *.cmo alpha3_ck M3interp
