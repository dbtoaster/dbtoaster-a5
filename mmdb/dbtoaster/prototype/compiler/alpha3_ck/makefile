all:
	ocamlc -c Util.ml
	ocamlc -c SemiRing.ml
	ocamlc -c Calculus.mli
	ocamlc -c Calculus.ml
	ocamlc -c Compiler.ml
	ocamlmktop -o alpha3_ck Util.cmo SemiRing.cmo Calculus.cmo Compiler.ml

clean:
	rm -f *.cmi *.cmo alpha3_ck
