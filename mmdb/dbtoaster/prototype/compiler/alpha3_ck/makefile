all:
	ocamlc -c Util.ml
	ocamlc -c SemiRing.ml
	ocamlc -c Algebra.mli
	ocamlc -c Algebra.ml
	ocamlc -c Compiler.ml
	ocamlmktop -o alpha3_ck Util.cmo SemiRing.cmo Algebra.cmo Compiler.ml

clean:
	rm -f *.cmi *.cmo alpha3_ck
