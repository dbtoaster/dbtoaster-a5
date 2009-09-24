all:
	ocamlc -c Util.ml
	ocamlc -c SemiRing.ml
	ocamlc -c RelAlg.mli
	ocamlc -c RelAlg.ml
	ocamlc -c MapAlg.mli
	ocamlc -c MapAlg.ml
	ocamlc -c Compiler.ml
	ocamlmktop -o alpha3_ck Util.cmo SemiRing.cmo RelAlg.cmo MapAlg.cmo Compiler.ml

without-MapAlg.mli:
	mv MapAlg.mli MapAlg.mli-tmp
	ocamlc -c Util.ml
	ocamlc -c SemiRing.ml
	ocamlc -c RelAlg.mli
	ocamlc -c RelAlg.ml
	ocamlc -c MapAlg.ml
	ocamlc -c Compiler.ml
	ocamlmktop -o alpha3_ck Util.cmo SemiRing.cmo RelAlg.cmo MapAlg.cmo Compiler.ml
	mv MapAlg.mli-tmp MapAlg.mli

clean:
	rm -f *.cmi *.cmo alpha3_ck
