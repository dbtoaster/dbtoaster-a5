
OBJ_FILES = Util.cmo \
						Ring.cmo \
						SuperMap.cmo \
						Calculus.cmo \
						M3.cmo \
						Compiler.cmo \
						M3Eval.cmo

all: $(OBJ_FILES)
	ocamlmktop -o alpha3_ok $(OBJ_FILES)

$(OBJ_FILES) : %.cmo : %.ml 
	@if [ -f $*.mli ] ; then echo ocamlc -c $*.mli; ocamlc -c $*.mli; fi
	ocamlc -c $<

clean:
	rm -f *.cmi *.cmo alpha3_ok
