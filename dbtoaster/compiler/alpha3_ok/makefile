
OBJ_FILES = Util.cmo \
						Ring.cmo \
						SuperMap.cmo \
						Calculus.cmo \
						M3.cmo \
						CalcToM3.cmo \
						Compiler.cmo \
						M3Eval.cmo

COMPILE_CMD = ocamlc -g -c

all: $(OBJ_FILES)
	ocamlmktop -g -o alpha3_ok $(OBJ_FILES)

$(OBJ_FILES) : %.cmo : %.ml 
	@if [ -f $*.mli ] ; then echo $(COMPILE_CMD) $*.mli; $(COMPILE_CMD) $*.mli; fi
	$(COMPILE_CMD) $<

clean:
	rm -f *.cmi *.cmo alpha3_ok
