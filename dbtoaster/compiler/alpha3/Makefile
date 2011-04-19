OCAMLC=ocamlc
OCAMLOPT=ocamlopt
OCAMLMKTOP=ocamlmktop
OCAMLDEP=ocamldep
OCAMLLEX=ocamllex
OCAMLYACC=ocamlyacc

STDLIB=/Users/yanif/software/ocaml/lib/ocaml/std-lib

INCLUDES=
OCAMLFLAGS=-g $(INCLUDES)
OCAMLOPTFLAGS=$(INCLUDES)

LIBS=str.cma unix.cma

DBTOASTER_GEN = Sqllexer.ml Sqlparser.ml

DBTOASTER_CORE_OBJS = \
		Util.cmo SemiRing.cmo Algebra.cmo \
		Bytecode.cmo Codegen.cmo Compiler.cmo \
		Sqlparser.cmo Sqllexer.cmo

DBTOASTER_OBJS = $(DBTOASTER_CORE_OBJS) \
		Runtime.cmo Driver.cmo DBToaster.cmo

all: dbtoaster

dbtoaster: $(DBTOASTER_GEN) $(DBTOASTER_OBJS)
	$(OCAMLC) -o dbtoaster $(OCAMLFLAGS) $(LIBS) $(DBTOASTER_OBJS)

top: $(DBTOASTER_GEN) $(DBTOASTER_CORE_OBJS)
	$(OCAMLMKTOP) -o dbtoaster.top $(LIBS) $(DBTOASTER_CORE_OBJS) DBToasterTop.ml

# Common rules
.SUFFIXES: .mll .mly .mli .ml .cmi .cmo .cmx

.mll.ml:
	$(OCAMLLEX) $<

.mly.ml:
	$(OCAMLYACC) $<

.mli.cmi:
	$(OCAMLC) $(OCAMLFLAGS) -c $<

.ml.cmo:
	$(OCAMLC) $(OCAMLFLAGS) -c $<

.ml.cmx:
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -c $<

# Clean up
clean:
	rm -f dbtoaster dbtoaster.top
	rm -f *.cm[iox]
	rm -f Sqllexer.ml Sqlparser.mli Sqlparser.ml
	rm -f .depend

# Dependencies
.depend: $(DBTOASTER_GEN)
	$(OCAMLDEP) $(INCLUDES) *.mli *.ml > .depend

include .depend
