FILES=\
	src/util/ListAsSet\
	src/util/ListExtras\
	src/util/Debug\
	src/GlobalTypes\
	src/DBSchema\
	src/ring/Ring\
	src/ring/Arithmetic\
	src/ring/Calculus\
	src/plan/M3\
	src/compiler/Datastructure\
	src/compiler/Compiler

DIRS=\
	src/\
	src/util/\
	src/ring/\
	src/plan/

INCLUDE_OBJ=\
	str.cma\
	unix.cma

OCAML_FLAGS = -g
OPT_FLAGS   = -ccopt -O3 -nodynlink -unsafe -noassert

OCAMLCC   =ocamlc
OCAMLOPT  =ocamlopt
OCAMLMKTOP=ocamlmktop
OCAMLDEP  =ocamldep
OCAMLYACC =ocamlyacc
OCAMLLEX  =ocamllex

#################################################

C_FILES    =$(patsubst %,%.cmo,$(FILES))
C_INCLUDES =$(patsubst %,%.cmi,$(FILES))
O_FILES    =$(patsubst %,%.cmx,$(FILES))
O_INCLUDES =$(patsubst %,%.cmxi,$(FILES))

OCAML_FLAGS +=\
	$(patsubst %, -I %,$(DIRS)) \
	$(INCLUDE_OBJ)

OPT_FLAGS +=\
	$(patsubst %, -I %,$(DIRS))\
	$(patsubst %.cma,%.cmxa,$(INCLUDE_OBJ))

CLEAN_FILES=\
	$(C_FILES) $(C_INCLUDES) \
	$(O_FILES) $(O_INCLUDES) \
	$(patsubst %,%.o,$(FILES)) \
	bin/dbtoaster bin/dbtoaster_top bin/dbtoaster_debug \
	makefile.deps

#################################################

all: bin/dbtoaster_top bin/dbtoaster_debug bin/dbtoaster  

bin/dbtoaster: $(O_FILES)
	@echo "Linking DBToaster (Optimized)"
	@$(OCAMLOPT) $(OPT_FLAGS) -o $@ $^

bin/dbtoaster_debug: $(C_FILES)
	@echo "Linking DBToaster (Debug)"
	@$(OCAMLCC) $(OCAML_FLAGS) -o $@ $^

bin/dbtoaster_top: $(C_FILES)
	@echo "Linking DBToaster Top"
	@$(OCAMLMKTOP) $(OCAML_FLAGS) -o $@ $^
	
clean: 
	rm -f $(CLEAN_FILES)

test: bin/dbtoaster_top
	@DIRS="$(DIRS)" make -C test

.PHONY: all clean test

#################################################

$(C_FILES) : %.cmo : %.ml
	@if [ -f $(*).mli ] ; then \
		echo Compiling Header $<;\
		$(OCAMLCC) $(OCAML_FLAGS) -c $(*).mli;\
	fi	
	@echo Compiling $<
	@$(OCAMLCC) $(OCAML_FLAGS) -c $<

$(O_FILES) : %.cmx : %.ml
	@if [ -f $(*).mli ] ; then \
		echo Compiling Optimized Header $<;\
		$(OCAMLOPT) $(OPT_FLAGS) -c $(*).mli;\
	fi	
	@echo Compiling Optimized $<
	@$(OCAMLOPT) $(OPT_FLAGS) -c $<

#################################################

makefile.deps: makefile $(patsubst %,%.ml,$(FILES))
	@echo Computing Dependency Graph
	@$(OCAMLDEP) $(patsubst %, -I %,$(DIRS)) $^ > $@

include makefile.deps