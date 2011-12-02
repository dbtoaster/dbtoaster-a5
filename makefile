FILES=\
	src/util/ListAsSet\
	src/util/ListExtras\
	src/util/Debug\
	src/GlobalTypes\
	src/DBSchema\
	src/sql/Sql\
	src/ring/Ring\
	src/ring/Arithmetic\
	src/ring/Calculus\
	src/ring/SqlToCalculus\
	src/maps/M3\
	src/compiler/Datastructure\
	src/compiler/Statement\
	src/compiler/Compiler

LEXERS=\
	src/sql/Sqllexer

PARSERS=\
	src/sql/Sqlparser

DIRS=\
	src\
	src/util\
	src/sql\
	src/ring\
	src/maps\
	src/compiler

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

GENERATED_FILES = $(PARSERS) $(LEXERS) 
FILES += $(GENERATED_FILES)

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
	$(patsubst %,%.ml,$(GENERATED_FILES))\
	$(patsubst %,%.mli,$(PARSERS))\
	$(C_FILES) $(C_INCLUDES) \
	$(O_FILES) $(O_INCLUDES) \
	$(patsubst %,%.o,$(FILES)) \
	bin/dbtoaster bin/dbtoaster_top bin/dbtoaster_debug \
	src/Driver.cmi src/Driver.cmo src/Driver.cmx src/Driver.cmxi src/Driver.o \
	makefile.deps

#################################################

all: bin/dbtoaster_top bin/dbtoaster_debug bin/dbtoaster  

bin/dbtoaster: $(O_FILES) src/Driver.ml
	@echo "Linking DBToaster (Optimized)"
	@$(OCAMLOPT) $(OPT_FLAGS) -o $@ $(O_FILES) src/Driver.ml

bin/dbtoaster_debug: $(C_FILES) src/Driver.ml
	@echo "Linking DBToaster (Debug)"
	@$(OCAMLCC) $(OCAML_FLAGS) -o $@ $(C_FILES) src/Driver.ml

bin/dbtoaster_top: $(C_FILES)
	@echo "Linking DBToaster Top"
	@$(OCAMLMKTOP) $(OCAML_FLAGS) -o $@ $(C_FILES)
	
clean: 
	rm -f $(CLEAN_FILES)

test: bin/dbtoaster_top
	@DIRS="$(DIRS)" make -C test

.PHONY: all clean test

#################################################

$(C_FILES) : %.cmo : %.ml
	@if [ -f $(*).mli ] ; then \
		echo Compiling Header $(*);\
		$(OCAMLCC) $(OCAML_FLAGS) -c $(*).mli;\
	fi	
	@echo Compiling $(*)
	@$(OCAMLCC) $(OCAML_FLAGS) -c $<

$(O_FILES) : %.cmx : %.ml
	@if [ -f $(*).mli ] ; then \
		echo Compiling Optimized Header $(*);\
		$(OCAMLOPT) $(OPT_FLAGS) -c $(*).mli;\
	fi	
	@echo Compiling Optimized $(*)
	@$(OCAMLOPT) $(OPT_FLAGS) -c $<

$(patsubst %,%.ml,$(LEXERS)) : %.ml : %.mll
	@echo Building Lexer $(*)
	@$(OCAMLLEX) $<

$(patsubst %,%.ml,$(PARSERS)) : %.ml : %.mly
	@echo Building Parser $(*)
	@$(OCAMLYACC) $<

# Ignore generated CMI dependencies.  They get autocompiled along with the
# object files
$(patsubst %,%.cmi,$(FILES)) : 
$(patsubst %,%.cmxi,$(FILES)) : 


#################################################

makefile.deps: makefile $(patsubst %,%.ml,$(FILES))
	@echo Computing Dependency Graph
	@$(OCAMLDEP) $(patsubst %, -I %,$(DIRS)) $^ > $@

include makefile.deps