include makefile.inc

FILES=\
	src/util/Debug\
	src/util/ListAsSet\
	src/util/ListExtras\
	src/util/Function\
	src/util/HyperGraph\
	src/util/Fixpoint\
	src/util/ExternalCompiler\
	src/util/SourceCode\
	src/global/Types\
	src/global/Schema\
	src/sql/Sql\
	src/sql/SqlClient\
	src/ring/Ring\
	src/ring/Arithmetic\
	src/calculus/Calculus\
	src/calculus/CalculusPrinter\
	src/calculus/CalculusTransforms\
	src/calculus/CalculusDeltas\
	src/calculus/CalculusDecomposition\
	src/calculus/SqlToCalculus\
	src/compiler/Plan\
        src/compiler/IVC\
	src/compiler/Heuristics\
	src/compiler/Compiler\
	src/maps/M3\
	src/maps/Patterns\
	src/maps/M3DM\
	src/functional/K3\
	src/functional/K3Typechecker\
	src/functional/M3ToK3\
	src/functional/K3Optimizer\
	src/functional/K3Codegen\
	src/functional/K3Compiler\
	src/imperative/Imperative\
	src/imperative/K3ToImperative\
	src/imperative/ImperativeCompiler\
	src/lib/Sources\
	src/lib/StandardAdaptors\
	src/lib/SliceableMap\
	src/lib/Values\
	src/lib/Database\
	src/lib/Runtime\
	src/codegen/K3Interpreter\

TOPLEVEL_FILES=\
	src/global/Driver\
	src/global/UnitTest\

LEXERS=\
	src/parsers/Sqllexer\
	src/parsers/Calculuslexer\
	src/parsers/K3lexer\

PARSERS=\
	src/parsers/Sqlparser\
	src/parsers/Calculusparser\
	src/parsers/K3parser\

DIRS=\
	src/util\
	src/global\
	src/parsers\
	src/sql\
	src/ring\
	src/calculus\
	src/compiler\
	src/maps\
	src/functional\
	src/imperative\
	src/codegen\
	src/lib\

INCLUDE_OBJ=\
	str.cma\
	unix.cma

#################################################

BASE_FILES     := $(FILES)
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

#################################################

all: bin/dbtoaster_top bin/dbtoaster_debug bin/dbtoaster runtimelibs

runtimelibs: $(O_FILES) $(C_FILES)
	@echo Making Runtime Libraries
	@make -C lib runtimelibs

lib/.deps:
	@echo Making Library Dependencies
	@make -C lib dependencies
	@touch lib/.deps

bin/dbtoaster: $(O_FILES) lib/.deps src/global/Driver.ml
	@echo "Linking DBToaster (Optimized)"
	@$(OCAMLOPT) $(OPT_FLAGS) -o $@ $(O_FILES) src/global/Driver.ml

bin/dbtoaster_debug: $(C_FILES) lib/.deps src/global/Driver.ml
	@echo "Linking DBToaster (Debug)"
	@$(OCAMLCC) $(OCAML_FLAGS) -o $@ $(C_FILES) src/global/Driver.ml

bin/dbtoaster_top: $(C_FILES) lib/.deps src/global/UnitTest.ml
	@echo "Linking DBToaster Top"
	@$(OCAMLMKTOP) $(OCAML_FLAGS) -o $@ $(C_FILES) src/global/UnitTest.ml

states: $(patsubst %,%.states,$(PARSERS))

test: bin/dbtoaster_top bin/dbtoaster
	@make -C test all

querytest: bin/dbtoaster
	@make -C test query bigquery
	
queries: bin/dbtoaster
	make -C test/queries

doc: $(C_FILES) $(patsubst %,%.ml,$(TOPLEVEL_FILES))
	@FILES="$(patsubst %,../%.ml,$(FILES) $(TOPLEVEL_FILES))"\
	 DIRS="$(patsubst %,../%,$(DIRS))"\
	 make -C doc

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
	@$(OCAMLLEX) $< 2>&1 | sed 's/^/  /'

$(patsubst %,%.ml,$(PARSERS)) : %.ml : %.mly
	@echo Building Parser $(*)
	@$(OCAMLYACC) $< 2>&1 | sed 's/^/  /'

$(patsubst %,%.states,$(PARSERS)) : %.states : %.mly
	@echo Extracting State Transitions For $(*)
	@$(OCAMLYACC) -v $< 2>&1 | sed 's/^/  /'
	mv $(*).output $@

# Ignore generated CMI dependencies.  They get autocompiled along with the
# object files
$(patsubst %,%.cmi,$(FILES)) : 
$(patsubst %,%.cmxi,$(FILES)) : 

#################################################

makefile.deps: makefile $(patsubst %,%.ml,$(BASE_FILES))
	@echo Computing Dependency Graph
	@$(OCAMLDEP) $(patsubst %, -I %,$(DIRS)) \
			$(patsubst %,%.ml,$(BASE_FILES)) > $@

include makefile.deps
include makefile.parserdeps

#################################################

clean: 
	rm -f $(patsubst %,%.ml,$(GENERATED_FILES))
	rm -f $(patsubst %,%.mli,$(PARSERS))
	rm -f $(patsubst %,%.states,$(PARSERS))
	rm -f $(C_FILES) $(C_INCLUDES)
	rm -f $(O_FILES) $(O_INCLUDES)
	rm -f $(patsubst %,%.o,$(FILES))
	rm -f $(patsubst %,%.annot,$(FILES))
	rm -f bin/dbtoaster bin/dbtoaster_top bin/dbtoaster_debug
	rm -f src/global/Driver.cmi src/global/Driver.cmo src/global/Driver.cmx
	rm -f src/global/Driver.cmxi src/global/Driver.o
	rm -f doc/*.aux doc/*.synctex.gz doc/*.log
	rm -f makefile.deps
	make -C lib clean
	rm -f lib/.deps

distclean: clean
	make -C doc clean
	make -C test/queries clean

#################################################

.PHONY: all clean distclean test states doc runtimelibs

