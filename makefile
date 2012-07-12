include makefile.inc 

FILES=\
	src/util/Debug\
	src/util/ListAsSet\
	src/util/ListExtras\
	src/util/ListAsFunction\
	src/util/HyperGraph\
	src/util/Fixpoint\
	src/util/MainCpp\
	src/util/ExternalCompiler\
	src/util/SourceCode\
	src/util/FreshVariable\
	src/util/ParsingExtras\
	src/global/Types\
	src/global/Constants\
	src/global/Schema\
	src/global/Functions\
	src/sql/Sql\
	src/sql/SqlClient\
	src/ring/Ring\
	src/ring/Arithmetic\
	src/calculus/Calculus\
	src/calculus/CalculusPrinter\
	src/calculus/CalculusTransforms\
	src/calculus/CalculusDomains\
	src/calculus/CalculusDeltas\
	src/calculus/CalculusDecomposition\
	src/calculus/SqlToCalculus\
	src/calculus/Provenance\
	src/compiler/Plan\
	src/compiler/IVC\
	src/compiler/Heuristics\
	src/compiler/Compiler\
	src/maps/M3\
	src/maps/DistributedM3\
	src/maps/M3DM\
	src/maps/Patterns\
	src/functional/K3\
	src/functional/K3Typechecker\
	src/functional/M3ToK3\
	src/functional/DMToK3\
	src/functional/K3Optimizer\
	src/functional/K3Codegen\
	src/functional/K3Compiler\
	src/imperative/Imperative\
	src/imperative/K3ToImperative\
	src/imperative/ImperativeCompiler\
        src/lib/Sources\
	src/lib/StandardFunctions\
	src/lib/StandardAdaptors\
	src/lib/SliceableMap\
	src/lib/Values\
	src/lib/Database\
	src/lib/DBChecker\
	src/lib/Runtime\
	src/codegen/K3Interpreter\
	src/codegen/K3Scalagen\

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

all: makefile.local versioncheck bin/dbtoaster_top bin/dbtoaster_debug \
		 bin/dbtoaster_release bin/dbtoaster runtimelibs

versioncheck:
	@if [ $(shell ocaml -version | sed 's/.*version \(.*\)$$/\1/' | \
	                  awk -F. '{print ($$1+1000) ($$2+1000) ($$3+1000)}')\
	     -lt 100310121001 ] ; then \
	  echo "Your OCaml version is too low.  OCaml 3.12.1 is required, you have"\
	       $(shell ocaml -version); exit -1; fi

runtimelibs: $(O_FILES) $(C_FILES)
	@echo Making Runtime Libraries
	@make -C lib runtimelibs

lib/.deps:
	@echo Making Library Dependencies
	@make -C lib dependencies
	@touch lib/.deps

bin/dbtoaster_release: $(O_FILES) lib/.deps src/global/Driver.ml
	@echo "Linking DBToaster (Optimized)"
	@$(OCAMLOPT) $(OPT_FLAGS) -o $@ $(O_FILES) src/global/Driver.ml

bin/dbtoaster_debug: $(C_FILES) lib/.deps src/global/Driver.ml
	@echo "Linking DBToaster (Debug)"
	@$(OCAMLCC) $(OCAML_FLAGS) -o $@ $(C_FILES) src/global/Driver.ml

bin/dbtoaster_top: $(C_FILES) lib/.deps src/global/UnitTest.ml
	@echo "Linking DBToaster Top"
	@$(OCAMLMKTOP) $(OCAML_FLAGS) -o $@ $(C_FILES) src/global/UnitTest.ml

bin/dbtoaster: bin/dbtoaster_release 

fast: bin/dbtoaster_debug bin/dbtoaster_top

dist: bin/dbtoaster
	make -C lib runtimelibs_force
	make -C dist 

disttest:
	make -C lib runtimelibs_force
	make -C dist test	

#################################################

states: $(patsubst %,%.states,$(PARSERS))

doc: $(C_FILES) $(patsubst %,%.ml,$(TOPLEVEL_FILES))
	@FILES="$(patsubst %,../%.ml,$(FILES) $(TOPLEVEL_FILES))"\
	 DIRS="$(patsubst %,../%,$(DIRS))"\
	 make -C doc

#################################################

test: bin/dbtoaster_top bin/dbtoaster
	@make -C test $(TEST_TARGET)

localtest: bin/dbtoaster_top
	@make -C test local

querytest: bin/dbtoaster
	@make -C test query bigquery
	
queries: bin/dbtoaster
	make -C test/queries

#################################################


src/util/MainCpp.ml : lib/dbt_c++/main.cpp
	@echo "Generating $@ from $< :"
	@echo "let code = \"" > $@
	@cat $< | sed s/\"/\\\\\"/g >> $@
	@echo "\";;" >> $@

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
			$(patsubst %,%.ml,$(BASE_FILES)) | tr \\\\ / | sed 's:/$$:\\: '> $@

makefile.local:
	@echo Initializing local configuration file
	@cp config/makefile.local.default makefile.local

include makefile.deps
include makefile.parserdeps
include makefile.local

#################################################

clean: 
	rm -f $(patsubst %,%.ml,$(GENERATED_FILES))
	rm -f $(patsubst %,%.mli,$(PARSERS))
	rm -f $(patsubst %,%.states,$(PARSERS))
	rm -f $(C_FILES) $(C_INCLUDES)
	rm -f $(O_FILES) $(O_INCLUDES)
	rm -f $(patsubst %,%.o,$(FILES))
	rm -f $(patsubst %,%.annot,$(FILES))
	rm -f bin/dbtoaster_release bin/dbtoaster_top bin/dbtoaster_debug
	rm -f src/global/*.cmi src/global/*.cmo src/global/*.annot src/global/Driver.cmx
	rm -f src/global/Driver.cmxi src/global/Driver.o
	rm -f doc/*.aux doc/*.synctex.gz doc/*.log
	rm -f makefile.deps
	make -C lib clean
	make -C dist clean
	rm -f lib/.deps

superclean: clean
	rm -f $(shell find src -name "*.cmi") $(shell find src -name "*.cmo")
	rm -f $(shell find src -name "*.cmxi") $(shell find src -name "*.cmx")
	rm -f $(shell find src -name "*.annot") $(shell find src -name "*.o")
	      

distclean: superclean
	make -C doc clean
	make -C test/queries clean

#################################################

.PHONY: all clean distclean test states doc runtimelibs fast querytest queries\
        versioncheck localtest superclean dist
