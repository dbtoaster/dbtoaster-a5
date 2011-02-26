
DBTOASTER_DIR=../../prototype/compiler/alpha4

DBTOASTER_OBJ = \
	build/Util.cmo \
	build/SourceCode.cmo \
	build/Ring.cmo \
	build/Calculus.cmo \
	build/PipedCalculus.cmo\
	build/M3.cmo \
	build/M3Common.cmo \
	build/Compiler.cmo \
	build/PipedCompiler.cmo \
	build/CalcToM3.cmo \
	build/M3Compiler.cmo \
	build/K3.cmo \
	build/K3Optimizer.cmo \
	build/K3Builder.cmo \
	build/K3Typechecker.cmo \
	build/K3Compiler.cmo \
	build/SliceableMap.cmo \
	build/Values.cmo \
	build/Database.cmo \
	build/Sources.cmo \
	build/StandardAdaptors.cmo \
	build/Runtime.cmo \
	build/M3Interpreter.cmo \
	build/M3OCamlgen.cmo \
	build/K3Interpreter.cmo \
	build/K3Plsql.cmo \
	build/K3OCamlgen.cmo


SOURCE=\
	message/Combinations.ml \
	message/StmtToMapAccessList.ml \
	message/Partitions.ml \
	message/UpdateApply.ml \
	message/EnumerateMessages.ml

ML_OBJ = $(subst .ml,.cmo,$(filter %.ml,$(SOURCE)))
MLI_OBJ = $(subst .mli,.cmi,$(filter %.mli,$(SOURCE)))

INCLUDE_DIR = \
	-I build \
	-I message
INCLUDE_OBJ = \
	str.cma \
	unix.cma 

OC_FLAGS = $(INCLUDE_DIR)  $(INCLUDE_OBJ)

cumulus_top: $(ML_OBJ)
	@echo Linking cumulus_top
	@ocamlmktop $(OC_FLAGS) -o cumulus_top $(DBTOASTER_OBJ) $(ML_OBJ)

$(ML_OBJ) : %.cmo : %.ml
	@echo Compiling $*
	@ocamlc $(OC_FLAGS) -c $<

$(MLI_OBJ) : %.cmi : %.mli
	@echo Compiling Header $*
	@ocamlc $(OC_FLAGS) -c $<

dbtoaster: build
	make -C $(DBTOASTER_DIR) top
	cp `find $(DBTOASTER_DIR) -name *.cmo` `find $(DBTOASTER_DIR) -name *.cmi` build

build:
	mkdir build

test: cumulus_top
	./cumulus_top $(INCLUDE_DIR) test/testCombinations.ml
	./cumulus_top $(INCLUDE_DIR) test/testPartitions.ml
	./cumulus_top $(INCLUDE_DIR) test/testStmtToMapAccessList.ml
	./cumulus_top $(INCLUDE_DIR) test/testEnumerateMessages.ml
	./cumulus_top $(INCLUDE_DIR) test/testUpdateApply.ml

.PHONY: dbtoaster test
