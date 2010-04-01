SOURCE = \
	Util.ml \
	Ring.ml \
	Calculus.mli \
	Calculus.ml \
	M3.ml \
	M3Common.mli \
	M3Common.ml \
	Compiler.mli \
	Compiler.ml \
	CalcToM3.mli \
	CalcToM3.ml \
	M3Codegen.mli \
	M3Compiler.ml \
	lib/ocaml/SliceableMap.mli \
	lib/ocaml/SliceableMap.ml \
	lib/ocaml/M3OCaml.ml \
	lib/ocaml/StandardAdaptors.ml \
	codegen/ocaml/M3Interpreter.ml \
	codegen/ocaml/M3OCamlgen.ml

LEX_SOURCE  = parser/Sqllexer.mll
YACC_SOURCE = parser/Sqlparser.mly

GENERATED_SOURCE = $(subst .mly,.mli,$(YACC_SOURCE)) \
									 $(subst .mly,.ml,$(YACC_SOURCE)) \
									 $(subst .mll,.ml,$(LEX_SOURCE)) 
SOURCE += $(GENERATED_SOURCE)

OBJ = $(patsubst %.ml,%.cmo,$(filter %.ml,$(SOURCE)))
XOPBJ = $(patsubst %.ml,%.cmx,$(filter %.ml,$(SOURCE)))

RUNTIME = \
	Util.ml \
	M3.ml \
	M3Common.ml \
	lib/ocaml/SliceableMap.mli \
	lib/ocaml/SliceableMap.ml \
	lib/ocaml/M3OCaml.ml \
	lib/ocaml/StandardAdaptors.ml

INCLUDE_DIR = \
	-I codegen/ocaml/ \
	-I parser/ \
	-I lib/ocaml
INCLUDE_OBJ = \
	str.cma \
	unix.cma
INCLUDE_OPT = $(subst .cma,.cmxa,$(INCLUDE_OBJ))
 
OC_FLAGS = $(INCLUDE_DIR) $(INCLUDE_OBJ) -g
OOPT_FLAGS = $(INCLUDE_DIR) $(INCLUDE_OPT)

all: top driver

lex_yacc:
	ocamlyacc $(YACC_SOURCE)
	ocamllex $(LEX_SOURCE)

bytecode: lex_yacc
	ocamlc $(OC_FLAGS) -c $(SOURCE)

native: lex_yacc
	ocamlopt $(OOPT_FLAGS) -ccopt -O3 -nodynlink -unsafe -noassert -c $(SOURCE)

top: bytecode
	ocamlmktop $(OC_FLAGS) -o dbtoaster_top $(OBJ)

driver: bytecode
	ocamlc $(OC_FLAGS) -o dbtoaster $(OBJ) Driver.ml

benchmark: native
	ocamlopt $(OOPT_FLAGS) -o m3_ex unix.cmxa $(patsubst %.ml,%.cmx,$(filter %.ml,$(SOURCE))) test/m3_ex.ml

vwap: native
	ocamlopt $(OOPT_FLAGS) -ccopt -O3 -o vwap unix.cmxa $(XOBJ) test/vwap_m3_ex.ml

test: top
	./dbtoaster_top test/m3_ex.ml

runtime:
	ocamlopt $(INCLUDES) -ccopt -O3 -c $(RUNTIME)

query: runtime query.ml
	ocamlopt $(INCLUDES) -ccopt -O3 -nodynlink -unsafe -noassert -o query str.cmxa unix.cmxa $(XOBJ) query.ml

clean:
	rm -f *.cmi *.cmo *.cmx *.o \
	    codegen/ocaml/*.cmi codegen/ocaml/*.cmo codegen/ocaml/*.cmx codegen/ocaml/*.o \
	    lib/ocaml/*.cmi lib/ocaml/*.cmo lib/ocaml/*.cmx lib/ocaml/*.o \
	    test/*.cmi test/*.cmo test/*.cmx test/*.o dbtoaster dbtoaster_top m3_ex vwap $(GENERATED_SOURCE)
