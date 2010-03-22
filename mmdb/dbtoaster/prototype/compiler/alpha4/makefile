
SOURCE = \
	Util.ml \
	Ring.ml \
	Calculus.mli \
	Calculus.ml \
	M3.ml \
	M3Common.ml \
	CalcToM3.mli \
	CalcToM3.ml \
	Compiler.ml \
	M3Codegen.mli \
	M3Compiler.ml \
	codegen/ocaml/SliceableMap.mli \
	codegen/ocaml/SliceableMap.ml \
	codegen/ocaml/M3OCaml.ml \
	codegen/ocaml/M3Interpreter.ml \
	codegen/ocaml/M3OCamlgen.ml

LEX  = parser/Sqllexer.mll
YACC = parser/Sqlparser.mly

GENERATED_SOURCE = $(subst .mly,.mli,$(YACC)) $(subst .mly,.ml,$(YACC)) $(subst .mll,.ml,$(LEX)) 
SOURCE += $(GENERATED_SOURCE)

RUNTIME = \
	Util.ml \
	M3.ml \
	M3Common.ml \
	codegen/ocaml/SliceableMap.mli \
	codegen/ocaml/SliceableMap.ml \
	codegen/ocaml/M3OCaml.ml

INCLUDE = \
	-I codegen/ocaml/ \
	-I parser/ \
	str.cma

all: top

lex_yacc:
	ocamlyacc $(YACC)
	ocamllex $(LEX)

bytecode: lex_yacc
	ocamlc $(INCLUDE) -c $(SOURCE)

native:
	ocamlopt $(INCLUDE) -ccopt -O3 -nodynlink -unsafe -noassert -c $(SOURCE)

top: bytecode
	ocamlmktop $(INCLUDE) -o dbtoaster_top unix.cma $(patsubst %.ml,%.cmo,$(filter %.ml,$(SOURCE))) 

benchmark: native
	ocamlopt $(INCLUDE) -o m3_ex unix.cmxa $(patsubst %.ml,%.cmx,$(filter %.ml,$(SOURCE))) test/m3_ex.ml

vwap: native
	ocamlopt $(INCLUDE) -ccopt -O3 -o vwap str.cmxa unix.cmxa $(patsubst %.ml,%.cmx,$(filter %.ml,$(SOURCE))) test/vwap_m3_ex.ml

test: top
	./dbtoaster_top test/m3_ex.ml

runtime:
	ocamlopt -I codegen/ocaml -ccopt -O3 -c $(RUNTIME)

query: runtime query.ml
	ocamlopt -I codegen/ocaml -ccopt -O3 -nodynlink -unsafe -noassert -o query str.cmxa unix.cmxa $(patsubst %.ml,%.cmx,$(filter %.ml,$(RUNTIME))) query.ml

clean:
	rm -f *.cmi *.cmo *.cmx *.o codegen/ocaml/*.cmi codegen/ocaml/*.cmo codegen/ocaml/*.cmx codegen/ocaml/*.o test/*.cmi test/*.cmo test/*.cmx test/*.o dbtoaster_top m3_ex vwap parser/*.cmo parser/*.cmi $(GENERATED_SOURCE)
