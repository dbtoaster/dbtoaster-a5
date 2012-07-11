include ../../makefile.inc

FILES := $(patsubst %,../../src/%,\
	util/ListExtras\
	util/ListAsSet\
	util/Debug\
	global/Functions\
	global/Types\
	global/Schema\
	ring/Ring\
	ring/Arithmetic\
	calculus/Calculus\
	compiler/Plan\
	maps/M3\
	maps/Patterns\
	functional/K3\
	lib/SliceableMap\
	lib/Values\
	lib/Database\
	lib/Sources\
	lib/StandardAdaptors\
	lib/Runtime\
)

DIRS=\
	src/util\
	src/global\
	src/ring\
	src/lib\

#################################################

STD_FILES := $(patsubst %,%.cmo,$(FILES))
OPT_FILES := $(patsubst %,%.cmx,$(FILES))

DIRS := $(patsubst %,-I %,../src/ocaml $(DIRS))

#################################################

all: dbtoaster.cma dbtoaster.cmxa

dbtoaster.cma: makefile $(STD_FILES)
	@echo Building DBToaster Runtime Library
	@$(OCAMLCC) $(OCAML_FLAGS) $(DIRS) -a $(filter %.cmo,$^) -o $@

dbtoaster.cmxa: makefile $(OPT_FILES) 
	@echo Building Optimized DBToaster Runtime Library
	@$(OCAMLOPT) $(OPT_FLAGS) $(DIRS) -a $(filter %.cmx,$^) -o $@

../../src/lib/%.cmo : 
	@echo "Run the top level makefile first"\
			  "($(patsubst ../../src/lib/%.cmo,%,$@) unbuilt)"

../../src/lib/%.cmx : 
	@echo "Run the top level makefile first"\
			  "($(patsubst ../../src/lib/%.cmx,%,$@) unbuilt)"

clean: 
	rm -f dbtoaster.cma dbtoaster.cmxa dbtoaster.a

.PHONY: all clean
	echo $(OCAMLCC)
