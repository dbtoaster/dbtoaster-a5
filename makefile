FILES=\
	src/util/ListAsSet.cmo\
	src/util/ListExtras.cmo\
	src/util/Debug.cmo\
	src/GlobalTypes.cmo\
	src/ring/Ring.cmo\
	src/ring/Arithmetic.cmo\
	src/calc/Calculus.cmo

DIRS=\
	src/\
	src/util/\
	src/ring/\
	src/calc/

INCLUDE_OBJ=\
	str.cma\
	unix.cma

OCAMLCC   =ocamlc
OCAMLOPT  =ocamlopt
OCAMLMKTOP=ocamlmktop

#################################################

INCLUDES   =$(patsubst %.cmo,%.cmi,$(FILES))
O_FILES    =$(patsubst %.cmo,%.cmx,$(FILES))
O_INCLUDES =$(patsubst %.cmo,%.cmxi,$(FILES))

OCAML_FLAGS=\
	$(patsubst %, -I %,$(DIRS)) \
	$(INCLUDE_OBJ)

OPT_FLAGS=\
	$(patsubst %, -I %,$(DIRS))\
	$(patsubst %.cma,%.cmx,$(INCLUDE_OBJ))

CLEAN_FILES=\
	$(FILES) $(INCLUDES) \
	$(O_FILES) $(O_INCLUDES) \
	bin/dbtoaster bin/dbtoaster_top bin/dbtoaster_debug

#################################################

all: bin/dbtoaster bin/dbtoaster_debug bin/dbtoaster_top 

bin/dbtoaster: $(OPT_FILES)
	@echo "Linking DBToaster (Unoptimized)"
	@$(OCAMLCC) $(OCAML_FLAGS) -o $@ $^

bin/dbtoaster_debug: $(FILES)
	@echo "Linking DBToaster (Optimized)"
	@$(OCAMLOPT) $(OPT_FLAGS) -o $@ $^

bin/dbtoaster_top: $(FILES)
	@echo "Linking DBToaster Top"
	@$(OCAMLMKTOP) $(OCAML_FLAGS) -o $@ $^
	
clean: 
	rm -f $(CLEAN_FILES)

test: bin/dbtoaster_top
	@DIRS="$(DIRS)" make -C test

.PHONY: all clean test

#################################################

$(FILES) : %.cmo : %.ml
	@if [ -f $(*).mli ] ; then \
		echo Compiling Header $<;\
		$(OCAMLCC) $(OCAML_FLAGS) -c $(*).mli;\
	fi	
	@echo Compiling $<
	@$(OCAMLCC) $(OCAML_FLAGS) -c $<

$(O_FILES) : %.cmx : %.ml
	@if [ -f $(*).mli ] ; then \
		echo Compiling Header $<;\
		$(OCAMLOPT) $(OPT_FLAGS) -c $(*).mli;\
	fi	
	@echo Compiling $<
	@$(OCAMLOPT) $(OPT_FLAGS) -c $<