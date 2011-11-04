FILES=\
	util/ListAsSet.cmo\
	util/ListExtras.cmo\
	util/Debug.cmo\
	stages/GlobalTypes.cmo\
	stages/ring/Ring.cmo\
	stages/ring/Arithmetic.cmo\
	stages/ring/Calculus.cmo

DIRS=\
	util/\
	stages/\
	stages/ring/

INCLUDE_OBJ=\
	str.cma\
	unix.cma

OCAMLCC   =ocamlc
OCAMLMKTOP=ocamlmktop

#################################################

INCLUDES   =$(patsubst %.cmo,%.cmi,$(FILES))

OCAML_FLAGS=\
	$(patsubst %, -I %,$(DIRS)) \
	$(INCLUDE_OBJ)

CLEAN_FILES=\
	$(FILES) \
	$(INCLUDES) \
	bin/dbtoaster bin/dbtoaster_top

#################################################

all: bin/dbtoaster_top bin/dbtoaster

bin/dbtoaster: $(FILES)
	@echo "Linking DBToaster"
	@$(OCAMLCC) $(OCAML_FLAGS) -o $@ $^

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