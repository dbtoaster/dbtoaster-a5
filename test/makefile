
DBTOASTER_TOP=../bin/dbtoaster_top $(patsubst %,-I ../%,$(DIRS))
UNIT_TEST=sh -c 'echo ==== $$1 ====; $(DBTOASTER_TOP) $$1' --


all: unit

unit: 
	@$(UNIT_TEST) unit/simplification.ml
	@$(UNIT_TEST) unit/calculus.ml


.PHONY: all unit