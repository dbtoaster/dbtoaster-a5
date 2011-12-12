
DBTOASTER_TOP=../bin/dbtoaster_top $(patsubst %,-I ../%,$(DIRS))
UNIT_TEST=sh -c 'echo ==== $$1 ====; $(DBTOASTER_TOP) $$1' --


all: development unit

unit: 
	@$(UNIT_TEST) unit/calculus.ml

development:
	@$(UNIT_TEST) unit/calculusoptimizer.ml

.PHONY: all unit development