
DBTOASTER_TOP=../bin/dbtoaster_unit
UNIT_TEST=sh -c 'echo ==== $$1 ====; $(DBTOASTER_TOP) $$1' --


all: development unit

unit: 
	@$(UNIT_TEST) unit/ring.ml
	@$(UNIT_TEST) unit/calculus.ml
	@$(UNIT_TEST) unit/calculusoptimizer.ml

development:
	@$(UNIT_TEST) unit/calculusoptimizer.ml
	@$(UNIT_TEST) unit/compiler.ml

.PHONY: all unit development