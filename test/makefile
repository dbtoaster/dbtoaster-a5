
DBTOASTER_TOP=../bin/dbtoaster_unit
UNIT_TEST=sh -c 'echo ==== $$1 ====; $(DBTOASTER_TOP) $$1' --


all: development unit

unit: 
	@$(UNIT_TEST) unit/ring.ml
	@$(UNIT_TEST) unit/calculus.ml
	@$(UNIT_TEST) unit/calculustransforms.ml
	@$(UNIT_TEST) unit/calculusdeltas.ml
	@$(UNIT_TEST) unit/compiler.ml

development:

queries:
	make -C queries

.PHONY: all unit development queries