
DBTOASTER_TOP=../bin/dbtoaster_unit
UNIT_TEST=sh -c 'echo ==== $$1 ====; $(DBTOASTER_TOP) $$1' --
QUERY_TEST=scripts/query_test.rb

all: unit

unit: 
	@$(UNIT_TEST) unit/ring.ml
	@$(UNIT_TEST) unit/calculus.ml
	@$(UNIT_TEST) unit/calculustransforms.ml
	@$(UNIT_TEST) unit/calculusdeltas.ml
	@$(UNIT_TEST) unit/heuristics.ml
	@$(UNIT_TEST) unit/compiler.ml
	@$(UNIT_TEST) unit/compiler.ml
	@$(UNIT_TEST) unit/k3interpreter.ml
	@$(UNIT_TEST) unit/m3tok3.ml
	
query:
	@$(QUERY_TEST) rst
	@$(QUERY_TEST) tpch3

.PHONY: all unit development query