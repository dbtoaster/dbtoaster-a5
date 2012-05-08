
DBTOASTER_TOP=../bin/dbtoaster_unit
UNIT_TEST=sh -c 'echo ==== $$1 ====; $(DBTOASTER_TOP) $$1' --
QUERY_TEST=scripts/query_test.rb

all: unit query

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
	@$(QUERY_TEST) axfinder
	@$(QUERY_TEST) brokerspread
	@$(QUERY_TEST) brokervariance
#	@$(QUERY_TEST) missedtrades -d K3-NO-OPTIMIZE #results incorrect
	@$(QUERY_TEST) pricespread
	@$(QUERY_TEST) r_agtb
	@$(QUERY_TEST) r_bigsumstar
	@$(QUERY_TEST) r_btimesa
	@$(QUERY_TEST) r_btimesacorrelated
	@$(QUERY_TEST) r_count
	@$(QUERY_TEST) r_gbasumb
	@$(QUERY_TEST) r_nogroupby
	@$(QUERY_TEST) r_smallstar
	@$(QUERY_TEST) r_sumadivsumb
	@$(QUERY_TEST) r_sumstar
	@$(QUERY_TEST) rst
#	@$(QUERY_TEST) ssb4 -d K3-NO-OPTIMIZE #slow
#	@$(QUERY_TEST) tpch11 -d K3-NO-OPTIMIZE #results incorrect
	@$(QUERY_TEST) tpch17
#	@$(QUERY_TEST) tpch18  #results incorrect
#	@$(QUERY_TEST) tpch22  #results incorrect
	@$(QUERY_TEST) tpch3
#	@$(QUERY_TEST) vwap    #slow


.PHONY: all unit development query