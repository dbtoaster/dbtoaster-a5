
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

bigquery:	
#	@$(QUERY_TEST) missedtrades -d K3-NO-OPTIMIZE #results incorrect
	@$(QUERY_TEST) axfinder
	@$(QUERY_TEST) pricespread
	@$(QUERY_TEST) brokerspread
	@$(QUERY_TEST) tpch17
#	@$(QUERY_TEST) ssb4 -d K3-NO-OPTIMIZE #slow
#	@$(QUERY_TEST) tpch11 -d K3-NO-OPTIMIZE #results incorrect
#	@$(QUERY_TEST) tpch18  #results incorrect
#	@$(QUERY_TEST) tpch22  #results incorrect
#	@$(QUERY_TEST) vwap    #slow
	
query:
	@$(QUERY_TEST) brokervariance
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
	@$(QUERY_TEST) employee/query01
	@$(QUERY_TEST) employee/query01a
	@$(QUERY_TEST) employee/query02
	@$(QUERY_TEST) employee/query02a
	@$(QUERY_TEST) employee/query03
	@$(QUERY_TEST) employee/query03a
	@$(QUERY_TEST) employee/query05
	@$(QUERY_TEST) employee/query06
	@$(QUERY_TEST) employee/query07
	@$(QUERY_TEST) employee/query08
	@$(QUERY_TEST) employee/query08a
	@$(QUERY_TEST) employee/query09
	@$(QUERY_TEST) employee/query09a
	@$(QUERY_TEST) employee/query10
	@$(QUERY_TEST) employee/query10a
	@$(QUERY_TEST) employee/query11b
	@$(QUERY_TEST) employee/query12a
	@$(QUERY_TEST) employee/query16
	@$(QUERY_TEST) employee/query16a
	@$(QUERY_TEST) employee/query17a
	@$(QUERY_TEST) employee/query22
	@$(QUERY_TEST) employee/query35b   
	@$(QUERY_TEST) employee/query35c
	@$(QUERY_TEST) employee/query36b  
	@$(QUERY_TEST) employee/query36c 
	@$(QUERY_TEST) employee/query48
	@$(QUERY_TEST) tpch3

employee:
	@$(QUERY_TEST) employee/query01
	@$(QUERY_TEST) employee/query01a
	@$(QUERY_TEST) employee/query02
	@$(QUERY_TEST) employee/query02a
	@$(QUERY_TEST) employee/query03
	@$(QUERY_TEST) employee/query03a
	@$(QUERY_TEST) employee/query04
	@$(QUERY_TEST) employee/query04a
	@$(QUERY_TEST) employee/query05
	@$(QUERY_TEST) employee/query06
	@$(QUERY_TEST) employee/query07
	@$(QUERY_TEST) employee/query08
	@$(QUERY_TEST) employee/query08a
	@$(QUERY_TEST) employee/query09
	@$(QUERY_TEST) employee/query09a
	@$(QUERY_TEST) employee/query10
	@$(QUERY_TEST) employee/query10a
#	@$(QUERY_TEST) employee/query11   # unsupported IN operator
#	@$(QUERY_TEST) employee/query11a  # unsupported IN operator	
	@$(QUERY_TEST) employee/query11b
#	@$(QUERY_TEST) employee/query12   # unsupported IN operator
	@$(QUERY_TEST) employee/query12a
#	@$(QUERY_TEST) employee/query13   # unsupported LIKE operator
#	@$(QUERY_TEST) employee/query14   # unsupported LIKE operator
#	@$(QUERY_TEST) employee/query15   # unsupported LIKE operator
	@$(QUERY_TEST) employee/query16
	@$(QUERY_TEST) employee/query16a
#	@$(QUERY_TEST) employee/query17   # unsupported NULL
	@$(QUERY_TEST) employee/query17a
#	@$(QUERY_TEST) employee/query18   # unsupported ORDER BY
#	@$(QUERY_TEST) employee/query19   # unsupported ORDER BY
#	@$(QUERY_TEST) employee/query20   # unsupported ORDER BY
#	@$(QUERY_TEST) employee/query21   # unsupported ORDER BY
	@$(QUERY_TEST) employee/query22
#	@$(QUERY_TEST) employee/query23   # unsupported MAX, MIN, AVG aggregates
#	@$(QUERY_TEST) employee/query23a  # BUG
#	@$(QUERY_TEST) employee/query24   # unsupported MAX, MIN, AVG aggregates
#	@$(QUERY_TEST) employee/query24a  # BUG
#	@$(QUERY_TEST) employee/query25   # unsupported DATE functions
#	@$(QUERY_TEST) employee/query26   # unsupported DATE functions
#	@$(QUERY_TEST) employee/query27   # unsupported HAVING
#	@$(QUERY_TEST) employee/query28   # unsupported HAVING and DATE functions
#	@$(QUERY_TEST) employee/query29   # unsupported HAVING, DATE functions and IN operator
#	@$(QUERY_TEST) employee/query30   # unsupported HAVING and DATE functions
#	@$(QUERY_TEST) employee/query31   # unsupported DATE functions
#	@$(QUERY_TEST) employee/query32   # unsupported DATE functions
#	@$(QUERY_TEST) employee/query33   # unsupported HAVING and DATE functions
#	@$(QUERY_TEST) employee/query34   # unsupported MAX aggregate
#	@$(QUERY_TEST) employee/query35   # unsupported IN operator 
#	@$(QUERY_TEST) employee/query35a  # unsupported DISTINCT 
	@$(QUERY_TEST) employee/query35b   
	@$(QUERY_TEST) employee/query35c
#	@$(QUERY_TEST) employee/query36   # unsupported IN operator
#	@$(QUERY_TEST) employee/query36a  # unsupported DISTINCT
	@$(QUERY_TEST) employee/query36b  
	@$(QUERY_TEST) employee/query36c 
#	@$(QUERY_TEST) employee/query37   # BUG
#	@$(QUERY_TEST) employee/query38   # unsupported GROUP BY in nested subqueries
#	@$(QUERY_TEST) employee/query38a  # BUG: IVC
#	@$(QUERY_TEST) employee/query39   # BUG: IVC
#	@$(QUERY_TEST) employee/query40   # BUG: IVC
#	@$(QUERY_TEST) employee/query41   # unsupported MAX aggregate
#	@$(QUERY_TEST) employee/query43   # unsupported ALL operator
#	@$(QUERY_TEST) employee/query44   # unsupported ANY operator
#	@$(QUERY_TEST) employee/query45   # BUG: IVC
#	@$(QUERY_TEST) employee/query46   # BUG: IVC
#	@$(QUERY_TEST) employee/query47   # unsupported AVG aggregate
	@$(QUERY_TEST) employee/query48

.PHONY: all unit development query
