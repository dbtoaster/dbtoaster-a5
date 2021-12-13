
include ../makefile.local

DBTOASTER_TOP=../bin/dbtoaster_unit
UNIT_TEST=sh -c 'echo ==== $$1 ====; $(DBTOASTER_TOP) $$1' --

QUERY_TEST := scripts/query_test.rb -d SQL-INLIST-AS-DISJUNCTION $(QUERY_TEST_FLAGS)

ifneq (,$(findstring CYGWIN,$(shell uname)))
	QUERY_TEST += --path-delim \;
endif

ifeq ($(TEST_INTERPRETER), 1)
	QUERY_TEST += -t interpreter 
endif
ifeq ($(TEST_CPP), 1)
	QUERY_TEST += -t cpp $(CPP_TEST_FLAGS)
endif
ifeq ($(TEST_SCALA), 1)
	QUERY_TEST += -t scala $(SCALA_TEST_FLAGS)
endif
ifeq ($(TEST_SCALAOPT), 1)
	QUERY_TEST += -t scalaopt $(SCALAOPT_TEST_FLAGS)
endif
ifeq ($(TEST_ALL), 1)
	QUERY_TEST += -t all $(CPP_TEST_FLAGS) $(SCALA_TEST_FLAGS) $(SCALAOPT_TEST_FLAGS)
endif

FAST_QUERY_TEST=$(QUERY_TEST) -d weak-expr-equiv
ERROR_TEST=scripts/error_test.sh
DATA_FILES=$(shell echo \
	../../dbtoaster-experiments-data/employee/*.dat\
	../../dbtoaster-experiments-data/tpch/tiny/*.csv\
	| sort)

all: unit query err

datahash:
	@echo "Data file hash is:" $(shell cat $(DATA_FILES) | md5)

testdatahash:
#	@if [ $(shell basename $(shell which md5)) = md5 ] ; then \
#	  if [ $(shell cat ../../dbtoaster-experiments-data/employee/*.dat \
#	                   ../../dbtoaster-experiments-data/tpch/tiny/*.csv \
#	                    | md5) != 4aafddbcd29e2e52a053651761605ea4 ] ; then \
#	echo "Your test data is out of date, update your experiments directory";\
#	exit -1; fi;\
#	else echo "Warning: MD5 is installed, cannot check test data integrity"; fi

unit: 
	@$(UNIT_TEST) unit/sql.ml
	@$(UNIT_TEST) unit/ring.ml
	@$(UNIT_TEST) unit/calculus.ml
	@$(UNIT_TEST) unit/calculustransforms.ml
	@$(UNIT_TEST) unit/calculusdeltas.ml
	@$(UNIT_TEST) unit/heuristics.ml
	@$(UNIT_TEST) unit/compiler.ml
	@$(UNIT_TEST) unit/compiler.ml
	@$(UNIT_TEST) unit/k3interpreter.ml
	@$(UNIT_TEST) unit/m3tok3.ml
	@$(UNIT_TEST) unit/k3optimizer.ml
#	@$(UNIT_TEST) unit/sqlclient.ml

bigquery:	testdatahash
	@$(QUERY_TEST) vwap 
	@$(QUERY_TEST) missedtrades
	@$(QUERY_TEST) axfinder
	@$(QUERY_TEST) pricespread
	@$(QUERY_TEST) brokerspread
	@$(QUERY_TEST) brokervariance
	@$(QUERY_TEST) tpch3
	@$(QUERY_TEST) tpch11
	@$(QUERY_TEST) tpch17
	@$(QUERY_TEST) tpch18
	@$(QUERY_TEST) tpch22
	@$(QUERY_TEST) ssb4
	
query: testdatahash query_simple query_zeus query_employee query_tpch query_financial

query_simple:
	@$(QUERY_TEST) simple/r_aggcomparison
	@$(QUERY_TEST) simple/r_aggofnested
	@$(QUERY_TEST) simple/r_aggofnestedagg
	@$(QUERY_TEST) simple/r_agtb
	@$(QUERY_TEST) simple/r_agtbexists
	@$(QUERY_TEST) simple/r_avg
	@$(QUERY_TEST) simple/r_bigsumstar
	@$(QUERY_TEST) simple/r_btimesa
	@$(QUERY_TEST) simple/r_btimesacorrelated
	@$(QUERY_TEST) simple/r_count
	@$(QUERY_TEST) simple/r_case
	@$(QUERY_TEST) simple/r_divb
	@$(QUERY_TEST) simple/r_existsnestedagg
	@$(QUERY_TEST) simple/r_gbasumb
	@$(QUERY_TEST) simple/r_gtealldynamic
	@$(QUERY_TEST) simple/r_gtesomedynamic
	@$(QUERY_TEST) simple/r_gtsomedynamic
	@$(QUERY_TEST) simple/r_indynamic
	@$(QUERY_TEST) simple/r_instatic
	@$(QUERY_TEST) simple/r_ltallagg
	@$(QUERY_TEST) simple/r_ltallavg
	@$(QUERY_TEST) simple/r_ltalldynamic
	@$(QUERY_TEST) simple/r_natselfjoin
	@$(QUERY_TEST) simple/r_nogroupby
	@$(QUERY_TEST) simple/r_selectstar
	@$(QUERY_TEST) simple/r_simplenest
	@$(QUERY_TEST) simple/r_smallstar
	@$(QUERY_TEST) simple/r_starofnested
	@$(QUERY_TEST) simple/r_starofnestedagg
	@$(QUERY_TEST) simple/r_sumadivsumb
	@$(QUERY_TEST) simple/r_sumdivgrp
	@$(QUERY_TEST) simple/r_sumnestedintarget
	@$(QUERY_TEST) simple/r_sumnestedintargetwitheq
	@$(QUERY_TEST) simple/r_sumstar
	@$(QUERY_TEST) simple/t_lifttype
	@$(QUERY_TEST) simple/r_ltallcorravg
	@$(QUERY_TEST) simple/inequality_selfjoin
	@$(QUERY_TEST) simple/miraculous_minus
	@$(QUERY_TEST) simple/miraculous_minus2
	@$(QUERY_TEST) simple/m3k3unable2escalate
	@$(QUERY_TEST) simple/r_sumoutsideofagg
	@$(QUERY_TEST) simple/rst
	@$(QUERY_TEST) simple/r_count_of_one
	@$(QUERY_TEST) simple/r_count_of_one_prime
	@$(QUERY_TEST) simple/r_deepscoping
	@$(QUERY_TEST) simple/r_impossibleineq
	@$(QUERY_TEST) simple/r_ineqandeq
	@$(QUERY_TEST) simple/r_lift_of_count
	@$(QUERY_TEST) simple/r_nonjoineq
	@$(QUERY_TEST) simple/r_possibleineq
	@$(QUERY_TEST) simple/r_possibleineqwitheq
	@$(QUERY_TEST) simple/r_sum_gb_all_out_of_aggregate
	@$(QUERY_TEST) simple/r_sum_gb_out_of_aggregate
	@$(QUERY_TEST) simple/r_sum_out_of_aggregate
	@$(QUERY_TEST) simple/r_unique_counts_by_a
	@$(QUERY_TEST) simple/r_multinest
	@$(QUERY_TEST) simple/r_nestedfrom
	@$(QUERY_TEST) simple/rr_ormyself
	@$(QUERY_TEST) simple/rs
	@$(QUERY_TEST) simple/rs_cmpnest
	@$(QUERY_TEST) simple/rs_eqineq
	@$(QUERY_TEST) simple/rs_ineqonnestedagg
	@$(QUERY_TEST) simple/rs_inequality
	@$(QUERY_TEST) simple/rs_ineqwithnestedagg
	@$(QUERY_TEST) simple/rs_joinon
	@$(QUERY_TEST) simple/rs_joinwithnestedagg
	@$(QUERY_TEST) simple/rs_natjoin
	@$(QUERY_TEST) simple/rs_natjoinineq
	@$(QUERY_TEST) simple/rs_natjoinpartstar
	@$(QUERY_TEST) simple/rs_selectconstcmp
	@$(QUERY_TEST) simple/rs_selectpartstar
	@$(QUERY_TEST) simple/rs_selectstar
	@$(QUERY_TEST) simple/rs_simple
	@$(QUERY_TEST) simple/rs_stringjoin
	@$(QUERY_TEST) simple/rstar
	@$(QUERY_TEST) simple/rtt_or_with_stars
	@$(QUERY_TEST) simple/singleton_renaming_conflict
	@$(QUERY_TEST) simple/ss_math
	@$(QUERY_TEST) simple/invalid_schema_fn
	@$(QUERY_TEST) simple/rs_column_mapping_1
	@$(QUERY_TEST) simple/rs_column_mapping_2
	@$(QUERY_TEST) simple/rs_column_mapping_3
	@$(QUERY_TEST) simple/r_multiors

query_zeus:
	@$(QUERY_TEST) zeus/11564068
	@$(QUERY_TEST) zeus/12811747
	@$(QUERY_TEST) zeus/37494577
	@$(QUERY_TEST) zeus/39765730
	@$(QUERY_TEST) zeus/48183500
	@$(QUERY_TEST) zeus/52548748
	@$(QUERY_TEST) zeus/59977251
	@$(QUERY_TEST) zeus/75453299
	@$(QUERY_TEST) zeus/94384934
	@$(QUERY_TEST) zeus/95497049
	@$(QUERY_TEST) zeus/96434723

query_employee:
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
#	@$(QUERY_TEST) employee/query35b   
	@$(QUERY_TEST) employee/query35c
#	@$(QUERY_TEST) employee/query36b  
	@$(QUERY_TEST) employee/query36c 
	@$(QUERY_TEST) employee/query37
	@$(QUERY_TEST) employee/query38a
	@$(QUERY_TEST) employee/query39
	@$(QUERY_TEST) employee/query40
	@$(QUERY_TEST) employee/query48
	@$(QUERY_TEST) employee/query49
	@$(QUERY_TEST) employee/query50
	@$(QUERY_TEST) employee/query51
	@$(QUERY_TEST) employee/query52a
	@$(QUERY_TEST) employee/query54
	@$(QUERY_TEST) employee/query55
	@$(QUERY_TEST) employee/query56a
	@$(QUERY_TEST) employee/query57a
	@$(QUERY_TEST) employee/query58a
	@$(QUERY_TEST) employee/query59
	@$(QUERY_TEST) employee/query60
	@$(QUERY_TEST) employee/query61
	@$(QUERY_TEST) employee/query62a
	@$(QUERY_TEST) employee/query63a
	@$(QUERY_TEST) employee/query64a
	@$(QUERY_TEST) employee/query65a

tpch_test:
	@$(QUERY_TEST) tpch1
	@$(QUERY_TEST) tpch2
	@$(QUERY_TEST) tpch3
	@$(QUERY_TEST) tpch4 
	@$(QUERY_TEST) tpch5 
#	@$(QUERY_TEST) tpch6  #F.P. error on the interpreter
	@$(QUERY_TEST) tpch7 
	@$(QUERY_TEST) tpch8   #slow compilation
	@$(QUERY_TEST) tpch9   #slow compilation
	@$(QUERY_TEST) tpch10 
	@$(QUERY_TEST) tpch11 
	@$(QUERY_TEST) tpch11a 
	@$(QUERY_TEST) tpch12 
	@$(QUERY_TEST) tpch13 
	@$(QUERY_TEST) tpch14
	@$(QUERY_TEST) tpch15  
	@$(QUERY_TEST) tpch16 
	@$(QUERY_TEST) tpch17 
	@$(QUERY_TEST) tpch17a 
	@$(QUERY_TEST) tpch18 
	@$(QUERY_TEST) tpch18a
	@$(QUERY_TEST) tpch19 
	@$(QUERY_TEST) tpch20
	@$(QUERY_TEST) tpch21 
	@$(QUERY_TEST) tpch22 
	@$(QUERY_TEST) tpch22a
	@$(QUERY_TEST) ssb4

financial_test:
	@$(QUERY_TEST) vwap
	@$(QUERY_TEST) vwap -d HEURISTICS-ENABLE-INPUTVARS
	@$(QUERY_TEST) brokervariance
	@$(QUERY_TEST) brokerspread
#	@$(QUERY_TEST) missedtrades
	@$(QUERY_TEST) axfinder
	@$(QUERY_TEST) pricespread

query_tpch:
	@$(QUERY_TEST) tpch1 --dataset tiny
	@$(QUERY_TEST) tpch2 --dataset tiny
	@$(QUERY_TEST) tpch3 --dataset tiny
	@$(QUERY_TEST) tpch3
	@$(QUERY_TEST) tpch4 --dataset tiny
	@$(QUERY_TEST) tpch5 --dataset tiny
#	@$(QUERY_TEST) tpch6 --dataset tiny  #F.P. error on the interpreter
	@$(QUERY_TEST) tpch7 --dataset tiny
	@$(QUERY_TEST) tpch8 --dataset tiny  #slow compilation
	@$(QUERY_TEST) tpch9 --dataset tiny  #slow compilation
	@$(QUERY_TEST) tpch10 --dataset tiny
	@$(QUERY_TEST) tpch11 --dataset tiny
	@$(QUERY_TEST) tpch11 
	@$(QUERY_TEST) tpch11a --dataset tiny
#	@$(QUERY_TEST) tpch11c --dataset tiny  #slow compilation
#	@$(QUERY_TEST) tpch11c  # ~90s
	@$(QUERY_TEST) tpch12 --dataset tiny
	@$(QUERY_TEST) tpch13 --dataset tiny
	@$(QUERY_TEST) tpch14 --dataset tiny
#	@$(QUERY_TEST) tpch15 --dataset tiny  
	@$(QUERY_TEST) tpch16 --dataset tiny
	@$(QUERY_TEST) tpch17 --dataset tiny
	@$(QUERY_TEST) tpch17 
	@$(QUERY_TEST) tpch17a --dataset tiny
	@$(QUERY_TEST) tpch17a 
	@$(QUERY_TEST) tpch18 --dataset tiny
#	@$(QUERY_TEST) tpch18 #sloooow
	@$(QUERY_TEST) tpch18a --dataset tiny
	@$(QUERY_TEST) tpch18a
	@$(QUERY_TEST) tpch19 --dataset tiny
	@$(QUERY_TEST) tpch20 --dataset tiny
	@$(QUERY_TEST) tpch21 --dataset tiny
	@$(QUERY_TEST) tpch22 --dataset tiny
	@$(QUERY_TEST) tpch22a --dataset tiny
	@$(QUERY_TEST) tpch22a
	@$(QUERY_TEST) ssb4 --dataset tiny
#	@$(QUERY_TEST) ssb4

query_tpch_standard:
	@$(QUERY_TEST) tpch1 --dataset standard
	@$(QUERY_TEST) tpch2 --dataset standard
	@$(QUERY_TEST) tpch3 --dataset standard
	@$(QUERY_TEST) tpch4 --dataset standard
	@$(QUERY_TEST) tpch5 --dataset standard
#	@$(QUERY_TEST) tpch6 --dataset standard  #F.P. error on the interpreter
	@$(QUERY_TEST) tpch7 --dataset standard
	@$(QUERY_TEST) tpch8 --dataset standard  #slow compilation
	@$(QUERY_TEST) tpch9 --dataset standard  #slow compilation
	@$(QUERY_TEST) tpch10 --dataset standard
	@$(QUERY_TEST) tpch11 --dataset standard
	@$(QUERY_TEST) tpch11a --dataset standard
#	@$(QUERY_TEST) tpch11c --dataset standard  #slow compilation
#	@$(QUERY_TEST) tpch11c  # ~90s
	@$(QUERY_TEST) tpch12 --dataset standard
	@$(QUERY_TEST) tpch13 --dataset standard
	@$(QUERY_TEST) tpch14 --dataset standard
#	@$(QUERY_TEST) tpch15 --dataset standard  
	@$(QUERY_TEST) tpch16 --dataset standard
	@$(QUERY_TEST) tpch17 --dataset standard
	@$(QUERY_TEST) tpch17a --dataset standard
	@$(QUERY_TEST) tpch18 --dataset standard
#	@$(QUERY_TEST) tpch18 #sloooow
	@$(QUERY_TEST) tpch18a --dataset standard
	@$(QUERY_TEST) tpch19 --dataset standard
	@$(QUERY_TEST) tpch20 --dataset standard
	@$(QUERY_TEST) tpch21 --dataset standard
	@$(QUERY_TEST) tpch22 --dataset standard
	@$(QUERY_TEST) tpch22a --dataset standard
	@$(QUERY_TEST) ssb4 --dataset standard
#	@$(QUERY_TEST) ssb4

query_tpch_big:
	@$(QUERY_TEST) tpch1 --dataset big
	@$(QUERY_TEST) tpch2 --dataset big
	@$(QUERY_TEST) tpch3 --dataset big
	@$(QUERY_TEST) tpch4 --dataset big
	@$(QUERY_TEST) tpch5 --dataset big
#	@$(QUERY_TEST) tpch6 --dataset big  #F.P. error on the interpreter
	@$(QUERY_TEST) tpch7 --dataset big
	@$(QUERY_TEST) tpch8 --dataset big  #slow compilation
	@$(QUERY_TEST) tpch9 --dataset big  #slow compilation
	@$(QUERY_TEST) tpch10 --dataset big
	@$(QUERY_TEST) tpch11 --dataset big
	@$(QUERY_TEST) tpch11a --dataset big
#	@$(QUERY_TEST) tpch11c --dataset big  #slow compilation
#	@$(QUERY_TEST) tpch11c  # ~90s
	@$(QUERY_TEST) tpch12 --dataset big
	@$(QUERY_TEST) tpch13 --dataset big
	@$(QUERY_TEST) tpch14 --dataset big
#	@$(QUERY_TEST) tpch15 --dataset big  
	@$(QUERY_TEST) tpch16 --dataset big
	@$(QUERY_TEST) tpch17 --dataset big
	@$(QUERY_TEST) tpch17a --dataset big
	@$(QUERY_TEST) tpch18 --dataset big
#	@$(QUERY_TEST) tpch18 #sloooow
	@$(QUERY_TEST) tpch18a --dataset big
	@$(QUERY_TEST) tpch19 --dataset big
	@$(QUERY_TEST) tpch20 --dataset big
	@$(QUERY_TEST) tpch21 --dataset big
	@$(QUERY_TEST) tpch22 --dataset big
	@$(QUERY_TEST) tpch22a --dataset big
	@$(QUERY_TEST) ssb4 --dataset big
#	@$(QUERY_TEST) ssb4

query_financial:
	@$(QUERY_TEST) vwap --dataset tiny
#	@$(QUERY_TEST) vwap
	@$(QUERY_TEST) vwap --dataset tiny -d HEURISTICS-ENABLE-INPUTVARS
	@$(QUERY_TEST) vwap -d HEURISTICS-ENABLE-INPUTVARS
	@$(QUERY_TEST) brokervariance --dataset tiny
	@$(QUERY_TEST) brokervariance
	@$(QUERY_TEST) brokerspread --dataset tiny
	@$(QUERY_TEST) brokerspread
	@$(QUERY_TEST) missedtrades --dataset tiny
#	@$(QUERY_TEST) missedtrades
	@$(QUERY_TEST) axfinder --dataset tiny
	@$(QUERY_TEST) axfinder
	@$(QUERY_TEST) pricespread --dataset tiny
	@$(QUERY_TEST) pricespread

local: 
	@for i in $(LOCAL_UNIT_TESTS); do \
		if $(UNIT_TEST) $$i; then echo; else exit -1; fi; done

err:
	@$(ERROR_TEST) ambiguous_names
	@$(ERROR_TEST) ambiguous_sources bug
	@$(ERROR_TEST) scope_from
	@$(ERROR_TEST) scope_nesting
	@$(ERROR_TEST) multitargetnestedtarget
	@$(ERROR_TEST) dupsource
	@$(ERROR_TEST) multitargetin
	@$(ERROR_TEST) multitargetall
	@$(ERROR_TEST) multitargetwherels 
	@$(ERROR_TEST) unbound_var

employee: testdatahash
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
	@$(QUERY_TEST) employee/query45   
	@$(QUERY_TEST) employee/query46   
#	@$(QUERY_TEST) employee/query47   # unsupported AVG aggregate
	@$(QUERY_TEST) employee/query48
	@$(QUERY_TEST) employee/query49
	@$(QUERY_TEST) employee/query50
	@$(QUERY_TEST) employee/query51
#	@$(QUERY_TEST) employee/query52   # unsupported HAVING
	@$(QUERY_TEST) employee/query52a
#	@$(QUERY_TEST) employee/query53   # unsupported HAVING and ORDER BY
#	@$(QUERY_TEST) employee/query53a  # BUG
	@$(QUERY_TEST) employee/query54
	@$(QUERY_TEST) employee/query55
#	@$(QUERY_TEST) employee/query56   # unsupported ORDER BY
	@$(QUERY_TEST) employee/query56a
#	@$(QUERY_TEST) employee/query57   # unsupported ORDER BY
	@$(QUERY_TEST) employee/query57a
#	@$(QUERY_TEST) employee/query58   # unsupported ORDER BY
	@$(QUERY_TEST) employee/query58a
	@$(QUERY_TEST) employee/query59
	@$(QUERY_TEST) employee/query60
	@$(QUERY_TEST) employee/query61
#	@$(QUERY_TEST) employee/query62   # unsupported LEFT JOIN operator
	@$(QUERY_TEST) employee/query62a
#	@$(QUERY_TEST) employee/query63   # unsupported LEFT JOIN and IN  
	@$(QUERY_TEST) employee/query63a
#	@$(QUERY_TEST) employee/query64   # unsupported LEFT JOIN and UNION
	@$(QUERY_TEST) employee/query64a
#	@$(QUERY_TEST) employee/query65   # unsupported IN and UNION ALL operators
	@$(QUERY_TEST) employee/query65a
#	@$(QUERY_TEST) employee/query66   # unsupported IN and INTERSECTION operators
	@$(QUERY_TEST) employee/query66a

.PHONY: all unit query bigquery employee err datahash testdatahash local
