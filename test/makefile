
include ../makefile.local

DBTOASTER_TOP=../bin/dbtoaster_unit
UNIT_TEST=sh -c 'echo ==== $$1 ====; $(DBTOASTER_TOP) $$1' --

QUERY_TEST := scripts/query_test.rb -t interpreter

ifeq ($(COMPILE_SCALA), 1)
	QUERY_TEST += -t scala
endif
ifeq ($(COMPILE_CPP), 1)
	QUERY_TEST += -t cpp $(CPP_TEST_FLAGS)
endif

FAST_QUERY_TEST=$(QUERY_TEST) -d weak-expr-equiv
ERROR_TEST=scripts/error_test.sh
DATA_FILES=$(shell echo \
	../../../experiments/data/employee/*.dat\
	../../../experiments/data/tpch_tiny/*.csv\
	| sort)

all: unit query err

datahash:
	@echo "Data file hash is:" $(shell cat $(DATA_FILES) | md5)

testdatahash:
#	@if [ $(shell basename $(shell which md5)) = md5 ] ; then \
#	  if [ $(shell cat ../../../experiments/data/employee/*.dat \
#	                   ../../../experiments/data/tpch_tiny/*.csv \
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
#	@$(UNIT_TEST) unit/sqlclient.ml

bigquery:	testdatahash
	@$(QUERY_TEST) missedtrades
	@$(QUERY_TEST) axfinder
	@$(QUERY_TEST) pricespread
	@$(QUERY_TEST) brokerspread
	@$(QUERY_TEST) tpch11 -d expressive-tlqs
	@$(QUERY_TEST) tpch17
	@$(QUERY_TEST) tpch18
	@$(QUERY_TEST) tpch22 -d weak-expr-equiv -d HEURISTICS-ALWAYS-UPDATE
	@$(QUERY_TEST) ssb4
	@$(QUERY_TEST) simple/r_ltallcorravg
	
query: testdatahash
	@$(FAST_QUERY_TEST) simple/r_aggcomparison
	@$(FAST_QUERY_TEST) simple/r_aggofnested
	@$(FAST_QUERY_TEST) simple/r_aggofnestedagg
	@$(FAST_QUERY_TEST) simple/r_agtb
	@$(FAST_QUERY_TEST) simple/r_agtbexists
	@$(FAST_QUERY_TEST) simple/r_avg
	@$(FAST_QUERY_TEST) simple/r_bigsumstar
	@$(FAST_QUERY_TEST) simple/r_btimesa
	@$(FAST_QUERY_TEST) simple/r_btimesacorrelated
	@$(FAST_QUERY_TEST) simple/r_count
	@$(FAST_QUERY_TEST) simple/r_divb
	@$(FAST_QUERY_TEST) simple/r_existsnestedagg
	@$(FAST_QUERY_TEST) simple/r_gbasumb
	@$(FAST_QUERY_TEST) simple/r_gtealldynamic
	@$(FAST_QUERY_TEST) simple/r_gtesomedynamic
	@$(FAST_QUERY_TEST) simple/r_gtsomedynamic
	@$(FAST_QUERY_TEST) simple/r_indynamic
	@$(FAST_QUERY_TEST) simple/r_instatic
	@$(FAST_QUERY_TEST) simple/r_ltallagg
	@$(FAST_QUERY_TEST) simple/r_ltallavg
	@$(FAST_QUERY_TEST) simple/r_ltalldynamic
	@$(FAST_QUERY_TEST) simple/r_multinest -d expressive-tlqs
	@$(FAST_QUERY_TEST) simple/r_natselfjoin
	@$(FAST_QUERY_TEST) simple/r_nogroupby
	@$(FAST_QUERY_TEST) simple/r_selectstar
	@$(FAST_QUERY_TEST) simple/r_simplenest
	@$(FAST_QUERY_TEST) simple/r_smallstar
	@$(FAST_QUERY_TEST) simple/r_starofnested
	@$(FAST_QUERY_TEST) simple/r_starofnestedagg -d expressive-tlqs
	@$(FAST_QUERY_TEST) simple/r_sumadivsumb
	@$(FAST_QUERY_TEST) simple/r_sumdivgrp
	@$(FAST_QUERY_TEST) simple/r_sumnestedintarget
	@$(FAST_QUERY_TEST) simple/r_sumnestedintargetwitheq
	@$(FAST_QUERY_TEST) simple/r_sumstar
	@$(FAST_QUERY_TEST) simple/t_lifttype
	@$(FAST_QUERY_TEST) simple/inequality_selfjoin
	@$(FAST_QUERY_TEST) simple/miraculous_minus
	@$(FAST_QUERY_TEST) simple/miraculous_minus2
	@$(FAST_QUERY_TEST) simple/m3k3unable2escalate
	@$(FAST_QUERY_TEST) simple/r_sumoutsideofagg -d expressive-tlqs
	@$(FAST_QUERY_TEST) simple/rst
	@$(FAST_QUERY_TEST) simple/r_count_of_one
	@$(FAST_QUERY_TEST) simple/r_count_of_one_prime
	@$(FAST_QUERY_TEST) simple/r_deepscoping
	@$(FAST_QUERY_TEST) simple/r_impossibleineq
	@$(FAST_QUERY_TEST) simple/r_ineqandeq
	@$(FAST_QUERY_TEST) simple/r_lift_of_count -d expressive-tlqs
	@$(FAST_QUERY_TEST) simple/r_nonjoineq
	@$(FAST_QUERY_TEST) simple/r_possibleineq
	@$(FAST_QUERY_TEST) simple/r_possibleineqwitheq
	@$(FAST_QUERY_TEST) simple/r_sum_gb_all_out_of_aggregate -d expressive-tlqs
	@$(FAST_QUERY_TEST) simple/r_sum_gb_out_of_aggregate -d expressive-tlqs
	@$(FAST_QUERY_TEST) simple/r_sum_out_of_aggregate
	@$(FAST_QUERY_TEST) simple/r_unique_counts_by_a -d expressive-tlqs
	@$(FAST_QUERY_TEST) simple/rr_ormyself
	@$(FAST_QUERY_TEST) simple/rs
	@$(FAST_QUERY_TEST) simple/rs_cmpnest
	@$(FAST_QUERY_TEST) simple/rs_eqineq
	@$(FAST_QUERY_TEST) simple/rs_ineqonnestedagg
	@$(FAST_QUERY_TEST) simple/rs_inequality
	@$(FAST_QUERY_TEST) simple/rs_ineqwithnestedagg
	@$(FAST_QUERY_TEST) simple/rs_joinon
	@$(FAST_QUERY_TEST) simple/rs_joinwithnestedagg
	@$(FAST_QUERY_TEST) simple/rs_natjoin
	@$(FAST_QUERY_TEST) simple/rs_natjoinineq
	@$(FAST_QUERY_TEST) simple/rs_natjoinpartstar
	@$(FAST_QUERY_TEST) simple/rs_selectconstcmp
	@$(FAST_QUERY_TEST) simple/rs_selectpartstar
	@$(FAST_QUERY_TEST) simple/rs_selectstar
	@$(FAST_QUERY_TEST) simple/rs_simple
	@$(FAST_QUERY_TEST) simple/rs_stringjoin
	@$(FAST_QUERY_TEST) simple/rstar
	@$(FAST_QUERY_TEST) simple/rtt_or_with_stars
	@$(FAST_QUERY_TEST) simple/singleton_renaming_conflict
	@$(FAST_QUERY_TEST) simple/ss_math
	@$(FAST_QUERY_TEST) simple/invalid_schema_fn
	@$(FAST_QUERY_TEST) zeus/11564068
	@$(FAST_QUERY_TEST) zeus/12811747
	@$(FAST_QUERY_TEST) zeus/37494577
	@$(FAST_QUERY_TEST) zeus/39765730
	@$(FAST_QUERY_TEST) zeus/48183500
	@$(FAST_QUERY_TEST) zeus/52548748
	@$(FAST_QUERY_TEST) zeus/59977251
	@$(FAST_QUERY_TEST) zeus/75453299
	@$(FAST_QUERY_TEST) zeus/94384934
	@$(FAST_QUERY_TEST) zeus/95497049
	@$(FAST_QUERY_TEST) zeus/96434723
	@$(FAST_QUERY_TEST) employee/query01
	@$(FAST_QUERY_TEST) employee/query01a
	@$(FAST_QUERY_TEST) employee/query02
	@$(FAST_QUERY_TEST) employee/query02a
	@$(FAST_QUERY_TEST) employee/query03
	@$(FAST_QUERY_TEST) employee/query03a
	@$(FAST_QUERY_TEST) employee/query05
	@$(FAST_QUERY_TEST) employee/query06
	@$(FAST_QUERY_TEST) employee/query07
	@$(FAST_QUERY_TEST) employee/query08
	@$(FAST_QUERY_TEST) employee/query08a
	@$(FAST_QUERY_TEST) employee/query09
	@$(FAST_QUERY_TEST) employee/query09a
	@$(FAST_QUERY_TEST) employee/query10
	@$(FAST_QUERY_TEST) employee/query10a
	@$(FAST_QUERY_TEST) employee/query11b
	@$(FAST_QUERY_TEST) employee/query12a
	@$(FAST_QUERY_TEST) employee/query16
	@$(FAST_QUERY_TEST) employee/query16a
	@$(FAST_QUERY_TEST) employee/query17a
	@$(FAST_QUERY_TEST) employee/query22
	@$(FAST_QUERY_TEST) employee/query35b   
	@$(FAST_QUERY_TEST) employee/query35c
	@$(FAST_QUERY_TEST) employee/query36b  
	@$(FAST_QUERY_TEST) employee/query36c 
	@$(FAST_QUERY_TEST) employee/query37
	@$(FAST_QUERY_TEST) employee/query38a
	@$(FAST_QUERY_TEST) employee/query39
	@$(FAST_QUERY_TEST) employee/query40
	@$(FAST_QUERY_TEST) employee/query48
	@$(FAST_QUERY_TEST) employee/query49
	@$(FAST_QUERY_TEST) employee/query50
	@$(FAST_QUERY_TEST) employee/query51
	@$(FAST_QUERY_TEST) employee/query52a
	@$(FAST_QUERY_TEST) employee/query54
	@$(FAST_QUERY_TEST) employee/query55
	@$(FAST_QUERY_TEST) employee/query56a
	@$(FAST_QUERY_TEST) employee/query57a
	@$(FAST_QUERY_TEST) employee/query58a
	@$(FAST_QUERY_TEST) employee/query59
	@$(FAST_QUERY_TEST) employee/query60
	@$(FAST_QUERY_TEST) employee/query61
	@$(FAST_QUERY_TEST) employee/query62a
	@$(FAST_QUERY_TEST) employee/query63a
	@$(FAST_QUERY_TEST) employee/query64a
	@$(FAST_QUERY_TEST) employee/query65a
	@$(FAST_QUERY_TEST) tpch3 --dataset tiny
	@$(FAST_QUERY_TEST) tpch11 --dataset tiny -d expressive-tlqs
	@$(FAST_QUERY_TEST) tpch17 --dataset tiny
	@$(FAST_QUERY_TEST) tpch18 --dataset tiny
	@$(FAST_QUERY_TEST) tpch22 -d weak-expr-equiv  -d HEURISTICS-ALWAYS-UPDATE
	@$(FAST_QUERY_TEST) vwap --dataset tiny
	@$(FAST_QUERY_TEST) brokervariance

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
	@$(ERROR_TEST) adaptormismatch

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
