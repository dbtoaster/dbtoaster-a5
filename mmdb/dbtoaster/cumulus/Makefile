SQL_TESTDATA=data/tpch_LONG.unit data/tpch_q12.unit data/tpch_PPSS.unit data/rst_join.unit data/tpch_PPSS_nid.unit data/tpch_demand.unit data/tpch_debt.unit
AUTO_SQL_TESTS=rst_join
SQL_LOCALTESTS=data/tpch_PPSS_local.unit

all: recursive $(SQL_TESTDATA) $(SQL_LOCALTESTS)

recursive:
	make -C src $(TARGET)
	make -C bin $(TARGET)

clean: 
	make recursive TARGET=clean
	rm -f $(SQL_TESTDATA)
	
test_data: all $(SQL_TESTDATA)

$(SQL_TESTDATA) : %.unit : %.sql src/toaster/toaster.rb data/wl08.nodes
	./bin/toaster.sh $(subst .unit,.sql,$@) -o $@ -b $(subst .unit,.boot,$@) -p tpch/pfile/100m data/wl10.nodes -l local.properties

$(SQL_LOCALTESTS) : %_local.unit : %.sql src/toaster/toaster.rb data/localhost.nodes
	./bin/toaster.sh $(subst _local.unit,.sql,$@) -o $@ -b $(subst .unit,.boot,$@) data/localhost.nodes -l local.properties

test: all test_naive test_switch $(AUTO_SQL_TESTS)

test_naive:
	./bin/unit.sh data/naive_test_1.unit -e data/naive_test_1.expect -c

test_switch:
	./bin/unit.sh data/switch_test_1.unit -e data/switch_test_1.expect -c

$(AUTO_SQL_TESTS) : % : data/%.unit
	if [ -f data/$@.expect ] ; then ./bin/unit.sh data/$@.unit -e data/$@.expect; else ./bin/unit.sh data/$@.unit; fi

measurements: measurements/.timestamp

measurements/.timestamp: $(wildcard measurements/*.run) src/bin/graph-launcher.rb
	./bin/graph.sh
	touch measurements/.timestamp
