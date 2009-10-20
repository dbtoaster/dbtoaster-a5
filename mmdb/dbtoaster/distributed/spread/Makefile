SQL_TESTDATA=data/rst_join.unit
SQL_TESTS=$(patsubst %,test_%,$(SQL_TESTDATA))

all: 
	make -C src $(TARGET)
	make -C bin $(TARGET)

clean: 
	make TARGET=clean
	
test_data: $(SQL_TESTDATA)

$(SQL_TESTDATA) : %.unit : all %.sql
	./bin/toaster.sh $(subst .unit,.sql,$@) -o $@

test: all test_naive test_switch $(SQL_TESTS)

test_naive:
	./bin/unit.sh data/naive_test_1.unit -e data/naive_test_1.expect -c

test_switch:
	./bin/unit.sh data/switch_test_1.unit -e data/switch_test_1.expect -c

$(SQL_TESTS) : test_% : %
	if [ -f $(patsubst test_%.unit,%.expect,$@) ] ; then ./bin/unit.sh $(patsubst test_%,%,$@) -e $(patsubst test_%.unit,%.expect,$@); else ./bin/unit.sh $(patsubst test_%,%,$@); fi