all: 
	make -C src $(TARGET)
	make -C bin $(TARGET) 

clean: 
	make TARGET=clean
	

test: all test_naive test_switch

test_naive:
	./bin/unit.sh data/naive_test_1.unit -e data/naive_test_1.expect -c

test_switch:
	./bin/unit.sh data/switch_test_1.unit -e data/switch_test_1.expect -c