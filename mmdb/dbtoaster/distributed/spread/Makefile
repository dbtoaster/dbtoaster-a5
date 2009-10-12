all: 
	make -C src $(TARGET)
	make -C scripts $(TARGET)

clean: 
	make TARGET=clean

test: all
	./scripts/unit.sh data/naive_test_1.unit -e data/naive_test_1.expect -c