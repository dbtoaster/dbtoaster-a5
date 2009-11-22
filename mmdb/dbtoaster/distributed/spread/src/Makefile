
all: timestamps/validation timestamps/thrift #gen-java/spread_thrift.jar
	#make -C tpch-simple

gen-java/spread_thrift.jar: timestamps/thrift
	javac -cp `echo ../lib/*.jar | tr ' ' ':'` gen-java/*.java
	cd gen-java;jar cf spread_thrift.jar *.class

timestamps/thrift: common/spread.thrift 
	thrift --gen rb common/spread.thrift
	touch timestamps/thrift

timestamps/validation: common/spread.thrift node/node.rb switch/switch.rb slicer/slicer.rb
	bin/validateServices.rb common/spread.thrift node/node.rb switch/switch.rb slicer/slicer.rb
	touch timestamps/validation

clean:
	rm -rf gen-* timestamps/*
	make -C tpch-simple clean