
all: timestamps/validation timestamps/thrift 
	make -C tpch-simple

timestamps/thrift: common/spread.thrift 
	thrift --gen rb --gen cpp common/spread.thrift
	touch timestamps/thrift

timestamps/validation: common/spread.thrift node/node.rb switch/switch.rb
	bin/validateServices.rb common/spread.thrift node/node.rb switch/switch.rb
	touch timestamps/validation

clean:
	rm -rf gen-rb gen-cpp timestamps/*
	make -C tpch-simple clean