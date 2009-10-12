
gen-rb: common/spread.thrift validateServices
	thrift --gen rb common/spread.thrift

validateServices: 
	bin/validateServices.rb common/spread.thrift node/node.rb

clean:
	rm -rf gen-rb
