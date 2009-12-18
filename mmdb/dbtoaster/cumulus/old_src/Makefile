
CLASSFILES=\
	node/MultiKeyMapJavaImpl.class
CLASSPATH=../lib/je.jar

all: timestamps/validation timestamps/thrift bin/jrb_compat.jar bin/spread_thrift.jar

bin/spread_thrift.jar: timestamps/thrift
	javac -cp `echo ../lib/*.jar | tr ' ' ':'` gen-java/*.java
	cd gen-java;jar cf ../bin/spread_thrift.jar *.class

timestamps/thrift: common/spread.thrift 
	thrift --gen rb --gen java common/spread.thrift
	touch timestamps/thrift

timestamps/validation: common/spread.thrift node/node.rb switch/switch.rb slicer/slicer.rb
	bin/validateServices.rb common/spread.thrift node/node.rb switch/switch.rb slicer/slicer.rb
	touch timestamps/validation

$(CLASSFILES) : %.class : %.java
	javac -cp $(CLASSPATH) $<

bin/jrb_compat.jar: $(CLASSFILES)
	jar cf $@ $(CLASSFILES) $(subst $$,\$$, $(wildcard $(subst .class,\$$*.class,$(CLASSFILES))))

clean:
	rm -rf gen-* timestamps/* $(CLASSFILES) bin/jrb_compat.jar bin/spread_thrift.jar
	make -C tpch-simple clean