include ../../makefile.inc

SCALA := scala
SCALAC := scalac
LIBJAR := dbtlib.jar
TUPLEGENJAR := tuplegen.jar
FILES := $(shell find src/org/dbtoaster/dbtoasterlib -name *.scala)
TUPLEGENFILES := $(shell find tuplegen -name *.scala)
TUPLEFILE := src/org/dbtoaster/dbtoasterlib/Tuples.scala

TARGET:=$(shell which $(SCALAC) &>/dev/null && echo $(LIBJAR) && echo $(TUPLEGENJAR) || echo warn)

all: $(TARGET)

warn: $(FILES)
	@echo 
	@echo "------------------------------------------------------------"
	@echo "Warning: Scala library will not be built: $(SCALAC) not found!"
	@echo "------------------------------------------------------------"
	@echo 

	
$(LIBJAR): $(FILES) $(TUPLEGENFILES)
	make tuplegen.jar
	@echo Building Scala Library
	@rm -f "$(LIBJAR)"
	@$(SCALAC) -d "$(LIBJAR)" $(FILES)

$(TUPLEGENJAR): $(TUPLEGENFILES)
	@echo Generating tuples
	@rm -f "$(TUPLEGENJAR)"
	@$(SCALAC) -d "$(TUPLEGENJAR)" $(TUPLEGENFILES)
	@$(SCALA) -classpath "$(TUPLEGENJAR)" TupleGenerator 100 > $(TUPLEFILE)

clean: 
	rm -f "$(LIBJAR)"
	rm -f "$(TUPLEGENJAR)"

.PHONY: all clean
