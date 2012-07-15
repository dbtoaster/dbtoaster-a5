include ../../makefile.inc

SCALAC := scalac
LIBJAR := dbtlib.jar
FILES := $(shell find src/org/dbtoaster/dbtoasterlib -name *.scala)


TARGET:=$(shell which $(SCALAC) &>/dev/null && echo $(LIBJAR) || echo warn)

all: $(TARGET)

warn: $(FILES)
	@echo -e "\n\n----------------------------------------"
	@echo "Warning: Scala library will not be built: $(SCALAC) not found!"
	@echo -e "----------------------------------------\n\n"

	
$(LIBJAR): $(FILES)
	@echo Building Scala Library
	@rm -f "$(LIBJAR)"
	@$(SCALAC) -d "$(LIBJAR)" $(FILES)

clean: 
	rm -f "$(LIBJAR)"

.PHONY: all clean
