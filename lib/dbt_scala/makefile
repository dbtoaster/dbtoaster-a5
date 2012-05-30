SCALAC := scalac
LIBJAR := dbtlib.jar
FILES := $(shell find src/org/dbtoaster/dbtoasterlib -name *.scala)

all: $(LIBJAR)

$(LIBJAR): $(FILES)
	@echo Building Scala Library
	@rm -f "$(LIBJAR)"
	@$(SCALAC) -d "$(LIBJAR)" $(FILES)

clean: 
	rm "$(LIBJAR)"

.PHONY: all clean
