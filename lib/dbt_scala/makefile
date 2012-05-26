SCALAC := scalac
LIBJAR := dbtlib.jar
FILES := src/org/dbtoaster/dbtoasterlib/*

all: 
	-rm "$(LIBJAR)"
	@$(SCALAC) -d "$(LIBJAR)" $(FILES)

clean: 
	rm "$(LIBJAR)"

.PHONY: all clean
