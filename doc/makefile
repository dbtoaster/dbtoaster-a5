
FILES := $(shell echo $$FILES)
DIRS  := $(patsubst %,-I %,$(shell echo $$DIRS))

HTMLDOC = ocamldoc -html -d html $(DIRS)

all: html
	@echo Building HTML Documentation (This can take a while)
	@$(HTMLDOC) $(FILES)

html:
	mkdir html

clean:
	rm -rf html

.PHONY: all clean