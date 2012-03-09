
FILES := $(shell echo $$FILES)
DIRS  := $(patsubst %,-I %,$(shell echo $$DIRS))

HTMLDOC = ocamldoc -html -d html $(DIRS)

all: html
	$(HTMLDOC) $(FILES)

html:
	mkdir html
