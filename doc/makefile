include ../makefile.local

FILES := $(shell echo $$FILES)
DIRS  := $(patsubst %,-I %,$(shell echo $$DIRS))

SITE_SYNC_FILES := index.php style.css pages

HTMLDOC = ocamldoc -html -d html $(DIRS)

all: html
	@echo "Building HTML Documentation (This can take a while)"
	@$(HTMLDOC) $(FILES)

html:
	mkdir html

clean:
	rm -rf html

push_site:
	@if [ "$(SITE_USERNAME)" ] ; then\
		scp -r site/pages/*.php \
			$(SITE_USERNAME)_dbtoaster@ssh.phx.nearlyfreespeech.net:pages/ ; \
	else echo "Set the SITE_USERNAME variable in makefile.local"; fi

pull_site:
	@if [ "$(SITE_USERNAME)" ] ; then\
		scp -r \
			$(patsubst %,$(SITE_USERNAME)_dbtoaster@ssh.phx.nearlyfreespeech.net:%, \
								 $(SITE_SYNC_FILES)) site/ ;\
	else echo "Set the SITE_USERNAME variable in makefile.local"; fi

.PHONY: all clean push_site