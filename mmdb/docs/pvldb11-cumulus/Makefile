GRAPHICS=\
	graphics/Architecture.pdf\
	graphics/Datastructures.pdf\
	graphics/GCflowchart.pdf\
	graphics/cycle_graph.pdf\
	graphics/message_passing.pdf\
	graphics/q12_graph.pdf

cumulus.pdf: cumulus.tex bib
	pdflatex cumulus
	pdflatex cumulus

bib: main.bib
	pdflatex cumulus
	bibtex cumulus

all: cumulus.pdf graphics
	open cumulus.pdf

.PHONY: graphics bib

graphcis: $(GRAPHICS)

$(GRAPHICS): %.pdf : %.graffle
	@echo Generating $@
	@osascript \
		-e "tell application \"OmniGraffle 5\" to set myfile to (open \"$(shell pwd)/$<\")"\
		-e "tell application \"OmniGraffle 5\" to save myfile in \"$(shell pwd)/$@\""
#		-e "tell application \"OmniGraffle 5\" to close myfile"
