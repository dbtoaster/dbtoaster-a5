PDFLATEX=pdflatex
BIBTEX=bibtex

all:
	$(PDFLATEX) -output-directory=doc/ src/designdoc.tex
	cp src/designdoc.bib doc/ && cd doc && $(BIBTEX) designdoc   
	$(PDFLATEX) -output-directory=doc/ src/designdoc.tex
	$(PDFLATEX) -output-directory=doc/ src/designdoc.tex
	rm doc/designdoc.bib
