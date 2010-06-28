PDFLATEX=pdflatex
BIBTEX=bibtex
TEXTARGET=pvldb11-dbtoaster

all:
	env TEXINPUTS="$TEXINPUTS:src:" $(PDFLATEX) -output-directory=doc/ $(TEXTARGET).tex
	env TEXINPUTS="$TEXINPUTS:src:" $(PDFLATEX) -output-directory=doc/ $(TEXTARGET).tex
#	cp src/ref.bib doc/ && cd doc && bibtex $(TEXTARGET) && rm ref.bib
	env TEXINPUTS="$TEXINPUTS:src:" $(PDFLATEX) -output-directory=doc/ $(TEXTARGET).tex

clean:
	rm doc/*
