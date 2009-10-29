PDFLATEX=/usr/texbin/pdflatex
BIBTEX=/usr/texbin/bibtex
TEXTARGET=sigmod10-dbtoaster

all:
	env TEXINPUTS="src:" $(PDFLATEX) -output-directory=doc/ $(TEXTARGET).tex
	env TEXINPUTS="src:" $(PDFLATEX) -output-directory=doc/ $(TEXTARGET).tex
	cp src/ref.bib doc/ && cd doc && bibtex $(TEXTARGET) && rm ref.bib
	env TEXINPUTS="src:" $(PDFLATEX) -output-directory=doc/ $(TEXTARGET).tex
