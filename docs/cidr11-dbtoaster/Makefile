PDFLATEX=pdflatex
BIBTEX=bibtex
TEXTARGET=cidr11-dbtoaster
BIBTARGET=ref.bib

all:
	env TEXINPUTS="$TEXINPUTS:src:" $(PDFLATEX) -output-directory=doc/ $(TEXTARGET).tex
	env TEXINPUTS="$TEXINPUTS:src:" $(PDFLATEX) -output-directory=doc/ $(TEXTARGET).tex
	env TEXINPUTS="$TEXINPUTS:src:" $(PDFLATEX) -output-directory=doc/ $(TEXTARGET).tex

bib:
	env TEXINPUTS="$TEXINPUTS:src:" $(PDFLATEX) -output-directory=doc/ $(TEXTARGET).tex
	cp src/$(BIBTARGET) doc/ && cd doc && bibtex $(TEXTARGET) && rm $(BIBTARGET)
	

clean:
	rm doc/*
