all: spread.pdf
	if [ `uname` == "Darwin" ] ; then open spread.pdf; fi

spread.pdf: spread.tex spread.bbl
	pdflatex spread
	pdflatex spread

spread.bbl: spread.bib
	pdflatex spread
	bibtex spread

clean:
	rm -f spread.blg spread.aux spread.pdf spread.log spread.bbl