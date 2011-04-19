all: main.pdf
#	if [ `uname` == "Darwin" ] ; then open main.pdf; fi

main.pdf: main.bbl $(wildcard *.tex)
	pdflatex main
	pdflatex main

main.bbl: main.bib
	pdflatex main
	bibtex main

clean:
	rm -f main.blg main.aux main.pdf main.log main.bbl
