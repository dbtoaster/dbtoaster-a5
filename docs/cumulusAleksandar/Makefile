all: main.pdf

main.pdf: main.bbl $(wildcard *.tex)
	pdflatex main
	pdflatex main

main.bbl: main.bib refs.bib
	pdflatex main
	bibtex main

clean:
	rm -f main.blg main.aux main.pdf main.log main.bbl
