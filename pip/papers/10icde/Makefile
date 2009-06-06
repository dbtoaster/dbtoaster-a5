FILES := $(wildcard *.tex) $(wildcard graphics/*.pdf)

all: pip.pdf
	if [ `uname` = "Darwin" ] ; then open pip.pdf; fi

pip.pdf: pip.bbl $(FILES)
	pdflatex pip
	pdflatex pip

pip.bbl: bibtex.bib
	pdflatex pip
	bibtex pip

clean:
	rm -f pip.pdf pip.aux pip.bbl pip.blg pip.log 