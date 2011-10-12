
TEX_FILES=$(wildcard src/*.tex)

TEX_CMD=pdflatex -halt-on-error
BIB_CMD=bibtex

all: doc doc/sigmod12-dbtoaster.pdf

doc:
	mkdir doc

bib: doc/sigmod12-dbtoaster.bbl

doc/sigmod12-dbtoaster.pdf: $(TEX_FILES) doc/sigmod12-dbtoaster.bbl
	cd src; $(TEX_CMD) -output-directory=../doc sigmod12-dbtoaster
	cd src; $(TEX_CMD) -output-directory=../doc sigmod12-dbtoaster

doc/sigmod12-dbtoaster.bbl: src/sigmod12-dbtoaster.bib
	cd doc; cp ../src/sigmod12-dbtoaster.bib .; $(BIB_CMD) sigmod12-dbtoaster;\
	rm sigmod12-dbtoaster.bib

open: doc/sigmod12-dbtoaster.pdf
	open $<

clean:
	rm doc/*

.PHONY: bib