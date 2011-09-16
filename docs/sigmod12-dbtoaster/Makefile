
TEX_FILES=$(wildcard src/*.tex)

TEX_CMD=pdflatex -halt-on-error

all: doc doc/sigmod12-dbtoaster.pdf

doc:
	mkdir doc

doc/sigmod12-dbtoaster.pdf: $(TEX_FILES)
	cd src; $(TEX_CMD) -output-directory=../doc sigmod12-dbtoaster
	cd src; $(TEX_CMD) -output-directory=../doc sigmod12-dbtoaster

open: doc/sigmod12-dbtoaster.pdf
	open $<

clean:
	rm doc/*