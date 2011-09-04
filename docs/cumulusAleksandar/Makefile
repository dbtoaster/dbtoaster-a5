all: corrective.pdf DBToasterAnalysis.pdf distributedOrder.pdf experimentsPaper.pdf looseCumulus.pdf

.PHONY: epsConverter clean

corrective.pdf: corrective.bbl corrective.tex
	pdflatex corrective
	pdflatex corrective

corrective.bbl: main.bib refs.bib
	pdflatex corrective
	bibtex corrective


DBToasterAnalysis.pdf: DBToasterAnalysis.tex epsConverter
	pdflatex DBToasterAnalysis
	pdflatex DBToasterAnalysis


distributedOrder.pdf: epsConverter distributedOrder.bbl distributedOrder.tex
	pdflatex distributedOrder
	pdflatex distributedOrder

distributedOrder.bbl: main.bib refs.bib candRefs.bib
	pdflatex distributedOrder
	bibtex distributedOrder


experimentsPaper.pdf: experimentsPaper.tex epsConverter
	pdflatex experimentsPaper
	pdflatex experimentsPaper


looseCumulus.pdf: looseCumulus.tex
	pdflatex looseCumulus
	pdflatex looseCumulus


epsConverter:
	./epsConverter.sh

clean:
	rm -f *.blg *.aux *.log *.bbl graphics/*-generated.pdf \
corrective.pdf DBToasterAnalysis.pdf \
distributedOrder.pdf experimentsPaper.pdf \
looseCumulus.pdf
