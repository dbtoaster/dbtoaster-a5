CC=g++
ECPG=/usr/bin/ecpg

VWAPDIR=vwap
SSBDIR=ssb
LRDIR=linearroad

PGINC=/usr/include/postgresql
BOOSTINC=/home/yanif/software/boost/include/boost-1_38/

PGLIB=-lecpg
BOOSTLIB=

#OPTIMIZATION=-funroll-loops -fvariable-expansion-in-unroller -fwhole-program -fprofile-use -fgcse-sm -fgcse-las -fgcse-after-reload -funsafe-loop-optimizations -fipa-pta -fipa-cp -ftree-loop-linear -ftree-loop-ivcanon -fivopts -fvect-cost-model

OPTIMIZATION=-funroll-loops -fvariable-expansion-in-unroller -fwhole-program -fgcse-sm -fgcse-las -fgcse-after-reload -funsafe-loop-optimizations -ftree-loop-linear -ftree-loop-ivcanon -fivopts -fvect-cost-model

WARNINGS=-Wunsafe-loop-optimizations

#CFLAGS=$(WARNINGS) $(OPTIMIZATION) -pg -O3
CFLAGS=$(WARNINGS) $(OPTIMIZATION) -O3

INCLUDES=-I$(PGINC) -I$(BOOSTINC)
LIBS=$(PGLIB) $(BOOSTLIB)

all: vwap linearroad ssb

clean:
	rm $(VWAPDIR)/vwap_ecpg.cc $(VWAPDIR)/vwap
	rm $(LRDIR)/linearroad
	rm $(SSBDIR)/ssb_ecpg.cc $(SSBDIR)/ssb

$(VWAPDIR)/vwap_ecpg.cc: 
	test -f $(VWAPDIR)/vwap_ecpg.cc && \
	    (echo "Cleaning ECPG..." && rm $(VWAPDIR)/vwap_ecpg.cc) || \
	    echo "Generating ECPG..."
	$(ECPG) -o $(VWAPDIR)/vwap_ecpg.cc $(VWAPDIR)/vwap.cc
	
vwap: $(VWAPDIR)/vwap_ecpg.cc
	$(CC) $(CFLAGS) -o $(VWAPDIR)/vwap $(INCLUDES) $(VWAPDIR)/vwap_ecpg.cc $(LIBS)

$(LRDIR)/linearroad:
	$(CC) $(CFLAGS) -o $(LRDIR)/linearroad $(LRDIR)/linearroad-accident.cc 

linearroad: $(LRDIR)/linearroad

$(SSBDIR)/ssb_ecpg.cc:
	test -f $(SSBDIR)/ssb_ecpg.cc && \
	     (echo "Cleaning ECPG..." && rm $(SSBDIR)/ssb_ecpg.cc) || \
	     echo "Generating ECPG..."
	$(ECPG) -o $(SSBDIR)/ssb_ecpg.cc $(SSBDIR)/ssb.cc

ssb: $(SSBDIR)/ssb_ecpg.cc
	$(CC) $(CFLAGS) -o $(SSBDIR)/ssb $(INCLUDES) $(SSBDIR)/ssb_ecpg.cc $(LIBS)
