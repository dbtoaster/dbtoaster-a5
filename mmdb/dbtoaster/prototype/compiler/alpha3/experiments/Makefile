CXX=g++44
CXXFLAGS=-O3 -DENABLE_SIMPLE_PROFILER -I/home/yanif/workspace/dbtc_alpha3/include -I/home/yanif/software/boost/include
LDFLAGS=-L/home/yanif/software/boost/lib -lboost_system -lboost_thread -lboost_program_options

all:    vwap        vwapr1        vwapr1b        vwapn \
	pricespread pricespreadr1 pricespreadn \
	axfinder    axfinderr1    axfindern \
	ssb         ssbr1         ssbn \
	query3      query3r1      query3n \
	query5      query5r1      query5n \
	query11     query11r1     query11r1b     query11n \
	query17     query17r1     query17n \
	query18     query18r1     query18n \
	query22     query22r1     query22n

# VWAP

VWAP_OBJS = src/vwap/vwap.o
VWAP_R1_OBJS = src/vwap/vwap-r1.o
VWAP_R1B_OBJS = src/vwap/vwap-r1b.o
VWAP_N_OBJS = naive/vwap/vwap.o

vwap: $(VWAP_OBJS)
	$(CXX) -o src/vwap/vwap $(CXXFLAGS) $(VWAP_OBJS) $(LDFLAGS)

vwapr1: $(VWAP_R1_OBJS)
	$(CXX) -o src/vwap/vwap-r1 $(CXXFLAGS) $(VWAP_R1_OBJS) $(LDFLAGS)

vwapr1b: $(VWAP_R1B_OBJS)
	$(CXX) -o src/vwap/vwap-r1b $(CXXFLAGS) $(VWAP_R1B_OBJS) $(LDFLAGS)

vwapn: $(VWAP_N_OBJS)
	$(CXX) -o naive/vwap/vwap-n $(CXXFLAGS) $(VWAP_N_OBJS) $(LDFLAGS)

# Pricespread
PS_OBJS = src/finance/pricespread/pricespread.o
PS_R1_OBJS = src/finance/pricespread/pricespread-r1.o
PS_N_OBJS = naive/finance/pricespread/pricespread.o

pricespread: $(PS_OBJS)
	$(CXX) -o src/finance/pricespread/pricespread $(CXXFLAGS) $(PS_OBJS) $(LDFLAGS)

pricespreadr1: $(PS_R1_OBJS)
	$(CXX) -o src/finance/pricespread/pricespread-r1 $(CXXFLAGS) $(PS_R1_OBJS) $(LDFLAGS)

pricespreadn: $(PS_N_OBJS)
	$(CXX) -o naive/finance/pricespread/pricespread-n $(CXXFLAGS) $(PS_N_OBJS) $(LDFLAGS)
	

# Ax finder
AX_OBJS = src/finance/axfinder/axfinder.o
AX_R1_OBJS = src/finance/axfinder/axfinder-r1.o
AX_N_OBJS = naive/finance/axfinder/axfinder.o

axfinder: $(AX_OBJS)
	$(CXX) -o src/finance/axfinder/axfinder $(CXXFLAGS) $(AX_OBJS) $(LDFLAGS)

axfinderr1: $(AX_R1_OBJS)
	$(CXX) -o naive/finance/axfinder/axfinder-r1 $(CXXFLAGS) $(AX_R1_OBJS) $(LDFLAGS)

axfindern: $(AX_N_OBJS)
	$(CXX) -o naive/finance/axfinder/axfinder-n $(CXXFLAGS) $(AX_N_OBJS) $(LDFLAGS)


# SSB queries

SSB_OBJS = src/tpch/ssb/ssb.o
SSB_R1_OBJS = src/tpch/ssb/ssb-r1.o
SSB_N_OBJS = naive/tpch/ssb/ssb.o

ssb: $(SSB_OBJS)
	$(CXX) -o src/tpch/ssb/ssb $(CXXFLAGS) $(SSB_OBJS) $(LDFLAGS)

ssbr1: $(SSB_R1_OBJS)
	$(CXX) -o src/tpch/ssb/ssb-r1 $(CXXFLAGS) $(SSB_R1_OBJS) $(LDFLAGS)

ssbn: $(SSB_N_OBJS)
	$(CXX) -o naive/tpch/ssb/ssb-n $(CXXFLAGS) $(SSB_N_OBJS) $(LDFLAGS)


# TPCH queries

QUERY3_OBJS = src/tpch/query3/query3.o
QUERY3_R1_OBJS = src/tpch/query3/query3-r1.o
QUERY3_N_OBJS = naive/tpch/query3/query3.o

QUERY5_OBJS = src/tpch/query5/query5.o
QUERY5_R1_OBJS = src/tpch/query5/query5-r1.o
QUERY5_N_OBJS = naive/tpch/query5/query5.o

QUERY11_OBJS = src/tpch/query11/query11.o
QUERY11_R1_OBJS = src/tpch/query11/query11-r1.o
QUERY11_R1B_OBJS = src/tpch/query11/query11-r1b.o
QUERY11_N_OBJS = naive/tpch/query11/query11.o

QUERY17_OBJS = src/tpch/query17/query17.o
QUERY17_R1_OBJS = src/tpch/query17/query17-r1.o
QUERY17_N_OBJS = naive/tpch/query17/query17.o

QUERY18_OBJS = src/tpch/query18/query18.o
QUERY18_R1_OBJS = src/tpch/query18/query18-r1.o
QUERY18_N_OBJS = naive/tpch/query18/query18.o

QUERY22_OBJS = src/tpch/query22/query22.o
QUERY22_R1_OBJS = src/tpch/query22/query22-r1.o
QUERY22_N_OBJS = naive/tpch/query22/query22.o


# Q3
query3: $(QUERY3_OBJS)
	$(CXX) -o src/tpch/query3/query3 $(CXXFLAGS) $(QUERY3_OBJS) $(LDFLAGS)

query3r1: $(QUERY3_R1_OBJS)
	$(CXX) -o src/tpch/query3/query3-r1 $(CXXFLAGS) $(QUERY3_R1_OBJS) $(LDFLAGS)

query3n: $(QUERY3_N_OBJS)
	$(CXX) -o naive/tpch/query3/query3-n $(CXXFLAGS) $(QUERY3_N_OBJS) $(LDFLAGS)


# Q5
query5: $(QUERY5_OBJS)
	$(CXX) -o src/tpch/query5/query5 $(CXXFLAGS) $(QUERY5_OBJS) $(LDFLAGS)

query5r1: $(QUERY5_R1_OBJS)
	$(CXX) -o src/tpch/query5/query5-r1 $(CXXFLAGS) $(QUERY5_R1_OBJS) $(LDFLAGS)

query5n: $(QUERY5_N_OBJS)
	$(CXX) -o naive/tpch/query5/query5-n $(CXXFLAGS) $(QUERY5_N_OBJS) $(LDFLAGS)


# Q11
query11: $(QUERY11_OBJS)
	$(CXX) -o src/tpch/query11/query11 $(CXXFLAGS) $(QUERY11_OBJS) $(LDFLAGS)

query11r1: $(QUERY11_R1_OBJS)
	$(CXX) -o src/tpch/query11/query11-r1 $(CXXFLAGS) $(QUERY11_R1_OBJS) $(LDFLAGS)

query11r1b: $(QUERY11_R1B_OBJS)
	$(CXX) -o src/tpch/query11/query11-r1b $(CXXFLAGS) $(QUERY11_R1B_OBJS) $(LDFLAGS)

query11n: $(QUERY11_N_OBJS)
	$(CXX) -o naive/tpch/query11/query11-n $(CXXFLAGS) $(QUERY11_N_OBJS) $(LDFLAGS)


# Q17
query17: $(QUERY17_OBJS)
	$(CXX) -o src/tpch/query17/query17 $(CXXFLAGS) $(QUERY17_OBJS) $(LDFLAGS)

query17r1: $(QUERY17_R1_OBJS)
	$(CXX) -o src/tpch/query17/query17-r1 $(CXXFLAGS) $(QUERY17_R1_OBJS) $(LDFLAGS)

query17n: $(QUERY17_N_OBJS)
	$(CXX) -o naive/tpch/query17/query17-n $(CXXFLAGS) $(QUERY17_N_OBJS) $(LDFLAGS)


# Q18
query18: $(QUERY18_OBJS)
	$(CXX) -o src/tpch/query18/query18 $(CXXFLAGS) $(QUERY18_OBJS) $(LDFLAGS)

query18r1: $(QUERY18_R1_OBJS)
	$(CXX) -o src/tpch/query18/query18-r1 $(CXXFLAGS) $(QUERY18_R1_OBJS) $(LDFLAGS)

query18n: $(QUERY18_N_OBJS)
	$(CXX) -o naive/tpch/query18/query18-n $(CXXFLAGS) $(QUERY18_N_OBJS) $(LDFLAGS)


# Q22
query22: $(QUERY22_OBJS)
	$(CXX) -o src/tpch/query22/query22 $(CXXFLAGS) $(QUERY22_OBJS) $(LDFLAGS)

query22r1: $(QUERY22_R1_OBJS)
	$(CXX) -o src/tpch/query22/query22-r1 $(CXXFLAGS) $(QUERY22_R1_OBJS) $(LDFLAGS)

query22n: $(QUERY22_N_OBJS)
	$(CXX) -o naive/tpch/query22/query22-n $(CXXFLAGS) $(QUERY22_N_OBJS) $(LDFLAGS)


# Common rules
.SUFFIXES: .cc .o

.cc.o:
	$(CXX) $(CXXFLAGS) -o $@ -c $<


# Clean up
clean:
	rm -f src/vwap/vwap src/vwap/vwap-r1 src/vwap/vwap-r1b src/vwap/*.o
	rm -f src/tpch/ssb/ssb src/tpch/ssb/ssb-r1 src/tpch/ssb/*.o
	rm -f src/tpch/query3/query3 src/tpch/query3/query3-r1 src/tpch/query3/*.o
	rm -f src/tpch/query5/query5 src/tpch/query5/query5-r1 src/tpch/query5/*.o
	rm -f src/tpch/query11/query11 src/tpch/query11/query11-r1 src/tpch/query11/query11-r1b src/tpch/query11/*.o
	rm -f src/tpch/query17/query17 src/tpch/query17/query17-r1 src/tpch/query17/*.o
	rm -f src/tpch/query18/query18 src/tpch/query18/query18-r1 src/tpch/query18/*.o
	rm -f src/tpch/query22/query22 src/tpch/query22/query22-r1 src/tpch/query22/*.o
	rm -f naive/vwap/vwap-n naive/vwap/*.o
	rm -f naive/tpch/ssb/ssb-n naive/tpch/ssb/*.o
	rm -f naive/tpch/query3/query3-n naive/tpch/query3/*.o
	rm -f naive/tpch/query5/query5-n naive/tpch/query5/*.o
	rm -f naive/tpch/query11/query11-n naive/tpch/query11/*.o
	rm -f naive/tpch/query17/query17-n naive/tpch/query17/*.o
	rm -f naive/tpch/query18/query18-n naive/tpch/query18/*.o
	rm -f naive/tpch/query22/query22-n naive/tpch/query22/*.o

expclean:
	rm -f src/vwap/engine.* src/vwap/log
	rm -f src/tpch/ssb/engine.* src/tpch/ssb/log
	rm -f src/tpch/query3/engine.* src/tpch/query3/log
	rm -f src/tpch/query5/engine.* src/tpch/query5/log
	rm -f src/tpch/query11/engine.* src/tpch/query11/log
	rm -f src/tpch/query17/engine.* src/tpch/query17/log
	rm -f src/tpch/query18/engine.* src/tpch/query18/log
	rm -f src/tpch/query22/engine.* src/tpch/query22/log
	rm -f naive/tpch/query3/engine.* naive/tpch/query3/log
	rm -f naive/tpch/query5/engine.* naive/tpch/query5/log
	rm -f naive/tpch/query11/engine.* naive/tpch/query11/log
	rm -f naive/tpch/query17/engine.* naive/tpch/query17/log
	rm -f naive/tpch/query18/engine.* naive/tpch/query18/log
	rm -f naive/tpch/query22/engine.* naive/tpch/query22/log
