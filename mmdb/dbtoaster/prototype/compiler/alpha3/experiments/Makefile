CXX=g++44
CXXFLAGS=-g -DENABLE_SIMPLE_PROFILER -I/home/yanif/workspace/dbtc_alpha3/include -I/home/yanif/software/boost/include
LDFLAGS=-L/home/yanif/software/boost/lib -lboost_system -lboost_thread -lboost_program_options

all: vwap ssb query11 query18 query22 vwapr1 vwapr1b ssbr1 query11r1 query18r1 query22r1

# VWAP

VWAP_OBJS = src/vwap/vwap.o
VWAP_R1_OBJS = src/vwap/vwap-r1.o
VWAP_R1B_OBJS = src/vwap/vwap-r1b.o

vwap: $(VWAP_OBJS)
	$(CXX) -o src/vwap/vwap $(CXXFLAGS) $(VWAP_OBJS) $(LDFLAGS)

vwapr1: $(VWAP_R1_OBJS)
	$(CXX) -o src/vwap/vwap-r1 $(CXXFLAGS) $(VWAP_R1_OBJS) $(LDFLAGS)

vwapr1b: $(VWAP_R1B_OBJS)
	$(CXX) -o src/vwap/vwap-r1b $(CXXFLAGS) $(VWAP_R1B_OBJS) $(LDFLAGS)

# SSB queries

SSB_OBJS = src/tpch/ssb/ssb.o
SSB_R1_OBJS = src/tpch/ssb/ssb-r1.o

ssb: $(SSB_OBJS)
	$(CXX) -o src/tpch/ssb/ssb $(CXXFLAGS) $(SSB_OBJS) $(LDFLAGS)

ssbr1: $(SSB_R1_OBJS)
	$(CXX) -o src/tpch/ssb/ssb-r1 $(CXXFLAGS) $(SSB_R1_OBJS) $(LDFLAGS)


# TPCH queries

QUERY11_OBJS = src/tpch/query11/query11.o
QUERY11_R1_OBJS = src/tpch/query11/query11-r1.o
QUERY18_OBJS = src/tpch/query18/query18.o
QUERY18_R1_OBJS = src/tpch/query18/query18-r1.o
QUERY22_OBJS = src/tpch/query22/query22.o
QUERY22_R1_OBJS = src/tpch/query22/query22-r1.o


query11: $(QUERY11_OBJS)
	$(CXX) -o src/tpch/query11/query11 $(CXXFLAGS) $(QUERY11_OBJS) $(LDFLAGS)

query11r1: $(QUERY11_R1_OBJS)
	$(CXX) -o src/tpch/query11/query11-r1 $(CXXFLAGS) $(QUERY11_R1_OBJS) $(LDFLAGS)

query18: $(QUERY18_OBJS)
	$(CXX) -o src/tpch/query18/query18 $(CXXFLAGS) $(QUERY18_OBJS) $(LDFLAGS)

query18r1: $(QUERY18_R1_OBJS)
	$(CXX) -o src/tpch/query18/query18-r1 $(CXXFLAGS) $(QUERY18_R1_OBJS) $(LDFLAGS)

query22: $(QUERY22_OBJS)
	$(CXX) -o src/tpch/query22/query22 $(CXXFLAGS) $(QUERY22_OBJS) $(LDFLAGS)

query22r1: $(QUERY22_R1_OBJS)
	$(CXX) -o src/tpch/query22/query22-r1 $(CXXFLAGS) $(QUERY22_R1_OBJS) $(LDFLAGS)

# Common rules
.SUFFIXES: .cc .o

.cc.o:
	$(CXX) $(CXXFLAGS) -o $@ -c $<


# Clean up
clean:
	rm -f src/vwap/vwap src/vwap/vwap-r1 src/vwap/vwap-r1b src/vwap/*.o
	rm -f src/tpch/ssb/ssb src/tpch/ssb/ssb-r1 src/tpch/ssb/*.o
	rm -f src/tpch/query11/query11 src/tpch/query11/query11-r1 src/tpch/query11/*.o
	rm -f src/tpch/query18/query18 src/tpch/query18/query18-r1 src/tpch/query18/*.o
	rm -f src/tpch/query22/query22 src/tpch/query22/query22-r1 src/tpch/query22/*.o

expclean:
	rm -f src/vwap/engine.* src/vwap/log
	rm -f src/tpch/ssb/engine.* src/tpch/ssb/log
	rm -f src/tpch/query11/engine.* src/tpch/query11/log
	rm -f src/tpch/query18/engine.* src/tpch/query18/log
	rm -f src/tpch/query22/engine.* src/tpch/query22/log
