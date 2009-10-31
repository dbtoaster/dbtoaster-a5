CXX=g++
CXXFLAGS=-DENABLE_SIMPLE_PROFILER -I../include -I/Users/yanif/software/boost/include
LDFLAGS=-L/Users/yanif/software/boost/lib -lboost_system -lboost_thread -lboost_program_options

all: vwap ssb query11 query18 query22

VWAP_OBJS = src/vwap/vwap.o

vwap: $(VWAP_OBJS)
	$(CXX) -o src/vwap/vwap $(CXXFLAGS) $(VWAP_OBJS) $(LDFLAGS)

SSB_OBJS = src/tpch/ssb/ssb.o

ssb: $(SSB_OBJS)
	$(CXX) -o src/tpch/ssb/ssb $(CXXFLAGS) $(SSB_OBJS) $(LDFLAGS)

QUERY11_OBJS = src/tpch/query11/query11.o

query11: $(QUERY11_OBJS)
	$(CXX) -o src/tpch/query11/query11 $(CXXFLAGS) $(QUERY11_OBJS) $(LDFLAGS)

QUERY18_OBJS = src/tpch/query18/query18.o

query18: $(QUERY18_OBJS)
	$(CXX) -o src/tpch/query18/query18 $(CXXFLAGS) $(QUERY18_OBJS) $(LDFLAGS)

QUERY22_OBJS = src/tpch/query22/query22.o

query22: $(QUERY22_OBJS)
	$(CXX) -o src/tpch/query22/query22 $(CXXFLAGS) $(QUERY22_OBJS) $(LDFLAGS)

# Common rules
.SUFFIXES: .cc .o

.cc.o:
	$(CXX) $(CXXFLAGS) -o $@ -c $<


# Clean up
clean:
	rm -f src/vwap/vwap src/vwap/*.o
	rm -f src/tpch/ssb/ssb src/tpch/ssb/*.o
	rm -f src/tpch/query11/query11 src/tpch/query11/*.o
	rm -f src/tpch/query18/query18 src/tpch/query18/*.o
	rm -f src/tpch/query22/query22 src/tpch/query22/*.o
