CXX=g++
CXXFLAGS=-DENABLE_SIMPLE_PROFILER -I../include -I/Users/yanif/software/boost/include
LDFLAGS=-L/Users/yanif/software/boost/lib -lboost_system -lboost_thread -lboost_program_options

all: vwap

VWAP_OBJS = src/vwap/vwap.o

vwap: $(VWAP_OBJS)
	$(CXX) -o src/vwap/vwap $(CXXFLAGS) $(VWAP_OBJS) $(LDFLAGS)

# Common rules
.SUFFIXES: .cc .o

.cc.o:
	$(CXX) $(CXXFLAGS) -o $@ -c $<


# Clean up
clean:
	rm -f src/vwap/vwap src/vwap/*.o
