CC = g++
DEBUG = -g
CFLAGS = -std=c++0x -Wall -c
LFLAGS = -std=c++0x -Wall

INCLUDE = -I /usr/local/boost_1_46_1


OBJS = MultiKey.o WriteStorage.o WSCollection.o


.PHONY: clean

all: test MultiKeyTest

test : $(OBJS)
	$(CC) $(LFLAGS) $(INCLUDE) $(OBJS) main.cpp -o test.exe

MultiKeyTest : MultiKey.hpp
	$(CC) $(LFLAGS) $(INCLUDE) MultiKey.cpp testMultiKey.cpp -o multiKeyTest.exe

WSCollection.o : WriteStorage.o WSCollection.hpp
	$(CC) $(CFLAGS) $(INCLUDE) WSCollection.cpp

WriteStorage.o : WriteStorage.hpp MultiKey.o
	$(CC) $(CFLAGS) $(INCLUDE) WriteStorage.cpp

MultiKey.o : Common.hpp MultiKey.hpp
	$(CC) $(CFLAGS) $(INCLUDE) MultiKey.cpp

clean :
	rm -rf *.o *.exe
