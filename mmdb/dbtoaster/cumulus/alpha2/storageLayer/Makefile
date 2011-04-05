CC = g++
DEBUG = -g
CFLAGS = -std=c++0x -Wall -c
LFLAGS = -std=c++0x -Wall

INCLUDE = -I /usr/local/boost_1_46_1


OBJS = WriteStorage.o WriteLog.o


.PHONY: clean

all: test

test : $(OBJS)
	$(CC) $(LFLAGS) $(INCLUDE) $(OBJS) main.cpp -o test.exe

WriteLog.o : WriteStorage.o
	$(CC) $(CFLAGS) $(INCLUDE) WriteLog.cpp

WriteStorage.o : WriteStorage.hpp Generated.hpp Common.hpp
	$(CC) $(CFLAGS) $(INCLUDE) WriteStorage.cpp

clean :
	rm -rf *.o *.exe
