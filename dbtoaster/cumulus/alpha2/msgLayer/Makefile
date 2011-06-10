# Run with the following command (dynamic libraries are necessary):
# env LD_LIBRARY_PATH="/usr/local/boost_1_46_1/stage/lib:$LD_LIBRARY_PATH" ./EXECUTABLE

# All boost library are compiled (some of them failed).
# When deciding which libs to include, only take a look at #include directives of particular example

CC = g++

DEBUG = -g 
# BOOST_ASIO_ENABLE_BUFFER_DEBUGGING

CFLAGS = -std=c++0x -Wall -c
LFLAGS = -std=c++0x -Wall

# not working for now
# -static
# g++ option forces all libraries to statically compile into exe

INCLUDE = -I /usr/local/boost_1_46_1
LIBPATH = -L /usr/local/boost_1_46_1/stage/lib
LIBS = -lboost_system -lboost_date_time -lboost_serialization

.PHONY: clean


all : msg_server msg_client
	
msg_client: connection.hpp msgs.hpp
	$(CC) $(LFLAGS) $(INCLUDE) $(LIBPATH) $(LIBS) msg_client.cpp -o msg_client.exe

msg_server: connection.hpp msgs.hpp
	$(CC) $(LFLAGS) $(INCLUDE) $(LIBPATH) $(LIBS) msg_server.cpp -o msg_server.exe


clean :
	rm -rf *.o *.exe
