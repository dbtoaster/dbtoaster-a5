

#test.make

test: n_vwap.o Debugger.o datasets_constants.o vwap_types.o vwap_constants.o datasets_types.o
	g++ -o test -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp datasets_types.o vwap_constants.o vwap_types.o datasets_constants.o Debugger.o n_vwap.o -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift
	
#thrist_test: simple_test.o Debugger.o datasets_constants.o vwap_types.o vwap_constants.o datasets_types.o
#	g++ -o thrift_test -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift #-I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp datasets_types.o vwap_constants.o vwap_types.o datasets_constants.o Debugger.o simple_test.o #-L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt #-L/home/anton/software/thrift/lib -lthrift
	
datasets_constants.o: vwap_debugger/gen-cpp/datasets_constants.h  vwap_debugger/gen-cpp/datasets_constants.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp  vwap_debugger/gen-cpp/datasets_constants.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift
	
datasets_types.o: vwap_debugger/gen-cpp/datasets_types.h  vwap_debugger/gen-cpp/datasets_types.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp  vwap_debugger/gen-cpp/datasets_types.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift
	
vwap_constants.o: vwap_debugger/gen-cpp/vwap_constants.h  vwap_debugger/gen-cpp/vwap_constants.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp  vwap_debugger/gen-cpp/vwap_constants.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift
		
vwap_types.o: vwap_debugger/gen-cpp/vwap_types.h  vwap_debugger/gen-cpp/vwap_types.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp  vwap_debugger/gen-cpp/vwap_types.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift
	
Debugger.o: vwap_debugger/gen-cpp/Debugger.h  vwap_debugger/gen-cpp/Debugger.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp  vwap_debugger/gen-cpp/Debugger.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift

n_vwap.o: datasets/streamengine.h datasets/adaptors.h datasets/dataclient.h n_vwap.cc
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp  n_vwap.cc -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift
	
simple_test.o: Debugger.o simple_test.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp  Debugger.o simple_test.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift


clean:
	rm *.o