
#test.make

test: GuiData.o GUICommunication_constants.o Debugger.o datasets_constants.o vwap_types.o vwap_constants.o datasets_types.o DataTuple.h WriterConnection.o OrderManager.o ReaderConnection.o ToasterConnection.o test.cpp
	g++ -o test -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp -I/home/anton/software/TradingClient/AlgoServer/gen-cpp GuiData.o GUICommunication_types.o GUICommunication_constants.o Debugger.o datasets_constants.o vwap_types.o vwap_constants.o datasets_types.o ToasterConnection.o WriterConnection.o OrderManager.o ReaderConnection.o test.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift

datasets_constants.o: vwap_debugger/gen-cpp/datasets_constants.h  vwap_debugger/gen-cpp/datasets_constants.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp  vwap_debugger/gen-cpp/datasets_constants.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift
	
GUICommunication_constants.o: AlgoServer/gen-cpp/GUICommunication_constants.h  AlgoServer/gen-cpp/GUICommunication_constants.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/TradingClient/AlgoServer/gen-cpp   AlgoServer/gen-cpp/GUICommunication_constants.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift
	
GUICommunication_types.o: AlgoServer/gen-cpp/GUICommunication_types.h  AlgoServer/gen-cpp/GUICommunication_types.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/TradingClient/AlgoServer/gen-cpp   AlgoServer/gen-cpp/GUICommunication_types.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift	

GuiData.o: GUICommunication_constants.o GUICommunication_types.o AlgoServer/gen-cpp/GuiData.h  AlgoServer/gen-cpp/GuiData.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/TradingClient/AlgoServer/gen-cpp   GUICommunication_types.o GUICommunication_constants.o AlgoServer/gen-cpp/GuiData.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift

datasets_types.o: vwap_debugger/gen-cpp/datasets_types.h  vwap_debugger/gen-cpp/datasets_types.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp  vwap_debugger/gen-cpp/datasets_types.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift

vwap_constants.o: vwap_debugger/gen-cpp/vwap_constants.h  vwap_debugger/gen-cpp/vwap_constants.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp  vwap_debugger/gen-cpp/vwap_constants.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift

vwap_types.o: vwap_debugger/gen-cpp/vwap_types.h  vwap_debugger/gen-cpp/vwap_types.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp  vwap_debugger/gen-cpp/vwap_types.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift

Debugger.o: vwap_debugger/gen-cpp/Debugger.h  vwap_debugger/gen-cpp/Debugger.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp  vwap_debugger/gen-cpp/Debugger.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift

ReaderConnection.o: DataTuple.h ReaderConnection.h ReaderConnection.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp ReaderConnection.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift

	
WriterConnection.o: DataTuple.h WriterConnection.h WriterConnection.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp WriterConnection.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift
	
ToasterConnection.o: Debugger.o datasets_constants.o vwap_types.o vwap_constants.o datasets_types.o ToasterConnection.h ToasterConnection.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp Debugger.o datasets_constants.o vwap_types.o vwap_constants.o datasets_types.o ToasterConnection.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift
	
OrderManager.o: WriterConnection.o AlgoTypeDefs.h DataTuple.h OrderManager.h OrderManager.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp WriterConnection.o OrderManager.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift
	
AlgorithmsEgine.o: AlgorithmsEngine.h AlgorithmsEnine.cpp
	g++ -c -I/home/anton/software/boost/include/boost-1_39 -I/home/anton/software/thrift/include/thrift -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp AlgorithmsEngine.cpp -L/home/anton/software/boost/lib -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt -L/home/anton/software/thrift/lib -lthrift


clean:
	rm *.o