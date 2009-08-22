
#test.make

BOOST_INCLUDE  = -I/home/anton/software/boost/include/boost-1_39
THRIFT_INCLUDE = -I/home/anton/software/thrift/include/thrift
DEBUGGER_INCLUDE = -I/home/anton/software/vwapConnection/vwap_debugger/gen-cpp
GUI_SERVER_INCLUDE = -I/home/anton/software/TradingClient/AlgoServer/gen-cpp
QUERY_INCLUDE  = -I/home/anton/tmp/dbtoaster
PROFILER = -I/home/anton/software/DBToaster/compiler/alpha2/profiler/protocol/gen-cpp

BOOST_LIB = -L/home/anton/software/boost/lib
THRIFT_LIB = -L/home/anton/software/thrift/lib

DEBUGGER_PATH = vwap_debugger/gen-cpp
GUI_SERV_PATH = AlgoServer/gen-cpp
QUERY_PATH = /home/anton/tmp/dbtoaster
PROFILER_PATH = /home/anton/software/DBToaster/compiler/alpha2/profiler/protocol/gen-cpp

BIDS_NAME = query19
ASKS_NAME = query20

ASKS=$(ASKS_NAME)/thrift/gen-cpp/
BIDS=$(BIDS_NAME)/thrift/gen-cpp/

INCLUDES = $(BOOST_INCLUDE) $(THRIFT_INCLUDE) $(DEBUGGER_INCLUDE) $(GUI_SERVER_INCLUDE) $(QUERY_INCLUDE) $(QUERY_INCLUDE)/$(ASKS) $(QUERY_INCLUDE)/$(BIDS) $(PROFILER)
LIBS = $(BOOST_LIB) -lboost_iostreams-gcc42-mt -lboost_serialization-gcc42-mt -lboost_thread-gcc42-mt -lboost_system-gcc42-mt $(THRIFT_LIB) -lthrift

test: GuiData.o GUICommunication_constants.o Debugger.o datasets_constants.o vwap_types.o vwap_constants.o datasets_types.o DataTuple.h WriterConnection.o OrderManager.o ReaderConnection.o ToasterConnection.o profiler_constants.o profiler_types.o bids_access.o asks_access.o $(BIDS_NAME)_constants.o $(ASKS_NAME)_constants.o $(ASKS_NAME)_types.o $(BIDS_NAME)_types.o profiler_operators.o Profiler.o test.cpp
	g++ -o test $(INCLUDES) Profiler.o profiler_constants.o profiler_operators.o profiler_types.o bids_access.o asks_access.o $(BIDS_NAME)_constants.o $(ASKS_NAME)_constants.o $(ASKS_NAME)_types.o $(BIDS_NAME)_types.o GuiData.o GUICommunication_types.o GUICommunication_constants.o Debugger.o datasets_constants.o vwap_types.o vwap_constants.o datasets_types.o ToasterConnection.o WriterConnection.o OrderManager.o ReaderConnection.o test.cpp $(LIBS)
	
profiler_operators.o: $(PROFILER_PATH)/profiler_operators.cpp
		g++ -c $(INCLUDES) $(PROFILER_PATH)/profiler_operators.cpp $(LIBS)
	
profiler_types.o: profiler_operators.o $(PROFILER_PATH)/profiler_types.h $(PROFILER_PATH)/profiler_types.cpp
	g++ -c $(INCLUDES) profiler_operators.o $(PROFILER_PATH)/profiler_types.cpp $(LIBS)
		
profiler_constants.o: profiler_types.o $(PROFILER_PATH)/profiler_constants.h $(PROFILER_PATH)/profiler_constants.cpp
	g++ -c $(INCLUDES) profiler_types.o $(PROFILER_PATH)/profiler_constants.cpp $(LIBS)
	
Profiler.o: profiler_types.o profiler_constants.o $(PROFILER_PATH)/Profiler.h $(PROFILER_PATH)/Profiler.cpp
	g++ -c $(INCLUDES) profiler_constants.o profiler_types.o $(PROFILER_PATH)/Profiler.cpp $(LIBS)
		
$(BIDS_NAME)_types.o: $(QUERY_PATH)/$(BIDS)$(BIDS_NAME)_types.h $(QUERY_PATH)/$(BIDS)$(BIDS_NAME)_types.cpp
	g++ -c $(INCLUDES) $(QUERY_PATH)/$(BIDS)$(BIDS_NAME)_types.cpp $(LIBS)

$(ASKS_NAME)_types.o: $(QUERY_PATH)/$(ASKS)$(ASKS_NAME)_types.h $(QUERY_PATH)/$(ASKS)$(ASKS_NAME)_types.cpp
	g++ -c $(INCLUDES) $(QUERY_PATH)/$(ASKS)$(ASKS_NAME)_types.cpp $(LIBS)
	
$(ASKS_NAME)_constants.o: $(ASKS_NAME)_types.o $(QUERY_PATH)/$(ASKS)$(ASKS_NAME)_constants.h $(QUERY_PATH)/$(ASKS)$(ASKS_NAME)_constants.cpp
	g++ -c $(INCLUDES) $(ASKS_NAME)_types.o $(QUERY_PATH)/$(ASKS)$(ASKS_NAME)_constants.cpp $(LIBS)

$(BIDS_NAME)_constants.o: $(BIDS_NAME)_types.o $(QUERY_PATH)/$(BIDS)$(BIDS_NAME)_constants.h $(QUERY_PATH)/$(BIDS)$(BIDS_NAME)_constants.cpp
	g++ -c $(INCLUDES) $(BIDS_NAME)_types.o $(QUERY_PATH)/$(BIDS)$(BIDS_NAME)_constants.cpp $(LIBS)

asks_access.o: $(ASKS_NAME)_constants.o $(ASKS_NAME)_types.o $(QUERY_PATH)/$(ASKS)AccessMethod.h $(QUERY_PATH)/$(ASKS)AccessMethod.cpp
	g++ -c -o asks_access.o $(INCLUDES) $(ASKS_NAME)_constants.o $(ASKS_NAME)_types.o $(QUERY_PATH)/$(ASKS)AccessMethod.cpp $(LIBS)

bids_access.o: $(BIDS_NAME)_types.o $(BIDS_NAME)_constants.o $(QUERY_PATH)/$(BIDS)AccessMethod.h $(QUERY_PATH)/$(BIDS)AccessMethod.cpp
	g++ -c -o bids_access.o $(INCLUDES) $(BIDS_NAME)_types.o $(QUERY_PATH)/$(BIDS)AccessMethod.cpp $(LIBS)

datasets_constants.o: $(DEBUGGER_PATH)/datasets_constants.h  $(DEBUGGER_PATH)/datasets_constants.cpp
	g++ -c  $(INCLUDES) $(DEBUGGER_PATH)/datasets_constants.cpp $(LIBS)
	
GUICommunication_constants.o: $(GUI_SERV_PATH)/GUICommunication_constants.h  $(GUI_SERV_PATH)/GUICommunication_constants.cpp
	g++ -c $(INCLUDES)   $(GUI_SERV_PATH)/GUICommunication_constants.cpp $(LIBS)
	
GUICommunication_types.o: $(GUI_SERV_PATH)/GUICommunication_types.h  $(GUI_SERV_PATH)/GUICommunication_types.cpp
	g++ -c $(INCLUDES)   $(GUI_SERV_PATH)/GUICommunication_types.cpp $(LIBS)	

GuiData.o: GUICommunication_constants.o GUICommunication_types.o $(GUI_SERV_PATH)/GuiData.h  $(GUI_SERV_PATH)/GuiData.cpp
	g++ -c $(INCLUDES)   GUICommunication_types.o GUICommunication_constants.o $(GUI_SERV_PATH)/GuiData.cpp $(LIBS)

datasets_types.o: $(DEBUGGER_PATH)/datasets_types.h  $(DEBUGGER_PATH)/datasets_types.cpp
	g++ -c $(INCLUDES)  $(DEBUGGER_PATH)/datasets_types.cpp $(LIBS)

vwap_constants.o: $(DEBUGGER_PATH)/vwap_constants.h  $(DEBUGGER_PATH)/vwap_constants.cpp
	g++ -c $(INCLUDES)  $(DEBUGGER_PATH)/vwap_constants.cpp $(LIBS)

vwap_types.o: $(DEBUGGER_PATH)/vwap_types.h  $(DEBUGGER_PATH)/vwap_types.cpp
	g++ -c $(INCLUDES)  $(DEBUGGER_PATH)/vwap_types.cpp $(LIBS)

Debugger.o: $(DEBUGGER_PATH)/Debugger.h  $(DEBUGGER_PATH)/Debugger.cpp
	g++ -c $(INCLUDES)  $(DEBUGGER_PATH)/Debugger.cpp $(LIBS)

ReaderConnection.o: DataTuple.h ReaderConnection.h ReaderConnection.cpp
	g++ -c $(INCLUDES) ReaderConnection.cpp $(LIBS)
	
WriterConnection.o: DataTuple.h WriterConnection.h WriterConnection.cpp
	g++ -c $(INCLUDES) WriterConnection.cpp $(LIBS)
	
ToasterConnection.o: asks_access.o bids_access.o Debugger.o datasets_constants.o vwap_types.o vwap_constants.o datasets_types.o ToasterConnection.h ToasterConnection.cpp
	g++ -c $(INCLUDES) asks_access.o bids_access.o Debugger.o datasets_constants.o vwap_types.o vwap_constants.o datasets_types.o ToasterConnection.cpp $(LIBS)
	
OrderManager.o: WriterConnection.o AlgoTypeDefs.h DataTuple.h OrderManager.h OrderManager.cpp
	g++ -c $(INCLUDES) WriterConnection.o OrderManager.cpp $(LIBS)
	
AlgorithmsEgine.o: AlgorithmsEngine.h AlgorithmsEnine.cpp
	g++ -c $(INCLUDES) AlgorithmsEngine.cpp $(LIBS)


clean:
	rm *.o