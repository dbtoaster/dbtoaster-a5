#test.make

test: testingConnection.o
	g++ -o test -I/Users/antonmorozov/software/boost/include/boost-1_39 testingConnection.o -L/Users/antonmorozov/software/boost/lib -lboost_iostreams-xgcc40-mt -lboost_serialization-xgcc40-mt -lboost_thread-xgcc40-mt -lboost_system-xgcc40-mt

testingConnection.o: datasets.h dataclient.h testingConnection.cpp
	g++ -c -I/Users/antonmorozov/software/boost/include/boost-1_39 testingConnection.cpp -L/Users/antonmorozov/software/boost/lib -lboost_iostreams-xgcc40-mt -lboost_serialization-xgcc40-mt -lboost_thread-xgcc40-mt -lboost_system-xgcc40-mt


clean:
	rm *.o