
#include <cmath>
#include <cstdio>
#include <cstdlib>

#include <iostream>
#include <string>


#include <tr1/tuple>
#include <tr1/unordered_set>

using namespace std;
using namespace tr1;

#include <boost/any.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>

#include "Debugger.h"
#include <transport/TSocket.h>
#include <protocol/TBinaryProtocol.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
//using namespace apache::thrift::server;

using boost::shared_ptr;
using namespace DBToaster::Debugger;

//#define host "127.0.0.1"
//#define port 8807

int main(int argc, char **argv) 
{
    string host;
    host="127.0.0.1";
    int port;
    port=5500;
    
    boost::shared_ptr<TSocket>           toasterSocket(new TSocket(host, port));
    
    toasterSocket->open();
    boost::shared_ptr<TBinaryProtocol>   socketProtocol(new TBinaryProtocol(toasterSocket));
    boost::shared_ptr<DebuggerClient>    toasterClient(new DebuggerClient(socketProtocol));
    
    
    
    int out=toasterClient->get_var2();
    
    cout<<"got :"<<out<<", from Toaster"<<endl;
    
    return 0;
}