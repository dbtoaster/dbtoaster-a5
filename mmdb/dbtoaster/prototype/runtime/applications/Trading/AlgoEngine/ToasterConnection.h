
#ifndef TOASTER_CONNECTION_DEMO
#define TOASTER_CONNECTION_DEMO

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

#include "AlgoTypeDefs.h"


#define host "127.0.0.1"
#define port 5500

namespace DBToaster
{
    namespace DemoAlgEngine
    {
        class ToasterConnection
        {
        public:
            
            ToasterConnection():
             toasterSocket(new TSocket(host, port)),
             socketProtocol(new TBinaryProtocol(toasterSocket)),
             toasterClient(new DebuggerClient(socketProtocol))
            {
                toasterSocket->open();
            }
            
            int getSummary()
            {
                return toasterClient->get_var2();
            }
            
            list<PVpair> getAskList()
            {
                PVpair t;
                get<0>(t)=toasterClient->get_var2();
                get<1>(t)=toasterClient->get_var2();
                
                list<PVpair> l;
                l.push_back(t);   
                
                return l;                
            }
            
            list<PVpair> getBidList()
            {
                PVpair t;
                get<0>(t)=toasterClient->get_var2();
                get<1>(t)=toasterClient->get_var2();
                
                list<PVpair> l;
                l.push_back(t);   
                
                return l;                
            }
            
        private:
            
            boost::shared_ptr<TSocket>           toasterSocket;
            boost::shared_ptr<TBinaryProtocol>   socketProtocol;
            boost::shared_ptr<DebuggerClient>    toasterClient;
            
        };
    }
}


#endif

