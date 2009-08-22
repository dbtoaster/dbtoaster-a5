
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


#include <transport/TSocket.h>
#include <protocol/TBinaryProtocol.h>

#include "/home/anton/tmp/dbtoaster/query19/thrift/gen-cpp/AccessMethod.h"
#include "/home/anton/tmp/dbtoaster/query20/thrift/gen-cpp/AccessMethod.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
//using namespace apache::thrift::server;

using boost::shared_ptr;
//using namespace DBToaster::Debugger;
using namespace DBToaster::Viewer::query19;
using namespace DBToaster::Viewer::query20;

#include "AlgoTypeDefs.h"


#define host "127.0.0.1"
#define bid_port 5500
#define ask_port 5510

namespace DBToaster
{
    namespace DemoAlgEngine
    {
        class ToasterConnection
        {
        public:
            
            ToasterConnection():
             bid_toasterSocket(new TSocket(host, bid_port)),
             bid_socketProtocol(new TBinaryProtocol(bid_toasterSocket)),
             bid_toasterClient(new AccessMethodClient(bid_socketProtocol)),
             ask_toasterSocket(new TSocket(host, ask_port)),
             ask_socketProtocol(new TBinaryProtocol(ask_toasterSocket)),
             ask_toasterClient(new AccessMethodClient(ask_socketProtocol))
            {
                bid_toasterSocket->open();
                ask_toasterSocket->open();
            }
            
            int getBidsPV()
            {
                return bid_toasterClient->get_var0();
            }
            int getBidsV()
            {
                return bid_toasterClient->get_var1();
            }
            int getAsksPV()
            {
                return ask_toasterClient->get_var0();
            }
            int getAsksV()
            {
                return ask_toasterClient->get_var1();
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
            
            boost::shared_ptr<TSocket>           bid_toasterSocket;
            boost::shared_ptr<TBinaryProtocol>   bid_socketProtocol;
            boost::shared_ptr<query19::AccessMethodClient>    bid_toasterClient;
            
            boost::shared_ptr<TSocket>           ask_toasterSocket;
            boost::shared_ptr<TBinaryProtocol>   ask_socketProtocol;
            boost::shared_ptr<query20::AccessMethodClient>    ask_toasterClient;
            
        };
    }
}


#endif

