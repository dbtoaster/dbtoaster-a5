
#ifndef TOASTER_CONNECTION_DEMO
#define TOASTER_CONNECTION_DEMO

#include <cmath>
#include <cstdio>
#include <cstdlib>

#include <iostream>
#include <string>

using namespace std;

#include <tr1/tuple>
#include <tr1/unordered_set>

using namespace tr1;

#include <boost/any.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
using boost::shared_ptr;

#include <transport/TSocket.h>
#include <protocol/TBinaryProtocol.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

#include "/home/anton/tmp/dbtoaster/query19/thrift/gen-cpp/AccessMethod.h"
using namespace DBToaster::Viewer::query19;

#include "/home/anton/tmp/dbtoaster/query20/thrift/gen-cpp/AccessMethod.h"
using namespace DBToaster::Viewer::query20;

//#include "/home/anton/tmp/dbtoaster/query19/thrift/gen-cpp/AccessMethod.h"
//#include "/home/anton/tmp/dbtoaster/query20/thrift/gen-cpp/AccessMethod.h"

//using namespace apache::thrift;
//using namespace apache::thrift::protocol;
//using namespace apache::thrift::transport;
//using namespace apache::thrift::server;


//using namespace DBToaster::Debugger;

#include "AlgoTypeDefs.h"

#define host "127.0.0.1"
#define bid_port 8807
#define ask_port 20000


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
             bid_toasterClient(new DBToaster::Viewer::query19::AccessMethodClient(bid_socketProtocol)),
             ask_toasterSocket(new TSocket(host, ask_port)),
             ask_socketProtocol(new TBinaryProtocol(ask_toasterSocket)),
             ask_toasterClient(new DBToaster::Viewer::query20::AccessMethodClient(ask_socketProtocol))
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
 /*           
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
 */           
        private:
            
            boost::shared_ptr<TSocket>           bid_toasterSocket;
            boost::shared_ptr<TBinaryProtocol>   bid_socketProtocol;
            boost::shared_ptr<DBToaster::Viewer::query19::AccessMethodClient>    bid_toasterClient;
            
            boost::shared_ptr<TSocket>           ask_toasterSocket;
            boost::shared_ptr<TBinaryProtocol>   ask_socketProtocol;
            boost::shared_ptr<DBToaster::Viewer::query20::AccessMethodClient>    ask_toasterClient;
            
        };
    }
}


#endif

