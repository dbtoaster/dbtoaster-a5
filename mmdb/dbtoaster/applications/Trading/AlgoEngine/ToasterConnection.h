
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

#include "/Users/antonmorozov/tmp/query19/thrift/gen-cpp/AccessMethod.h"
using namespace DBToaster::Viewer::query19;

#include "/Users/antonmorozov/tmp/query20/thrift/gen-cpp/AccessMethod.h"
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
#define bid_port 20001
#define ask_port 20000

/*
 * This is class connects to DBToaste Runtime
 * 
 * Needs to be re implemented as soon as DBToaster Runtime has a thrift connector
 *
 * Currently collects data from two hand added querries written by DBToaster
 */

namespace DBToaster
{
    namespace DemoAlgEngine
    {
        class ToasterConnection
        {
        public:
            
            //Constructor creates client connections with two queries
            ToasterConnection():
             bid_toasterSocket(new TSocket(host, bid_port)),
             bid_socketProtocol(new TBinaryProtocol(bid_toasterSocket)),
             bid_toasterClient(new DBToaster::Viewer::query19::AccessMethodClient(bid_socketProtocol)),
             ask_toasterSocket(new TSocket(host, ask_port)),
             ask_socketProtocol(new TBinaryProtocol(ask_toasterSocket)),
             ask_toasterClient(new DBToaster::Viewer::query20::AccessMethodClient(ask_socketProtocol))
            {
                //opens connections
                bid_toasterSocket->open();
                ask_toasterSocket->open();
            }
            
            //gets bids sum(price*volume)
            int getBidsPV()
            {
                return bid_toasterClient->get_var0();
            }
            //gets bids sum(volume)
            int getBidsV()
            {
                return bid_toasterClient->get_var1();
            }
            //gets asks sum(price*volume)
            int getAsksPV()
            {
                return ask_toasterClient->get_var0();
            }
            //gets asks sum(volume)
            int getAsksV()
            {
                return ask_toasterClient->get_var1();
            }

            /*
            //functions needed by the Market Maker algorithm
            
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
            
            //client connections to the bids and ask SOBI stats
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

