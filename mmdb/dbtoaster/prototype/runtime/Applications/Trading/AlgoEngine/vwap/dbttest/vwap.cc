// DBToaster includes.
#include <cmath>
#include <cstdio>
#include <cstdlib>

#include <iostream>
#include <map>
#include <list>
#include <set>

#include <tr1/tuple>
#include <tr1/unordered_set>

using namespace std;
using namespace tr1;

// Stream engine includes.
#include "streamengine.h"
#include "datasets/adaptors.h"
#include "boost/bind.hpp"
#include <boost/asio.hpp>
#include "dataclient.h"

// Thrift includes
#include "Debugger.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

using boost::shared_ptr;

int var2;
map<int,int> map0;
set<int> dom0;
multiset<tuple<int,int> > B;
map<int,int> map1;
set<int> dom1;
map<int,int> map2;
map<int,int> map3;

int on_insert_B(int P,int V) {
    int var0;
    {
        int var3;
        int var5;
        
        set<int>::iterator dom0_it6 = dom0.begin();
        set<int>::iterator dom0_end6 = dom0.end();
        for(; dom0_it6 != dom0_end6; ++dom0_it6)
        {
            int P0 = *dom0_it6;

            {
                {
                    int var4;
                    if ( P>P0 ) {
                        var4 = var4 + 1;
                    }

                    map0[P0] = map0[P0] + 0.25 * V + V * var4;
                }

                if ( map0[P0]<0 ) {
                    var3 = min(var3, P0);
                }

            }

        }
        dom0.insert(P);
        B.insert(make_tuple(P, V));
        if ( map0.find(P)==map0.end() ) {
            {
                {
                    int var6;
                    int var7;
                    
                    multiset<tuple<int,int> >::iterator B_it7 = B.begin();
                    multiset<tuple<int,int> >::iterator B_end7 = B.end();
                    for(; B_it7 != B_end7; ++B_it7)
                    {
                        int P1 = get<0>(*B_it7);
                        int V1 = get<1>(*B_it7);

                        var6 = var6 + V1;
                    }
                    
                    multiset<tuple<int,int> >::iterator B_it8 = B.begin();
                    multiset<tuple<int,int> >::iterator B_end8 = B.end();
                    for(; B_it8 != B_end8; ++B_it8)
                    {
                        int P12 = get<0>(*B_it8);
                        int V12 = get<1>(*B_it8);

                        if ( P12>P ) {
                            var7 = var7 + V12;
                        }

                    }
                    map0[P] = 0.25 * var6 + var7;
                }

                if ( map0[P]<0 ) {
                    var5 = min(var5, P);
                }

            }

        }

        var0 = min(var3, var5);
    }

    
    map<int,int>::iterator map1_it9 = map1.begin();
    map<int,int>::iterator map1_end9 = map1.end();
    for(; map1_it9 != map1_end9; ++map1_it9)
    {
        int var9 = map1_it9->first;

        {
            int var8;
            if ( P>=var9 ) {
                var8 = var8 + 1;
            }

            map1[var9] = map1[var9] + P * V * var8;
        }

    }
    if ( map1.find(var0)==map1.end() ) {
        {
            int var10;
            
            multiset<tuple<int,int> >::iterator B_it10 = B.begin();
            multiset<tuple<int,int> >::iterator B_end10 = B.end();
            for(; B_it10 != B_end10; ++B_it10)
            {
                int P0 = get<0>(*B_it10);
                int V0 = get<1>(*B_it10);

                if ( P0>=var0 ) {
                    var10 = var10 + P0 * V0;
                }

            }
            map1[var0] = var10;
        }

    }

    return map1[var0];
}

int on_delete_B(int P,int V) {
    int var1;
    {
        int var11;
        int var13;
        set<int>::iterator dom1_it11 = dom1.find(P);
        dom1.erase(dom1_it11);
        {
            int var12;
            if ( P>P ) {
                var12 = var12 + 1;
            }

            map2[P] = map2[P] - 0.25 * V + V * var12;
        }

        if ( map2[P]<0 ) {
            var11 = min(var11, P);
        }

        var1 = min(P, var11);
        if ( P>=var1 ) {
            var13 = var13 + 1;
        }

        map3[var1] = map3[var1] - P * V * var13;
    }

    
    map<int,int>::iterator map3_it12 = map3.begin();
    map<int,int>::iterator map3_end12 = map3.end();
    for(; map3_it12 != map3_end12; ++map3_it12)
    {
        int var14 = map3_it12->first;

        {
            int var13;
            if ( P>=var1 ) {
                var13 = var13 + 1;
            }

            map3[var14] = map3[var14] - P * V * var13;
        }

    }
    return map3[var1];
}

DBToaster::DemoDatasets::VwapFileStream VwapBids("20081201.csv",1000);

shared_ptr<DBToaster::DemoDatasets::VwapTupleAdaptor> VwapBids_adaptor(new DBToaster::DemoDatasets::VwapTupleAdaptor());
static int streamVwapBidsId = 0;

struct on_insert_B_fun_obj { 
    int operator()(boost::any data) { 
        DBToaster::DemoDatasets::VwapTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::VwapTupleAdaptor::Result>(data); 
        on_insert_B(input.price,input.volume);
    }
};

on_insert_B_fun_obj fo_on_insert_B_0;

struct on_delete_B_fun_obj { 
    int operator()(boost::any data) { 
        DBToaster::DemoDatasets::VwapTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::VwapTupleAdaptor::Result>(data); 
        on_delete_B(input.price,input.volume);
    }
};

on_delete_B_fun_obj fo_on_delete_B_1;


struct dispatcher_001{
    VwapTupleAdaptor adaptor;
    
    int operator()(VwapTuple& data) {
        DBToasterTuple tuple = adaptor(rTuple, nextTuple);
        if (tuple.type == insertTuple)
        {
            DBToaster::DemoDatasets::VwapTupleAdaptor::Result input = 
                boost::any_cast<DBToaster::DemoDatasets::VwapTupleAdaptor::Result>(tuple.data); 
            on_insert_B(input.price,input.volume);
        }
        else
        {
             DBToaster::DemoDatasets::VwapTupleAdaptor::Result input = 
                    boost::any_cast<DBToaster::DemoDatasets::VwapTupleAdaptor::Result>(tuple.data); 
                on_delete_B(input.price,input.volume);
        }
        
    }
};


void init(DBToaster::StandaloneEngine::Multiplexer& sources,
    DBToaster::StandaloneEngine::Dispatcher& router)
{
    sources.addStream<DBToaster::DemoDatasets::VwapTuple>(&VwapBids, *VwapBids_adaptor, streamVwapBidsId);
    router.addHandler(streamVwapBidsId,DBToaster::StandaloneEngine::insertTuple,fo_on_insert_B_0);
    router.addHandler(streamVwapBidsId,DBToaster::StandaloneEngine::deleteTuple,fo_on_delete_B_1);
}

using namespace DBToaster::Debugger;

class DebuggerHandler : virtual public DebuggerIf
{
//    DBToaster::StandaloneEngine::Multiplexer& sources;
    
    boost::asio::io_service& localIOService;
    VwapDataClient my_client;
    DBToaster::StandaloneEngine::Dispatcher& router;

    public:
    DebuggerHandler(
            boost::asio::io_service& io_service,
            DBToaster::StandaloneEngine::Dispatcher& r)
        : localIOService(io_service), my_client(io_service), router(r)
    {}

    void step_VwapBids(const ThriftVwapTuple& input)
    {
        DBToaster::StandaloneEngine::DBToasterTuple dbtInput;
        dbtInput.id = input.id;
        dbtInput.type = static_cast<DBToaster::StandaloneEngine::DmlType>(input.type);
        dbtInput.data = boost::any(input.data);
        router.dispatch(dbtInput);
    }
    

    void stepn_VwapBids(const int32_t n)
    {
        for (int32_t i = 0; i < n; ++i)
        {
            if ( !sources.streamHasInputs() ) break;
            DBToaster::StandaloneEngine::DBToasterTuple t = sources.nextInput();
            router.dispatch(t);
        }
    }
    

    int32_t get_var2()
    {
        int32_t r = static_cast<int32_t>(var2);
        return r;
    }

    inline void insert_thrift_map0(map<int32_t,int32_t>& dest, pair<const int,int>& src)
    {
        dest.insert(dest.begin(), src);
    }

    void get_map0(map<int32_t,int32_t>& _return)
    {
        map<int,int>::iterator map0_it13 = map0.begin();
        map<int,int>::iterator map0_end13 = map0.end();
        for_each(map0_it13, map0_end13,
            boost::bind(&DebuggerHandler::insert_thrift_map0, this, _return, _1));
    }

    inline void insert_thrift_dom0(set<int32_t>& dest, const int& src)
    {
        dest.insert(dest.begin(), src);
    }

    void get_dom0(set<int32_t>& _return)
    {
        set<int>::iterator dom0_it14 = dom0.begin();
        set<int>::iterator dom0_end14 = dom0.end();
        for_each(dom0_it14, dom0_end14,
            boost::bind(&DebuggerHandler::insert_thrift_dom0, this, _return, _1));
    }

    inline void insert_thrift_B(vector<B_elem>& dest, const tuple<int,int>& src)
    {
        B_elem r;
        r.P1 = get<0>(src);
        r.V1 = get<1>(src);
        dest.insert(dest.begin(), r);
    }

    void get_B(vector<B_elem>& _return)
    {
        multiset<tuple<int,int> >::iterator B_it15 = B.begin();
        multiset<tuple<int,int> >::iterator B_end15 = B.end();
        for_each(B_it15, B_end15,
            boost::bind(&DebuggerHandler::insert_thrift_B, this, _return, _1));
    }

    inline void insert_thrift_map1(map<int32_t,int32_t>& dest, pair<const int,int>& src)
    {
        dest.insert(dest.begin(), src);
    }

    void get_map1(map<int32_t,int32_t>& _return)
    {
        map<int,int>::iterator map1_it16 = map1.begin();
        map<int,int>::iterator map1_end16 = map1.end();
        for_each(map1_it16, map1_end16,
            boost::bind(&DebuggerHandler::insert_thrift_map1, this, _return, _1));
    }

    inline void insert_thrift_dom1(set<int32_t>& dest, const int& src)
    {
        dest.insert(dest.begin(), src);
    }

    void get_dom1(set<int32_t>& _return)
    {
        set<int>::iterator dom1_it17 = dom1.begin();
        set<int>::iterator dom1_end17 = dom1.end();
        for_each(dom1_it17, dom1_end17,
            boost::bind(&DebuggerHandler::insert_thrift_dom1, this, _return, _1));
    }

    inline void insert_thrift_map2(map<int32_t,int32_t>& dest, pair<const int,int>& src)
    {
        dest.insert(dest.begin(), src);
    }

    void get_map2(map<int32_t,int32_t>& _return)
    {
        map<int,int>::iterator map2_it18 = map2.begin();
        map<int,int>::iterator map2_end18 = map2.end();
        for_each(map2_it18, map2_end18,
            boost::bind(&DebuggerHandler::insert_thrift_map2, this, _return, _1));
    }

    inline void insert_thrift_map3(map<int32_t,int32_t>& dest, pair<const int,int>& src)
    {
        dest.insert(dest.begin(), src);
    }

    void get_map3(map<int32_t,int32_t>& _return)
    {
        map<int,int>::iterator map3_it19 = map3.begin();
        map<int,int>::iterator map3_end19 = map3.end();
        for_each(map3_it19, map3_end19,
            boost::bind(&DebuggerHandler::insert_thrift_map3, this, _return, _1));
    }

};

int main(int argc, char **argv) 
{
    DBToaster::StandaloneEngine::Multiplexer sources(12345, 20);
    DBToaster::StandaloneEngine::Dispatcher router;
    init(sources, router);
    
    boost::asio::io_service io_service;
//    VwapDataClient my_client(io_service);

    int port = (70457>>3);
    shared_ptr<DebuggerHandler> handler(new DebuggerHandler(io_service, router));
    shared_ptr<TProcessor> processor(new DebuggerProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
}