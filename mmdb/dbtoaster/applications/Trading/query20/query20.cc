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
#include "profiler.h"
#include "datasets/adaptors_a.h"
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>


using namespace DBToaster::Profiler;
// Thrift includes
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

using namespace boost;
using boost::shared_ptr;



// Viewer includes.
#include "AccessMethod.h"

const int32_t on_insert_asks_prof_id = 0;
const int32_t insert_asks_prof_3 = 1;
const int32_t insert_asks_prof_2 = 2;
const int32_t insert_asks_prof_1 = 3;
const int32_t insert_asks_prof_0 = 4;
const int32_t insert_asks_prof_4 = 5;
const int32_t on_delete_asks_prof_id = 6;
const int32_t delete_asks_prof_3 = 7;
const int32_t delete_asks_prof_2 = 8;
const int32_t delete_asks_prof_1 = 9;
const int32_t delete_asks_prof_0 = 10;
const int32_t delete_asks_prof_4 = 11;
int var0;
int var1;
int t_volume=0;
map<int,float> map0;
map<int,int> dom0;
multiset<tuple<int,int,int,int,int> > asks;
map<int,int> map1;
map<int,int> map2;
tuple<int,int> var15;

tuple< int,int > on_insert_asks(int t,int id,int broker_id,int p,int v) {
    
     cout<<"on insert "<<t<<" "<<id<<" "<<broker_id<<" "<<p<<" "<<v<<endl;
     p=p/10000;
     cout<<"on insert "<<t<<" "<<id<<" "<<broker_id<<" "<<p<<" "<<v<<endl;
     
     t_volume+=v;
     
    int bind0;
    { START_PROFILE("cpu", insert_asks_prof_3)
    {
        {
            int var2=std::numeric_limits<int>::min();;
            int var4=std::numeric_limits<int>::min();;
            
            map<int,int>::iterator dom0_it6 = dom0.begin();
            map<int,int>::iterator dom0_end6 = dom0.end();
            for(; dom0_it6 != dom0_end6; ++dom0_it6)
            {
                int p0 = dom0_it6->first;

                { START_PROFILE("cpu", insert_asks_prof_0)
                {
                    {
                        int var3=0;
                        if ( p<p0 ) {
                            var3 = (var3 + 1);
                        }
                        cout<<"b_map0_p0 "<<map0[p0]<<" p "<<p<<" p0 "<<p0<<endl;
                        map0[p0] = (map0[p0] + ((0.25 * v) - (v * var3)));
                        cout<<"map0_p0 "<<map0[p0]<<endl;
                    }

                    if ( map0[p0]>0 ) {
                        var2 = max(var2, p0);
                    }
                     cout<< "v2 "<<var2<<endl;

                }

                END_PROFILE("cpu", insert_asks_prof_0) }

            }
            if ( dom0.find(p) == dom0.end() ) { dom0[p] = 1; }
            else { ++dom0[p]; }
            { START_PROFILE("cpu", insert_asks_prof_1)
            {
                asks.insert(make_tuple(t, id, broker_id, p, v));
                if ( map0.find(p)==map0.end() ) {
                    {
                        {
                            int var5=0;
                            int var6=0;
                            
                            multiset<tuple<int,int,int,int,int> >::iterator asks_it7 = asks.begin();
                            multiset<tuple<int,int,int,int,int> >::iterator asks_end7 = asks.end();
                            for(; asks_it7 != asks_end7; ++asks_it7)
                            {
                                int t11 = get<0>(*asks_it7);
                                int id11 = get<1>(*asks_it7);
                                int broker_id11 = get<2>(*asks_it7);
                                int p11 = get<3>(*asks_it7);
                                int v11 = get<4>(*asks_it7);

                                var5 = (var5 + v11);
                                cout<<"v5 "<<var5<<endl;
                            }
                            
                            multiset<tuple<int,int,int,int,int> >::iterator asks_it8 = asks.begin();
                            multiset<tuple<int,int,int,int,int> >::iterator asks_end8 = asks.end();
                            for(; asks_it8 != asks_end8; ++asks_it8)
                            {
                                int t22 = get<0>(*asks_it8);
                                int id22 = get<1>(*asks_it8);
                                int broker_id22 = get<2>(*asks_it8);
                                int p22 = get<3>(*asks_it8);
                                int v22 = get<4>(*asks_it8);

                                if ( p22<p ) {
                                    var6 = (var6 + v22);
                                }
                                cout<<"p22 "<<p22<<" p "<<p<<" v6 "<<var6<<endl;

                            }
                            map0[p] = ((0.25 * var5) - var6);
                             cout<<"map0_p "<<map0[p]<<endl;
                        }

                        if ( map0[p]>0 ) {
                            var4 = max(var4, p);
                        }
                        cout << "v4 "<< var4<< endl;

                    }

                }

            }

            END_PROFILE("cpu", insert_asks_prof_1) }

            bind0 = max(var2, var4);
            if (bind0 == std::numeric_limits<int>::max())
            {
                cout<<"can't get the correct value for the var2 or var4"<<endl;
            }
            cout<< "bind0 "<<bind0<<endl;
        }

        { START_PROFILE("cpu", insert_asks_prof_2)
        {
            
            map<int,int>::iterator map1_it9 = map1.begin();
            map<int,int>::iterator map1_end9 = map1.end();
            
            if (map1.find(bind0)==map1.end())
            {
                cout<<"bind0 not found here "<<bind0<<endl;
            }
            else
            {
                cout<<"bind0 found "<<map1[bind0]<<endl;
            }
            
            for(; map1_it9 != map1_end9; ++map1_it9)
            {
                int var8 = map1_it9->first;

                {
                    int var7=0;
                    if ( p<=var8 ) {
                        var7 = (var7 + 1);
                    }
                    cout<<"var7 "<<var7<<" map1_var8 "<<map1[var8]<<" v8 "<<var8<<endl;
                    map1[var8] = (map1[var8] + ((p * v) * var7));
                    cout<<"map1_var8 "<<map1[var8]<<endl;
                }

            }
            if ( map1.find(bind0)==map1.end() ) {
                {
                    int var9=0;
                    
                    multiset<tuple<int,int,int,int,int> >::iterator asks_it10 = asks.begin();
                    multiset<tuple<int,int,int,int,int> >::iterator asks_end10 = asks.end();
                    for(; asks_it10 != asks_end10; ++asks_it10)
                    {
                        int t0 = get<0>(*asks_it10);
                        int id0 = get<1>(*asks_it10);
                        int broker_id0 = get<2>(*asks_it10);
                        int p0 = get<3>(*asks_it10);
                        int v0 = get<4>(*asks_it10);

                        if ( p0<=bind0 ) {
                            var9 = (var9 + (p0 * v0));
                        }
                        cout<<"v9 "<<var9<<endl;

                    }
                    map1[bind0] = var9;
                    cout<<"map1_bind0 "<<var9<<endl;
                }

            }

//            var0 = (var0 + map1[bind0]);
            cout<<"map1_bind0 "<<map1[bind0]<< " bind0 "<<bind0<<endl;
            var0=0;
            var0 = (var0 + map1[bind0]);
            cout<<"v0 "<<var0<<endl;
        }

        END_PROFILE("cpu", insert_asks_prof_2) }

    }

    END_PROFILE("cpu", insert_asks_prof_3) }

    { START_PROFILE("cpu", insert_asks_prof_4)
    {
        
        map<int,int>::iterator map2_it11 = map2.begin();
        map<int,int>::iterator map2_end11 = map2.end();
        for(; map2_it11 != map2_end11; ++map2_it11)
        {
            int var11 = map2_it11->first;

            {
                int var10=0;
                if ( p<=var11 ) {
                    var10 = (var10 + 1);
                }

                map2[var11] = (map2[var11] + (v * var10));
                cout<<"map2_var11 "<<map2[var11]<<endl;
            }

        }
        if ( map2.find(bind0)==map2.end() ) {
            {
                int var12=0;
                
                multiset<tuple<int,int,int,int,int> >::iterator asks_it12 = asks.begin();
                multiset<tuple<int,int,int,int,int> >::iterator asks_end12 = asks.end();
                for(; asks_it12 != asks_end12; ++asks_it12)
                {
                    int t0 = get<0>(*asks_it12);
                    int id0 = get<1>(*asks_it12);
                    int broker_id0 = get<2>(*asks_it12);
                    int p0 = get<3>(*asks_it12);
                    int v0 = get<4>(*asks_it12);

                    if ( p0<=bind0 ) {
                        var12 = (var12 + v0);
                    }
                    cout<<"v12 "<<var12<<endl;

                }
                map2[bind0] = var12;
                cout<<"map2_bind0 "<<map2[bind0]<<endl;;
            }

        }

//        var1 = (var1 + map2[bind0]);
        
         var1=0;
        var1 = (var1 + map2[bind0]);
        cout<< "v1 "<<var1<<" map2_bind0 "<<map2[bind0]<<" bind0 "<<bind0<<endl;
    }

    END_PROFILE("cpu", insert_asks_prof_4) }
    
    
        cout<<"map19 "<<map1[19]<<" t_v "<<t_volume<<endl;
    cout<<"v0 "<<var0<<" v1 "<<var1<<endl;
    if (var0<0)
    {
        exit(-1);
    }

    var15 = make_tuple(var0,var1);;
    return var15;
}

tuple< int,int > on_delete_asks(int t,int id,int broker_id,int p,int v) {
    
     cout<<"on delete "<<t<<" "<<id<<" "<<broker_id<<" "<<p<<" "<<v<<endl;
     p=p/10000;
     cout<<"on delete "<<t<<" "<<id<<" "<<broker_id<<" "<<p<<" "<<v<<endl;
    
    multiset<tuple<int,int,int,int,int> >::iterator check_asks_it = asks.find(make_tuple(t, id, broker_id, p, v));
    if (check_asks_it != asks.end()){
        
        t_volume-=v;
    
    int bind0=0;
    { START_PROFILE("cpu", delete_asks_prof_3)
    {
        {
            int var16=std::numeric_limits<int>::min();;
            int var19=std::numeric_limits<int>::min();;
            
            if ( dom0.find(p) != dom0.end() ) {
                --dom0[p];
                if ( dom0[p] == 0 ) { dom0.erase(p); }
            }
            { START_PROFILE("cpu", delete_asks_prof_0)
            {
                multiset<tuple<int,int,int,int,int> >::iterator asks_it13 = asks.find(make_tuple(t, id, broker_id, p, v));
                
                asks.erase(asks_it13);
                if ( dom0.find(p)==dom0.end() ) {
                    map0.erase(p);
                }

            }

            END_PROFILE("cpu", delete_asks_prof_0) }

            
            map<int,int>::iterator dom0_it14 = dom0.begin();
            map<int,int>::iterator dom0_end14 = dom0.end();
            for(; dom0_it14 != dom0_end14; ++dom0_it14)
            {
                int p0 = dom0_it14->first;

                { START_PROFILE("cpu", delete_asks_prof_1)
                {
                    {
                        int var20=0;
                        if ( p<p0 ) {
                            var20 = (var20 + 1);
                        }

                        map0[p0] = (map0[p0] - ((0.25 * v) - (v * var20)));
                    }

                    if ( map0[p0]>0 ) {
                        var19 = max(var19, p0);
                    }

                }

                END_PROFILE("cpu", delete_asks_prof_1) }

            }
            bind0 = max(var16, var19);
        }

        { START_PROFILE("cpu", delete_asks_prof_2)
        {
            
            map<int,int>::iterator map1_it15 = map1.begin();
            map<int,int>::iterator map1_end15 = map1.end();
            map<int,int>::iterator resume_map1_it16 = map1_it15;
            
            if ( resume_map1_it16 != map1_end15 ) 
            { ++resume_map1_it16; }
            
            map<int,int>::key_type resume_map1_val17;
            
            if (resume_map1_it16 != map1_end15 ) 
            { resume_map1_val17 = resume_map1_it16->first; }

            for(; map1_it15 != map1_end15; ++map1_it15)
            {
                int var22 = map1_it15->first;

                ++resume_map1_it16;
                bool resume_end = ( resume_map1_it16 == map1_end15 );
                if ( !resume_end ) 
                {
                   resume_map1_val17 = resume_map1_it16->first;
                }

                {
                    {
                        int var21=0;
                        if ( p<=bind0 ) {
                            var21 = (var21 + 1);
                        }

                        map1[var22] = (map1[var22] - ((p * v) * var21));
                         cout<<"delete_map1_var22 "<<map1[var22]<<endl;
                    }

                    if ( (var22==bind0) and (map1[var22]==0) ) {
                        {
                            map1.erase(var22);
                            if ( !resume_end ) {
                                resume_map1_it16 = map1.find(resume_map1_val17);
                            }
                            else { resume_map1_it16 = map1_end15; }
                            continue;
                        }

                    }

                }

            }
 //           var0 = (var0 - map1[bind0]);
            var0 = map1[bind0];
        }

        END_PROFILE("cpu", delete_asks_prof_2) }

    }

    END_PROFILE("cpu", delete_asks_prof_3) }

    { START_PROFILE("cpu", delete_asks_prof_4)
    {
        
        map<int,int>::iterator map2_it18 = map2.begin();
        map<int,int>::iterator map2_end18 = map2.end();
        map<int,int>::iterator resume_map2_it19 = map2_it18;
        if ( resume_map2_it19 != map2_end18 ) { ++resume_map2_it19; }
        map<int,int>::key_type resume_map2_val20;
        if (resume_map2_it19 != map2_end18 ) { resume_map2_val20 = resume_map2_it19->first; }

        for(; map2_it18 != map2_end18; ++map2_it18)
        {
            int var24 = map2_it18->first;

            ++resume_map2_it19;
            bool resume_end = ( resume_map2_it19 == map2_end18 );
            if ( !resume_end ) {
               resume_map2_val20 = resume_map2_it19->first;
            }

            {
                {
                    int var23=0;
                    if ( p<=bind0 ) {
                        var23 = (var23 + 1);
                    }

                    map2[var24] = (map2[var24] - (v * var23));
                }

                if ( (var24==bind0) and (map2[var24]==0) ) {
                    {
                        map2.erase(var24);
                        if ( !resume_end ) {
                            resume_map2_it19 = map2.find(resume_map2_val20);
                        }
                        else { resume_map2_it19 = map2_end18; }
                        continue;
                    }

                }

            }

        }
//        var1 = (var1 - map2[bind0]);
        var1 = map2[bind0];
    }

    END_PROFILE("cpu", delete_asks_prof_4) }
}
else{
    cout<<"delete_not_found "<<t<<" "<<id<<" "<<broker_id<<" "<<v<<" "<<p<<endl;
}
     cout<<"map19 "<<map1[19]<<" t_v "<<t_volume<<endl;
    cout<<"delete_ v0 "<<var0<<" v1 "<<var1<<endl;
    
    if (var0<0)
    {
        exit(-1);
    }

    return make_tuple(var0,var1);
}

boost::asio::io_service io_service;

struct dispatch_AsksOrderbook1_tuple
{
    DBToaster::DemoDatasets::OrderbookTupleAdaptor adaptor;

    boost::function<tuple< int,int > (int t,int id,int broker_id,int p,int v)> on_insert_asks_ptr;
    boost::function<tuple< int,int > (int t,int id,int broker_id,int p,int v)> on_delete_asks_ptr;


    dispatch_AsksOrderbook1_tuple()
    {
        on_insert_asks_ptr = boost::bind(&on_insert_asks,_1,_2,_3,_4,_5);
        on_delete_asks_ptr = boost::bind(&on_delete_asks,_1,_2,_3,_4,_5);
    }

    void operator()(boost::any& inTuple)
    {
        DBToaster::StandaloneEngine::DBToasterTuple tuple;
        adaptor(tuple, inTuple);
        if ( tuple.type == DBToaster::StandaloneEngine::insertTuple )
        {
            DBToaster::DemoDatasets::OrderbookTupleAdaptor::Result input = 
                boost::any_cast<DBToaster::DemoDatasets::OrderbookTupleAdaptor::Result>(tuple.data);
            cout<<"insert_dispatcher: input.t"<<input.t<<" input.id "<<input.id<<" r.broker_id "<<input.broker_id<<" input.p "<<input.price<<" input.v "<<input.volume<<endl;
            on_insert_asks(input.t,input.id,input.broker_id,input.price,input.volume);
        }
        else if ( tuple.type == DBToaster::StandaloneEngine::deleteTuple )
        {
            DBToaster::DemoDatasets::OrderbookTupleAdaptor::Result input = 
                boost::any_cast<DBToaster::DemoDatasets::OrderbookTupleAdaptor::Result>(tuple.data);
            cout<<"delete_dispatcher: input.t"<<input.t<<" input.id "<<input.id<<" r.broker_id "<<input.broker_id<<" input.p "<<input.price<<" input.v "<<input.volume<<endl;
            on_delete_asks(input.t,input.id,input.broker_id,input.price,input.volume);
        }
        else { cerr << "Invalid DML type!" << endl; }
    }
};

dispatch_AsksOrderbook1_tuple AsksOrderbook1_dispatcher;
DBToaster::DemoDatasets::OrderbookSocketStream AsksOrderbook1(io_service,AsksOrderbook1_dispatcher);

void init(DBToaster::StandaloneEngine::SocketMultiplexer& sources)
{
    sources.addStream(static_cast<DBToaster::StandaloneEngine::SocketStream*>(&AsksOrderbook1));
}

using namespace DBToaster::Viewer::query20;

class AccessMethodHandler : virtual public AccessMethodIf
{
    public:
    AccessMethodHandler()
    {
        PROFILER_INITIALIZATION
    }
        int32_t get_var0()
        {
            int32_t r = static_cast<int32_t>(var0);
            return r;
        }

        int32_t get_var1()
        {
            int32_t r = static_cast<int32_t>(var1);
            return r;
        }

        inline void insert_thrift_map0(map<int32_t,double>& dest, pair<const int,float>& src)
        {
            dest.insert(dest.begin(), src);
        }

        void get_map0(map<int32_t,double>& _return)
        {
            map<int,float>::iterator map0_it21 = map0.begin();
            map<int,float>::iterator map0_end21 = map0.end();
            for_each(map0_it21, map0_end21,
                boost::bind(&AccessMethodHandler::insert_thrift_map0, this, _return, _1));
        }

        inline void insert_thrift_dom0(map<int32_t,int32_t>& dest, pair<const int,int>& src)
        {
            dest.insert(dest.begin(), src);
        }

        void get_dom0(map<int32_t,int32_t>& _return)
        {
            map<int,int>::iterator dom0_it22 = dom0.begin();
            map<int,int>::iterator dom0_end22 = dom0.end();
            for_each(dom0_it22, dom0_end22,
                boost::bind(&AccessMethodHandler::insert_thrift_dom0, this, _return, _1));
        }

        inline void insert_thrift_asks(vector<asks_elem>& dest, const tuple<int,int,int,int,int>& src)
        {
            asks_elem r;
            r.t11 = get<0>(src);
            r.id11 = get<1>(src);
            r.broker_id11 = get<2>(src);
            r.p11 = get<3>(src);
            r.v11 = get<4>(src);
            dest.insert(dest.begin(), r);
        }

        void get_asks(vector<asks_elem>& _return)
        {
            multiset<tuple<int,int,int,int,int> >::iterator asks_it23 = asks.begin();
            multiset<tuple<int,int,int,int,int> >::iterator asks_end23 = asks.end();
            for_each(asks_it23, asks_end23,
                boost::bind(&AccessMethodHandler::insert_thrift_asks, this, _return, _1));
        }

        inline void insert_thrift_map1(map<int32_t,int32_t>& dest, pair<const int,int>& src)
        {
            dest.insert(dest.begin(), src);
        }

        void get_map1(map<int32_t,int32_t>& _return)
        {
            map<int,int>::iterator map1_it24 = map1.begin();
            map<int,int>::iterator map1_end24 = map1.end();
            for_each(map1_it24, map1_end24,
                boost::bind(&AccessMethodHandler::insert_thrift_map1, this, _return, _1));
        }

        inline void insert_thrift_map2(map<int32_t,int32_t>& dest, pair<const int,int>& src)
        {
            dest.insert(dest.begin(), src);
        }

        void get_map2(map<int32_t,int32_t>& _return)
        {
            map<int,int>::iterator map2_it25 = map2.begin();
            map<int,int>::iterator map2_end25 = map2.end();
            for_each(map2_it25, map2_end25,
                boost::bind(&AccessMethodHandler::insert_thrift_map2, this, _return, _1));
        }

        void get_var15(var15_tuple& _return)
        {
            _return.var0 = var0;
            _return.var1 = var1;
        }

        PROFILER_SERVICE_METHOD_IMPLEMENTATION
};

DBToaster::StandaloneEngine::SocketMultiplexer sources;
void runReadLoop()
{
    sources.read(boost::bind(&runReadLoop));
}


int main(int argc, char** argv)
{
    init(sources);
    runReadLoop();
    boost::thread t(boost::bind(
        &boost::asio::io_service::run, &io_service));
    int port = 20000;
    boost::shared_ptr<AccessMethodHandler> handler(new AccessMethodHandler());
    boost::shared_ptr<TProcessor> processor(new AccessMethodProcessor(handler));
    boost::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    boost::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    boost::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
}