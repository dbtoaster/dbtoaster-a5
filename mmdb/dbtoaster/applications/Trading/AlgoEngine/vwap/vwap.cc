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


void init(DBToaster::StandaloneEngine::Multiplexer& sources,
    DBToaster::StandaloneEngine::Dispatcher& router)
{
    sources.addStream<DBToaster::DemoDatasets::VwapTuple>(&VwapBids, *VwapBids_adaptor, streamVwapBidsId);
    router.addHandler(streamVwapBidsId,DBToaster::StandaloneEngine::insertTuple,fo_on_insert_B_0);
    router.addHandler(streamVwapBidsId,DBToaster::StandaloneEngine::deleteTuple,fo_on_delete_B_1);
}

int main(int argc, char** argv)
{
{
    DBToaster::StandaloneEngine::Multiplexer sources(12345, 20);
    DBToaster::StandaloneEngine::Dispatcher router;
    init(sources, router);
    while ( sources.streamHasInputs() ) {
        DBToaster::StandaloneEngine::DBToasterTuple t = sources.nextInput();
        router.dispatch(t);
    }
}