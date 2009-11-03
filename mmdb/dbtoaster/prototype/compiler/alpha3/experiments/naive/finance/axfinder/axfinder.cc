// DBToaster includes.
#include <cmath>
#include <cstdio>
#include <cstdlib>

#include <iostream>
#include <map>
#include <list>
#include <set>

#include <tr1/cstdint>
#include <tr1/tuple>
#include <tr1/unordered_set>

using namespace std;
using namespace tr1;



// Stream engine includes.
#include "standalone/streamengine.h"
#include "profiler/profiler.h"
#include "datasets/adaptors.h"
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/pool/pool.hpp>
#include <boost/pool/pool_alloc.hpp>


using namespace DBToaster::Profiler;
multiset<tuple<double,int,int,double,double> > BIDS;
multiset<tuple<double,int,int,double,double> > ASKS;
map<int,double> q;

double on_insert_ASKS_sec_span = 0.0;
double on_insert_ASKS_usec_span = 0.0;
double on_insert_BIDS_sec_span = 0.0;
double on_insert_BIDS_usec_span = 0.0;
double on_delete_ASKS_sec_span = 0.0;
double on_delete_ASKS_usec_span = 0.0;
double on_delete_BIDS_sec_span = 0.0;
double on_delete_BIDS_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
   cout << "BIDS size: " << (((sizeof(multiset<tuple<double,int,int,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * BIDS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<double,int,int,double,double> >::key_type, multiset<tuple<double,int,int,double,double> >::value_type, _Identity<multiset<tuple<double,int,int,double,double> >::value_type>, multiset<tuple<double,int,int,double,double> >::key_compare>))) << endl;

   (*stats) << "m," << "BIDS" << "," << (((sizeof(multiset<tuple<double,int,int,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * BIDS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<double,int,int,double,double> >::key_type, multiset<tuple<double,int,int,double,double> >::value_type, _Identity<multiset<tuple<double,int,int,double,double> >::value_type>, multiset<tuple<double,int,int,double,double> >::key_compare>))) << endl;

   cout << "ASKS size: " << (((sizeof(multiset<tuple<double,int,int,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * ASKS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<double,int,int,double,double> >::key_type, multiset<tuple<double,int,int,double,double> >::value_type, _Identity<multiset<tuple<double,int,int,double,double> >::value_type>, multiset<tuple<double,int,int,double,double> >::key_compare>))) << endl;

   (*stats) << "m," << "ASKS" << "," << (((sizeof(multiset<tuple<double,int,int,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * ASKS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<double,int,int,double,double> >::key_type, multiset<tuple<double,int,int,double,double> >::value_type, _Identity<multiset<tuple<double,int,int,double,double> >::value_type>, multiset<tuple<double,int,int,double,double> >::key_compare>))) << endl;

   cout << "q size: " << (((sizeof(map<int,double>::key_type)
       + sizeof(map<int,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int,double>::key_type, map<int,double>::value_type, _Select1st<map<int,double>::value_type>, map<int,double>::key_compare>))) << endl;

   (*stats) << "m," << "q" << "," << (((sizeof(map<int,double>::key_type)
       + sizeof(map<int,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int,double>::key_type, map<int,double>::value_type, _Select1st<map<int,double>::value_type>, map<int,double>::key_compare>))) << endl;

}

void analyse_handler_usage(ofstream* stats)
{
   cout << "on_insert_ASKS cost: " << (on_insert_ASKS_sec_span + (on_insert_ASKS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_ASKS" << "," << (on_insert_ASKS_sec_span + (on_insert_ASKS_usec_span / 1000000.0)) << endl;
   cout << "on_insert_BIDS cost: " << (on_insert_BIDS_sec_span + (on_insert_BIDS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_BIDS" << "," << (on_insert_BIDS_sec_span + (on_insert_BIDS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_ASKS cost: " << (on_delete_ASKS_sec_span + (on_delete_ASKS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_ASKS" << "," << (on_delete_ASKS_sec_span + (on_delete_ASKS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_BIDS cost: " << (on_delete_BIDS_sec_span + (on_delete_BIDS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_BIDS" << "," << (on_delete_BIDS_sec_span + (on_delete_BIDS_usec_span / 1000000.0)) << endl;
}


void on_insert_ASKS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);

    map<int,double>::iterator q_it18 = q.begin();
    map<int,double>::iterator q_end17 = q.end();
    for (; q_it18 != q_end17; ++q_it18)
    {
        int B__BROKER_ID = q_it18->first;
        q[B__BROKER_ID] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it4 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end3 = 
            BIDS.end();
        for (; BIDS_it4 != BIDS_end3; ++BIDS_it4)
        {
            double protect_B__T = get<0>(*BIDS_it4);
            int protect_B__ID = get<1>(*BIDS_it4);
            int protect_B__BROKER_ID = get<2>(*BIDS_it4);
            double protect_B__P = get<3>(*BIDS_it4);
            double protect_B__V = get<4>(*BIDS_it4);
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_it2 = 
                ASKS.begin();
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_end1 = ASKS.end();
            for (; ASKS_it2 != ASKS_end1; ++ASKS_it2)
            {
                double protect_A__T = get<0>(*ASKS_it2);
                int protect_A__ID = get<1>(*ASKS_it2);
                int protect_A__BROKER_ID = get<2>(*ASKS_it2);
                double protect_A__P = get<3>(*ASKS_it2);
                double protect_A__V = get<4>(*ASKS_it2);
                int var6 = 0;
                var6 += B__BROKER_ID;
                int var7 = 0;
                var7 += protect_B__BROKER_ID;
                if ( var6 == var7 )
                {
                    int var3 = 0;
                    var3 += 1000;
                    double var4 = 0;
                    var4 += protect_A__P;
                    double var5 = 1;
                    var5 *= -1;
                    var5 *= protect_B__P;
                    var4 += var5;
                    if ( var3 < var4 )
                    {
                        int var1 = 0;
                        var1 += protect_B__BROKER_ID;
                        int var2 = 0;
                        var2 += protect_A__BROKER_ID;
                        if ( var1 == var2 )
                        {
                            q[B__BROKER_ID] += protect_A__V;
                        }
                    }
                }
            }
        }
        double var8 = 1;
        var8 *= -1;
        double var9 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it8 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end7 = 
            ASKS.end();
        for (; ASKS_it8 != ASKS_end7; ++ASKS_it8)
        {
            double protect_A__T = get<0>(*ASKS_it8);
            int protect_A__ID = get<1>(*ASKS_it8);
            int protect_A__BROKER_ID = get<2>(*ASKS_it8);
            double protect_A__P = get<3>(*ASKS_it8);
            double protect_A__V = get<4>(*ASKS_it8);
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_it6 = 
                BIDS.begin();
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_end5 = BIDS.end();
            for (; BIDS_it6 != BIDS_end5; ++BIDS_it6)
            {
                double protect_B__T = get<0>(*BIDS_it6);
                int protect_B__ID = get<1>(*BIDS_it6);
                int protect_B__BROKER_ID = get<2>(*BIDS_it6);
                double protect_B__P = get<3>(*BIDS_it6);
                double protect_B__V = get<4>(*BIDS_it6);
                int var14 = 0;
                var14 += 1000;
                double var15 = 0;
                var15 += protect_A__P;
                double var16 = 1;
                var16 *= -1;
                var16 *= protect_B__P;
                var15 += var16;
                if ( var14 < var15 )
                {
                    int var12 = 0;
                    var12 += protect_B__BROKER_ID;
                    int var13 = 0;
                    var13 += protect_A__BROKER_ID;
                    if ( var12 == var13 )
                    {
                        int var10 = 0;
                        var10 += B__BROKER_ID;
                        int var11 = 0;
                        var11 += protect_B__BROKER_ID;
                        if ( var10 == var11 )
                        {
                            var9 += protect_B__V;
                        }
                    }
                }
            }
        }
        var8 *= var9;
        q[B__BROKER_ID] += var8;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it12 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end11 = 
            BIDS.end();
        for (; BIDS_it12 != BIDS_end11; ++BIDS_it12)
        {
            double protect_B__T = get<0>(*BIDS_it12);
            int protect_B__ID = get<1>(*BIDS_it12);
            int protect_B__BROKER_ID = get<2>(*BIDS_it12);
            double protect_B__P = get<3>(*BIDS_it12);
            double protect_B__V = get<4>(*BIDS_it12);
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it10 = ASKS.begin();
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_end9 = ASKS.end();
            for (; ASKS_it10 != ASKS_end9; ++ASKS_it10)
            {
                double protect_A__T = get<0>(*ASKS_it10);
                int protect_A__ID = get<1>(*ASKS_it10);
                int protect_A__BROKER_ID = get<2>(*ASKS_it10);
                double protect_A__P = get<3>(*ASKS_it10);
                double protect_A__V = get<4>(*ASKS_it10);
                int var22 = 0;
                var22 += B__BROKER_ID;
                int var23 = 0;
                var23 += protect_B__BROKER_ID;
                if ( var22 == var23 )
                {
                    int var19 = 0;
                    var19 += 1000;
                    double var20 = 0;
                    var20 += protect_B__P;
                    double var21 = 1;
                    var21 *= -1;
                    var21 *= protect_A__P;
                    var20 += var21;
                    if ( var19 < var20 )
                    {
                        int var17 = 0;
                        var17 += protect_B__BROKER_ID;
                        int var18 = 0;
                        var18 += protect_A__BROKER_ID;
                        if ( var17 == var18 )
                        {
                            q[B__BROKER_ID] += protect_A__V;
                        }
                    }
                }
            }
        }
        double var24 = 1;
        var24 *= -1;
        double var25 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it16 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end15 = 
            ASKS.end();
        for (; ASKS_it16 != ASKS_end15; ++ASKS_it16)
        {
            double protect_A__T = get<0>(*ASKS_it16);
            int protect_A__ID = get<1>(*ASKS_it16);
            int protect_A__BROKER_ID = get<2>(*ASKS_it16);
            double protect_A__P = get<3>(*ASKS_it16);
            double protect_A__V = get<4>(*ASKS_it16);
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it14 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end13 
                = BIDS.end();
            for (; BIDS_it14 != BIDS_end13; ++BIDS_it14)
            {
                double protect_B__T = get<0>(*BIDS_it14);
                int protect_B__ID = get<1>(*BIDS_it14);
                int protect_B__BROKER_ID = get<2>(*BIDS_it14);
                double protect_B__P = get<3>(*BIDS_it14);
                double protect_B__V = get<4>(*BIDS_it14);
                int var30 = 0;
                var30 += 1000;
                double var31 = 0;
                var31 += protect_B__P;
                double var32 = 1;
                var32 *= -1;
                var32 *= protect_A__P;
                var31 += var32;
                if ( var30 < var31 )
                {
                    int var28 = 0;
                    var28 += protect_B__BROKER_ID;
                    int var29 = 0;
                    var29 += protect_A__BROKER_ID;
                    if ( var28 == var29 )
                    {
                        int var26 = 0;
                        var26 += B__BROKER_ID;
                        int var27 = 0;
                        var27 += protect_B__BROKER_ID;
                        if ( var26 == var27 )
                        {
                            var25 += protect_B__V;
                        }
                    }
                }
            }
        }
        var24 *= var25;
        q[B__BROKER_ID] += var24;
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_ASKS_sec_span, on_insert_ASKS_usec_span);
}

void on_insert_BIDS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    BIDS.insert(make_tuple(T,ID,BROKER_ID,P,V));
    if ( q.find(BROKER_ID) == q.end() )
    {
        q[BROKER_ID] += 0;
    }
    map<int,double>::iterator q_it36 = q.begin();
    map<int,double>::iterator q_end35 = q.end();
    for (; q_it36 != q_end35; ++q_it36)
    {
        int BROKER_ID = q_it36->first;
        q[BROKER_ID] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it22 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end21 = 
            BIDS.end();
        for (; BIDS_it22 != BIDS_end21; ++BIDS_it22)
        {
            double protect_B__T = get<0>(*BIDS_it22);
            int protect_B__ID = get<1>(*BIDS_it22);
            int protect_B__BROKER_ID = get<2>(*BIDS_it22);
            double protect_B__P = get<3>(*BIDS_it22);
            double protect_B__V = get<4>(*BIDS_it22);
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it20 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end19 
                = ASKS.end();
            for (; ASKS_it20 != ASKS_end19; ++ASKS_it20)
            {
                double protect_A__T = get<0>(*ASKS_it20);
                int protect_A__ID = get<1>(*ASKS_it20);
                int protect_A__BROKER_ID = get<2>(*ASKS_it20);
                double protect_A__P = get<3>(*ASKS_it20);
                double protect_A__V = get<4>(*ASKS_it20);
                int var38 = 0;
                var38 += BROKER_ID;
                int var39 = 0;
                var39 += protect_B__BROKER_ID;
                if ( var38 == var39 )
                {
                    int var35 = 0;
                    var35 += 1000;
                    double var36 = 0;
                    var36 += protect_A__P;
                    double var37 = 1;
                    var37 *= -1;
                    var37 *= protect_B__P;
                    var36 += var37;
                    if ( var35 < var36 )
                    {
                        int var33 = 0;
                        var33 += protect_B__BROKER_ID;
                        int var34 = 0;
                        var34 += protect_A__BROKER_ID;
                        if ( var33 == var34 )
                        {
                            q[BROKER_ID] += protect_A__V;
                        }
                    }
                }
            }
        }
        double var40 = 1;
        var40 *= -1;
        double var41 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it26 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end25 = 
            ASKS.end();
        for (; ASKS_it26 != ASKS_end25; ++ASKS_it26)
        {
            double protect_A__T = get<0>(*ASKS_it26);
            int protect_A__ID = get<1>(*ASKS_it26);
            int protect_A__BROKER_ID = get<2>(*ASKS_it26);
            double protect_A__P = get<3>(*ASKS_it26);
            double protect_A__V = get<4>(*ASKS_it26);
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it24 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end23 
                = BIDS.end();
            for (; BIDS_it24 != BIDS_end23; ++BIDS_it24)
            {
                double protect_B__T = get<0>(*BIDS_it24);
                int protect_B__ID = get<1>(*BIDS_it24);
                int protect_B__BROKER_ID = get<2>(*BIDS_it24);
                double protect_B__P = get<3>(*BIDS_it24);
                double protect_B__V = get<4>(*BIDS_it24);
                int var46 = 0;
                var46 += 1000;
                double var47 = 0;
                var47 += protect_A__P;
                double var48 = 1;
                var48 *= -1;
                var48 *= protect_B__P;
                var47 += var48;
                if ( var46 < var47 )
                {
                    int var44 = 0;
                    var44 += protect_B__BROKER_ID;
                    int var45 = 0;
                    var45 += protect_A__BROKER_ID;
                    if ( var44 == var45 )
                    {
                        int var42 = 0;
                        var42 += BROKER_ID;
                        int var43 = 0;
                        var43 += protect_B__BROKER_ID;
                        if ( var42 == var43 )
                        {
                            var41 += protect_B__V;
                        }
                    }
                }
            }
        }
        var40 *= var41;
        q[BROKER_ID] += var40;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it30 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end29 = 
            BIDS.end();
        for (; BIDS_it30 != BIDS_end29; ++BIDS_it30)
        {
            double protect_B__T = get<0>(*BIDS_it30);
            int protect_B__ID = get<1>(*BIDS_it30);
            int protect_B__BROKER_ID = get<2>(*BIDS_it30);
            double protect_B__P = get<3>(*BIDS_it30);
            double protect_B__V = get<4>(*BIDS_it30);
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it28 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end27 
                = ASKS.end();
            for (; ASKS_it28 != ASKS_end27; ++ASKS_it28)
            {
                double protect_A__T = get<0>(*ASKS_it28);
                int protect_A__ID = get<1>(*ASKS_it28);
                int protect_A__BROKER_ID = get<2>(*ASKS_it28);
                double protect_A__P = get<3>(*ASKS_it28);
                double protect_A__V = get<4>(*ASKS_it28);
                int var54 = 0;
                var54 += BROKER_ID;
                int var55 = 0;
                var55 += protect_B__BROKER_ID;
                if ( var54 == var55 )
                {
                    int var51 = 0;
                    var51 += 1000;
                    double var52 = 0;
                    var52 += protect_B__P;
                    double var53 = 1;
                    var53 *= -1;
                    var53 *= protect_A__P;
                    var52 += var53;
                    if ( var51 < var52 )
                    {
                        int var49 = 0;
                        var49 += protect_B__BROKER_ID;
                        int var50 = 0;
                        var50 += protect_A__BROKER_ID;
                        if ( var49 == var50 )
                        {
                            q[BROKER_ID] += protect_A__V;
                        }
                    }
                }
            }
        }
        double var56 = 1;
        var56 *= -1;
        double var57 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it34 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end33 = 
            ASKS.end();
        for (; ASKS_it34 != ASKS_end33; ++ASKS_it34)
        {
            double protect_A__T = get<0>(*ASKS_it34);
            int protect_A__ID = get<1>(*ASKS_it34);
            int protect_A__BROKER_ID = get<2>(*ASKS_it34);
            double protect_A__P = get<3>(*ASKS_it34);
            double protect_A__V = get<4>(*ASKS_it34);
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it32 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end31 
                = BIDS.end();
            for (; BIDS_it32 != BIDS_end31; ++BIDS_it32)
            {
                double protect_B__T = get<0>(*BIDS_it32);
                int protect_B__ID = get<1>(*BIDS_it32);
                int protect_B__BROKER_ID = get<2>(*BIDS_it32);
                double protect_B__P = get<3>(*BIDS_it32);
                double protect_B__V = get<4>(*BIDS_it32);
                int var62 = 0;
                var62 += 1000;
                double var63 = 0;
                var63 += protect_B__P;
                double var64 = 1;
                var64 *= -1;
                var64 *= protect_A__P;
                var63 += var64;
                if ( var62 < var63 )
                {
                    int var60 = 0;
                    var60 += protect_B__BROKER_ID;
                    int var61 = 0;
                    var61 += protect_A__BROKER_ID;
                    if ( var60 == var61 )
                    {
                        int var58 = 0;
                        var58 += BROKER_ID;
                        int var59 = 0;
                        var59 += protect_B__BROKER_ID;
                        if ( var58 == var59 )
                        {
                            var57 += protect_B__V;
                        }
                    }
                }
            }
        }
        var56 *= var57;
        q[BROKER_ID] += var56;
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_BIDS_sec_span, on_insert_BIDS_usec_span);
}

void on_delete_ASKS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    ASKS.erase(make_tuple(T,ID,BROKER_ID,P,V));

    map<int,double>::iterator q_it54 = q.begin();
    map<int,double>::iterator q_end53 = q.end();
    for (; q_it54 != q_end53; ++q_it54)
    {
        int B__BROKER_ID = q_it54->first;
        q[B__BROKER_ID] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it40 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end39 = 
            BIDS.end();
        for (; BIDS_it40 != BIDS_end39; ++BIDS_it40)
        {
            double protect_B__T = get<0>(*BIDS_it40);
            int protect_B__ID = get<1>(*BIDS_it40);
            int protect_B__BROKER_ID = get<2>(*BIDS_it40);
            double protect_B__P = get<3>(*BIDS_it40);
            double protect_B__V = get<4>(*BIDS_it40);
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it38 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end37 
                = ASKS.end();
            for (; ASKS_it38 != ASKS_end37; ++ASKS_it38)
            {
                double protect_A__T = get<0>(*ASKS_it38);
                int protect_A__ID = get<1>(*ASKS_it38);
                int protect_A__BROKER_ID = get<2>(*ASKS_it38);
                double protect_A__P = get<3>(*ASKS_it38);
                double protect_A__V = get<4>(*ASKS_it38);
                int var70 = 0;
                var70 += B__BROKER_ID;
                int var71 = 0;
                var71 += protect_B__BROKER_ID;
                if ( var70 == var71 )
                {
                    int var67 = 0;
                    var67 += 1000;
                    double var68 = 0;
                    var68 += protect_A__P;
                    double var69 = 1;
                    var69 *= -1;
                    var69 *= protect_B__P;
                    var68 += var69;
                    if ( var67 < var68 )
                    {
                        int var65 = 0;
                        var65 += protect_B__BROKER_ID;
                        int var66 = 0;
                        var66 += protect_A__BROKER_ID;
                        if ( var65 == var66 )
                        {
                            q[B__BROKER_ID] += protect_A__V;
                        }
                    }
                }
            }
        }
        double var72 = 1;
        var72 *= -1;
        double var73 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it44 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end43 = 
            ASKS.end();
        for (; ASKS_it44 != ASKS_end43; ++ASKS_it44)
        {
            double protect_A__T = get<0>(*ASKS_it44);
            int protect_A__ID = get<1>(*ASKS_it44);
            int protect_A__BROKER_ID = get<2>(*ASKS_it44);
            double protect_A__P = get<3>(*ASKS_it44);
            double protect_A__V = get<4>(*ASKS_it44);
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it42 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end41 
                = BIDS.end();
            for (; BIDS_it42 != BIDS_end41; ++BIDS_it42)
            {
                double protect_B__T = get<0>(*BIDS_it42);
                int protect_B__ID = get<1>(*BIDS_it42);
                int protect_B__BROKER_ID = get<2>(*BIDS_it42);
                double protect_B__P = get<3>(*BIDS_it42);
                double protect_B__V = get<4>(*BIDS_it42);
                int var78 = 0;
                var78 += 1000;
                double var79 = 0;
                var79 += protect_A__P;
                double var80 = 1;
                var80 *= -1;
                var80 *= protect_B__P;
                var79 += var80;
                if ( var78 < var79 )
                {
                    int var76 = 0;
                    var76 += protect_B__BROKER_ID;
                    int var77 = 0;
                    var77 += protect_A__BROKER_ID;
                    if ( var76 == var77 )
                    {
                        int var74 = 0;
                        var74 += B__BROKER_ID;
                        int var75 = 0;
                        var75 += protect_B__BROKER_ID;
                        if ( var74 == var75 )
                        {
                            var73 += protect_B__V;
                        }
                    }
                }
            }
        }
        var72 *= var73;
        q[B__BROKER_ID] += var72;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it48 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end47 = 
            BIDS.end();
        for (; BIDS_it48 != BIDS_end47; ++BIDS_it48)
        {
            double protect_B__T = get<0>(*BIDS_it48);
            int protect_B__ID = get<1>(*BIDS_it48);
            int protect_B__BROKER_ID = get<2>(*BIDS_it48);
            double protect_B__P = get<3>(*BIDS_it48);
            double protect_B__V = get<4>(*BIDS_it48);
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it46 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end45 
                = ASKS.end();
            for (; ASKS_it46 != ASKS_end45; ++ASKS_it46)
            {
                double protect_A__T = get<0>(*ASKS_it46);
                int protect_A__ID = get<1>(*ASKS_it46);
                int protect_A__BROKER_ID = get<2>(*ASKS_it46);
                double protect_A__P = get<3>(*ASKS_it46);
                double protect_A__V = get<4>(*ASKS_it46);
                int var86 = 0;
                var86 += B__BROKER_ID;
                int var87 = 0;
                var87 += protect_B__BROKER_ID;
                if ( var86 == var87 )
                {
                    int var83 = 0;
                    var83 += 1000;
                    double var84 = 0;
                    var84 += protect_B__P;
                    double var85 = 1;
                    var85 *= -1;
                    var85 *= protect_A__P;
                    var84 += var85;
                    if ( var83 < var84 )
                    {
                        int var81 = 0;
                        var81 += protect_B__BROKER_ID;
                        int var82 = 0;
                        var82 += protect_A__BROKER_ID;
                        if ( var81 == var82 )
                        {
                            q[B__BROKER_ID] += protect_A__V;
                        }
                    }
                }
            }
        }
        double var88 = 1;
        var88 *= -1;
        double var89 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it52 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end51 = 
            ASKS.end();
        for (; ASKS_it52 != ASKS_end51; ++ASKS_it52)
        {
            double protect_A__T = get<0>(*ASKS_it52);
            int protect_A__ID = get<1>(*ASKS_it52);
            int protect_A__BROKER_ID = get<2>(*ASKS_it52);
            double protect_A__P = get<3>(*ASKS_it52);
            double protect_A__V = get<4>(*ASKS_it52);
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it50 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end49 
                = BIDS.end();
            for (; BIDS_it50 != BIDS_end49; ++BIDS_it50)
            {
                double protect_B__T = get<0>(*BIDS_it50);
                int protect_B__ID = get<1>(*BIDS_it50);
                int protect_B__BROKER_ID = get<2>(*BIDS_it50);
                double protect_B__P = get<3>(*BIDS_it50);
                double protect_B__V = get<4>(*BIDS_it50);
                int var94 = 0;
                var94 += 1000;
                double var95 = 0;
                var95 += protect_B__P;
                double var96 = 1;
                var96 *= -1;
                var96 *= protect_A__P;
                var95 += var96;
                if ( var94 < var95 )
                {
                    int var92 = 0;
                    var92 += protect_B__BROKER_ID;
                    int var93 = 0;
                    var93 += protect_A__BROKER_ID;
                    if ( var92 == var93 )
                    {
                        int var90 = 0;
                        var90 += B__BROKER_ID;
                        int var91 = 0;
                        var91 += protect_B__BROKER_ID;
                        if ( var90 == var91 )
                        {
                            var89 += protect_B__V;
                        }
                    }
                }
            }
        }
        var88 *= var89;
        q[B__BROKER_ID] += var88;
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_ASKS_sec_span, on_delete_ASKS_usec_span);
}

void on_delete_BIDS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    BIDS.erase(make_tuple(T,ID,BROKER_ID,P,V));
    if ( q.find(BROKER_ID) == q.end() )
    {
        q[BROKER_ID] += 0;
    }
    map<int,double>::iterator q_it72 = q.begin();
    map<int,double>::iterator q_end71 = q.end();
    for (; q_it72 != q_end71; ++q_it72)
    {
        int BROKER_ID = q_it72->first;
        q[BROKER_ID] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it58 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end57 = 
            BIDS.end();
        for (; BIDS_it58 != BIDS_end57; ++BIDS_it58)
        {
            double protect_B__T = get<0>(*BIDS_it58);
            int protect_B__ID = get<1>(*BIDS_it58);
            int protect_B__BROKER_ID = get<2>(*BIDS_it58);
            double protect_B__P = get<3>(*BIDS_it58);
            double protect_B__V = get<4>(*BIDS_it58);
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it56 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end55 
                = ASKS.end();
            for (; ASKS_it56 != ASKS_end55; ++ASKS_it56)
            {
                double protect_A__T = get<0>(*ASKS_it56);
                int protect_A__ID = get<1>(*ASKS_it56);
                int protect_A__BROKER_ID = get<2>(*ASKS_it56);
                double protect_A__P = get<3>(*ASKS_it56);
                double protect_A__V = get<4>(*ASKS_it56);
                int var102 = 0;
                var102 += BROKER_ID;
                int var103 = 0;
                var103 += protect_B__BROKER_ID;
                if ( var102 == var103 )
                {
                    int var99 = 0;
                    var99 += 1000;
                    double var100 = 0;
                    var100 += protect_A__P;
                    double var101 = 1;
                    var101 *= -1;
                    var101 *= protect_B__P;
                    var100 += var101;
                    if ( var99 < var100 )
                    {
                        int var97 = 0;
                        var97 += protect_B__BROKER_ID;
                        int var98 = 0;
                        var98 += protect_A__BROKER_ID;
                        if ( var97 == var98 )
                        {
                            q[BROKER_ID] += protect_A__V;
                        }
                    }
                }
            }
        }
        double var104 = 1;
        var104 *= -1;
        double var105 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it62 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end61 = 
            ASKS.end();
        for (; ASKS_it62 != ASKS_end61; ++ASKS_it62)
        {
            double protect_A__T = get<0>(*ASKS_it62);
            int protect_A__ID = get<1>(*ASKS_it62);
            int protect_A__BROKER_ID = get<2>(*ASKS_it62);
            double protect_A__P = get<3>(*ASKS_it62);
            double protect_A__V = get<4>(*ASKS_it62);
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it60 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end59 
                = BIDS.end();
            for (; BIDS_it60 != BIDS_end59; ++BIDS_it60)
            {
                double protect_B__T = get<0>(*BIDS_it60);
                int protect_B__ID = get<1>(*BIDS_it60);
                int protect_B__BROKER_ID = get<2>(*BIDS_it60);
                double protect_B__P = get<3>(*BIDS_it60);
                double protect_B__V = get<4>(*BIDS_it60);
                int var110 = 0;
                var110 += 1000;
                double var111 = 0;
                var111 += protect_A__P;
                double var112 = 1;
                var112 *= -1;
                var112 *= protect_B__P;
                var111 += var112;
                if ( var110 < var111 )
                {
                    int var108 = 0;
                    var108 += protect_B__BROKER_ID;
                    int var109 = 0;
                    var109 += protect_A__BROKER_ID;
                    if ( var108 == var109 )
                    {
                        int var106 = 0;
                        var106 += BROKER_ID;
                        int var107 = 0;
                        var107 += protect_B__BROKER_ID;
                        if ( var106 == var107 )
                        {
                            var105 += protect_B__V;
                        }
                    }
                }
            }
        }
        var104 *= var105;
        q[BROKER_ID] += var104;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it66 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end65 = 
            BIDS.end();
        for (; BIDS_it66 != BIDS_end65; ++BIDS_it66)
        {
            double protect_B__T = get<0>(*BIDS_it66);
            int protect_B__ID = get<1>(*BIDS_it66);
            int protect_B__BROKER_ID = get<2>(*BIDS_it66);
            double protect_B__P = get<3>(*BIDS_it66);
            double protect_B__V = get<4>(*BIDS_it66);
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it64 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end63 
                = ASKS.end();
            for (; ASKS_it64 != ASKS_end63; ++ASKS_it64)
            {
                double protect_A__T = get<0>(*ASKS_it64);
                int protect_A__ID = get<1>(*ASKS_it64);
                int protect_A__BROKER_ID = get<2>(*ASKS_it64);
                double protect_A__P = get<3>(*ASKS_it64);
                double protect_A__V = get<4>(*ASKS_it64);
                int var118 = 0;
                var118 += BROKER_ID;
                int var119 = 0;
                var119 += protect_B__BROKER_ID;
                if ( var118 == var119 )
                {
                    int var115 = 0;
                    var115 += 1000;
                    double var116 = 0;
                    var116 += protect_B__P;
                    double var117 = 1;
                    var117 *= -1;
                    var117 *= protect_A__P;
                    var116 += var117;
                    if ( var115 < var116 )
                    {
                        int var113 = 0;
                        var113 += protect_B__BROKER_ID;
                        int var114 = 0;
                        var114 += protect_A__BROKER_ID;
                        if ( var113 == var114 )
                        {
                            q[BROKER_ID] += protect_A__V;
                        }
                    }
                }
            }
        }
        double var120 = 1;
        var120 *= -1;
        double var121 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it70 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end69 = 
            ASKS.end();
        for (; ASKS_it70 != ASKS_end69; ++ASKS_it70)
        {
            double protect_A__T = get<0>(*ASKS_it70);
            int protect_A__ID = get<1>(*ASKS_it70);
            int protect_A__BROKER_ID = get<2>(*ASKS_it70);
            double protect_A__P = get<3>(*ASKS_it70);
            double protect_A__V = get<4>(*ASKS_it70);
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it68 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end67 
                = BIDS.end();
            for (; BIDS_it68 != BIDS_end67; ++BIDS_it68)
            {
                double protect_B__T = get<0>(*BIDS_it68);
                int protect_B__ID = get<1>(*BIDS_it68);
                int protect_B__BROKER_ID = get<2>(*BIDS_it68);
                double protect_B__P = get<3>(*BIDS_it68);
                double protect_B__V = get<4>(*BIDS_it68);
                int var126 = 0;
                var126 += 1000;
                double var127 = 0;
                var127 += protect_B__P;
                double var128 = 1;
                var128 *= -1;
                var128 *= protect_A__P;
                var127 += var128;
                if ( var126 < var127 )
                {
                    int var124 = 0;
                    var124 += protect_B__BROKER_ID;
                    int var125 = 0;
                    var125 += protect_A__BROKER_ID;
                    if ( var124 == var125 )
                    {
                        int var122 = 0;
                        var122 += BROKER_ID;
                        int var123 = 0;
                        var123 += protect_B__BROKER_ID;
                        if ( var122 == var123 )
                        {
                            var121 += protect_B__V;
                        }
                    }
                }
            }
        }
        var120 *= var121;
        q[BROKER_ID] += var120;
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_BIDS_sec_span, on_delete_BIDS_usec_span);
}

DBToaster::DemoDatasets::OrderbookFileStream VwapAsks("/home/yanif/datasets/orderbook/MSFT/20081201.csv",10000);

boost::shared_ptr<DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor> VwapAsks_adaptor(new DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor());
static int streamVwapAsksId = 0;

struct on_insert_ASKS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor::Result>(data); 
        on_insert_ASKS(results, log, stats, input.t,input.id,input.broker_id,input.price,input.volume);
    }
};

on_insert_ASKS_fun_obj fo_on_insert_ASKS_0;

DBToaster::DemoDatasets::OrderbookFileStream VwapBids("/home/yanif/datasets/orderbook/MSFT/20081201.csv",10000);

boost::shared_ptr<DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor> VwapBids_adaptor(new DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor());
static int streamVwapBidsId = 1;

struct on_insert_BIDS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor::Result>(data); 
        on_insert_BIDS(results, log, stats, input.t,input.id,input.broker_id,input.price,input.volume);
    }
};

on_insert_BIDS_fun_obj fo_on_insert_BIDS_1;

struct on_delete_ASKS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor::Result>(data); 
        on_delete_ASKS(results, log, stats, input.t,input.id,input.broker_id,input.price,input.volume);
    }
};

on_delete_ASKS_fun_obj fo_on_delete_ASKS_2;

struct on_delete_BIDS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor::Result>(data); 
        on_delete_BIDS(results, log, stats, input.t,input.id,input.broker_id,input.price,input.volume);
    }
};

on_delete_BIDS_fun_obj fo_on_delete_BIDS_3;


void init(DBToaster::StandaloneEngine::FileMultiplexer& sources,
    DBToaster::StandaloneEngine::FileStreamDispatcher& router, 
    ofstream* results, ofstream* log, ofstream* stats)
{
    router.setFiles(results, log, stats);
    sources.addStream<DBToaster::DemoDatasets::OrderbookTuple>(&VwapAsks, boost::ref(*VwapAsks_adaptor), streamVwapAsksId);
    router.addHandler(streamVwapAsksId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_ASKS_0));
    sources.addStream<DBToaster::DemoDatasets::OrderbookTuple>(&VwapBids, boost::ref(*VwapBids_adaptor), streamVwapBidsId);
    router.addHandler(streamVwapBidsId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_BIDS_1));
    router.addHandler(streamVwapAsksId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_ASKS_2));
    router.addHandler(streamVwapBidsId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_BIDS_3));
    sources.initStream();
    cout << "Initialized input stream..." << endl;
}

DBToaster::StandaloneEngine::FileMultiplexer sources(12345, 20);
DBToaster::StandaloneEngine::FileStreamDispatcher router;


void runMultiplexer(ofstream* results, ofstream* log, ofstream* stats)
{
    unsigned long tuple_counter = 0;
    double tup_sec_span = 0.0;
    double tup_usec_span = 0.0;
    struct timeval tvs, tve;
    gettimeofday(&tvs, NULL);
    while ( sources.streamHasInputs() ) {
        struct timeval tups, tupe;
        gettimeofday(&tups, NULL);
        DBToaster::StandaloneEngine::DBToasterTuple t = sources.nextInput();
        router.dispatch(t);
        ++tuple_counter;
        gettimeofday(&tupe, NULL);
        DBToaster::Profiler::accumulate_time_span(tups, tupe, tup_sec_span, tup_usec_span);
        if ( (tuple_counter % 100) == 0 )
        {
            DBToaster::Profiler::reset_time_span_printing_global(
                "tuples", tuple_counter, 100, tvs, tup_sec_span, tup_usec_span, "query", log);
            analyse_mem_usage(stats);
            analyse_handler_usage(stats);
        }
    }
    DBToaster::Profiler::reset_time_span_printing_global(
        "tuples", tuple_counter, (tuple_counter%100), tvs, tup_sec_span, tup_usec_span, "query", log);
    analyse_handler_usage(stats);
    analyse_mem_usage(stats);
}


int main(int argc, char* argv[])
{
    DBToaster::StandaloneEngine::EngineOptions opts;
    opts(argc,argv);
    ofstream* log = new ofstream(opts.log_file_name.c_str());
    ofstream* results = new ofstream(opts.results_file_name.c_str());
    ofstream* stats = new ofstream(opts.stats_file_name.c_str());
    init(sources, router, results, log, stats);
    runMultiplexer(results, log, stats);
    log->flush(); log->close();
    results->flush(); results->close();
}
