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
double q;

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
    ASKS.insert(make_tuple(T,ID,BROKER_ID,P,V));
    q = 0;
    double var1 = 1;
    double var2 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_it4 = 
        ASKS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_end3 = 
        ASKS.end();
    for (; ASKS_it4 != ASKS_end3; ++ASKS_it4)
    {
        double protect_A__T = get<0>(*ASKS_it4);
        int protect_A__ID = get<1>(*ASKS_it4);
        int protect_A__BROKER_ID = get<2>(*ASKS_it4);
        double protect_A__P = get<3>(*ASKS_it4);
        double protect_A__V = get<4>(*ASKS_it4);
        double var3 = 0;
        var3 += protect_A__V;
        double var4 = 0;
        double var5 = 1;
        var5 *= 0.0001;
        double var6 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it2 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end1 = 
            ASKS.end();
        for (; ASKS_it2 != ASKS_end1; ++ASKS_it2)
        {
            double A1__T = get<0>(*ASKS_it2);
            int A1__ID = get<1>(*ASKS_it2);
            int A1__BROKER_ID = get<2>(*ASKS_it2);
            double A1__P = get<3>(*ASKS_it2);
            double A1__V = get<4>(*ASKS_it2);
            var6 += A1__P;
        }
        var5 *= var6;
        var4 += var5;
        if ( var3 < var4 )
        {
            var2 += protect_A__P;
        }
    }
    var1 *= var2;
    double var7 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_it8 = 
        BIDS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_end7 = 
        BIDS.end();
    for (; BIDS_it8 != BIDS_end7; ++BIDS_it8)
    {
        double protect_B__T = get<0>(*BIDS_it8);
        int protect_B__ID = get<1>(*BIDS_it8);
        int protect_B__BROKER_ID = get<2>(*BIDS_it8);
        double protect_B__P = get<3>(*BIDS_it8);
        double protect_B__V = get<4>(*BIDS_it8);
        double var8 = 0;
        double var9 = 1;
        var9 *= 0.0001;
        double var10 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it6 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end5 = 
            BIDS.end();
        for (; BIDS_it6 != BIDS_end5; ++BIDS_it6)
        {
            double B1__T = get<0>(*BIDS_it6);
            int B1__ID = get<1>(*BIDS_it6);
            int B1__BROKER_ID = get<2>(*BIDS_it6);
            double B1__P = get<3>(*BIDS_it6);
            double B1__V = get<4>(*BIDS_it6);
            var10 += B1__P;
        }
        var9 *= var10;
        var8 += var9;
        double var11 = 0;
        var11 += protect_B__V;
        if ( var8 < var11 )
        {
            var7 += 1;
        }
    }
    var1 *= var7;
    q += var1;
    double var12 = 1;
    var12 *= -1;
    double var13 = 0;
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
        double var14 = 0;
        double var15 = 1;
        var15 *= 0.0001;
        double var16 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it10 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end9 = 
            BIDS.end();
        for (; BIDS_it10 != BIDS_end9; ++BIDS_it10)
        {
            double B1__T = get<0>(*BIDS_it10);
            int B1__ID = get<1>(*BIDS_it10);
            int B1__BROKER_ID = get<2>(*BIDS_it10);
            double B1__P = get<3>(*BIDS_it10);
            double B1__V = get<4>(*BIDS_it10);
            var16 += B1__P;
        }
        var15 *= var16;
        var14 += var15;
        double var17 = 0;
        var17 += protect_B__V;
        if ( var14 < var17 )
        {
            var13 += protect_B__P;
        }
    }
    var12 *= var13;
    double var18 = 0;
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
        double var19 = 0;
        var19 += protect_A__V;
        double var20 = 0;
        double var21 = 1;
        var21 *= 0.0001;
        double var22 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it14 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end13 = 
            ASKS.end();
        for (; ASKS_it14 != ASKS_end13; ++ASKS_it14)
        {
            double A1__T = get<0>(*ASKS_it14);
            int A1__ID = get<1>(*ASKS_it14);
            int A1__BROKER_ID = get<2>(*ASKS_it14);
            double A1__P = get<3>(*ASKS_it14);
            double A1__V = get<4>(*ASKS_it14);
            var22 += A1__P;
        }
        var21 *= var22;
        var20 += var21;
        if ( var19 < var20 )
        {
            var18 += 1;
        }
    }
    var12 *= var18;
    q += var12;
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_ASKS_sec_span, on_insert_ASKS_usec_span);
    (*results) << "on_insert_ASKS" << "," << q << endl;
}

void on_insert_BIDS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    BIDS.insert(make_tuple(T,ID,BROKER_ID,P,V));
    q = 0;
    double var23 = 1;
    double var24 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_it20 = 
        ASKS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_end19 = 
        ASKS.end();
    for (; ASKS_it20 != ASKS_end19; ++ASKS_it20)
    {
        double protect_A__T = get<0>(*ASKS_it20);
        int protect_A__ID = get<1>(*ASKS_it20);
        int protect_A__BROKER_ID = get<2>(*ASKS_it20);
        double protect_A__P = get<3>(*ASKS_it20);
        double protect_A__V = get<4>(*ASKS_it20);
        double var25 = 0;
        var25 += protect_A__V;
        double var26 = 0;
        double var27 = 1;
        var27 *= 0.0001;
        double var28 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it18 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end17 = 
            ASKS.end();
        for (; ASKS_it18 != ASKS_end17; ++ASKS_it18)
        {
            double A1__T = get<0>(*ASKS_it18);
            int A1__ID = get<1>(*ASKS_it18);
            int A1__BROKER_ID = get<2>(*ASKS_it18);
            double A1__P = get<3>(*ASKS_it18);
            double A1__V = get<4>(*ASKS_it18);
            var28 += A1__P;
        }
        var27 *= var28;
        var26 += var27;
        if ( var25 < var26 )
        {
            var24 += protect_A__P;
        }
    }
    var23 *= var24;
    double var29 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_it24 = 
        BIDS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_end23 = 
        BIDS.end();
    for (; BIDS_it24 != BIDS_end23; ++BIDS_it24)
    {
        double protect_B__T = get<0>(*BIDS_it24);
        int protect_B__ID = get<1>(*BIDS_it24);
        int protect_B__BROKER_ID = get<2>(*BIDS_it24);
        double protect_B__P = get<3>(*BIDS_it24);
        double protect_B__V = get<4>(*BIDS_it24);
        double var30 = 0;
        double var31 = 1;
        var31 *= 0.0001;
        double var32 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it22 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end21 = 
            BIDS.end();
        for (; BIDS_it22 != BIDS_end21; ++BIDS_it22)
        {
            double B1__T = get<0>(*BIDS_it22);
            int B1__ID = get<1>(*BIDS_it22);
            int B1__BROKER_ID = get<2>(*BIDS_it22);
            double B1__P = get<3>(*BIDS_it22);
            double B1__V = get<4>(*BIDS_it22);
            var32 += B1__P;
        }
        var31 *= var32;
        var30 += var31;
        double var33 = 0;
        var33 += protect_B__V;
        if ( var30 < var33 )
        {
            var29 += 1;
        }
    }
    var23 *= var29;
    q += var23;
    double var34 = 1;
    var34 *= -1;
    double var35 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_it28 = 
        BIDS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_end27 = 
        BIDS.end();
    for (; BIDS_it28 != BIDS_end27; ++BIDS_it28)
    {
        double protect_B__T = get<0>(*BIDS_it28);
        int protect_B__ID = get<1>(*BIDS_it28);
        int protect_B__BROKER_ID = get<2>(*BIDS_it28);
        double protect_B__P = get<3>(*BIDS_it28);
        double protect_B__V = get<4>(*BIDS_it28);
        double var36 = 0;
        double var37 = 1;
        var37 *= 0.0001;
        double var38 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it26 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end25 = 
            BIDS.end();
        for (; BIDS_it26 != BIDS_end25; ++BIDS_it26)
        {
            double B1__T = get<0>(*BIDS_it26);
            int B1__ID = get<1>(*BIDS_it26);
            int B1__BROKER_ID = get<2>(*BIDS_it26);
            double B1__P = get<3>(*BIDS_it26);
            double B1__V = get<4>(*BIDS_it26);
            var38 += B1__P;
        }
        var37 *= var38;
        var36 += var37;
        double var39 = 0;
        var39 += protect_B__V;
        if ( var36 < var39 )
        {
            var35 += protect_B__P;
        }
    }
    var34 *= var35;
    double var40 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_it32 = 
        ASKS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_end31 = 
        ASKS.end();
    for (; ASKS_it32 != ASKS_end31; ++ASKS_it32)
    {
        double protect_A__T = get<0>(*ASKS_it32);
        int protect_A__ID = get<1>(*ASKS_it32);
        int protect_A__BROKER_ID = get<2>(*ASKS_it32);
        double protect_A__P = get<3>(*ASKS_it32);
        double protect_A__V = get<4>(*ASKS_it32);
        double var41 = 0;
        var41 += protect_A__V;
        double var42 = 0;
        double var43 = 1;
        var43 *= 0.0001;
        double var44 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it30 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end29 = 
            ASKS.end();
        for (; ASKS_it30 != ASKS_end29; ++ASKS_it30)
        {
            double A1__T = get<0>(*ASKS_it30);
            int A1__ID = get<1>(*ASKS_it30);
            int A1__BROKER_ID = get<2>(*ASKS_it30);
            double A1__P = get<3>(*ASKS_it30);
            double A1__V = get<4>(*ASKS_it30);
            var44 += A1__P;
        }
        var43 *= var44;
        var42 += var43;
        if ( var41 < var42 )
        {
            var40 += 1;
        }
    }
    var34 *= var40;
    q += var34;
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_BIDS_sec_span, on_insert_BIDS_usec_span);
    (*results) << "on_insert_BIDS" << "," << q << endl;
}

void on_delete_ASKS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    ASKS.erase(make_tuple(T,ID,BROKER_ID,P,V));
    q = 0;
    double var45 = 1;
    double var46 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_it36 = 
        ASKS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_end35 = 
        ASKS.end();
    for (; ASKS_it36 != ASKS_end35; ++ASKS_it36)
    {
        double protect_A__T = get<0>(*ASKS_it36);
        int protect_A__ID = get<1>(*ASKS_it36);
        int protect_A__BROKER_ID = get<2>(*ASKS_it36);
        double protect_A__P = get<3>(*ASKS_it36);
        double protect_A__V = get<4>(*ASKS_it36);
        double var47 = 0;
        var47 += protect_A__V;
        double var48 = 0;
        double var49 = 1;
        var49 *= 0.0001;
        double var50 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it34 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end33 = 
            ASKS.end();
        for (; ASKS_it34 != ASKS_end33; ++ASKS_it34)
        {
            double A1__T = get<0>(*ASKS_it34);
            int A1__ID = get<1>(*ASKS_it34);
            int A1__BROKER_ID = get<2>(*ASKS_it34);
            double A1__P = get<3>(*ASKS_it34);
            double A1__V = get<4>(*ASKS_it34);
            var50 += A1__P;
        }
        var49 *= var50;
        var48 += var49;
        if ( var47 < var48 )
        {
            var46 += protect_A__P;
        }
    }
    var45 *= var46;
    double var51 = 0;
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
        double var52 = 0;
        double var53 = 1;
        var53 *= 0.0001;
        double var54 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it38 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end37 = 
            BIDS.end();
        for (; BIDS_it38 != BIDS_end37; ++BIDS_it38)
        {
            double B1__T = get<0>(*BIDS_it38);
            int B1__ID = get<1>(*BIDS_it38);
            int B1__BROKER_ID = get<2>(*BIDS_it38);
            double B1__P = get<3>(*BIDS_it38);
            double B1__V = get<4>(*BIDS_it38);
            var54 += B1__P;
        }
        var53 *= var54;
        var52 += var53;
        double var55 = 0;
        var55 += protect_B__V;
        if ( var52 < var55 )
        {
            var51 += 1;
        }
    }
    var45 *= var51;
    q += var45;
    double var56 = 1;
    var56 *= -1;
    double var57 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_it44 = 
        BIDS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_end43 = 
        BIDS.end();
    for (; BIDS_it44 != BIDS_end43; ++BIDS_it44)
    {
        double protect_B__T = get<0>(*BIDS_it44);
        int protect_B__ID = get<1>(*BIDS_it44);
        int protect_B__BROKER_ID = get<2>(*BIDS_it44);
        double protect_B__P = get<3>(*BIDS_it44);
        double protect_B__V = get<4>(*BIDS_it44);
        double var58 = 0;
        double var59 = 1;
        var59 *= 0.0001;
        double var60 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it42 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end41 = 
            BIDS.end();
        for (; BIDS_it42 != BIDS_end41; ++BIDS_it42)
        {
            double B1__T = get<0>(*BIDS_it42);
            int B1__ID = get<1>(*BIDS_it42);
            int B1__BROKER_ID = get<2>(*BIDS_it42);
            double B1__P = get<3>(*BIDS_it42);
            double B1__V = get<4>(*BIDS_it42);
            var60 += B1__P;
        }
        var59 *= var60;
        var58 += var59;
        double var61 = 0;
        var61 += protect_B__V;
        if ( var58 < var61 )
        {
            var57 += protect_B__P;
        }
    }
    var56 *= var57;
    double var62 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_it48 = 
        ASKS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_end47 = 
        ASKS.end();
    for (; ASKS_it48 != ASKS_end47; ++ASKS_it48)
    {
        double protect_A__T = get<0>(*ASKS_it48);
        int protect_A__ID = get<1>(*ASKS_it48);
        int protect_A__BROKER_ID = get<2>(*ASKS_it48);
        double protect_A__P = get<3>(*ASKS_it48);
        double protect_A__V = get<4>(*ASKS_it48);
        double var63 = 0;
        var63 += protect_A__V;
        double var64 = 0;
        double var65 = 1;
        var65 *= 0.0001;
        double var66 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it46 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end45 = 
            ASKS.end();
        for (; ASKS_it46 != ASKS_end45; ++ASKS_it46)
        {
            double A1__T = get<0>(*ASKS_it46);
            int A1__ID = get<1>(*ASKS_it46);
            int A1__BROKER_ID = get<2>(*ASKS_it46);
            double A1__P = get<3>(*ASKS_it46);
            double A1__V = get<4>(*ASKS_it46);
            var66 += A1__P;
        }
        var65 *= var66;
        var64 += var65;
        if ( var63 < var64 )
        {
            var62 += 1;
        }
    }
    var56 *= var62;
    q += var56;
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_ASKS_sec_span, on_delete_ASKS_usec_span);
    (*results) << "on_delete_ASKS" << "," << q << endl;
}

void on_delete_BIDS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    BIDS.erase(make_tuple(T,ID,BROKER_ID,P,V));
    q = 0;
    double var67 = 1;
    double var68 = 0;
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
        double var69 = 0;
        var69 += protect_A__V;
        double var70 = 0;
        double var71 = 1;
        var71 *= 0.0001;
        double var72 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it50 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end49 = 
            ASKS.end();
        for (; ASKS_it50 != ASKS_end49; ++ASKS_it50)
        {
            double A1__T = get<0>(*ASKS_it50);
            int A1__ID = get<1>(*ASKS_it50);
            int A1__BROKER_ID = get<2>(*ASKS_it50);
            double A1__P = get<3>(*ASKS_it50);
            double A1__V = get<4>(*ASKS_it50);
            var72 += A1__P;
        }
        var71 *= var72;
        var70 += var71;
        if ( var69 < var70 )
        {
            var68 += protect_A__P;
        }
    }
    var67 *= var68;
    double var73 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_it56 = 
        BIDS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_end55 = 
        BIDS.end();
    for (; BIDS_it56 != BIDS_end55; ++BIDS_it56)
    {
        double protect_B__T = get<0>(*BIDS_it56);
        int protect_B__ID = get<1>(*BIDS_it56);
        int protect_B__BROKER_ID = get<2>(*BIDS_it56);
        double protect_B__P = get<3>(*BIDS_it56);
        double protect_B__V = get<4>(*BIDS_it56);
        double var74 = 0;
        double var75 = 1;
        var75 *= 0.0001;
        double var76 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it54 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end53 = 
            BIDS.end();
        for (; BIDS_it54 != BIDS_end53; ++BIDS_it54)
        {
            double B1__T = get<0>(*BIDS_it54);
            int B1__ID = get<1>(*BIDS_it54);
            int B1__BROKER_ID = get<2>(*BIDS_it54);
            double B1__P = get<3>(*BIDS_it54);
            double B1__V = get<4>(*BIDS_it54);
            var76 += B1__P;
        }
        var75 *= var76;
        var74 += var75;
        double var77 = 0;
        var77 += protect_B__V;
        if ( var74 < var77 )
        {
            var73 += 1;
        }
    }
    var67 *= var73;
    q += var67;
    double var78 = 1;
    var78 *= -1;
    double var79 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_it60 = 
        BIDS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_end59 = 
        BIDS.end();
    for (; BIDS_it60 != BIDS_end59; ++BIDS_it60)
    {
        double protect_B__T = get<0>(*BIDS_it60);
        int protect_B__ID = get<1>(*BIDS_it60);
        int protect_B__BROKER_ID = get<2>(*BIDS_it60);
        double protect_B__P = get<3>(*BIDS_it60);
        double protect_B__V = get<4>(*BIDS_it60);
        double var80 = 0;
        double var81 = 1;
        var81 *= 0.0001;
        double var82 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it58 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end57 = 
            BIDS.end();
        for (; BIDS_it58 != BIDS_end57; ++BIDS_it58)
        {
            double B1__T = get<0>(*BIDS_it58);
            int B1__ID = get<1>(*BIDS_it58);
            int B1__BROKER_ID = get<2>(*BIDS_it58);
            double B1__P = get<3>(*BIDS_it58);
            double B1__V = get<4>(*BIDS_it58);
            var82 += B1__P;
        }
        var81 *= var82;
        var80 += var81;
        double var83 = 0;
        var83 += protect_B__V;
        if ( var80 < var83 )
        {
            var79 += protect_B__P;
        }
    }
    var78 *= var79;
    double var84 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_it64 = 
        ASKS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_end63 = 
        ASKS.end();
    for (; ASKS_it64 != ASKS_end63; ++ASKS_it64)
    {
        double protect_A__T = get<0>(*ASKS_it64);
        int protect_A__ID = get<1>(*ASKS_it64);
        int protect_A__BROKER_ID = get<2>(*ASKS_it64);
        double protect_A__P = get<3>(*ASKS_it64);
        double protect_A__V = get<4>(*ASKS_it64);
        double var85 = 0;
        var85 += protect_A__V;
        double var86 = 0;
        double var87 = 1;
        var87 *= 0.0001;
        double var88 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it62 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end61 = 
            ASKS.end();
        for (; ASKS_it62 != ASKS_end61; ++ASKS_it62)
        {
            double A1__T = get<0>(*ASKS_it62);
            int A1__ID = get<1>(*ASKS_it62);
            int A1__BROKER_ID = get<2>(*ASKS_it62);
            double A1__P = get<3>(*ASKS_it62);
            double A1__V = get<4>(*ASKS_it62);
            var88 += A1__P;
        }
        var87 *= var88;
        var86 += var87;
        if ( var85 < var86 )
        {
            var84 += 1;
        }
    }
    var78 *= var84;
    q += var78;
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_BIDS_sec_span, on_delete_BIDS_usec_span);
    (*results) << "on_delete_BIDS" << "," << q << endl;
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
