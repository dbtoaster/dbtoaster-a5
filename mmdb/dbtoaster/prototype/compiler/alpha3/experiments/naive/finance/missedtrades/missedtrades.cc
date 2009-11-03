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
    ASKS.insert(make_tuple(T,ID,BROKER_ID,P,V));
    map<int,double>::iterator q_it26 = q.begin();
    map<int,double>::iterator q_end25 = q.end();
    for (; q_it26 != q_end25; ++q_it26)
    {
        int B__BROKER_ID = q_it26->first;
        q[B__BROKER_ID] = 0;
        double var1 = 1;
        double var2 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it6 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end5 = 
            ASKS.end();
        for (; ASKS_it6 != ASKS_end5; ++ASKS_it6)
        {
            double protect_A__T = get<0>(*ASKS_it6);
            int protect_A__ID = get<1>(*ASKS_it6);
            int protect_A__BROKER_ID = get<2>(*ASKS_it6);
            double protect_A__P = get<3>(*ASKS_it6);
            double protect_A__V = get<4>(*ASKS_it6);
            double var4 = 0;
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_it2 = 
                ASKS.begin();
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_end1 = ASKS.end();
            for (; ASKS_it2 != ASKS_end1; ++ASKS_it2)
            {
                double A2__T = get<0>(*ASKS_it2);
                int A2__ID = get<1>(*ASKS_it2);
                int A2__BROKER_ID = get<2>(*ASKS_it2);
                double A2__P = get<3>(*ASKS_it2);
                double A2__V = get<4>(*ASKS_it2);
                double var5 = 0;
                var5 += protect_A__P;
                double var6 = 0;
                var6 += A2__P;
                if ( var5 < var6 )
                {
                    var4 += A2__V;
                }
            }
            double var7 = 0;
            double var8 = 1;
            var8 *= 0.25;
            double var9 = 0;
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_it4 = 
                ASKS.begin();
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_end3 = ASKS.end();
            for (; ASKS_it4 != ASKS_end3; ++ASKS_it4)
            {
                double A1__T = get<0>(*ASKS_it4);
                int A1__ID = get<1>(*ASKS_it4);
                int A1__BROKER_ID = get<2>(*ASKS_it4);
                double A1__P = get<3>(*ASKS_it4);
                double A1__V = get<4>(*ASKS_it4);
                var9 += A1__V;
            }
            var8 *= var9;
            var7 += var8;
            if ( var4 < var7 )
            {
                double var3 = 1;
                var3 *= protect_A__P;
                var3 *= protect_A__V;
                var2 += var3;
            }
        }
        var1 *= var2;
        double var10 = 0;
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
            double var13 = 0;
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_it8 = 
                BIDS.begin();
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_end7 = BIDS.end();
            for (; BIDS_it8 != BIDS_end7; ++BIDS_it8)
            {
                double B2__T = get<0>(*BIDS_it8);
                int B2__ID = get<1>(*BIDS_it8);
                int B2__BROKER_ID = get<2>(*BIDS_it8);
                double B2__P = get<3>(*BIDS_it8);
                double B2__V = get<4>(*BIDS_it8);
                double var14 = 0;
                var14 += protect_B__P;
                double var15 = 0;
                var15 += B2__P;
                if ( var14 < var15 )
                {
                    var13 += B2__V;
                }
            }
            double var16 = 0;
            double var17 = 1;
            var17 *= 0.25;
            double var18 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it10 = BIDS.begin();
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_end9 = BIDS.end();
            for (; BIDS_it10 != BIDS_end9; ++BIDS_it10)
            {
                double B1__T = get<0>(*BIDS_it10);
                int B1__ID = get<1>(*BIDS_it10);
                int B1__BROKER_ID = get<2>(*BIDS_it10);
                double B1__P = get<3>(*BIDS_it10);
                double B1__V = get<4>(*BIDS_it10);
                var18 += B1__V;
            }
            var17 *= var18;
            var16 += var17;
            if ( var13 < var16 )
            {
                int var11 = 0;
                var11 += B__BROKER_ID;
                int var12 = 0;
                var12 += protect_B__BROKER_ID;
                if ( var11 == var12 )
                {
                    var10 += 1;
                }
            }
        }
        var1 *= var10;
        q[B__BROKER_ID] += var1;
        double var19 = 1;
        double var20 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it18 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end17 = 
            ASKS.end();
        for (; ASKS_it18 != ASKS_end17; ++ASKS_it18)
        {
            double protect_A__T = get<0>(*ASKS_it18);
            int protect_A__ID = get<1>(*ASKS_it18);
            int protect_A__BROKER_ID = get<2>(*ASKS_it18);
            double protect_A__P = get<3>(*ASKS_it18);
            double protect_A__V = get<4>(*ASKS_it18);
            double var21 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it14 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end13 
                = ASKS.end();
            for (; ASKS_it14 != ASKS_end13; ++ASKS_it14)
            {
                double A2__T = get<0>(*ASKS_it14);
                int A2__ID = get<1>(*ASKS_it14);
                int A2__BROKER_ID = get<2>(*ASKS_it14);
                double A2__P = get<3>(*ASKS_it14);
                double A2__V = get<4>(*ASKS_it14);
                double var22 = 0;
                var22 += protect_A__P;
                double var23 = 0;
                var23 += A2__P;
                if ( var22 < var23 )
                {
                    var21 += A2__V;
                }
            }
            double var24 = 0;
            double var25 = 1;
            var25 *= 0.25;
            double var26 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it16 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end15 
                = ASKS.end();
            for (; ASKS_it16 != ASKS_end15; ++ASKS_it16)
            {
                double A1__T = get<0>(*ASKS_it16);
                int A1__ID = get<1>(*ASKS_it16);
                int A1__BROKER_ID = get<2>(*ASKS_it16);
                double A1__P = get<3>(*ASKS_it16);
                double A1__V = get<4>(*ASKS_it16);
                var26 += A1__V;
            }
            var25 *= var26;
            var24 += var25;
            if ( var21 < var24 )
            {
                var20 += protect_A__P;
            }
        }
        var19 *= var20;
        var19 *= -1;
        double var27 = 0;
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
            double var31 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it20 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end19 
                = BIDS.end();
            for (; BIDS_it20 != BIDS_end19; ++BIDS_it20)
            {
                double B2__T = get<0>(*BIDS_it20);
                int B2__ID = get<1>(*BIDS_it20);
                int B2__BROKER_ID = get<2>(*BIDS_it20);
                double B2__P = get<3>(*BIDS_it20);
                double B2__V = get<4>(*BIDS_it20);
                double var32 = 0;
                var32 += protect_B__P;
                double var33 = 0;
                var33 += B2__P;
                if ( var32 < var33 )
                {
                    var31 += B2__V;
                }
            }
            double var34 = 0;
            double var35 = 1;
            var35 *= 0.25;
            double var36 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it22 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end21 
                = BIDS.end();
            for (; BIDS_it22 != BIDS_end21; ++BIDS_it22)
            {
                double B1__T = get<0>(*BIDS_it22);
                int B1__ID = get<1>(*BIDS_it22);
                int B1__BROKER_ID = get<2>(*BIDS_it22);
                double B1__P = get<3>(*BIDS_it22);
                double B1__V = get<4>(*BIDS_it22);
                var36 += B1__V;
            }
            var35 *= var36;
            var34 += var35;
            if ( var31 < var34 )
            {
                int var29 = 0;
                var29 += B__BROKER_ID;
                int var30 = 0;
                var30 += protect_B__BROKER_ID;
                if ( var29 == var30 )
                {
                    double var28 = 1;
                    var28 *= protect_B__P;
                    var28 *= protect_B__V;
                    var27 += var28;
                }
            }
        }
        var19 *= var27;
        q[B__BROKER_ID] += var19;
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
    map<int,double>::iterator q_it52 = q.begin();
    map<int,double>::iterator q_end51 = q.end();
    for (; q_it52 != q_end51; ++q_it52)
    {
        int BROKER_ID = q_it52->first;
        q[BROKER_ID] = 0;
        double var37 = 1;
        double var38 = 0;
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
            double var40 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it28 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end27 
                = ASKS.end();
            for (; ASKS_it28 != ASKS_end27; ++ASKS_it28)
            {
                double A2__T = get<0>(*ASKS_it28);
                int A2__ID = get<1>(*ASKS_it28);
                int A2__BROKER_ID = get<2>(*ASKS_it28);
                double A2__P = get<3>(*ASKS_it28);
                double A2__V = get<4>(*ASKS_it28);
                double var41 = 0;
                var41 += protect_A__P;
                double var42 = 0;
                var42 += A2__P;
                if ( var41 < var42 )
                {
                    var40 += A2__V;
                }
            }
            double var43 = 0;
            double var44 = 1;
            var44 *= 0.25;
            double var45 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it30 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end29 
                = ASKS.end();
            for (; ASKS_it30 != ASKS_end29; ++ASKS_it30)
            {
                double A1__T = get<0>(*ASKS_it30);
                int A1__ID = get<1>(*ASKS_it30);
                int A1__BROKER_ID = get<2>(*ASKS_it30);
                double A1__P = get<3>(*ASKS_it30);
                double A1__V = get<4>(*ASKS_it30);
                var45 += A1__V;
            }
            var44 *= var45;
            var43 += var44;
            if ( var40 < var43 )
            {
                double var39 = 1;
                var39 *= protect_A__P;
                var39 *= protect_A__V;
                var38 += var39;
            }
        }
        var37 *= var38;
        double var46 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it38 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end37 = 
            BIDS.end();
        for (; BIDS_it38 != BIDS_end37; ++BIDS_it38)
        {
            double protect_B__T = get<0>(*BIDS_it38);
            int protect_B__ID = get<1>(*BIDS_it38);
            int protect_B__BROKER_ID = get<2>(*BIDS_it38);
            double protect_B__P = get<3>(*BIDS_it38);
            double protect_B__V = get<4>(*BIDS_it38);
            double var49 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it34 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end33 
                = BIDS.end();
            for (; BIDS_it34 != BIDS_end33; ++BIDS_it34)
            {
                double B2__T = get<0>(*BIDS_it34);
                int B2__ID = get<1>(*BIDS_it34);
                int B2__BROKER_ID = get<2>(*BIDS_it34);
                double B2__P = get<3>(*BIDS_it34);
                double B2__V = get<4>(*BIDS_it34);
                double var50 = 0;
                var50 += protect_B__P;
                double var51 = 0;
                var51 += B2__P;
                if ( var50 < var51 )
                {
                    var49 += B2__V;
                }
            }
            double var52 = 0;
            double var53 = 1;
            var53 *= 0.25;
            double var54 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it36 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end35 
                = BIDS.end();
            for (; BIDS_it36 != BIDS_end35; ++BIDS_it36)
            {
                double B1__T = get<0>(*BIDS_it36);
                int B1__ID = get<1>(*BIDS_it36);
                int B1__BROKER_ID = get<2>(*BIDS_it36);
                double B1__P = get<3>(*BIDS_it36);
                double B1__V = get<4>(*BIDS_it36);
                var54 += B1__V;
            }
            var53 *= var54;
            var52 += var53;
            if ( var49 < var52 )
            {
                int var47 = 0;
                var47 += BROKER_ID;
                int var48 = 0;
                var48 += protect_B__BROKER_ID;
                if ( var47 == var48 )
                {
                    var46 += 1;
                }
            }
        }
        var37 *= var46;
        q[BROKER_ID] += var37;
        double var55 = 1;
        double var56 = 0;
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
            double var57 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it40 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end39 
                = ASKS.end();
            for (; ASKS_it40 != ASKS_end39; ++ASKS_it40)
            {
                double A2__T = get<0>(*ASKS_it40);
                int A2__ID = get<1>(*ASKS_it40);
                int A2__BROKER_ID = get<2>(*ASKS_it40);
                double A2__P = get<3>(*ASKS_it40);
                double A2__V = get<4>(*ASKS_it40);
                double var58 = 0;
                var58 += protect_A__P;
                double var59 = 0;
                var59 += A2__P;
                if ( var58 < var59 )
                {
                    var57 += A2__V;
                }
            }
            double var60 = 0;
            double var61 = 1;
            var61 *= 0.25;
            double var62 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it42 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end41 
                = ASKS.end();
            for (; ASKS_it42 != ASKS_end41; ++ASKS_it42)
            {
                double A1__T = get<0>(*ASKS_it42);
                int A1__ID = get<1>(*ASKS_it42);
                int A1__BROKER_ID = get<2>(*ASKS_it42);
                double A1__P = get<3>(*ASKS_it42);
                double A1__V = get<4>(*ASKS_it42);
                var62 += A1__V;
            }
            var61 *= var62;
            var60 += var61;
            if ( var57 < var60 )
            {
                var56 += protect_A__P;
            }
        }
        var55 *= var56;
        var55 *= -1;
        double var63 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it50 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end49 = 
            BIDS.end();
        for (; BIDS_it50 != BIDS_end49; ++BIDS_it50)
        {
            double protect_B__T = get<0>(*BIDS_it50);
            int protect_B__ID = get<1>(*BIDS_it50);
            int protect_B__BROKER_ID = get<2>(*BIDS_it50);
            double protect_B__P = get<3>(*BIDS_it50);
            double protect_B__V = get<4>(*BIDS_it50);
            double var67 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it46 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end45 
                = BIDS.end();
            for (; BIDS_it46 != BIDS_end45; ++BIDS_it46)
            {
                double B2__T = get<0>(*BIDS_it46);
                int B2__ID = get<1>(*BIDS_it46);
                int B2__BROKER_ID = get<2>(*BIDS_it46);
                double B2__P = get<3>(*BIDS_it46);
                double B2__V = get<4>(*BIDS_it46);
                double var68 = 0;
                var68 += protect_B__P;
                double var69 = 0;
                var69 += B2__P;
                if ( var68 < var69 )
                {
                    var67 += B2__V;
                }
            }
            double var70 = 0;
            double var71 = 1;
            var71 *= 0.25;
            double var72 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it48 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end47 
                = BIDS.end();
            for (; BIDS_it48 != BIDS_end47; ++BIDS_it48)
            {
                double B1__T = get<0>(*BIDS_it48);
                int B1__ID = get<1>(*BIDS_it48);
                int B1__BROKER_ID = get<2>(*BIDS_it48);
                double B1__P = get<3>(*BIDS_it48);
                double B1__V = get<4>(*BIDS_it48);
                var72 += B1__V;
            }
            var71 *= var72;
            var70 += var71;
            if ( var67 < var70 )
            {
                int var65 = 0;
                var65 += BROKER_ID;
                int var66 = 0;
                var66 += protect_B__BROKER_ID;
                if ( var65 == var66 )
                {
                    double var64 = 1;
                    var64 *= protect_B__P;
                    var64 *= protect_B__V;
                    var63 += var64;
                }
            }
        }
        var55 *= var63;
        q[BROKER_ID] += var55;
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
    map<int,double>::iterator q_it78 = q.begin();
    map<int,double>::iterator q_end77 = q.end();
    for (; q_it78 != q_end77; ++q_it78)
    {
        int B__BROKER_ID = q_it78->first;
        q[B__BROKER_ID] = 0;
        double var73 = 1;
        double var74 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it58 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end57 = 
            ASKS.end();
        for (; ASKS_it58 != ASKS_end57; ++ASKS_it58)
        {
            double protect_A__T = get<0>(*ASKS_it58);
            int protect_A__ID = get<1>(*ASKS_it58);
            int protect_A__BROKER_ID = get<2>(*ASKS_it58);
            double protect_A__P = get<3>(*ASKS_it58);
            double protect_A__V = get<4>(*ASKS_it58);
            double var76 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it54 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end53 
                = ASKS.end();
            for (; ASKS_it54 != ASKS_end53; ++ASKS_it54)
            {
                double A2__T = get<0>(*ASKS_it54);
                int A2__ID = get<1>(*ASKS_it54);
                int A2__BROKER_ID = get<2>(*ASKS_it54);
                double A2__P = get<3>(*ASKS_it54);
                double A2__V = get<4>(*ASKS_it54);
                double var77 = 0;
                var77 += protect_A__P;
                double var78 = 0;
                var78 += A2__P;
                if ( var77 < var78 )
                {
                    var76 += A2__V;
                }
            }
            double var79 = 0;
            double var80 = 1;
            var80 *= 0.25;
            double var81 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it56 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end55 
                = ASKS.end();
            for (; ASKS_it56 != ASKS_end55; ++ASKS_it56)
            {
                double A1__T = get<0>(*ASKS_it56);
                int A1__ID = get<1>(*ASKS_it56);
                int A1__BROKER_ID = get<2>(*ASKS_it56);
                double A1__P = get<3>(*ASKS_it56);
                double A1__V = get<4>(*ASKS_it56);
                var81 += A1__V;
            }
            var80 *= var81;
            var79 += var80;
            if ( var76 < var79 )
            {
                double var75 = 1;
                var75 *= protect_A__P;
                var75 *= protect_A__V;
                var74 += var75;
            }
        }
        var73 *= var74;
        double var82 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it64 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end63 = 
            BIDS.end();
        for (; BIDS_it64 != BIDS_end63; ++BIDS_it64)
        {
            double protect_B__T = get<0>(*BIDS_it64);
            int protect_B__ID = get<1>(*BIDS_it64);
            int protect_B__BROKER_ID = get<2>(*BIDS_it64);
            double protect_B__P = get<3>(*BIDS_it64);
            double protect_B__V = get<4>(*BIDS_it64);
            double var85 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it60 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end59 
                = BIDS.end();
            for (; BIDS_it60 != BIDS_end59; ++BIDS_it60)
            {
                double B2__T = get<0>(*BIDS_it60);
                int B2__ID = get<1>(*BIDS_it60);
                int B2__BROKER_ID = get<2>(*BIDS_it60);
                double B2__P = get<3>(*BIDS_it60);
                double B2__V = get<4>(*BIDS_it60);
                double var86 = 0;
                var86 += protect_B__P;
                double var87 = 0;
                var87 += B2__P;
                if ( var86 < var87 )
                {
                    var85 += B2__V;
                }
            }
            double var88 = 0;
            double var89 = 1;
            var89 *= 0.25;
            double var90 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it62 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end61 
                = BIDS.end();
            for (; BIDS_it62 != BIDS_end61; ++BIDS_it62)
            {
                double B1__T = get<0>(*BIDS_it62);
                int B1__ID = get<1>(*BIDS_it62);
                int B1__BROKER_ID = get<2>(*BIDS_it62);
                double B1__P = get<3>(*BIDS_it62);
                double B1__V = get<4>(*BIDS_it62);
                var90 += B1__V;
            }
            var89 *= var90;
            var88 += var89;
            if ( var85 < var88 )
            {
                int var83 = 0;
                var83 += B__BROKER_ID;
                int var84 = 0;
                var84 += protect_B__BROKER_ID;
                if ( var83 == var84 )
                {
                    var82 += 1;
                }
            }
        }
        var73 *= var82;
        q[B__BROKER_ID] += var73;
        double var91 = 1;
        double var92 = 0;
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
            double var93 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it66 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end65 
                = ASKS.end();
            for (; ASKS_it66 != ASKS_end65; ++ASKS_it66)
            {
                double A2__T = get<0>(*ASKS_it66);
                int A2__ID = get<1>(*ASKS_it66);
                int A2__BROKER_ID = get<2>(*ASKS_it66);
                double A2__P = get<3>(*ASKS_it66);
                double A2__V = get<4>(*ASKS_it66);
                double var94 = 0;
                var94 += protect_A__P;
                double var95 = 0;
                var95 += A2__P;
                if ( var94 < var95 )
                {
                    var93 += A2__V;
                }
            }
            double var96 = 0;
            double var97 = 1;
            var97 *= 0.25;
            double var98 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it68 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end67 
                = ASKS.end();
            for (; ASKS_it68 != ASKS_end67; ++ASKS_it68)
            {
                double A1__T = get<0>(*ASKS_it68);
                int A1__ID = get<1>(*ASKS_it68);
                int A1__BROKER_ID = get<2>(*ASKS_it68);
                double A1__P = get<3>(*ASKS_it68);
                double A1__V = get<4>(*ASKS_it68);
                var98 += A1__V;
            }
            var97 *= var98;
            var96 += var97;
            if ( var93 < var96 )
            {
                var92 += protect_A__P;
            }
        }
        var91 *= var92;
        var91 *= -1;
        double var99 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it76 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end75 = 
            BIDS.end();
        for (; BIDS_it76 != BIDS_end75; ++BIDS_it76)
        {
            double protect_B__T = get<0>(*BIDS_it76);
            int protect_B__ID = get<1>(*BIDS_it76);
            int protect_B__BROKER_ID = get<2>(*BIDS_it76);
            double protect_B__P = get<3>(*BIDS_it76);
            double protect_B__V = get<4>(*BIDS_it76);
            double var103 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it72 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end71 
                = BIDS.end();
            for (; BIDS_it72 != BIDS_end71; ++BIDS_it72)
            {
                double B2__T = get<0>(*BIDS_it72);
                int B2__ID = get<1>(*BIDS_it72);
                int B2__BROKER_ID = get<2>(*BIDS_it72);
                double B2__P = get<3>(*BIDS_it72);
                double B2__V = get<4>(*BIDS_it72);
                double var104 = 0;
                var104 += protect_B__P;
                double var105 = 0;
                var105 += B2__P;
                if ( var104 < var105 )
                {
                    var103 += B2__V;
                }
            }
            double var106 = 0;
            double var107 = 1;
            var107 *= 0.25;
            double var108 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it74 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end73 
                = BIDS.end();
            for (; BIDS_it74 != BIDS_end73; ++BIDS_it74)
            {
                double B1__T = get<0>(*BIDS_it74);
                int B1__ID = get<1>(*BIDS_it74);
                int B1__BROKER_ID = get<2>(*BIDS_it74);
                double B1__P = get<3>(*BIDS_it74);
                double B1__V = get<4>(*BIDS_it74);
                var108 += B1__V;
            }
            var107 *= var108;
            var106 += var107;
            if ( var103 < var106 )
            {
                int var101 = 0;
                var101 += B__BROKER_ID;
                int var102 = 0;
                var102 += protect_B__BROKER_ID;
                if ( var101 == var102 )
                {
                    double var100 = 1;
                    var100 *= protect_B__P;
                    var100 *= protect_B__V;
                    var99 += var100;
                }
            }
        }
        var91 *= var99;
        q[B__BROKER_ID] += var91;
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
    map<int,double>::iterator q_it104 = q.begin();
    map<int,double>::iterator q_end103 = q.end();
    for (; q_it104 != q_end103; ++q_it104)
    {
        int BROKER_ID = q_it104->first;
        q[BROKER_ID] = 0;
        double var109 = 1;
        double var110 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it84 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end83 = 
            ASKS.end();
        for (; ASKS_it84 != ASKS_end83; ++ASKS_it84)
        {
            double protect_A__T = get<0>(*ASKS_it84);
            int protect_A__ID = get<1>(*ASKS_it84);
            int protect_A__BROKER_ID = get<2>(*ASKS_it84);
            double protect_A__P = get<3>(*ASKS_it84);
            double protect_A__V = get<4>(*ASKS_it84);
            double var112 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it80 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end79 
                = ASKS.end();
            for (; ASKS_it80 != ASKS_end79; ++ASKS_it80)
            {
                double A2__T = get<0>(*ASKS_it80);
                int A2__ID = get<1>(*ASKS_it80);
                int A2__BROKER_ID = get<2>(*ASKS_it80);
                double A2__P = get<3>(*ASKS_it80);
                double A2__V = get<4>(*ASKS_it80);
                double var113 = 0;
                var113 += protect_A__P;
                double var114 = 0;
                var114 += A2__P;
                if ( var113 < var114 )
                {
                    var112 += A2__V;
                }
            }
            double var115 = 0;
            double var116 = 1;
            var116 *= 0.25;
            double var117 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it82 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end81 
                = ASKS.end();
            for (; ASKS_it82 != ASKS_end81; ++ASKS_it82)
            {
                double A1__T = get<0>(*ASKS_it82);
                int A1__ID = get<1>(*ASKS_it82);
                int A1__BROKER_ID = get<2>(*ASKS_it82);
                double A1__P = get<3>(*ASKS_it82);
                double A1__V = get<4>(*ASKS_it82);
                var117 += A1__V;
            }
            var116 *= var117;
            var115 += var116;
            if ( var112 < var115 )
            {
                double var111 = 1;
                var111 *= protect_A__P;
                var111 *= protect_A__V;
                var110 += var111;
            }
        }
        var109 *= var110;
        double var118 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it90 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end89 = 
            BIDS.end();
        for (; BIDS_it90 != BIDS_end89; ++BIDS_it90)
        {
            double protect_B__T = get<0>(*BIDS_it90);
            int protect_B__ID = get<1>(*BIDS_it90);
            int protect_B__BROKER_ID = get<2>(*BIDS_it90);
            double protect_B__P = get<3>(*BIDS_it90);
            double protect_B__V = get<4>(*BIDS_it90);
            double var121 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it86 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end85 
                = BIDS.end();
            for (; BIDS_it86 != BIDS_end85; ++BIDS_it86)
            {
                double B2__T = get<0>(*BIDS_it86);
                int B2__ID = get<1>(*BIDS_it86);
                int B2__BROKER_ID = get<2>(*BIDS_it86);
                double B2__P = get<3>(*BIDS_it86);
                double B2__V = get<4>(*BIDS_it86);
                double var122 = 0;
                var122 += protect_B__P;
                double var123 = 0;
                var123 += B2__P;
                if ( var122 < var123 )
                {
                    var121 += B2__V;
                }
            }
            double var124 = 0;
            double var125 = 1;
            var125 *= 0.25;
            double var126 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it88 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end87 
                = BIDS.end();
            for (; BIDS_it88 != BIDS_end87; ++BIDS_it88)
            {
                double B1__T = get<0>(*BIDS_it88);
                int B1__ID = get<1>(*BIDS_it88);
                int B1__BROKER_ID = get<2>(*BIDS_it88);
                double B1__P = get<3>(*BIDS_it88);
                double B1__V = get<4>(*BIDS_it88);
                var126 += B1__V;
            }
            var125 *= var126;
            var124 += var125;
            if ( var121 < var124 )
            {
                int var119 = 0;
                var119 += BROKER_ID;
                int var120 = 0;
                var120 += protect_B__BROKER_ID;
                if ( var119 == var120 )
                {
                    var118 += 1;
                }
            }
        }
        var109 *= var118;
        q[BROKER_ID] += var109;
        double var127 = 1;
        double var128 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it96 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end95 = 
            ASKS.end();
        for (; ASKS_it96 != ASKS_end95; ++ASKS_it96)
        {
            double protect_A__T = get<0>(*ASKS_it96);
            int protect_A__ID = get<1>(*ASKS_it96);
            int protect_A__BROKER_ID = get<2>(*ASKS_it96);
            double protect_A__P = get<3>(*ASKS_it96);
            double protect_A__V = get<4>(*ASKS_it96);
            double var129 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it92 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end91 
                = ASKS.end();
            for (; ASKS_it92 != ASKS_end91; ++ASKS_it92)
            {
                double A2__T = get<0>(*ASKS_it92);
                int A2__ID = get<1>(*ASKS_it92);
                int A2__BROKER_ID = get<2>(*ASKS_it92);
                double A2__P = get<3>(*ASKS_it92);
                double A2__V = get<4>(*ASKS_it92);
                double var130 = 0;
                var130 += protect_A__P;
                double var131 = 0;
                var131 += A2__P;
                if ( var130 < var131 )
                {
                    var129 += A2__V;
                }
            }
            double var132 = 0;
            double var133 = 1;
            var133 *= 0.25;
            double var134 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator ASKS_it94 = ASKS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator ASKS_end93 
                = ASKS.end();
            for (; ASKS_it94 != ASKS_end93; ++ASKS_it94)
            {
                double A1__T = get<0>(*ASKS_it94);
                int A1__ID = get<1>(*ASKS_it94);
                int A1__BROKER_ID = get<2>(*ASKS_it94);
                double A1__P = get<3>(*ASKS_it94);
                double A1__V = get<4>(*ASKS_it94);
                var134 += A1__V;
            }
            var133 *= var134;
            var132 += var133;
            if ( var129 < var132 )
            {
                var128 += protect_A__P;
            }
        }
        var127 *= var128;
        var127 *= -1;
        double var135 = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it102 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end101 = 
            BIDS.end();
        for (; BIDS_it102 != BIDS_end101; ++BIDS_it102)
        {
            double protect_B__T = get<0>(*BIDS_it102);
            int protect_B__ID = get<1>(*BIDS_it102);
            int protect_B__BROKER_ID = get<2>(*BIDS_it102);
            double protect_B__P = get<3>(*BIDS_it102);
            double protect_B__V = get<4>(*BIDS_it102);
            double var139 = 0;
            multiset<tuple<double,int,int,double,
                double> >::iterator BIDS_it98 = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end97 
                = BIDS.end();
            for (; BIDS_it98 != BIDS_end97; ++BIDS_it98)
            {
                double B2__T = get<0>(*BIDS_it98);
                int B2__ID = get<1>(*BIDS_it98);
                int B2__BROKER_ID = get<2>(*BIDS_it98);
                double B2__P = get<3>(*BIDS_it98);
                double B2__V = get<4>(*BIDS_it98);
                double var140 = 0;
                var140 += protect_B__P;
                double var141 = 0;
                var141 += B2__P;
                if ( var140 < var141 )
                {
                    var139 += B2__V;
                }
            }
            double var142 = 0;
            double var143 = 1;
            var143 *= 0.25;
            double var144 = 0;
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_it100 
                = BIDS.begin();
            multiset<tuple<double,int,int,double,double> >::iterator BIDS_end99 
                = BIDS.end();
            for (; BIDS_it100 != BIDS_end99; ++BIDS_it100)
            {
                double B1__T = get<0>(*BIDS_it100);
                int B1__ID = get<1>(*BIDS_it100);
                int B1__BROKER_ID = get<2>(*BIDS_it100);
                double B1__P = get<3>(*BIDS_it100);
                double B1__V = get<4>(*BIDS_it100);
                var144 += B1__V;
            }
            var143 *= var144;
            var142 += var143;
            if ( var139 < var142 )
            {
                int var137 = 0;
                var137 += BROKER_ID;
                int var138 = 0;
                var138 += protect_B__BROKER_ID;
                if ( var137 == var138 )
                {
                    double var136 = 1;
                    var136 *= protect_B__P;
                    var136 *= protect_B__V;
                    var135 += var136;
                }
            }
        }
        var127 *= var135;
        q[BROKER_ID] += var127;
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
