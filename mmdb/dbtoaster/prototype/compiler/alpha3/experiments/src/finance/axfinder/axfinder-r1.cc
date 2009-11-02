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
map<tuple<int,double>,int> qASKS1;
map<tuple<int,double>,double> qASKS2;
map<tuple<int,double>,int> qASKS3;
map<tuple<int,double>,double> qASKS4;
map<int,double> q;
map<tuple<int,double>,double> qBIDS1;
map<tuple<int,double>,int> qBIDS2;
map<tuple<int,double>,double> qBIDS3;
map<tuple<int,double>,int> qBIDS4;

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

   cout << "qASKS1 size: " << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qASKS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qASKS1" << "," << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qASKS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

   cout << "qASKS2 size: " << (((sizeof(map<tuple<int,double>,double>::key_type)
       + sizeof(map<tuple<int,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qASKS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,double>::key_type, map<tuple<int,double>,double>::value_type, _Select1st<map<tuple<int,double>,double>::value_type>, map<tuple<int,double>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qASKS2" << "," << (((sizeof(map<tuple<int,double>,double>::key_type)
       + sizeof(map<tuple<int,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qASKS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,double>::key_type, map<tuple<int,double>,double>::value_type, _Select1st<map<tuple<int,double>,double>::value_type>, map<tuple<int,double>,double>::key_compare>))) << endl;

   cout << "qASKS3 size: " << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qASKS3.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qASKS3" << "," << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qASKS3.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

   cout << "qASKS4 size: " << (((sizeof(map<tuple<int,double>,double>::key_type)
       + sizeof(map<tuple<int,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qASKS4.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,double>::key_type, map<tuple<int,double>,double>::value_type, _Select1st<map<tuple<int,double>,double>::value_type>, map<tuple<int,double>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qASKS4" << "," << (((sizeof(map<tuple<int,double>,double>::key_type)
       + sizeof(map<tuple<int,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qASKS4.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,double>::key_type, map<tuple<int,double>,double>::value_type, _Select1st<map<tuple<int,double>,double>::value_type>, map<tuple<int,double>,double>::key_compare>))) << endl;

   cout << "q size: " << (((sizeof(map<int,double>::key_type)
       + sizeof(map<int,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int,double>::key_type, map<int,double>::value_type, _Select1st<map<int,double>::value_type>, map<int,double>::key_compare>))) << endl;

   (*stats) << "m," << "q" << "," << (((sizeof(map<int,double>::key_type)
       + sizeof(map<int,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int,double>::key_type, map<int,double>::value_type, _Select1st<map<int,double>::value_type>, map<int,double>::key_compare>))) << endl;

   cout << "qBIDS1 size: " << (((sizeof(map<tuple<int,double>,double>::key_type)
       + sizeof(map<tuple<int,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,double>::key_type, map<tuple<int,double>,double>::value_type, _Select1st<map<tuple<int,double>,double>::value_type>, map<tuple<int,double>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS1" << "," << (((sizeof(map<tuple<int,double>,double>::key_type)
       + sizeof(map<tuple<int,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,double>::key_type, map<tuple<int,double>,double>::value_type, _Select1st<map<tuple<int,double>,double>::value_type>, map<tuple<int,double>,double>::key_compare>))) << endl;

   cout << "qBIDS2 size: " << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS2" << "," << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

   cout << "qBIDS3 size: " << (((sizeof(map<tuple<int,double>,double>::key_type)
       + sizeof(map<tuple<int,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS3.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,double>::key_type, map<tuple<int,double>,double>::value_type, _Select1st<map<tuple<int,double>,double>::value_type>, map<tuple<int,double>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS3" << "," << (((sizeof(map<tuple<int,double>,double>::key_type)
       + sizeof(map<tuple<int,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS3.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,double>::key_type, map<tuple<int,double>,double>::value_type, _Select1st<map<tuple<int,double>,double>::value_type>, map<tuple<int,double>,double>::key_compare>))) << endl;

   cout << "qBIDS4 size: " << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS4.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS4" << "," << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS4.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

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
    q[BROKER_ID] += -1*qASKS2[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*qASKS4[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += V*qASKS1[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += V*qASKS3[make_tuple(BROKER_ID,P)];
    map<tuple<int,double>,double>::iterator qBIDS1_it4 = qBIDS1.begin();
    map<tuple<int,double>,double>::iterator qBIDS1_end3 = qBIDS1.end();
    for (; qBIDS1_it4 != qBIDS1_end3; ++qBIDS1_it4)
    {
        int x_qBIDS_B__BROKER_ID = get<0>(qBIDS1_it4->first);
        double x_qBIDS_B__P = get<1>(qBIDS1_it4->first);
        qBIDS1[make_tuple(x_qBIDS_B__BROKER_ID,x_qBIDS_B__P)] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it2 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end1 = 
            ASKS.end();
        for (; ASKS_it2 != ASKS_end1; ++ASKS_it2)
        {
            double protect_A__T = get<0>(*ASKS_it2);
            int protect_A__ID = get<1>(*ASKS_it2);
            int protect_A__BROKER_ID = get<2>(*ASKS_it2);
            double protect_A__P = get<3>(*ASKS_it2);
            double protect_A__V = get<4>(*ASKS_it2);
            int var3 = 0;
            var3 += 1000;
            double var4 = 0;
            var4 += protect_A__P;
            double var5 = 1;
            var5 *= -1;
            var5 *= x_qBIDS_B__P;
            var4 += var5;
            if ( var3 < var4 )
            {
                int var1 = 0;
                var1 += x_qBIDS_B__BROKER_ID;
                int var2 = 0;
                var2 += protect_A__BROKER_ID;
                if ( var1 == var2 )
                {
                    qBIDS1[make_tuple(
                        x_qBIDS_B__BROKER_ID,x_qBIDS_B__P)] += protect_A__V;
                }
            }
        }
    }
    map<tuple<int,double>,int>::iterator qBIDS2_it8 = qBIDS2.begin();
    map<tuple<int,double>,int>::iterator qBIDS2_end7 = qBIDS2.end();
    for (; qBIDS2_it8 != qBIDS2_end7; ++qBIDS2_it8)
    {
        int x_qBIDS_B__BROKER_ID = get<0>(qBIDS2_it8->first);
        double x_qBIDS_B__P = get<1>(qBIDS2_it8->first);
        qBIDS2[make_tuple(x_qBIDS_B__BROKER_ID,x_qBIDS_B__P)] = 0;
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
            int var8 = 0;
            var8 += 1000;
            double var9 = 0;
            var9 += protect_A__P;
            double var10 = 1;
            var10 *= -1;
            var10 *= x_qBIDS_B__P;
            var9 += var10;
            if ( var8 < var9 )
            {
                int var6 = 0;
                var6 += x_qBIDS_B__BROKER_ID;
                int var7 = 0;
                var7 += protect_A__BROKER_ID;
                if ( var6 == var7 )
                {
                    qBIDS2[make_tuple(x_qBIDS_B__BROKER_ID,x_qBIDS_B__P)] += 1;
                }
            }
        }
    }
    map<tuple<int,double>,double>::iterator qBIDS3_it12 = qBIDS3.begin();
    map<tuple<int,double>,double>::iterator qBIDS3_end11 = qBIDS3.end();
    for (; qBIDS3_it12 != qBIDS3_end11; ++qBIDS3_it12)
    {
        int x_qBIDS_B__BROKER_ID = get<0>(qBIDS3_it12->first);
        double x_qBIDS_B__P = get<1>(qBIDS3_it12->first);
        qBIDS3[make_tuple(x_qBIDS_B__BROKER_ID,x_qBIDS_B__P)] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it10 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end9 = 
            ASKS.end();
        for (; ASKS_it10 != ASKS_end9; ++ASKS_it10)
        {
            double protect_A__T = get<0>(*ASKS_it10);
            int protect_A__ID = get<1>(*ASKS_it10);
            int protect_A__BROKER_ID = get<2>(*ASKS_it10);
            double protect_A__P = get<3>(*ASKS_it10);
            double protect_A__V = get<4>(*ASKS_it10);
            int var13 = 0;
            var13 += 1000;
            double var14 = 0;
            var14 += x_qBIDS_B__P;
            double var15 = 1;
            var15 *= -1;
            var15 *= protect_A__P;
            var14 += var15;
            if ( var13 < var14 )
            {
                int var11 = 0;
                var11 += x_qBIDS_B__BROKER_ID;
                int var12 = 0;
                var12 += protect_A__BROKER_ID;
                if ( var11 == var12 )
                {
                    qBIDS3[make_tuple(
                        x_qBIDS_B__BROKER_ID,x_qBIDS_B__P)] += protect_A__V;
                }
            }
        }
    }
    map<tuple<int,double>,int>::iterator qBIDS4_it16 = qBIDS4.begin();
    map<tuple<int,double>,int>::iterator qBIDS4_end15 = qBIDS4.end();
    for (; qBIDS4_it16 != qBIDS4_end15; ++qBIDS4_it16)
    {
        int x_qBIDS_B__BROKER_ID = get<0>(qBIDS4_it16->first);
        double x_qBIDS_B__P = get<1>(qBIDS4_it16->first);
        qBIDS4[make_tuple(x_qBIDS_B__BROKER_ID,x_qBIDS_B__P)] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it14 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end13 = 
            ASKS.end();
        for (; ASKS_it14 != ASKS_end13; ++ASKS_it14)
        {
            double protect_A__T = get<0>(*ASKS_it14);
            int protect_A__ID = get<1>(*ASKS_it14);
            int protect_A__BROKER_ID = get<2>(*ASKS_it14);
            double protect_A__P = get<3>(*ASKS_it14);
            double protect_A__V = get<4>(*ASKS_it14);
            int var18 = 0;
            var18 += 1000;
            double var19 = 0;
            var19 += x_qBIDS_B__P;
            double var20 = 1;
            var20 *= -1;
            var20 *= protect_A__P;
            var19 += var20;
            if ( var18 < var19 )
            {
                int var16 = 0;
                var16 += x_qBIDS_B__BROKER_ID;
                int var17 = 0;
                var17 += protect_A__BROKER_ID;
                if ( var16 == var17 )
                {
                    qBIDS4[make_tuple(x_qBIDS_B__BROKER_ID,x_qBIDS_B__P)] += 1;
                }
            }
        }
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
    q[BROKER_ID] += qBIDS1[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += qBIDS3[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*V*qBIDS2[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*V*qBIDS4[make_tuple(BROKER_ID,P)];
    map<tuple<int,double>,int>::iterator qASKS1_it20 = qASKS1.begin();
    map<tuple<int,double>,int>::iterator qASKS1_end19 = qASKS1.end();
    for (; qASKS1_it20 != qASKS1_end19; ++qASKS1_it20)
    {
        int x_qASKS_A__BROKER_ID = get<0>(qASKS1_it20->first);
        double x_qASKS_A__P = get<1>(qASKS1_it20->first);
        qASKS1[make_tuple(x_qASKS_A__BROKER_ID,x_qASKS_A__P)] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it18 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end17 = 
            BIDS.end();
        for (; BIDS_it18 != BIDS_end17; ++BIDS_it18)
        {
            double protect_B__T = get<0>(*BIDS_it18);
            int protect_B__ID = get<1>(*BIDS_it18);
            int protect_B__BROKER_ID = get<2>(*BIDS_it18);
            double protect_B__P = get<3>(*BIDS_it18);
            double protect_B__V = get<4>(*BIDS_it18);
            int var23 = 0;
            var23 += 1000;
            double var24 = 0;
            var24 += x_qASKS_A__P;
            double var25 = 1;
            var25 *= -1;
            var25 *= protect_B__P;
            var24 += var25;
            if ( var23 < var24 )
            {
                int var21 = 0;
                var21 += x_qASKS_A__BROKER_ID;
                int var22 = 0;
                var22 += protect_B__BROKER_ID;
                if ( var21 == var22 )
                {
                    qASKS1[make_tuple(x_qASKS_A__BROKER_ID,x_qASKS_A__P)] += 1;
                }
            }
        }
    }
    map<tuple<int,double>,double>::iterator qASKS2_it24 = qASKS2.begin();
    map<tuple<int,double>,double>::iterator qASKS2_end23 = qASKS2.end();
    for (; qASKS2_it24 != qASKS2_end23; ++qASKS2_it24)
    {
        int x_qASKS_A__BROKER_ID = get<0>(qASKS2_it24->first);
        double x_qASKS_A__P = get<1>(qASKS2_it24->first);
        qASKS2[make_tuple(x_qASKS_A__BROKER_ID,x_qASKS_A__P)] = 0;
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
            int var28 = 0;
            var28 += 1000;
            double var29 = 0;
            var29 += x_qASKS_A__P;
            double var30 = 1;
            var30 *= -1;
            var30 *= protect_B__P;
            var29 += var30;
            if ( var28 < var29 )
            {
                int var26 = 0;
                var26 += x_qASKS_A__BROKER_ID;
                int var27 = 0;
                var27 += protect_B__BROKER_ID;
                if ( var26 == var27 )
                {
                    qASKS2[make_tuple(
                        x_qASKS_A__BROKER_ID,x_qASKS_A__P)] += protect_B__V;
                }
            }
        }
    }
    map<tuple<int,double>,int>::iterator qASKS3_it28 = qASKS3.begin();
    map<tuple<int,double>,int>::iterator qASKS3_end27 = qASKS3.end();
    for (; qASKS3_it28 != qASKS3_end27; ++qASKS3_it28)
    {
        int x_qASKS_A__BROKER_ID = get<0>(qASKS3_it28->first);
        double x_qASKS_A__P = get<1>(qASKS3_it28->first);
        qASKS3[make_tuple(x_qASKS_A__BROKER_ID,x_qASKS_A__P)] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it26 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end25 = 
            BIDS.end();
        for (; BIDS_it26 != BIDS_end25; ++BIDS_it26)
        {
            double protect_B__T = get<0>(*BIDS_it26);
            int protect_B__ID = get<1>(*BIDS_it26);
            int protect_B__BROKER_ID = get<2>(*BIDS_it26);
            double protect_B__P = get<3>(*BIDS_it26);
            double protect_B__V = get<4>(*BIDS_it26);
            int var33 = 0;
            var33 += 1000;
            double var34 = 0;
            var34 += protect_B__P;
            double var35 = 1;
            var35 *= -1;
            var35 *= x_qASKS_A__P;
            var34 += var35;
            if ( var33 < var34 )
            {
                int var31 = 0;
                var31 += x_qASKS_A__BROKER_ID;
                int var32 = 0;
                var32 += protect_B__BROKER_ID;
                if ( var31 == var32 )
                {
                    qASKS3[make_tuple(x_qASKS_A__BROKER_ID,x_qASKS_A__P)] += 1;
                }
            }
        }
    }
    map<tuple<int,double>,double>::iterator qASKS4_it32 = qASKS4.begin();
    map<tuple<int,double>,double>::iterator qASKS4_end31 = qASKS4.end();
    for (; qASKS4_it32 != qASKS4_end31; ++qASKS4_it32)
    {
        int x_qASKS_A__BROKER_ID = get<0>(qASKS4_it32->first);
        double x_qASKS_A__P = get<1>(qASKS4_it32->first);
        qASKS4[make_tuple(x_qASKS_A__BROKER_ID,x_qASKS_A__P)] = 0;
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
            int var38 = 0;
            var38 += 1000;
            double var39 = 0;
            var39 += protect_B__P;
            double var40 = 1;
            var40 *= -1;
            var40 *= x_qASKS_A__P;
            var39 += var40;
            if ( var38 < var39 )
            {
                int var36 = 0;
                var36 += x_qASKS_A__BROKER_ID;
                int var37 = 0;
                var37 += protect_B__BROKER_ID;
                if ( var36 == var37 )
                {
                    qASKS4[make_tuple(
                        x_qASKS_A__BROKER_ID,x_qASKS_A__P)] += protect_B__V;
                }
            }
        }
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
    q[BROKER_ID] += -1*-1*qASKS2[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*-1*qASKS4[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*V*qASKS1[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*V*qASKS3[make_tuple(BROKER_ID,P)];
    map<tuple<int,double>,double>::iterator qBIDS1_it36 = qBIDS1.begin();
    map<tuple<int,double>,double>::iterator qBIDS1_end35 = qBIDS1.end();
    for (; qBIDS1_it36 != qBIDS1_end35; ++qBIDS1_it36)
    {
        int x_qBIDS_B__BROKER_ID = get<0>(qBIDS1_it36->first);
        double x_qBIDS_B__P = get<1>(qBIDS1_it36->first);
        qBIDS1[make_tuple(x_qBIDS_B__BROKER_ID,x_qBIDS_B__P)] = 0;
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
            int var43 = 0;
            var43 += 1000;
            double var44 = 0;
            var44 += protect_A__P;
            double var45 = 1;
            var45 *= -1;
            var45 *= x_qBIDS_B__P;
            var44 += var45;
            if ( var43 < var44 )
            {
                int var41 = 0;
                var41 += x_qBIDS_B__BROKER_ID;
                int var42 = 0;
                var42 += protect_A__BROKER_ID;
                if ( var41 == var42 )
                {
                    qBIDS1[make_tuple(
                        x_qBIDS_B__BROKER_ID,x_qBIDS_B__P)] += protect_A__V;
                }
            }
        }
    }
    map<tuple<int,double>,int>::iterator qBIDS2_it40 = qBIDS2.begin();
    map<tuple<int,double>,int>::iterator qBIDS2_end39 = qBIDS2.end();
    for (; qBIDS2_it40 != qBIDS2_end39; ++qBIDS2_it40)
    {
        int x_qBIDS_B__BROKER_ID = get<0>(qBIDS2_it40->first);
        double x_qBIDS_B__P = get<1>(qBIDS2_it40->first);
        qBIDS2[make_tuple(x_qBIDS_B__BROKER_ID,x_qBIDS_B__P)] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it38 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end37 = 
            ASKS.end();
        for (; ASKS_it38 != ASKS_end37; ++ASKS_it38)
        {
            double protect_A__T = get<0>(*ASKS_it38);
            int protect_A__ID = get<1>(*ASKS_it38);
            int protect_A__BROKER_ID = get<2>(*ASKS_it38);
            double protect_A__P = get<3>(*ASKS_it38);
            double protect_A__V = get<4>(*ASKS_it38);
            int var48 = 0;
            var48 += 1000;
            double var49 = 0;
            var49 += protect_A__P;
            double var50 = 1;
            var50 *= -1;
            var50 *= x_qBIDS_B__P;
            var49 += var50;
            if ( var48 < var49 )
            {
                int var46 = 0;
                var46 += x_qBIDS_B__BROKER_ID;
                int var47 = 0;
                var47 += protect_A__BROKER_ID;
                if ( var46 == var47 )
                {
                    qBIDS2[make_tuple(x_qBIDS_B__BROKER_ID,x_qBIDS_B__P)] += 1;
                }
            }
        }
    }
    map<tuple<int,double>,double>::iterator qBIDS3_it44 = qBIDS3.begin();
    map<tuple<int,double>,double>::iterator qBIDS3_end43 = qBIDS3.end();
    for (; qBIDS3_it44 != qBIDS3_end43; ++qBIDS3_it44)
    {
        int x_qBIDS_B__BROKER_ID = get<0>(qBIDS3_it44->first);
        double x_qBIDS_B__P = get<1>(qBIDS3_it44->first);
        qBIDS3[make_tuple(x_qBIDS_B__BROKER_ID,x_qBIDS_B__P)] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it42 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end41 = 
            ASKS.end();
        for (; ASKS_it42 != ASKS_end41; ++ASKS_it42)
        {
            double protect_A__T = get<0>(*ASKS_it42);
            int protect_A__ID = get<1>(*ASKS_it42);
            int protect_A__BROKER_ID = get<2>(*ASKS_it42);
            double protect_A__P = get<3>(*ASKS_it42);
            double protect_A__V = get<4>(*ASKS_it42);
            int var53 = 0;
            var53 += 1000;
            double var54 = 0;
            var54 += x_qBIDS_B__P;
            double var55 = 1;
            var55 *= -1;
            var55 *= protect_A__P;
            var54 += var55;
            if ( var53 < var54 )
            {
                int var51 = 0;
                var51 += x_qBIDS_B__BROKER_ID;
                int var52 = 0;
                var52 += protect_A__BROKER_ID;
                if ( var51 == var52 )
                {
                    qBIDS3[make_tuple(
                        x_qBIDS_B__BROKER_ID,x_qBIDS_B__P)] += protect_A__V;
                }
            }
        }
    }
    map<tuple<int,double>,int>::iterator qBIDS4_it48 = qBIDS4.begin();
    map<tuple<int,double>,int>::iterator qBIDS4_end47 = qBIDS4.end();
    for (; qBIDS4_it48 != qBIDS4_end47; ++qBIDS4_it48)
    {
        int x_qBIDS_B__BROKER_ID = get<0>(qBIDS4_it48->first);
        double x_qBIDS_B__P = get<1>(qBIDS4_it48->first);
        qBIDS4[make_tuple(x_qBIDS_B__BROKER_ID,x_qBIDS_B__P)] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it46 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end45 = 
            ASKS.end();
        for (; ASKS_it46 != ASKS_end45; ++ASKS_it46)
        {
            double protect_A__T = get<0>(*ASKS_it46);
            int protect_A__ID = get<1>(*ASKS_it46);
            int protect_A__BROKER_ID = get<2>(*ASKS_it46);
            double protect_A__P = get<3>(*ASKS_it46);
            double protect_A__V = get<4>(*ASKS_it46);
            int var58 = 0;
            var58 += 1000;
            double var59 = 0;
            var59 += x_qBIDS_B__P;
            double var60 = 1;
            var60 *= -1;
            var60 *= protect_A__P;
            var59 += var60;
            if ( var58 < var59 )
            {
                int var56 = 0;
                var56 += x_qBIDS_B__BROKER_ID;
                int var57 = 0;
                var57 += protect_A__BROKER_ID;
                if ( var56 == var57 )
                {
                    qBIDS4[make_tuple(x_qBIDS_B__BROKER_ID,x_qBIDS_B__P)] += 1;
                }
            }
        }
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
    q[BROKER_ID] += -1*-1*V*qBIDS2[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*-1*V*qBIDS4[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*qBIDS1[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*qBIDS3[make_tuple(BROKER_ID,P)];
    map<tuple<int,double>,int>::iterator qASKS1_it52 = qASKS1.begin();
    map<tuple<int,double>,int>::iterator qASKS1_end51 = qASKS1.end();
    for (; qASKS1_it52 != qASKS1_end51; ++qASKS1_it52)
    {
        int x_qASKS_A__BROKER_ID = get<0>(qASKS1_it52->first);
        double x_qASKS_A__P = get<1>(qASKS1_it52->first);
        qASKS1[make_tuple(x_qASKS_A__BROKER_ID,x_qASKS_A__P)] = 0;
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
            int var63 = 0;
            var63 += 1000;
            double var64 = 0;
            var64 += x_qASKS_A__P;
            double var65 = 1;
            var65 *= -1;
            var65 *= protect_B__P;
            var64 += var65;
            if ( var63 < var64 )
            {
                int var61 = 0;
                var61 += x_qASKS_A__BROKER_ID;
                int var62 = 0;
                var62 += protect_B__BROKER_ID;
                if ( var61 == var62 )
                {
                    qASKS1[make_tuple(x_qASKS_A__BROKER_ID,x_qASKS_A__P)] += 1;
                }
            }
        }
    }
    map<tuple<int,double>,double>::iterator qASKS2_it56 = qASKS2.begin();
    map<tuple<int,double>,double>::iterator qASKS2_end55 = qASKS2.end();
    for (; qASKS2_it56 != qASKS2_end55; ++qASKS2_it56)
    {
        int x_qASKS_A__BROKER_ID = get<0>(qASKS2_it56->first);
        double x_qASKS_A__P = get<1>(qASKS2_it56->first);
        qASKS2[make_tuple(x_qASKS_A__BROKER_ID,x_qASKS_A__P)] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it54 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end53 = 
            BIDS.end();
        for (; BIDS_it54 != BIDS_end53; ++BIDS_it54)
        {
            double protect_B__T = get<0>(*BIDS_it54);
            int protect_B__ID = get<1>(*BIDS_it54);
            int protect_B__BROKER_ID = get<2>(*BIDS_it54);
            double protect_B__P = get<3>(*BIDS_it54);
            double protect_B__V = get<4>(*BIDS_it54);
            int var68 = 0;
            var68 += 1000;
            double var69 = 0;
            var69 += x_qASKS_A__P;
            double var70 = 1;
            var70 *= -1;
            var70 *= protect_B__P;
            var69 += var70;
            if ( var68 < var69 )
            {
                int var66 = 0;
                var66 += x_qASKS_A__BROKER_ID;
                int var67 = 0;
                var67 += protect_B__BROKER_ID;
                if ( var66 == var67 )
                {
                    qASKS2[make_tuple(
                        x_qASKS_A__BROKER_ID,x_qASKS_A__P)] += protect_B__V;
                }
            }
        }
    }
    map<tuple<int,double>,int>::iterator qASKS3_it60 = qASKS3.begin();
    map<tuple<int,double>,int>::iterator qASKS3_end59 = qASKS3.end();
    for (; qASKS3_it60 != qASKS3_end59; ++qASKS3_it60)
    {
        int x_qASKS_A__BROKER_ID = get<0>(qASKS3_it60->first);
        double x_qASKS_A__P = get<1>(qASKS3_it60->first);
        qASKS3[make_tuple(x_qASKS_A__BROKER_ID,x_qASKS_A__P)] = 0;
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
            int var73 = 0;
            var73 += 1000;
            double var74 = 0;
            var74 += protect_B__P;
            double var75 = 1;
            var75 *= -1;
            var75 *= x_qASKS_A__P;
            var74 += var75;
            if ( var73 < var74 )
            {
                int var71 = 0;
                var71 += x_qASKS_A__BROKER_ID;
                int var72 = 0;
                var72 += protect_B__BROKER_ID;
                if ( var71 == var72 )
                {
                    qASKS3[make_tuple(x_qASKS_A__BROKER_ID,x_qASKS_A__P)] += 1;
                }
            }
        }
    }
    map<tuple<int,double>,double>::iterator qASKS4_it64 = qASKS4.begin();
    map<tuple<int,double>,double>::iterator qASKS4_end63 = qASKS4.end();
    for (; qASKS4_it64 != qASKS4_end63; ++qASKS4_it64)
    {
        int x_qASKS_A__BROKER_ID = get<0>(qASKS4_it64->first);
        double x_qASKS_A__P = get<1>(qASKS4_it64->first);
        qASKS4[make_tuple(x_qASKS_A__BROKER_ID,x_qASKS_A__P)] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it62 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end61 = 
            BIDS.end();
        for (; BIDS_it62 != BIDS_end61; ++BIDS_it62)
        {
            double protect_B__T = get<0>(*BIDS_it62);
            int protect_B__ID = get<1>(*BIDS_it62);
            int protect_B__BROKER_ID = get<2>(*BIDS_it62);
            double protect_B__P = get<3>(*BIDS_it62);
            double protect_B__V = get<4>(*BIDS_it62);
            int var78 = 0;
            var78 += 1000;
            double var79 = 0;
            var79 += protect_B__P;
            double var80 = 1;
            var80 *= -1;
            var80 *= x_qASKS_A__P;
            var79 += var80;
            if ( var78 < var79 )
            {
                int var76 = 0;
                var76 += x_qASKS_A__BROKER_ID;
                int var77 = 0;
                var77 += protect_B__BROKER_ID;
                if ( var76 == var77 )
                {
                    qASKS4[make_tuple(
                        x_qASKS_A__BROKER_ID,x_qASKS_A__P)] += protect_B__V;
                }
            }
        }
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
        if ( (tuple_counter % 10000) == 0 )
        {
            DBToaster::Profiler::reset_time_span_printing_global(
                "tuples", tuple_counter, 10000, tvs, tup_sec_span, tup_usec_span, "query", log);
            analyse_mem_usage(stats);
            analyse_handler_usage(stats);
        }
    }
    DBToaster::Profiler::reset_time_span_printing_global(
        "tuples", tuple_counter, (tuple_counter%10000), tvs, tup_sec_span, tup_usec_span, "query", log);
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