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
int q536;
int q494;
double q321;
int q322;
double q709;
double q667;
double q451;
int q668;
int q452;
double q283;
int q284;
int q710;
double q415;
int q416;
double q374;
int q375;
multiset<tuple<double,int,int,double,double> > BIDS;
double q53;
int q54;
double q163;
map<double,double> qBIDS1;
int q164;
double qBIDS2;
double qBIDS3;
map<double,int> qBIDS4;
double q632;
map<double,int> qBIDS5;
double q336;
int q633;
map<double,double> qBIDS6;
int q337;
double q592;
int q593;
double q16;
double q251;
double q125;
double q;
int q252;
int q17;
int q126;
double q550;
int q551;
double q0;
double q725;
int q1;
double q213;
int q726;
int q214;
double q688;
double q514;
double q472;
double q68;
int q689;
int q515;
int q473;
int q69;
double q177;
int q178;
double q302;
double q430;
int q303;
int q431;
double q648;
int q649;
double q266;
int q267;
double q395;
int q396;
multiset<tuple<double,int,int,double,double> > ASKS;
double q355;
int q356;
double q34;
int q35;
double q144;
int q145;
double q612;
int q613;
double q571;
int q572;
double q232;
int q233;
double q106;
int q107;
set<double> bigsum_A__V_dom;
set<double> bigsum_B__V_dom;
double q87;
double q195;
int q88;
int q196;
double q535;
double q493;

double on_insert_BIDS_sec_span = 0.0;
double on_insert_BIDS_usec_span = 0.0;
double on_insert_ASKS_sec_span = 0.0;
double on_insert_ASKS_usec_span = 0.0;
double on_delete_BIDS_sec_span = 0.0;
double on_delete_BIDS_usec_span = 0.0;
double on_delete_ASKS_sec_span = 0.0;
double on_delete_ASKS_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
   cout << "BIDS size: " << (((sizeof(multiset<tuple<double,int,int,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * BIDS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<double,int,int,double,double> >::key_type, multiset<tuple<double,int,int,double,double> >::value_type, _Identity<multiset<tuple<double,int,int,double,double> >::value_type>, multiset<tuple<double,int,int,double,double> >::key_compare>))) << endl;

   (*stats) << "m," << "BIDS" << "," << (((sizeof(multiset<tuple<double,int,int,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * BIDS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<double,int,int,double,double> >::key_type, multiset<tuple<double,int,int,double,double> >::value_type, _Identity<multiset<tuple<double,int,int,double,double> >::value_type>, multiset<tuple<double,int,int,double,double> >::key_compare>))) << endl;

   cout << "qBIDS1 size: " << (((sizeof(map<double,double>::key_type)
       + sizeof(map<double,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS1.size())  + (sizeof(struct _Rb_tree<map<double,double>::key_type, map<double,double>::value_type, _Select1st<map<double,double>::value_type>, map<double,double>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS1" << "," << (((sizeof(map<double,double>::key_type)
       + sizeof(map<double,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS1.size())  + (sizeof(struct _Rb_tree<map<double,double>::key_type, map<double,double>::value_type, _Select1st<map<double,double>::value_type>, map<double,double>::key_compare>))) << endl;

   cout << "qBIDS4 size: " << (((sizeof(map<double,int>::key_type)
       + sizeof(map<double,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS4.size())  + (sizeof(struct _Rb_tree<map<double,int>::key_type, map<double,int>::value_type, _Select1st<map<double,int>::value_type>, map<double,int>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS4" << "," << (((sizeof(map<double,int>::key_type)
       + sizeof(map<double,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS4.size())  + (sizeof(struct _Rb_tree<map<double,int>::key_type, map<double,int>::value_type, _Select1st<map<double,int>::value_type>, map<double,int>::key_compare>))) << endl;

   cout << "qBIDS5 size: " << (((sizeof(map<double,int>::key_type)
       + sizeof(map<double,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS5.size())  + (sizeof(struct _Rb_tree<map<double,int>::key_type, map<double,int>::value_type, _Select1st<map<double,int>::value_type>, map<double,int>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS5" << "," << (((sizeof(map<double,int>::key_type)
       + sizeof(map<double,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS5.size())  + (sizeof(struct _Rb_tree<map<double,int>::key_type, map<double,int>::value_type, _Select1st<map<double,int>::value_type>, map<double,int>::key_compare>))) << endl;

   cout << "qBIDS6 size: " << (((sizeof(map<double,double>::key_type)
       + sizeof(map<double,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS6.size())  + (sizeof(struct _Rb_tree<map<double,double>::key_type, map<double,double>::value_type, _Select1st<map<double,double>::value_type>, map<double,double>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS6" << "," << (((sizeof(map<double,double>::key_type)
       + sizeof(map<double,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS6.size())  + (sizeof(struct _Rb_tree<map<double,double>::key_type, map<double,double>::value_type, _Select1st<map<double,double>::value_type>, map<double,double>::key_compare>))) << endl;

   cout << "ASKS size: " << (((sizeof(multiset<tuple<double,int,int,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * ASKS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<double,int,int,double,double> >::key_type, multiset<tuple<double,int,int,double,double> >::value_type, _Identity<multiset<tuple<double,int,int,double,double> >::value_type>, multiset<tuple<double,int,int,double,double> >::key_compare>))) << endl;

   (*stats) << "m," << "ASKS" << "," << (((sizeof(multiset<tuple<double,int,int,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * ASKS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<double,int,int,double,double> >::key_type, multiset<tuple<double,int,int,double,double> >::value_type, _Identity<multiset<tuple<double,int,int,double,double> >::value_type>, multiset<tuple<double,int,int,double,double> >::key_compare>))) << endl;

   cout << "bigsum_A__V_dom size: " << (((sizeof(set<double>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_A__V_dom.size())  + (sizeof(struct _Rb_tree<set<double>::key_type, set<double>::value_type, _Identity<set<double>::value_type>, set<double>::key_compare>))) << endl;

   (*stats) << "m," << "bigsum_A__V_dom" << "," << (((sizeof(set<double>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_A__V_dom.size())  + (sizeof(struct _Rb_tree<set<double>::key_type, set<double>::value_type, _Identity<set<double>::value_type>, set<double>::key_compare>))) << endl;

   cout << "bigsum_B__V_dom size: " << (((sizeof(set<double>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_B__V_dom.size())  + (sizeof(struct _Rb_tree<set<double>::key_type, set<double>::value_type, _Identity<set<double>::value_type>, set<double>::key_compare>))) << endl;

   (*stats) << "m," << "bigsum_B__V_dom" << "," << (((sizeof(set<double>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_B__V_dom.size())  + (sizeof(struct _Rb_tree<set<double>::key_type, set<double>::value_type, _Identity<set<double>::value_type>, set<double>::key_compare>))) << endl;

}

void analyse_handler_usage(ofstream* stats)
{
   cout << "on_insert_BIDS cost: " << (on_insert_BIDS_sec_span + (on_insert_BIDS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_BIDS" << "," << (on_insert_BIDS_sec_span + (on_insert_BIDS_usec_span / 1000000.0)) << endl;
   cout << "on_insert_ASKS cost: " << (on_insert_ASKS_sec_span + (on_insert_ASKS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_ASKS" << "," << (on_insert_ASKS_sec_span + (on_insert_ASKS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_BIDS cost: " << (on_delete_BIDS_sec_span + (on_delete_BIDS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_BIDS" << "," << (on_delete_BIDS_sec_span + (on_delete_BIDS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_ASKS cost: " << (on_delete_ASKS_sec_span + (on_delete_ASKS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_ASKS" << "," << (on_delete_ASKS_sec_span + (on_delete_ASKS_usec_span / 1000000.0)) << endl;
}


void on_insert_BIDS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    bigsum_B__V_dom.insert(V);
    BIDS.insert(make_tuple(T,ID,BROKER_ID,P,V));
    q0 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it2 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end1 = bigsum_A__V_dom.end();
    for (; bigsum_A__V_dom_it2 != bigsum_A__V_dom_end1; ++bigsum_A__V_dom_it2)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it2;
        q0 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q1 = 0;
    set<double>::iterator bigsum_B__V_dom_it4 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end3 = bigsum_B__V_dom.end();
    for (; bigsum_B__V_dom_it4 != bigsum_B__V_dom_end3; ++bigsum_B__V_dom_it4)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it4;
        q1 += ( ( 0.0001*qBIDS3+0.0001*P < bigsum_B__V )?
             ( ( ( V == bigsum_B__V )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q += q0*q1;
    q106 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it6 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end5 = bigsum_B__V_dom.end();
    for (; bigsum_B__V_dom_it6 != bigsum_B__V_dom_end5; ++bigsum_B__V_dom_it6)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it6;
        q106 += ( ( ( 0.0001*qBIDS3+0.0001*P < bigsum_B__V ) && (
             bigsum_B__V <= 0.0001*qBIDS3 ) )? ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q107 = 0;
    set<double>::iterator bigsum_A__V_dom_it8 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end7 = bigsum_A__V_dom.end();
    for (; bigsum_A__V_dom_it8 != bigsum_A__V_dom_end7; ++bigsum_A__V_dom_it8)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it8;
        q107 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*q106*q107;
    q125 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it10 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end9 = bigsum_B__V_dom.end();
    for (; bigsum_B__V_dom_it10 != bigsum_B__V_dom_end9; ++bigsum_B__V_dom_it10)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it10;
        q125 += ( ( 0.0001*qBIDS3+0.0001*P < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q126 = 0;
    set<double>::iterator bigsum_A__V_dom_it12 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end11 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it12 != bigsum_A__V_dom_end11; ++bigsum_A__V_dom_it12)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it12;
        q126 += ( ( ( bigsum_A__V < 0.0001*qBIDS2 ) && (
             0.0001*qBIDS2 <= bigsum_A__V ) )? ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*q125*q126;
    q144 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it14 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end13 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it14 != bigsum_B__V_dom_end13; ++bigsum_B__V_dom_it14)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it14;
        q144 += ( ( ( bigsum_B__V <= 0.0001*qBIDS3+0.0001*P ) && (
             0.0001*qBIDS3 < bigsum_B__V ) )? ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q145 = 0;
    set<double>::iterator bigsum_A__V_dom_it16 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end15 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it16 != bigsum_A__V_dom_end15; ++bigsum_A__V_dom_it16)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it16;
        q145 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*q144*q145;
    q16 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it18 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end17 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it18 != bigsum_A__V_dom_end17; ++bigsum_A__V_dom_it18)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it18;
        q16 += ( ( ( bigsum_A__V < 0.0001*qBIDS2 ) && (
             0.0001*qBIDS2 <= bigsum_A__V ) )? ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q17 = 0;
    set<double>::iterator bigsum_B__V_dom_it20 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end19 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it20 != bigsum_B__V_dom_end19; ++bigsum_B__V_dom_it20)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it20;
        q17 += ( ( 0.0001*qBIDS3+0.0001*P < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += q16*q17;
    q163 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it22 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end21 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it22 != bigsum_B__V_dom_end21; ++bigsum_B__V_dom_it22)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it22;
        q163 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q164 = 0;
    set<double>::iterator bigsum_A__V_dom_it24 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end23 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it24 != bigsum_A__V_dom_end23; ++bigsum_A__V_dom_it24)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it24;
        q164 += ( ( ( 0.0001*qBIDS2 <= bigsum_A__V ) && (
             bigsum_A__V < 0.0001*qBIDS2 ) )? ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*q163*q164;
    q34 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it26 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end25 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it26 != bigsum_A__V_dom_end25; ++bigsum_A__V_dom_it26)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it26;
        q34 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q35 = 0;
    set<double>::iterator bigsum_B__V_dom_it28 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end27 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it28 != bigsum_B__V_dom_end27; ++bigsum_B__V_dom_it28)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it28;
        q35 += ( ( ( 0.0001*qBIDS3+0.0001*P < bigsum_B__V ) && (
             bigsum_B__V <= 0.0001*qBIDS3 ) )? ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += q34*q35;
    q53 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it30 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end29 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it30 != bigsum_A__V_dom_end29; ++bigsum_A__V_dom_it30)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it30;
        q53 += ( ( ( 0.0001*qBIDS2 <= bigsum_A__V ) && (
             bigsum_A__V < 0.0001*qBIDS2 ) )? ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q54 = 0;
    set<double>::iterator bigsum_B__V_dom_it32 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end31 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it32 != bigsum_B__V_dom_end31; ++bigsum_B__V_dom_it32)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it32;
        q54 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*q53*q54;
    q68 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it34 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end33 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it34 != bigsum_A__V_dom_end33; ++bigsum_A__V_dom_it34)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it34;
        q68 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q69 = 0;
    set<double>::iterator bigsum_B__V_dom_it36 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end35 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it36 != bigsum_B__V_dom_end35; ++bigsum_B__V_dom_it36)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it36;
        q69 += ( ( ( bigsum_B__V <= 0.0001*qBIDS3+0.0001*P ) && (
             0.0001*qBIDS3 < bigsum_B__V ) )? ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*q68*q69;
    q87 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it38 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end37 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it38 != bigsum_B__V_dom_end37; ++bigsum_B__V_dom_it38)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it38;
        q87 += ( ( 0.0001*qBIDS3+0.0001*P < bigsum_B__V )?
             ( P*( ( V == bigsum_B__V )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q88 = 0;
    set<double>::iterator bigsum_A__V_dom_it40 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end39 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it40 != bigsum_A__V_dom_end39; ++bigsum_A__V_dom_it40)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it40;
        q88 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*q87*q88;
    qBIDS3 = 0;
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
        qBIDS3 += B1__P;
    }
    map<double,int>::iterator qBIDS4_it46 = qBIDS4.begin();
    map<double,int>::iterator qBIDS4_end45 = qBIDS4.end();
    for (; qBIDS4_it46 != qBIDS4_end45; ++qBIDS4_it46)
    {
        double bigsum_B__V = qBIDS4_it46->first;
        qBIDS4[V] = 0;
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
            double var1 = 0;
            var1 += V;
            double var2 = 0;
            var2 += protect_B__V;
            if ( var1 == var2 )
            {
                qBIDS4[V] += 1;
            }
        }
    }
    map<double,double>::iterator qBIDS6_it50 = qBIDS6.begin();
    map<double,double>::iterator qBIDS6_end49 = qBIDS6.end();
    for (; qBIDS6_it50 != qBIDS6_end49; ++qBIDS6_it50)
    {
        double bigsum_B__V = qBIDS6_it50->first;
        qBIDS6[V] = 0;
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
            double var3 = 0;
            var3 += V;
            double var4 = 0;
            var4 += protect_B__V;
            if ( var3 == var4 )
            {
                qBIDS6[V] += protect_B__P;
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_BIDS_sec_span, on_insert_BIDS_usec_span);
    (*results) << "on_insert_BIDS" << "," << q << endl;
}

void on_insert_ASKS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    bigsum_A__V_dom.insert(V);
    ASKS.insert(make_tuple(T,ID,BROKER_ID,P,V));
    q177 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it52 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end51 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it52 != bigsum_A__V_dom_end51; ++bigsum_A__V_dom_it52)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it52;
        q177 += ( ( bigsum_A__V < 0.0001*qBIDS2+0.0001*P )?
             ( P*( ( V == bigsum_A__V )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q178 = 0;
    set<double>::iterator bigsum_B__V_dom_it54 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end53 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it54 != bigsum_B__V_dom_end53; ++bigsum_B__V_dom_it54)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it54;
        q178 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += q177*q178;
    q195 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it56 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end55 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it56 != bigsum_A__V_dom_end55; ++bigsum_A__V_dom_it56)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it56;
        q195 += ( ( ( bigsum_A__V < 0.0001*qBIDS2+0.0001*P ) && (
             0.0001*qBIDS2 <= bigsum_A__V ) )? ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q196 = 0;
    set<double>::iterator bigsum_B__V_dom_it58 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end57 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it58 != bigsum_B__V_dom_end57; ++bigsum_B__V_dom_it58)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it58;
        q196 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += q195*q196;
    q213 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it60 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end59 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it60 != bigsum_A__V_dom_end59; ++bigsum_A__V_dom_it60)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it60;
        q213 += ( ( bigsum_A__V < 0.0001*qBIDS2+0.0001*P )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q214 = 0;
    set<double>::iterator bigsum_B__V_dom_it62 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end61 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it62 != bigsum_B__V_dom_end61; ++bigsum_B__V_dom_it62)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it62;
        q214 += ( ( ( 0.0001*qBIDS3 < bigsum_B__V ) && (
             bigsum_B__V <= 0.0001*qBIDS3 ) )? ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += q213*q214;
    q232 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it64 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end63 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it64 != bigsum_A__V_dom_end63; ++bigsum_A__V_dom_it64)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it64;
        q232 += ( ( ( 0.0001*qBIDS2+0.0001*P <= bigsum_A__V ) && (
             bigsum_A__V < 0.0001*qBIDS2 ) )? ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q233 = 0;
    set<double>::iterator bigsum_B__V_dom_it66 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end65 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it66 != bigsum_B__V_dom_end65; ++bigsum_B__V_dom_it66)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it66;
        q233 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*q232*q233;
    q251 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it68 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end67 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it68 != bigsum_A__V_dom_end67; ++bigsum_A__V_dom_it68)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it68;
        q251 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q252 = 0;
    set<double>::iterator bigsum_B__V_dom_it70 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end69 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it70 != bigsum_B__V_dom_end69; ++bigsum_B__V_dom_it70)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it70;
        q252 += ( ( ( bigsum_B__V <= 0.0001*qBIDS3 ) && (
             0.0001*qBIDS3 < bigsum_B__V ) )? ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*q251*q252;
    q266 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it72 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end71 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it72 != bigsum_B__V_dom_end71; ++bigsum_B__V_dom_it72)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it72;
        q266 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q267 = 0;
    set<double>::iterator bigsum_A__V_dom_it74 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end73 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it74 != bigsum_A__V_dom_end73; ++bigsum_A__V_dom_it74)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it74;
        q267 += ( ( bigsum_A__V < 0.0001*qBIDS2+0.0001*P )?
             ( ( ( V == bigsum_A__V )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q += -1*q266*q267;
    q283 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it76 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end75 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it76 != bigsum_B__V_dom_end75; ++bigsum_B__V_dom_it76)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it76;
        q283 += ( ( ( 0.0001*qBIDS3 < bigsum_B__V ) && (
             bigsum_B__V <= 0.0001*qBIDS3 ) )? ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q284 = 0;
    set<double>::iterator bigsum_A__V_dom_it78 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end77 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it78 != bigsum_A__V_dom_end77; ++bigsum_A__V_dom_it78)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it78;
        q284 += ( ( bigsum_A__V < 0.0001*qBIDS2+0.0001*P )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*q283*q284;
    q302 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it80 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end79 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it80 != bigsum_B__V_dom_end79; ++bigsum_B__V_dom_it80)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it80;
        q302 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q303 = 0;
    set<double>::iterator bigsum_A__V_dom_it82 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end81 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it82 != bigsum_A__V_dom_end81; ++bigsum_A__V_dom_it82)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it82;
        q303 += ( ( ( bigsum_A__V < 0.0001*qBIDS2+0.0001*P ) && (
             0.0001*qBIDS2 <= bigsum_A__V ) )? ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*q302*q303;
    q321 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it84 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end83 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it84 != bigsum_B__V_dom_end83; ++bigsum_B__V_dom_it84)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it84;
        q321 += ( ( ( bigsum_B__V <= 0.0001*qBIDS3 ) && (
             0.0001*qBIDS3 < bigsum_B__V ) )? ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q322 = 0;
    set<double>::iterator bigsum_A__V_dom_it86 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end85 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it86 != bigsum_A__V_dom_end85; ++bigsum_A__V_dom_it86)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it86;
        q322 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*q321*q322;
    q336 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it88 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end87 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it88 != bigsum_B__V_dom_end87; ++bigsum_B__V_dom_it88)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it88;
        q336 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q337 = 0;
    set<double>::iterator bigsum_A__V_dom_it90 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end89 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it90 != bigsum_A__V_dom_end89; ++bigsum_A__V_dom_it90)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it90;
        q337 += ( ( ( 0.0001*qBIDS2+0.0001*P <= bigsum_A__V ) && (
             bigsum_A__V < 0.0001*qBIDS2 ) )? ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*q336*q337;
    map<double,double>::iterator qBIDS1_it94 = qBIDS1.begin();
    map<double,double>::iterator qBIDS1_end93 = qBIDS1.end();
    for (; qBIDS1_it94 != qBIDS1_end93; ++qBIDS1_it94)
    {
        double bigsum_A__V = qBIDS1_it94->first;
        qBIDS1[V] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it92 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end91 = 
            ASKS.end();
        for (; ASKS_it92 != ASKS_end91; ++ASKS_it92)
        {
            double protect_A__T = get<0>(*ASKS_it92);
            int protect_A__ID = get<1>(*ASKS_it92);
            int protect_A__BROKER_ID = get<2>(*ASKS_it92);
            double protect_A__P = get<3>(*ASKS_it92);
            double protect_A__V = get<4>(*ASKS_it92);
            double var5 = 0;
            var5 += V;
            double var6 = 0;
            var6 += protect_A__V;
            if ( var5 == var6 )
            {
                qBIDS1[V] += protect_A__P;
            }
        }
    }
    qBIDS2 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_it96 = 
        ASKS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_end95 = 
        ASKS.end();
    for (; ASKS_it96 != ASKS_end95; ++ASKS_it96)
    {
        double A1__T = get<0>(*ASKS_it96);
        int A1__ID = get<1>(*ASKS_it96);
        int A1__BROKER_ID = get<2>(*ASKS_it96);
        double A1__P = get<3>(*ASKS_it96);
        double A1__V = get<4>(*ASKS_it96);
        qBIDS2 += A1__P;
    }
    map<double,int>::iterator qBIDS5_it100 = qBIDS5.begin();
    map<double,int>::iterator qBIDS5_end99 = qBIDS5.end();
    for (; qBIDS5_it100 != qBIDS5_end99; ++qBIDS5_it100)
    {
        double bigsum_A__V = qBIDS5_it100->first;
        qBIDS5[V] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it98 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end97 = 
            ASKS.end();
        for (; ASKS_it98 != ASKS_end97; ++ASKS_it98)
        {
            double protect_A__T = get<0>(*ASKS_it98);
            int protect_A__ID = get<1>(*ASKS_it98);
            int protect_A__BROKER_ID = get<2>(*ASKS_it98);
            double protect_A__P = get<3>(*ASKS_it98);
            double protect_A__V = get<4>(*ASKS_it98);
            double var7 = 0;
            var7 += V;
            double var8 = 0;
            var8 += protect_A__V;
            if ( var7 == var8 )
            {
                qBIDS5[V] += 1;
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_ASKS_sec_span, on_insert_ASKS_usec_span);
    (*results) << "on_insert_ASKS" << "," << q << endl;
}

void on_delete_BIDS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    BIDS.erase(make_tuple(T,ID,BROKER_ID,P,V));
    q355 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it102 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end101 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it102 != bigsum_A__V_dom_end101; ++bigsum_A__V_dom_it102)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it102;
        q355 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q356 = 0;
    set<double>::iterator bigsum_B__V_dom_it104 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end103 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it104 != bigsum_B__V_dom_end103; ++bigsum_B__V_dom_it104)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it104;
        q356 += ( ( 0.0001*qBIDS3+-1*0.0001*P < bigsum_B__V )?
             ( ( ( V == bigsum_B__V )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q += -1*q355*q356;
    q374 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it106 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end105 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it106 != bigsum_A__V_dom_end105; ++bigsum_A__V_dom_it106)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it106;
        q374 += ( ( ( bigsum_A__V < 0.0001*qBIDS2 ) && (
             0.0001*qBIDS2 <= bigsum_A__V ) )? ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q375 = 0;
    set<double>::iterator bigsum_B__V_dom_it108 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end107 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it108 != bigsum_B__V_dom_end107; ++bigsum_B__V_dom_it108)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it108;
        q375 += ( ( 0.0001*qBIDS3+-1*0.0001*P < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*-1*q374*q375;
    q395 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it110 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end109 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it110 != bigsum_A__V_dom_end109; ++bigsum_A__V_dom_it110)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it110;
        q395 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q396 = 0;
    set<double>::iterator bigsum_B__V_dom_it112 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end111 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it112 != bigsum_B__V_dom_end111; ++bigsum_B__V_dom_it112)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it112;
        q396 += ( ( ( 0.0001*qBIDS3+-1*0.0001*P < bigsum_B__V ) && (
             bigsum_B__V <= 0.0001*qBIDS3 ) )? ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*-1*q395*q396;
    q415 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it114 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end113 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it114 != bigsum_A__V_dom_end113; ++bigsum_A__V_dom_it114)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it114;
        q415 += ( ( ( 0.0001*qBIDS2 <= bigsum_A__V ) && (
             bigsum_A__V < 0.0001*qBIDS2 ) )? ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q416 = 0;
    set<double>::iterator bigsum_B__V_dom_it116 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end115 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it116 != bigsum_B__V_dom_end115; ++bigsum_B__V_dom_it116)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it116;
        q416 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*q415*q416;
    q430 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it118 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end117 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it118 != bigsum_A__V_dom_end117; ++bigsum_A__V_dom_it118)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it118;
        q430 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q431 = 0;
    set<double>::iterator bigsum_B__V_dom_it120 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end119 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it120 != bigsum_B__V_dom_end119; ++bigsum_B__V_dom_it120)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it120;
        q431 += ( ( ( bigsum_B__V <= 0.0001*qBIDS3+-1*0.0001*P ) && (
             0.0001*qBIDS3 < bigsum_B__V ) )? ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*q430*q431;
    q451 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it122 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end121 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it122 != bigsum_B__V_dom_end121; ++bigsum_B__V_dom_it122)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it122;
        q451 += ( ( 0.0001*qBIDS3+-1*0.0001*P < bigsum_B__V )?
             ( P*( ( V == bigsum_B__V )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q452 = 0;
    set<double>::iterator bigsum_A__V_dom_it124 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end123 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it124 != bigsum_A__V_dom_end123; ++bigsum_A__V_dom_it124)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it124;
        q452 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q451*q452;
    q472 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it126 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end125 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it126 != bigsum_B__V_dom_end125; ++bigsum_B__V_dom_it126)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it126;
        q472 += ( ( ( 0.0001*qBIDS3+-1*0.0001*P < bigsum_B__V ) && (
             bigsum_B__V <= 0.0001*qBIDS3 ) )? ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q473 = 0;
    set<double>::iterator bigsum_A__V_dom_it128 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end127 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it128 != bigsum_A__V_dom_end127; ++bigsum_A__V_dom_it128)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it128;
        q473 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q472*q473;
    q493 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it130 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end129 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it130 != bigsum_B__V_dom_end129; ++bigsum_B__V_dom_it130)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it130;
        q493 += ( ( 0.0001*qBIDS3+-1*0.0001*P < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q494 = 0;
    set<double>::iterator bigsum_A__V_dom_it132 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end131 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it132 != bigsum_A__V_dom_end131; ++bigsum_A__V_dom_it132)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it132;
        q494 += ( ( ( bigsum_A__V < 0.0001*qBIDS2 ) && (
             0.0001*qBIDS2 <= bigsum_A__V ) )? ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q493*q494;
    q514 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it134 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end133 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it134 != bigsum_B__V_dom_end133; ++bigsum_B__V_dom_it134)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it134;
        q514 += ( ( ( bigsum_B__V <= 0.0001*qBIDS3+-1*0.0001*P ) && (
             0.0001*qBIDS3 < bigsum_B__V ) )? ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q515 = 0;
    set<double>::iterator bigsum_A__V_dom_it136 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end135 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it136 != bigsum_A__V_dom_end135; ++bigsum_A__V_dom_it136)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it136;
        q515 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q514*q515;
    q535 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it138 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end137 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it138 != bigsum_B__V_dom_end137; ++bigsum_B__V_dom_it138)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it138;
        q535 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q536 = 0;
    set<double>::iterator bigsum_A__V_dom_it140 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end139 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it140 != bigsum_A__V_dom_end139; ++bigsum_A__V_dom_it140)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it140;
        q536 += ( ( ( 0.0001*qBIDS2 <= bigsum_A__V ) && (
             bigsum_A__V < 0.0001*qBIDS2 ) )? ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q535*q536;
    qBIDS3 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_it142 = 
        BIDS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_end141 = 
        BIDS.end();
    for (; BIDS_it142 != BIDS_end141; ++BIDS_it142)
    {
        double B1__T = get<0>(*BIDS_it142);
        int B1__ID = get<1>(*BIDS_it142);
        int B1__BROKER_ID = get<2>(*BIDS_it142);
        double B1__P = get<3>(*BIDS_it142);
        double B1__V = get<4>(*BIDS_it142);
        qBIDS3 += B1__P;
    }
    map<double,int>::iterator qBIDS4_it146 = qBIDS4.begin();
    map<double,int>::iterator qBIDS4_end145 = qBIDS4.end();
    for (; qBIDS4_it146 != qBIDS4_end145; ++qBIDS4_it146)
    {
        double bigsum_B__V = qBIDS4_it146->first;
        qBIDS4[V] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it144 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end143 = 
            BIDS.end();
        for (; BIDS_it144 != BIDS_end143; ++BIDS_it144)
        {
            double protect_B__T = get<0>(*BIDS_it144);
            int protect_B__ID = get<1>(*BIDS_it144);
            int protect_B__BROKER_ID = get<2>(*BIDS_it144);
            double protect_B__P = get<3>(*BIDS_it144);
            double protect_B__V = get<4>(*BIDS_it144);
            double var9 = 0;
            var9 += V;
            double var10 = 0;
            var10 += protect_B__V;
            if ( var9 == var10 )
            {
                qBIDS4[V] += 1;
            }
        }
    }
    map<double,double>::iterator qBIDS6_it150 = qBIDS6.begin();
    map<double,double>::iterator qBIDS6_end149 = qBIDS6.end();
    for (; qBIDS6_it150 != qBIDS6_end149; ++qBIDS6_it150)
    {
        double bigsum_B__V = qBIDS6_it150->first;
        qBIDS6[V] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it148 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end147 = 
            BIDS.end();
        for (; BIDS_it148 != BIDS_end147; ++BIDS_it148)
        {
            double protect_B__T = get<0>(*BIDS_it148);
            int protect_B__ID = get<1>(*BIDS_it148);
            int protect_B__BROKER_ID = get<2>(*BIDS_it148);
            double protect_B__P = get<3>(*BIDS_it148);
            double protect_B__V = get<4>(*BIDS_it148);
            double var11 = 0;
            var11 += V;
            double var12 = 0;
            var12 += protect_B__V;
            if ( var11 == var12 )
            {
                qBIDS6[V] += protect_B__P;
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_BIDS_sec_span, on_delete_BIDS_usec_span);
    (*results) << "on_delete_BIDS" << "," << q << endl;
}

void on_delete_ASKS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    ASKS.erase(make_tuple(T,ID,BROKER_ID,P,V));
    q550 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it152 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end151 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it152 != bigsum_A__V_dom_end151; ++bigsum_A__V_dom_it152)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it152;
        q550 += ( ( bigsum_A__V < 0.0001*qBIDS2+-1*0.0001*P )?
             ( P*( ( V == bigsum_A__V )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q551 = 0;
    set<double>::iterator bigsum_B__V_dom_it154 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end153 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it154 != bigsum_B__V_dom_end153; ++bigsum_B__V_dom_it154)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it154;
        q551 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*q550*q551;
    q571 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it156 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end155 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it156 != bigsum_A__V_dom_end155; ++bigsum_A__V_dom_it156)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it156;
        q571 += ( ( ( bigsum_A__V < 0.0001*qBIDS2+-1*0.0001*P ) && (
             0.0001*qBIDS2 <= bigsum_A__V ) )? ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q572 = 0;
    set<double>::iterator bigsum_B__V_dom_it158 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end157 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it158 != bigsum_B__V_dom_end157; ++bigsum_B__V_dom_it158)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it158;
        q572 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*-1*q571*q572;
    q592 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it160 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end159 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it160 != bigsum_A__V_dom_end159; ++bigsum_A__V_dom_it160)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it160;
        q592 += ( ( bigsum_A__V < 0.0001*qBIDS2+-1*0.0001*P )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q593 = 0;
    set<double>::iterator bigsum_B__V_dom_it162 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end161 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it162 != bigsum_B__V_dom_end161; ++bigsum_B__V_dom_it162)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it162;
        q593 += ( ( ( 0.0001*qBIDS3 < bigsum_B__V ) && (
             bigsum_B__V <= 0.0001*qBIDS3 ) )? ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*-1*q592*q593;
    q612 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it164 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end163 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it164 != bigsum_A__V_dom_end163; ++bigsum_A__V_dom_it164)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it164;
        q612 += ( ( ( 0.0001*qBIDS2+-1*0.0001*P <= bigsum_A__V ) && (
             bigsum_A__V < 0.0001*qBIDS2 ) )? ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q613 = 0;
    set<double>::iterator bigsum_B__V_dom_it166 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end165 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it166 != bigsum_B__V_dom_end165; ++bigsum_B__V_dom_it166)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it166;
        q613 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*q612*q613;
    q632 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it168 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end167 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it168 != bigsum_A__V_dom_end167; ++bigsum_A__V_dom_it168)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it168;
        q632 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q633 = 0;
    set<double>::iterator bigsum_B__V_dom_it170 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end169 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it170 != bigsum_B__V_dom_end169; ++bigsum_B__V_dom_it170)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it170;
        q633 += ( ( ( bigsum_B__V <= 0.0001*qBIDS3 ) && (
             0.0001*qBIDS3 < bigsum_B__V ) )? ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*q632*q633;
    q648 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it172 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end171 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it172 != bigsum_B__V_dom_end171; ++bigsum_B__V_dom_it172)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it172;
        q648 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q649 = 0;
    set<double>::iterator bigsum_A__V_dom_it174 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end173 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it174 != bigsum_A__V_dom_end173; ++bigsum_A__V_dom_it174)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it174;
        q649 += ( ( bigsum_A__V < 0.0001*qBIDS2+-1*0.0001*P )?
             ( ( ( V == bigsum_A__V )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q += -1*-1*q648*q649;
    q667 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it176 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end175 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it176 != bigsum_B__V_dom_end175; ++bigsum_B__V_dom_it176)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it176;
        q667 += ( ( ( 0.0001*qBIDS3 < bigsum_B__V ) && (
             bigsum_B__V <= 0.0001*qBIDS3 ) )? ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q668 = 0;
    set<double>::iterator bigsum_A__V_dom_it178 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end177 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it178 != bigsum_A__V_dom_end177; ++bigsum_A__V_dom_it178)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it178;
        q668 += ( ( bigsum_A__V < 0.0001*qBIDS2+-1*0.0001*P )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q667*q668;
    q688 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it180 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end179 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it180 != bigsum_B__V_dom_end179; ++bigsum_B__V_dom_it180)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it180;
        q688 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q689 = 0;
    set<double>::iterator bigsum_A__V_dom_it182 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end181 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it182 != bigsum_A__V_dom_end181; ++bigsum_A__V_dom_it182)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it182;
        q689 += ( ( ( bigsum_A__V < 0.0001*qBIDS2+-1*0.0001*P ) && (
             0.0001*qBIDS2 <= bigsum_A__V ) )? ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q688*q689;
    q709 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it184 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end183 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it184 != bigsum_B__V_dom_end183; ++bigsum_B__V_dom_it184)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it184;
        q709 += ( ( ( bigsum_B__V <= 0.0001*qBIDS3 ) && (
             0.0001*qBIDS3 < bigsum_B__V ) )? ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q710 = 0;
    set<double>::iterator bigsum_A__V_dom_it186 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end185 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it186 != bigsum_A__V_dom_end185; ++bigsum_A__V_dom_it186)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it186;
        q710 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q709*q710;
    q725 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it188 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end187 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it188 != bigsum_B__V_dom_end187; ++bigsum_B__V_dom_it188)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it188;
        q725 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q726 = 0;
    set<double>::iterator bigsum_A__V_dom_it190 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end189 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it190 != bigsum_A__V_dom_end189; ++bigsum_A__V_dom_it190)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it190;
        q726 += ( ( ( 0.0001*qBIDS2+-1*0.0001*P <= bigsum_A__V ) && (
             bigsum_A__V < 0.0001*qBIDS2 ) )? ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q725*q726;
    map<double,double>::iterator qBIDS1_it194 = qBIDS1.begin();
    map<double,double>::iterator qBIDS1_end193 = qBIDS1.end();
    for (; qBIDS1_it194 != qBIDS1_end193; ++qBIDS1_it194)
    {
        double bigsum_A__V = qBIDS1_it194->first;
        qBIDS1[V] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it192 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end191 = 
            ASKS.end();
        for (; ASKS_it192 != ASKS_end191; ++ASKS_it192)
        {
            double protect_A__T = get<0>(*ASKS_it192);
            int protect_A__ID = get<1>(*ASKS_it192);
            int protect_A__BROKER_ID = get<2>(*ASKS_it192);
            double protect_A__P = get<3>(*ASKS_it192);
            double protect_A__V = get<4>(*ASKS_it192);
            double var13 = 0;
            var13 += V;
            double var14 = 0;
            var14 += protect_A__V;
            if ( var13 == var14 )
            {
                qBIDS1[V] += protect_A__P;
            }
        }
    }
    qBIDS2 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_it196 = 
        ASKS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_end195 = 
        ASKS.end();
    for (; ASKS_it196 != ASKS_end195; ++ASKS_it196)
    {
        double A1__T = get<0>(*ASKS_it196);
        int A1__ID = get<1>(*ASKS_it196);
        int A1__BROKER_ID = get<2>(*ASKS_it196);
        double A1__P = get<3>(*ASKS_it196);
        double A1__V = get<4>(*ASKS_it196);
        qBIDS2 += A1__P;
    }
    map<double,int>::iterator qBIDS5_it200 = qBIDS5.begin();
    map<double,int>::iterator qBIDS5_end199 = qBIDS5.end();
    for (; qBIDS5_it200 != qBIDS5_end199; ++qBIDS5_it200)
    {
        double bigsum_A__V = qBIDS5_it200->first;
        qBIDS5[V] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it198 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end197 = 
            ASKS.end();
        for (; ASKS_it198 != ASKS_end197; ++ASKS_it198)
        {
            double protect_A__T = get<0>(*ASKS_it198);
            int protect_A__ID = get<1>(*ASKS_it198);
            int protect_A__BROKER_ID = get<2>(*ASKS_it198);
            double protect_A__P = get<3>(*ASKS_it198);
            double protect_A__V = get<4>(*ASKS_it198);
            double var15 = 0;
            var15 += V;
            double var16 = 0;
            var16 += protect_A__V;
            if ( var15 == var16 )
            {
                qBIDS5[V] += 1;
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_ASKS_sec_span, on_delete_ASKS_usec_span);
    (*results) << "on_delete_ASKS" << "," << q << endl;
}

DBToaster::DemoDatasets::OrderbookFileStream VwapBids("/home/yanif/datasets/orderbook/MSFT/20081201.csv",10000);

boost::shared_ptr<DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor> VwapBids_adaptor(new DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor());
static int streamVwapBidsId = 0;

struct on_insert_BIDS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor::Result>(data); 
        on_insert_BIDS(results, log, stats, input.t,input.id,input.broker_id,input.price,input.volume);
    }
};

on_insert_BIDS_fun_obj fo_on_insert_BIDS_0;

DBToaster::DemoDatasets::OrderbookFileStream VwapAsks("/home/yanif/datasets/orderbook/MSFT/20081201.csv",10000);

boost::shared_ptr<DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor> VwapAsks_adaptor(new DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor());
static int streamVwapAsksId = 1;

struct on_insert_ASKS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor::Result>(data); 
        on_insert_ASKS(results, log, stats, input.t,input.id,input.broker_id,input.price,input.volume);
    }
};

on_insert_ASKS_fun_obj fo_on_insert_ASKS_1;

struct on_delete_BIDS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor::Result>(data); 
        on_delete_BIDS(results, log, stats, input.t,input.id,input.broker_id,input.price,input.volume);
    }
};

on_delete_BIDS_fun_obj fo_on_delete_BIDS_2;

struct on_delete_ASKS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor::Result>(data); 
        on_delete_ASKS(results, log, stats, input.t,input.id,input.broker_id,input.price,input.volume);
    }
};

on_delete_ASKS_fun_obj fo_on_delete_ASKS_3;


void init(DBToaster::StandaloneEngine::FileMultiplexer& sources,
    DBToaster::StandaloneEngine::FileStreamDispatcher& router, 
    ofstream* results, ofstream* log, ofstream* stats)
{
    router.setFiles(results, log, stats);
    sources.addStream<DBToaster::DemoDatasets::OrderbookTuple>(&VwapBids, boost::ref(*VwapBids_adaptor), streamVwapBidsId);
    router.addHandler(streamVwapBidsId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_BIDS_0));
    sources.addStream<DBToaster::DemoDatasets::OrderbookTuple>(&VwapAsks, boost::ref(*VwapAsks_adaptor), streamVwapAsksId);
    router.addHandler(streamVwapAsksId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_ASKS_1));
    router.addHandler(streamVwapBidsId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_BIDS_2));
    router.addHandler(streamVwapAsksId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_ASKS_3));
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