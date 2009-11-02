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
double q367;
int q368;
double q321;
int q322;
double q283;
int q284;
double q624;
double q583;
int q625;
int q584;
double q53;
int q54;
double q547;
double q163;
int q548;
map<double,double> qBIDS1;
int q164;
double qBIDS2;
double qBIDS3;
map<double,int> qBIDS4;
double q679;
map<double,int> qBIDS5;
double q505;
double q463;
double q336;
map<double,double> qBIDS6;
int q506;
int q464;
int q337;
double q16;
double q251;
double q125;
double q;
int q252;
int q17;
int q126;
double q721;
int q722;
int q680;
double q427;
double q0;
int q428;
double q386;
int q1;
int q387;
double q213;
int q214;
double q68;
int q69;
double q177;
int q178;
double q644;
int q645;
double q302;
int q303;
double q562;
double q266;
double q604;
int q267;
int q605;
int q563;
double q737;
int q738;
double q526;
double q484;
double q34;
int q527;
int q485;
int q35;
double q144;
int q145;
double q442;
int q443;
double q232;
int q233;
double q106;
int q107;
double q700;
set<double> bigsum_A__V_dom;
int q701;
double q660;
set<double> bigsum_B__V_dom;
double q87;
double q195;
int q661;
double q407;
int q88;
int q196;
int q408;

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
    qBIDS3 += P;
    map<double,int>::iterator qBIDS4_it42 = qBIDS4.begin();
    map<double,int>::iterator qBIDS4_end41 = qBIDS4.end();
    for (; qBIDS4_it42 != qBIDS4_end41; ++qBIDS4_it42)
    {
        double bigsum_B__V = qBIDS4_it42->first;
        qBIDS4[bigsum_B__V] += ( ( bigsum_B__V == V )? ( 1 ) : ( 0 ) );
    }
    map<double,double>::iterator qBIDS6_it44 = qBIDS6.begin();
    map<double,double>::iterator qBIDS6_end43 = qBIDS6.end();
    for (; qBIDS6_it44 != qBIDS6_end43; ++qBIDS6_it44)
    {
        double bigsum_B__V = qBIDS6_it44->first;
        qBIDS6[bigsum_B__V] += P*( ( bigsum_B__V == V )? ( 1 ) : ( 0 ) );
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
    q177 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it46 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end45 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it46 != bigsum_A__V_dom_end45; ++bigsum_A__V_dom_it46)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it46;
        q177 += ( ( bigsum_A__V < 0.0001*qBIDS2+0.0001*P )?
             ( P*( ( V == bigsum_A__V )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q178 = 0;
    set<double>::iterator bigsum_B__V_dom_it48 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end47 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it48 != bigsum_B__V_dom_end47; ++bigsum_B__V_dom_it48)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it48;
        q178 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += q177*q178;
    q195 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it50 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end49 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it50 != bigsum_A__V_dom_end49; ++bigsum_A__V_dom_it50)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it50;
        q195 += ( ( ( bigsum_A__V < 0.0001*qBIDS2+0.0001*P ) && (
             0.0001*qBIDS2 <= bigsum_A__V ) )? ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q196 = 0;
    set<double>::iterator bigsum_B__V_dom_it52 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end51 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it52 != bigsum_B__V_dom_end51; ++bigsum_B__V_dom_it52)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it52;
        q196 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += q195*q196;
    q213 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it54 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end53 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it54 != bigsum_A__V_dom_end53; ++bigsum_A__V_dom_it54)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it54;
        q213 += ( ( bigsum_A__V < 0.0001*qBIDS2+0.0001*P )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q214 = 0;
    set<double>::iterator bigsum_B__V_dom_it56 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end55 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it56 != bigsum_B__V_dom_end55; ++bigsum_B__V_dom_it56)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it56;
        q214 += ( ( ( 0.0001*qBIDS3 < bigsum_B__V ) && (
             bigsum_B__V <= 0.0001*qBIDS3 ) )? ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += q213*q214;
    q232 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it58 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end57 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it58 != bigsum_A__V_dom_end57; ++bigsum_A__V_dom_it58)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it58;
        q232 += ( ( ( 0.0001*qBIDS2+0.0001*P <= bigsum_A__V ) && (
             bigsum_A__V < 0.0001*qBIDS2 ) )? ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q233 = 0;
    set<double>::iterator bigsum_B__V_dom_it60 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end59 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it60 != bigsum_B__V_dom_end59; ++bigsum_B__V_dom_it60)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it60;
        q233 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*q232*q233;
    q251 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it62 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end61 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it62 != bigsum_A__V_dom_end61; ++bigsum_A__V_dom_it62)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it62;
        q251 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q252 = 0;
    set<double>::iterator bigsum_B__V_dom_it64 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end63 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it64 != bigsum_B__V_dom_end63; ++bigsum_B__V_dom_it64)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it64;
        q252 += ( ( ( bigsum_B__V <= 0.0001*qBIDS3 ) && (
             0.0001*qBIDS3 < bigsum_B__V ) )? ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*q251*q252;
    q266 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it66 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end65 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it66 != bigsum_B__V_dom_end65; ++bigsum_B__V_dom_it66)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it66;
        q266 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q267 = 0;
    set<double>::iterator bigsum_A__V_dom_it68 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end67 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it68 != bigsum_A__V_dom_end67; ++bigsum_A__V_dom_it68)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it68;
        q267 += ( ( bigsum_A__V < 0.0001*qBIDS2+0.0001*P )?
             ( ( ( V == bigsum_A__V )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q += -1*q266*q267;
    q283 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it70 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end69 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it70 != bigsum_B__V_dom_end69; ++bigsum_B__V_dom_it70)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it70;
        q283 += ( ( ( 0.0001*qBIDS3 < bigsum_B__V ) && (
             bigsum_B__V <= 0.0001*qBIDS3 ) )? ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q284 = 0;
    set<double>::iterator bigsum_A__V_dom_it72 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end71 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it72 != bigsum_A__V_dom_end71; ++bigsum_A__V_dom_it72)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it72;
        q284 += ( ( bigsum_A__V < 0.0001*qBIDS2+0.0001*P )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*q283*q284;
    q302 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it74 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end73 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it74 != bigsum_B__V_dom_end73; ++bigsum_B__V_dom_it74)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it74;
        q302 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q303 = 0;
    set<double>::iterator bigsum_A__V_dom_it76 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end75 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it76 != bigsum_A__V_dom_end75; ++bigsum_A__V_dom_it76)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it76;
        q303 += ( ( ( bigsum_A__V < 0.0001*qBIDS2+0.0001*P ) && (
             0.0001*qBIDS2 <= bigsum_A__V ) )? ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*q302*q303;
    q321 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it78 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end77 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it78 != bigsum_B__V_dom_end77; ++bigsum_B__V_dom_it78)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it78;
        q321 += ( ( ( bigsum_B__V <= 0.0001*qBIDS3 ) && (
             0.0001*qBIDS3 < bigsum_B__V ) )? ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q322 = 0;
    set<double>::iterator bigsum_A__V_dom_it80 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end79 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it80 != bigsum_A__V_dom_end79; ++bigsum_A__V_dom_it80)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it80;
        q322 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*q321*q322;
    q336 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it82 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end81 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it82 != bigsum_B__V_dom_end81; ++bigsum_B__V_dom_it82)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it82;
        q336 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q337 = 0;
    set<double>::iterator bigsum_A__V_dom_it84 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end83 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it84 != bigsum_A__V_dom_end83; ++bigsum_A__V_dom_it84)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it84;
        q337 += ( ( ( 0.0001*qBIDS2+0.0001*P <= bigsum_A__V ) && (
             bigsum_A__V < 0.0001*qBIDS2 ) )? ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*q336*q337;
    map<double,double>::iterator qBIDS1_it86 = qBIDS1.begin();
    map<double,double>::iterator qBIDS1_end85 = qBIDS1.end();
    for (; qBIDS1_it86 != qBIDS1_end85; ++qBIDS1_it86)
    {
        double bigsum_A__V = qBIDS1_it86->first;
        qBIDS1[bigsum_A__V] += P*( ( bigsum_A__V == V )? ( 1 ) : ( 0 ) );
    }
    qBIDS2 += P;
    map<double,int>::iterator qBIDS5_it88 = qBIDS5.begin();
    map<double,int>::iterator qBIDS5_end87 = qBIDS5.end();
    for (; qBIDS5_it88 != qBIDS5_end87; ++qBIDS5_it88)
    {
        double bigsum_A__V = qBIDS5_it88->first;
        qBIDS5[bigsum_A__V] += ( ( bigsum_A__V == V )? ( 1 ) : ( 0 ) );
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
    q367 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it90 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end89 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it90 != bigsum_A__V_dom_end89; ++bigsum_A__V_dom_it90)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it90;
        q367 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q368 = 0;
    set<double>::iterator bigsum_B__V_dom_it92 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end91 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it92 != bigsum_B__V_dom_end91; ++bigsum_B__V_dom_it92)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it92;
        q368 += ( ( 0.0001*qBIDS3+-1*0.0001*P < bigsum_B__V )?
             ( ( ( V == bigsum_B__V )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q += -1*q367*q368;
    q386 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it94 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end93 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it94 != bigsum_A__V_dom_end93; ++bigsum_A__V_dom_it94)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it94;
        q386 += ( ( ( bigsum_A__V < 0.0001*qBIDS2 ) && (
             0.0001*qBIDS2 <= bigsum_A__V ) )? ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q387 = 0;
    set<double>::iterator bigsum_B__V_dom_it96 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end95 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it96 != bigsum_B__V_dom_end95; ++bigsum_B__V_dom_it96)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it96;
        q387 += ( ( 0.0001*qBIDS3+-1*0.0001*P < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*-1*q386*q387;
    q407 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it98 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end97 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it98 != bigsum_A__V_dom_end97; ++bigsum_A__V_dom_it98)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it98;
        q407 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q408 = 0;
    set<double>::iterator bigsum_B__V_dom_it100 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end99 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it100 != bigsum_B__V_dom_end99; ++bigsum_B__V_dom_it100)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it100;
        q408 += ( ( ( 0.0001*qBIDS3+-1*0.0001*P < bigsum_B__V ) && (
             bigsum_B__V <= 0.0001*qBIDS3 ) )? ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*-1*q407*q408;
    q427 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it102 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end101 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it102 != bigsum_A__V_dom_end101; ++bigsum_A__V_dom_it102)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it102;
        q427 += ( ( ( 0.0001*qBIDS2 <= bigsum_A__V ) && (
             bigsum_A__V < 0.0001*qBIDS2 ) )? ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q428 = 0;
    set<double>::iterator bigsum_B__V_dom_it104 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end103 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it104 != bigsum_B__V_dom_end103; ++bigsum_B__V_dom_it104)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it104;
        q428 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*q427*q428;
    q442 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it106 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end105 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it106 != bigsum_A__V_dom_end105; ++bigsum_A__V_dom_it106)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it106;
        q442 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q443 = 0;
    set<double>::iterator bigsum_B__V_dom_it108 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end107 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it108 != bigsum_B__V_dom_end107; ++bigsum_B__V_dom_it108)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it108;
        q443 += ( ( ( bigsum_B__V <= 0.0001*qBIDS3+-1*0.0001*P ) && (
             0.0001*qBIDS3 < bigsum_B__V ) )? ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*q442*q443;
    q463 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it110 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end109 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it110 != bigsum_B__V_dom_end109; ++bigsum_B__V_dom_it110)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it110;
        q463 += ( ( 0.0001*qBIDS3+-1*0.0001*P < bigsum_B__V )?
             ( P*( ( V == bigsum_B__V )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q464 = 0;
    set<double>::iterator bigsum_A__V_dom_it112 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end111 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it112 != bigsum_A__V_dom_end111; ++bigsum_A__V_dom_it112)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it112;
        q464 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q463*q464;
    q484 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it114 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end113 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it114 != bigsum_B__V_dom_end113; ++bigsum_B__V_dom_it114)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it114;
        q484 += ( ( ( 0.0001*qBIDS3+-1*0.0001*P < bigsum_B__V ) && (
             bigsum_B__V <= 0.0001*qBIDS3 ) )? ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q485 = 0;
    set<double>::iterator bigsum_A__V_dom_it116 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end115 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it116 != bigsum_A__V_dom_end115; ++bigsum_A__V_dom_it116)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it116;
        q485 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q484*q485;
    q505 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it118 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end117 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it118 != bigsum_B__V_dom_end117; ++bigsum_B__V_dom_it118)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it118;
        q505 += ( ( 0.0001*qBIDS3+-1*0.0001*P < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q506 = 0;
    set<double>::iterator bigsum_A__V_dom_it120 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end119 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it120 != bigsum_A__V_dom_end119; ++bigsum_A__V_dom_it120)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it120;
        q506 += ( ( ( bigsum_A__V < 0.0001*qBIDS2 ) && (
             0.0001*qBIDS2 <= bigsum_A__V ) )? ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q505*q506;
    q526 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it122 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end121 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it122 != bigsum_B__V_dom_end121; ++bigsum_B__V_dom_it122)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it122;
        q526 += ( ( ( bigsum_B__V <= 0.0001*qBIDS3+-1*0.0001*P ) && (
             0.0001*qBIDS3 < bigsum_B__V ) )? ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q527 = 0;
    set<double>::iterator bigsum_A__V_dom_it124 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end123 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it124 != bigsum_A__V_dom_end123; ++bigsum_A__V_dom_it124)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it124;
        q527 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q526*q527;
    q547 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it126 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end125 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it126 != bigsum_B__V_dom_end125; ++bigsum_B__V_dom_it126)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it126;
        q547 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q548 = 0;
    set<double>::iterator bigsum_A__V_dom_it128 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end127 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it128 != bigsum_A__V_dom_end127; ++bigsum_A__V_dom_it128)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it128;
        q548 += ( ( ( 0.0001*qBIDS2 <= bigsum_A__V ) && (
             bigsum_A__V < 0.0001*qBIDS2 ) )? ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q547*q548;
    qBIDS3 += -1*P;
    map<double,int>::iterator qBIDS4_it130 = qBIDS4.begin();
    map<double,int>::iterator qBIDS4_end129 = qBIDS4.end();
    for (; qBIDS4_it130 != qBIDS4_end129; ++qBIDS4_it130)
    {
        double bigsum_B__V = qBIDS4_it130->first;
        qBIDS4[bigsum_B__V] += -1*( ( bigsum_B__V == V )? ( 1 ) : ( 0 ) );
    }
    map<double,double>::iterator qBIDS6_it132 = qBIDS6.begin();
    map<double,double>::iterator qBIDS6_end131 = qBIDS6.end();
    for (; qBIDS6_it132 != qBIDS6_end131; ++qBIDS6_it132)
    {
        double bigsum_B__V = qBIDS6_it132->first;
        qBIDS6[bigsum_B__V] += -1*P*( ( bigsum_B__V == V )? ( 1 ) : ( 0 ) );
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
    q562 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it134 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end133 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it134 != bigsum_A__V_dom_end133; ++bigsum_A__V_dom_it134)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it134;
        q562 += ( ( bigsum_A__V < 0.0001*qBIDS2+-1*0.0001*P )?
             ( P*( ( V == bigsum_A__V )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q563 = 0;
    set<double>::iterator bigsum_B__V_dom_it136 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end135 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it136 != bigsum_B__V_dom_end135; ++bigsum_B__V_dom_it136)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it136;
        q563 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*q562*q563;
    q583 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it138 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end137 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it138 != bigsum_A__V_dom_end137; ++bigsum_A__V_dom_it138)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it138;
        q583 += ( ( ( bigsum_A__V < 0.0001*qBIDS2+-1*0.0001*P ) && (
             0.0001*qBIDS2 <= bigsum_A__V ) )? ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q584 = 0;
    set<double>::iterator bigsum_B__V_dom_it140 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end139 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it140 != bigsum_B__V_dom_end139; ++bigsum_B__V_dom_it140)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it140;
        q584 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*-1*q583*q584;
    q604 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it142 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end141 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it142 != bigsum_A__V_dom_end141; ++bigsum_A__V_dom_it142)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it142;
        q604 += ( ( bigsum_A__V < 0.0001*qBIDS2+-1*0.0001*P )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q605 = 0;
    set<double>::iterator bigsum_B__V_dom_it144 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end143 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it144 != bigsum_B__V_dom_end143; ++bigsum_B__V_dom_it144)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it144;
        q605 += ( ( ( 0.0001*qBIDS3 < bigsum_B__V ) && (
             bigsum_B__V <= 0.0001*qBIDS3 ) )? ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*-1*q604*q605;
    q624 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it146 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end145 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it146 != bigsum_A__V_dom_end145; ++bigsum_A__V_dom_it146)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it146;
        q624 += ( ( ( 0.0001*qBIDS2+-1*0.0001*P <= bigsum_A__V ) && (
             bigsum_A__V < 0.0001*qBIDS2 ) )? ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q625 = 0;
    set<double>::iterator bigsum_B__V_dom_it148 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end147 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it148 != bigsum_B__V_dom_end147; ++bigsum_B__V_dom_it148)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it148;
        q625 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*q624*q625;
    q644 = 0.0;
    set<double>::iterator bigsum_A__V_dom_it150 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end149 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it150 != bigsum_A__V_dom_end149; ++bigsum_A__V_dom_it150)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it150;
        q644 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS1[bigsum_A__V] ) : ( 0 ) );
    }
    q645 = 0;
    set<double>::iterator bigsum_B__V_dom_it152 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end151 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it152 != bigsum_B__V_dom_end151; ++bigsum_B__V_dom_it152)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it152;
        q645 += ( ( ( bigsum_B__V <= 0.0001*qBIDS3 ) && (
             0.0001*qBIDS3 < bigsum_B__V ) )? ( qBIDS4[bigsum_B__V] ) : ( 0 ) );
    }
    q += -1*q644*q645;
    q660 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it154 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end153 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it154 != bigsum_B__V_dom_end153; ++bigsum_B__V_dom_it154)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it154;
        q660 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q661 = 0;
    set<double>::iterator bigsum_A__V_dom_it156 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end155 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it156 != bigsum_A__V_dom_end155; ++bigsum_A__V_dom_it156)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it156;
        q661 += ( ( bigsum_A__V < 0.0001*qBIDS2+-1*0.0001*P )?
             ( ( ( V == bigsum_A__V )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q += -1*-1*q660*q661;
    q679 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it158 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end157 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it158 != bigsum_B__V_dom_end157; ++bigsum_B__V_dom_it158)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it158;
        q679 += ( ( ( 0.0001*qBIDS3 < bigsum_B__V ) && (
             bigsum_B__V <= 0.0001*qBIDS3 ) )? ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q680 = 0;
    set<double>::iterator bigsum_A__V_dom_it160 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end159 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it160 != bigsum_A__V_dom_end159; ++bigsum_A__V_dom_it160)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it160;
        q680 += ( ( bigsum_A__V < 0.0001*qBIDS2+-1*0.0001*P )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q679*q680;
    q700 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it162 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end161 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it162 != bigsum_B__V_dom_end161; ++bigsum_B__V_dom_it162)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it162;
        q700 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q701 = 0;
    set<double>::iterator bigsum_A__V_dom_it164 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end163 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it164 != bigsum_A__V_dom_end163; ++bigsum_A__V_dom_it164)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it164;
        q701 += ( ( ( bigsum_A__V < 0.0001*qBIDS2+-1*0.0001*P ) && (
             0.0001*qBIDS2 <= bigsum_A__V ) )? ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q700*q701;
    q721 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it166 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end165 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it166 != bigsum_B__V_dom_end165; ++bigsum_B__V_dom_it166)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it166;
        q721 += ( ( ( bigsum_B__V <= 0.0001*qBIDS3 ) && (
             0.0001*qBIDS3 < bigsum_B__V ) )? ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q722 = 0;
    set<double>::iterator bigsum_A__V_dom_it168 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end167 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it168 != bigsum_A__V_dom_end167; ++bigsum_A__V_dom_it168)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it168;
        q722 += ( ( bigsum_A__V < 0.0001*qBIDS2 )?
             ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q721*q722;
    q737 = 0.0;
    set<double>::iterator bigsum_B__V_dom_it170 = bigsum_B__V_dom.begin();
    set<double>::iterator bigsum_B__V_dom_end169 = bigsum_B__V_dom.end();
    for (
        ; bigsum_B__V_dom_it170 != bigsum_B__V_dom_end169; ++bigsum_B__V_dom_it170)
    {
        double bigsum_B__V = *bigsum_B__V_dom_it170;
        q737 += ( ( 0.0001*qBIDS3 < bigsum_B__V )?
             ( qBIDS6[bigsum_B__V] ) : ( 0 ) );
    }
    q738 = 0;
    set<double>::iterator bigsum_A__V_dom_it172 = bigsum_A__V_dom.begin();
    set<double>::iterator bigsum_A__V_dom_end171 = bigsum_A__V_dom.end();
    for (
        ; bigsum_A__V_dom_it172 != bigsum_A__V_dom_end171; ++bigsum_A__V_dom_it172)
    {
        double bigsum_A__V = *bigsum_A__V_dom_it172;
        q738 += ( ( ( 0.0001*qBIDS2+-1*0.0001*P <= bigsum_A__V ) && (
             bigsum_A__V < 0.0001*qBIDS2 ) )? ( qBIDS5[bigsum_A__V] ) : ( 0 ) );
    }
    q += -1*-1*q737*q738;
    map<double,double>::iterator qBIDS1_it174 = qBIDS1.begin();
    map<double,double>::iterator qBIDS1_end173 = qBIDS1.end();
    for (; qBIDS1_it174 != qBIDS1_end173; ++qBIDS1_it174)
    {
        double bigsum_A__V = qBIDS1_it174->first;
        qBIDS1[bigsum_A__V] += -1*P*( ( bigsum_A__V == V )? ( 1 ) : ( 0 ) );
    }
    qBIDS2 += -1*P;
    map<double,int>::iterator qBIDS5_it176 = qBIDS5.begin();
    map<double,int>::iterator qBIDS5_end175 = qBIDS5.end();
    for (; qBIDS5_it176 != qBIDS5_end175; ++qBIDS5_it176)
    {
        double bigsum_A__V = qBIDS5_it176->first;
        qBIDS5[bigsum_A__V] += -1*( ( bigsum_A__V == V )? ( 1 ) : ( 0 ) );
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