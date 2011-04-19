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
double q323;
double q46;
double q451;
int q324;
int q47;
int q452;
double q110;
double q112;
double q411;
double q882;
double q412;
set<double> bigsum_A__P_dom;
double q883;
set<double> bigsum_B__P_dom;
double q758;
int q759;
double q201;
double q249;
double q202;
double q160;
multiset<tuple<double,int,int,double,double> > BIDS;
double q162;
double q674;
double q547;
int q548;
map<double,double> qBIDS1;
double q676;
map<double,double> qBIDS2;
double qBIDS3;
double q631;
map<double,double> qBIDS4;
double q632;
double qBIDS5;
double q505;
map<tuple<int,double>,int> qBIDS6;
int q506;
double q337;
map<double,double> qBIDS7;
map<tuple<double,int>,double> qBIDS8;
double q339;
int q250;
double q298;
map<int,double> q;
int q299;
double q937;
double q939;
double q385;
double q0;
int q1;
double q387;
double q854;
double q855;
double q22;
int q23;
double q812;
int q813;
double q477;
int q478;
double q135;
double q137;
double q730;
double q603;
int q731;
double q604;
double q436;
double q437;
double q71;
int q72;
multiset<tuple<double,int,int,double,double> > ASKS;
double q225;
int q226;
double q909;
double q186;
double q187;
double q827;
double q785;
double q658;
double q273;
int q786;
int q274;
double q829;
double q911;
double q574;
double q576;
double q360;
double q362;
double q532;
double q86;
double q702;
double q660;
int q533;
int q87;
int q703;

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
   cout << "bigsum_A__P_dom size: " << (((sizeof(set<double>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_A__P_dom.size())  + (sizeof(struct _Rb_tree<set<double>::key_type, set<double>::value_type, _Identity<set<double>::value_type>, set<double>::key_compare>))) << endl;

   (*stats) << "m," << "bigsum_A__P_dom" << "," << (((sizeof(set<double>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_A__P_dom.size())  + (sizeof(struct _Rb_tree<set<double>::key_type, set<double>::value_type, _Identity<set<double>::value_type>, set<double>::key_compare>))) << endl;

   cout << "bigsum_B__P_dom size: " << (((sizeof(set<double>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_B__P_dom.size())  + (sizeof(struct _Rb_tree<set<double>::key_type, set<double>::value_type, _Identity<set<double>::value_type>, set<double>::key_compare>))) << endl;

   (*stats) << "m," << "bigsum_B__P_dom" << "," << (((sizeof(set<double>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_B__P_dom.size())  + (sizeof(struct _Rb_tree<set<double>::key_type, set<double>::value_type, _Identity<set<double>::value_type>, set<double>::key_compare>))) << endl;

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

   cout << "qBIDS2 size: " << (((sizeof(map<double,double>::key_type)
       + sizeof(map<double,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS2.size())  + (sizeof(struct _Rb_tree<map<double,double>::key_type, map<double,double>::value_type, _Select1st<map<double,double>::value_type>, map<double,double>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS2" << "," << (((sizeof(map<double,double>::key_type)
       + sizeof(map<double,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS2.size())  + (sizeof(struct _Rb_tree<map<double,double>::key_type, map<double,double>::value_type, _Select1st<map<double,double>::value_type>, map<double,double>::key_compare>))) << endl;

   cout << "qBIDS4 size: " << (((sizeof(map<double,double>::key_type)
       + sizeof(map<double,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS4.size())  + (sizeof(struct _Rb_tree<map<double,double>::key_type, map<double,double>::value_type, _Select1st<map<double,double>::value_type>, map<double,double>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS4" << "," << (((sizeof(map<double,double>::key_type)
       + sizeof(map<double,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS4.size())  + (sizeof(struct _Rb_tree<map<double,double>::key_type, map<double,double>::value_type, _Select1st<map<double,double>::value_type>, map<double,double>::key_compare>))) << endl;

   cout << "qBIDS6 size: " << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS6.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS6" << "," << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS6.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

   cout << "qBIDS7 size: " << (((sizeof(map<double,double>::key_type)
       + sizeof(map<double,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS7.size())  + (sizeof(struct _Rb_tree<map<double,double>::key_type, map<double,double>::value_type, _Select1st<map<double,double>::value_type>, map<double,double>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS7" << "," << (((sizeof(map<double,double>::key_type)
       + sizeof(map<double,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS7.size())  + (sizeof(struct _Rb_tree<map<double,double>::key_type, map<double,double>::value_type, _Select1st<map<double,double>::value_type>, map<double,double>::key_compare>))) << endl;

   cout << "qBIDS8 size: " << (((sizeof(map<tuple<double,int>,double>::key_type)
       + sizeof(map<tuple<double,int>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS8.size())  + (sizeof(struct _Rb_tree<map<tuple<double,int>,double>::key_type, map<tuple<double,int>,double>::value_type, _Select1st<map<tuple<double,int>,double>::value_type>, map<tuple<double,int>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS8" << "," << (((sizeof(map<tuple<double,int>,double>::key_type)
       + sizeof(map<tuple<double,int>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS8.size())  + (sizeof(struct _Rb_tree<map<tuple<double,int>,double>::key_type, map<tuple<double,int>,double>::value_type, _Select1st<map<tuple<double,int>,double>::value_type>, map<tuple<double,int>,double>::key_compare>))) << endl;

   cout << "q size: " << (((sizeof(map<int,double>::key_type)
       + sizeof(map<int,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int,double>::key_type, map<int,double>::value_type, _Select1st<map<int,double>::value_type>, map<int,double>::key_compare>))) << endl;

   (*stats) << "m," << "q" << "," << (((sizeof(map<int,double>::key_type)
       + sizeof(map<int,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int,double>::key_type, map<int,double>::value_type, _Select1st<map<int,double>::value_type>, map<int,double>::key_compare>))) << endl;

   cout << "ASKS size: " << (((sizeof(multiset<tuple<double,int,int,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * ASKS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<double,int,int,double,double> >::key_type, multiset<tuple<double,int,int,double,double> >::value_type, _Identity<multiset<tuple<double,int,int,double,double> >::value_type>, multiset<tuple<double,int,int,double,double> >::key_compare>))) << endl;

   (*stats) << "m," << "ASKS" << "," << (((sizeof(multiset<tuple<double,int,int,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * ASKS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<double,int,int,double,double> >::key_type, multiset<tuple<double,int,int,double,double> >::value_type, _Identity<multiset<tuple<double,int,int,double,double> >::value_type>, multiset<tuple<double,int,int,double,double> >::key_compare>))) << endl;

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
    bigsum_B__P_dom.insert(P);
    BIDS.insert(make_tuple(T,ID,BROKER_ID,P,V));
    q0 = 0.0;
    set<double>::iterator bigsum_A__P_dom_it2 = bigsum_A__P_dom.begin();
    set<double>::iterator bigsum_A__P_dom_end1 = bigsum_A__P_dom.end();
    for (; bigsum_A__P_dom_it2 != bigsum_A__P_dom_end1; ++bigsum_A__P_dom_it2)
    {
        double bigsum_A__P = *bigsum_A__P_dom_it2;
        q0 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
             ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
    }
    q1 = 0;
    set<double>::iterator bigsum_B__P_dom_it4 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end3 = bigsum_B__P_dom.end();
    for (; bigsum_B__P_dom_it4 != bigsum_B__P_dom_end3; ++bigsum_B__P_dom_it4)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it4;
        q1 += ( ( qBIDS4[bigsum_B__P]+V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+0.25*V )? ( ( ( P == bigsum_B__P )?
             ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q[BROKER_ID] += q0*q1;
    q110 = 0.0;
    set<double>::iterator bigsum_A__P_dom_it6 = bigsum_A__P_dom.begin();
    set<double>::iterator bigsum_A__P_dom_end5 = bigsum_A__P_dom.end();
    for (; bigsum_A__P_dom_it6 != bigsum_A__P_dom_end5; ++bigsum_A__P_dom_it6)
    {
        double bigsum_A__P = *bigsum_A__P_dom_it6;
        q110 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
             ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
    }
    q112 = 0.0;
    set<double>::iterator bigsum_B__P_dom_it8 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end7 = bigsum_B__P_dom.end();
    for (; bigsum_B__P_dom_it8 != bigsum_B__P_dom_end7; ++bigsum_B__P_dom_it8)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it8;
        q112 += ( ( qBIDS4[bigsum_B__P]+V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+0.25*V )? ( ( ( P == bigsum_B__P )?
             ( P ) : ( 0 ) )*V ) : ( 0 ) );
    }
    q[BROKER_ID] += -1*q110*q112;
    map<int,double>::iterator q_it14 = q.begin();
    map<int,double>::iterator q_end13 = q.end();
    for (; q_it14 != q_end13; ++q_it14)
    {
        int BROKER_ID = q_it14->first;
        q135 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it10 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end9 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it10 != bigsum_A__P_dom_end9; ++bigsum_A__P_dom_it10)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it10;
            q135 += ( ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) && (
                 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q137 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it12 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end11 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it12 != bigsum_B__P_dom_end11; ++bigsum_B__P_dom_it12)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it12;
            q137 += ( ( qBIDS4[bigsum_B__P]+V*( ( bigsum_B__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+0.25*V )?
                 ( qBIDS8[make_tuple(bigsum_B__P,BROKER_ID)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*q135*q137;
    }
    map<int,double>::iterator q_it20 = q.begin();
    map<int,double>::iterator q_end19 = q.end();
    for (; q_it20 != q_end19; ++q_it20)
    {
        int BROKER_ID = q_it20->first;
        q160 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it16 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end15 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it16 != bigsum_A__P_dom_end15; ++bigsum_A__P_dom_it16)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it16;
            q160 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q162 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it18 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end17 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it18 != bigsum_B__P_dom_end17; ++bigsum_B__P_dom_it18)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it18;
            q162 += ( ( ( qBIDS4[bigsum_B__P]+V*( ( bigsum_B__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+0.25*V ) && (
                 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) )?
                 ( qBIDS8[make_tuple(bigsum_B__P,BROKER_ID)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*q160*q162;
    }
    map<int,double>::iterator q_it26 = q.begin();
    map<int,double>::iterator q_end25 = q.end();
    for (; q_it26 != q_end25; ++q_it26)
    {
        int BROKER_ID = q_it26->first;
        q186 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it22 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end21 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it22 != bigsum_A__P_dom_end21; ++bigsum_A__P_dom_it22)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it22;
            q186 += ( ( ( 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) && (
                 qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q187 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it24 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end23 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it24 != bigsum_B__P_dom_end23; ++bigsum_B__P_dom_it24)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it24;
            q187 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS8[make_tuple(bigsum_B__P,BROKER_ID)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*q186*q187;
    }
    map<int,double>::iterator q_it32 = q.begin();
    map<int,double>::iterator q_end31 = q.end();
    for (; q_it32 != q_end31; ++q_it32)
    {
        int BROKER_ID = q_it32->first;
        q201 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it28 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end27 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it28 != bigsum_A__P_dom_end27; ++bigsum_A__P_dom_it28)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it28;
            q201 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q202 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it30 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end29 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it30 != bigsum_B__P_dom_end29; ++bigsum_B__P_dom_it30)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it30;
            q202 += ( ( ( 0.25*qBIDS5+0.25*V <= qBIDS4[bigsum_B__P]+V*( (
                 bigsum_B__P < P )? ( 1 ) : ( 0 ) ) ) && (
                 qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) )?
                 ( qBIDS8[make_tuple(bigsum_B__P,BROKER_ID)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*q201*q202;
    }
    map<int,double>::iterator q_it38 = q.begin();
    map<int,double>::iterator q_end37 = q.end();
    for (; q_it38 != q_end37; ++q_it38)
    {
        int BROKER_ID = q_it38->first;
        q22 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it34 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end33 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it34 != bigsum_A__P_dom_end33; ++bigsum_A__P_dom_it34)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it34;
            q22 += ( ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) && (
                 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q23 = 0;
        set<double>::iterator bigsum_B__P_dom_it36 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end35 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it36 != bigsum_B__P_dom_end35; ++bigsum_B__P_dom_it36)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it36;
            q23 += ( ( qBIDS4[bigsum_B__P]+V*( ( bigsum_B__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+0.25*V )?
                 ( qBIDS6[make_tuple(BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[BROKER_ID] += q22*q23;
    }
    map<int,double>::iterator q_it44 = q.begin();
    map<int,double>::iterator q_end43 = q.end();
    for (; q_it44 != q_end43; ++q_it44)
    {
        int BROKER_ID = q_it44->first;
        q46 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it40 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end39 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it40 != bigsum_A__P_dom_end39; ++bigsum_A__P_dom_it40)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it40;
            q46 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q47 = 0;
        set<double>::iterator bigsum_B__P_dom_it42 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end41 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it42 != bigsum_B__P_dom_end41; ++bigsum_B__P_dom_it42)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it42;
            q47 += ( ( ( qBIDS4[bigsum_B__P]+V*( ( bigsum_B__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+0.25*V ) && (
                 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) )?
                 ( qBIDS6[make_tuple(BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[BROKER_ID] += q46*q47;
    }
    map<int,double>::iterator q_it50 = q.begin();
    map<int,double>::iterator q_end49 = q.end();
    for (; q_it50 != q_end49; ++q_it50)
    {
        int BROKER_ID = q_it50->first;
        q71 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it46 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end45 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it46 != bigsum_A__P_dom_end45; ++bigsum_A__P_dom_it46)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it46;
            q71 += ( ( ( 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) && (
                 qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q72 = 0;
        set<double>::iterator bigsum_B__P_dom_it48 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end47 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it48 != bigsum_B__P_dom_end47; ++bigsum_B__P_dom_it48)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it48;
            q72 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS6[make_tuple(BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*q71*q72;
    }
    map<int,double>::iterator q_it56 = q.begin();
    map<int,double>::iterator q_end55 = q.end();
    for (; q_it56 != q_end55; ++q_it56)
    {
        int BROKER_ID = q_it56->first;
        q86 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it52 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end51 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it52 != bigsum_A__P_dom_end51; ++bigsum_A__P_dom_it52)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it52;
            q86 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q87 = 0;
        set<double>::iterator bigsum_B__P_dom_it54 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end53 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it54 != bigsum_B__P_dom_end53; ++bigsum_B__P_dom_it54)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it54;
            q87 += ( ( ( 0.25*qBIDS5+0.25*V <= qBIDS4[bigsum_B__P]+V*( (
                 bigsum_B__P < P )? ( 1 ) : ( 0 ) ) ) && (
                 qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) )?
                 ( qBIDS6[make_tuple(BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*q86*q87;
    }
    map<double,double>::iterator qBIDS4_it60 = qBIDS4.begin();
    map<double,double>::iterator qBIDS4_end59 = qBIDS4.end();
    for (; qBIDS4_it60 != qBIDS4_end59; ++qBIDS4_it60)
    {
        double bigsum_B__P = qBIDS4_it60->first;
        qBIDS4[P] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it58 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end57 = 
            BIDS.end();
        for (; BIDS_it58 != BIDS_end57; ++BIDS_it58)
        {
            double B2__T = get<0>(*BIDS_it58);
            int B2__ID = get<1>(*BIDS_it58);
            int B2__BROKER_ID = get<2>(*BIDS_it58);
            double B2__P = get<3>(*BIDS_it58);
            double B2__V = get<4>(*BIDS_it58);
            double var1 = 0;
            var1 += P;
            double var2 = 0;
            var2 += B2__P;
            if ( var1 < var2 )
            {
                qBIDS4[P] += B2__V;
            }
        }
    }
    qBIDS5 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_it62 = 
        BIDS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_end61 = 
        BIDS.end();
    for (; BIDS_it62 != BIDS_end61; ++BIDS_it62)
    {
        double B1__T = get<0>(*BIDS_it62);
        int B1__ID = get<1>(*BIDS_it62);
        int B1__BROKER_ID = get<2>(*BIDS_it62);
        double B1__P = get<3>(*BIDS_it62);
        double B1__V = get<4>(*BIDS_it62);
        qBIDS5 += B1__V;
    }
    map<tuple<int,double>,int>::iterator qBIDS6_it66 = qBIDS6.begin();
    map<tuple<int,double>,int>::iterator qBIDS6_end65 = qBIDS6.end();
    for (; qBIDS6_it66 != qBIDS6_end65; ++qBIDS6_it66)
    {
        int BROKER_ID = get<0>(qBIDS6_it66->first);
        double bigsum_B__P = get<1>(qBIDS6_it66->first);
        qBIDS6[make_tuple(BROKER_ID,P)] = 0;
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
            double var5 = 0;
            var5 += P;
            double var6 = 0;
            var6 += protect_B__P;
            if ( var5 == var6 )
            {
                int var3 = 0;
                var3 += BROKER_ID;
                int var4 = 0;
                var4 += protect_B__BROKER_ID;
                if ( var3 == var4 )
                {
                    qBIDS6[make_tuple(BROKER_ID,P)] += 1;
                }
            }
        }
    }
    map<tuple<double,int>,double>::iterator qBIDS8_it70 = qBIDS8.begin();
    map<tuple<double,int>,double>::iterator qBIDS8_end69 = qBIDS8.end();
    for (; qBIDS8_it70 != qBIDS8_end69; ++qBIDS8_it70)
    {
        double bigsum_B__P = get<0>(qBIDS8_it70->first);
        int BROKER_ID = get<1>(qBIDS8_it70->first);
        qBIDS8[make_tuple(P,BROKER_ID)] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it68 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end67 = 
            BIDS.end();
        for (; BIDS_it68 != BIDS_end67; ++BIDS_it68)
        {
            double protect_B__T = get<0>(*BIDS_it68);
            int protect_B__ID = get<1>(*BIDS_it68);
            int protect_B__BROKER_ID = get<2>(*BIDS_it68);
            double protect_B__P = get<3>(*BIDS_it68);
            double protect_B__V = get<4>(*BIDS_it68);
            double var10 = 0;
            var10 += P;
            double var11 = 0;
            var11 += protect_B__P;
            if ( var10 == var11 )
            {
                int var8 = 0;
                var8 += BROKER_ID;
                int var9 = 0;
                var9 += protect_B__BROKER_ID;
                if ( var8 == var9 )
                {
                    double var7 = 1;
                    var7 *= P;
                    var7 *= protect_B__V;
                    qBIDS8[make_tuple(P,BROKER_ID)] += var7;
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_BIDS_sec_span, on_insert_BIDS_usec_span);
}

void on_insert_ASKS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    bigsum_A__P_dom.insert(P);
    ASKS.insert(make_tuple(T,ID,BROKER_ID,P,V));
    map<int,double>::iterator q_it76 = q.begin();
    map<int,double>::iterator q_end75 = q.end();
    for (; q_it76 != q_end75; ++q_it76)
    {
        int B__BROKER_ID = q_it76->first;
        q225 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it72 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end71 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it72 != bigsum_A__P_dom_end71; ++bigsum_A__P_dom_it72)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it72;
            q225 += ( ( qBIDS2[bigsum_A__P]+V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+0.25*V )? ( ( ( P == bigsum_A__P )?
                 ( P ) : ( 0 ) )*V ) : ( 0 ) );
        }
        q226 = 0;
        set<double>::iterator bigsum_B__P_dom_it74 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end73 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it74 != bigsum_B__P_dom_end73; ++bigsum_B__P_dom_it74)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it74;
            q226 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += q225*q226;
    }
    map<int,double>::iterator q_it82 = q.begin();
    map<int,double>::iterator q_end81 = q.end();
    for (; q_it82 != q_end81; ++q_it82)
    {
        int B__BROKER_ID = q_it82->first;
        q249 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it78 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end77 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it78 != bigsum_A__P_dom_end77; ++bigsum_A__P_dom_it78)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it78;
            q249 += ( ( ( qBIDS2[bigsum_A__P]+V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+0.25*V ) && (
                 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q250 = 0;
        set<double>::iterator bigsum_B__P_dom_it80 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end79 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it80 != bigsum_B__P_dom_end79; ++bigsum_B__P_dom_it80)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it80;
            q250 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += q249*q250;
    }
    map<int,double>::iterator q_it88 = q.begin();
    map<int,double>::iterator q_end87 = q.end();
    for (; q_it88 != q_end87; ++q_it88)
    {
        int B__BROKER_ID = q_it88->first;
        q273 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it84 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end83 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it84 != bigsum_A__P_dom_end83; ++bigsum_A__P_dom_it84)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it84;
            q273 += ( ( qBIDS2[bigsum_A__P]+V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+0.25*V )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q274 = 0;
        set<double>::iterator bigsum_B__P_dom_it86 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end85 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it86 != bigsum_B__P_dom_end85; ++bigsum_B__P_dom_it86)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it86;
            q274 += ( ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) && (
                 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += q273*q274;
    }
    map<int,double>::iterator q_it94 = q.begin();
    map<int,double>::iterator q_end93 = q.end();
    for (; q_it94 != q_end93; ++q_it94)
    {
        int B__BROKER_ID = q_it94->first;
        q298 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it90 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end89 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it90 != bigsum_A__P_dom_end89; ++bigsum_A__P_dom_it90)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it90;
            q298 += ( ( ( 0.25*qBIDS3+0.25*V <= qBIDS2[bigsum_A__P]+V*( (
                 bigsum_A__P < P )? ( 1 ) : ( 0 ) ) ) && (
                 qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q299 = 0;
        set<double>::iterator bigsum_B__P_dom_it92 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end91 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it92 != bigsum_B__P_dom_end91; ++bigsum_B__P_dom_it92)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it92;
            q299 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q298*q299;
    }
    map<int,double>::iterator q_it100 = q.begin();
    map<int,double>::iterator q_end99 = q.end();
    for (; q_it100 != q_end99; ++q_it100)
    {
        int B__BROKER_ID = q_it100->first;
        q323 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it96 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end95 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it96 != bigsum_A__P_dom_end95; ++bigsum_A__P_dom_it96)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it96;
            q323 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q324 = 0;
        set<double>::iterator bigsum_B__P_dom_it98 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end97 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it98 != bigsum_B__P_dom_end97; ++bigsum_B__P_dom_it98)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it98;
            q324 += ( ( ( 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) && (
                 qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q323*q324;
    }
    map<int,double>::iterator q_it106 = q.begin();
    map<int,double>::iterator q_end105 = q.end();
    for (; q_it106 != q_end105; ++q_it106)
    {
        int B__BROKER_ID = q_it106->first;
        q337 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it102 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end101 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it102 != bigsum_A__P_dom_end101; ++bigsum_A__P_dom_it102)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it102;
            q337 += ( ( qBIDS2[bigsum_A__P]+V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+0.25*V )? ( ( ( P == bigsum_A__P )?
                 ( P ) : ( 0 ) ) ) : ( 0 ) );
        }
        q339 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it104 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end103 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it104 != bigsum_B__P_dom_end103; ++bigsum_B__P_dom_it104)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it104;
            q339 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q337*q339;
    }
    map<int,double>::iterator q_it112 = q.begin();
    map<int,double>::iterator q_end111 = q.end();
    for (; q_it112 != q_end111; ++q_it112)
    {
        int B__BROKER_ID = q_it112->first;
        q360 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it108 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end107 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it108 != bigsum_A__P_dom_end107; ++bigsum_A__P_dom_it108)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it108;
            q360 += ( ( ( qBIDS2[bigsum_A__P]+V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+0.25*V ) && (
                 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q362 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it110 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end109 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it110 != bigsum_B__P_dom_end109; ++bigsum_B__P_dom_it110)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it110;
            q362 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q360*q362;
    }
    map<int,double>::iterator q_it118 = q.begin();
    map<int,double>::iterator q_end117 = q.end();
    for (; q_it118 != q_end117; ++q_it118)
    {
        int B__BROKER_ID = q_it118->first;
        q385 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it114 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end113 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it114 != bigsum_A__P_dom_end113; ++bigsum_A__P_dom_it114)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it114;
            q385 += ( ( qBIDS2[bigsum_A__P]+V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+0.25*V )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q387 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it116 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end115 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it116 != bigsum_B__P_dom_end115; ++bigsum_B__P_dom_it116)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it116;
            q387 += ( ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) && (
                 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q385*q387;
    }
    map<int,double>::iterator q_it124 = q.begin();
    map<int,double>::iterator q_end123 = q.end();
    for (; q_it124 != q_end123; ++q_it124)
    {
        int B__BROKER_ID = q_it124->first;
        q411 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it120 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end119 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it120 != bigsum_A__P_dom_end119; ++bigsum_A__P_dom_it120)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it120;
            q411 += ( ( ( 0.25*qBIDS3+0.25*V <= qBIDS2[bigsum_A__P]+V*( (
                 bigsum_A__P < P )? ( 1 ) : ( 0 ) ) ) && (
                 qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q412 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it122 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end121 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it122 != bigsum_B__P_dom_end121; ++bigsum_B__P_dom_it122)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it122;
            q412 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q411*q412;
    }
    map<int,double>::iterator q_it130 = q.begin();
    map<int,double>::iterator q_end129 = q.end();
    for (; q_it130 != q_end129; ++q_it130)
    {
        int B__BROKER_ID = q_it130->first;
        q436 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it126 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end125 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it126 != bigsum_A__P_dom_end125; ++bigsum_A__P_dom_it126)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it126;
            q436 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q437 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it128 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end127 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it128 != bigsum_B__P_dom_end127; ++bigsum_B__P_dom_it128)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it128;
            q437 += ( ( ( 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) && (
                 qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q436*q437;
    }
    map<double,double>::iterator qBIDS1_it134 = qBIDS1.begin();
    map<double,double>::iterator qBIDS1_end133 = qBIDS1.end();
    for (; qBIDS1_it134 != qBIDS1_end133; ++qBIDS1_it134)
    {
        double bigsum_A__P = qBIDS1_it134->first;
        qBIDS1[P] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it132 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end131 = 
            ASKS.end();
        for (; ASKS_it132 != ASKS_end131; ++ASKS_it132)
        {
            double protect_A__T = get<0>(*ASKS_it132);
            int protect_A__ID = get<1>(*ASKS_it132);
            int protect_A__BROKER_ID = get<2>(*ASKS_it132);
            double protect_A__P = get<3>(*ASKS_it132);
            double protect_A__V = get<4>(*ASKS_it132);
            double var13 = 0;
            var13 += P;
            double var14 = 0;
            var14 += protect_A__P;
            if ( var13 == var14 )
            {
                double var12 = 1;
                var12 *= P;
                var12 *= protect_A__V;
                qBIDS1[P] += var12;
            }
        }
    }
    map<double,double>::iterator qBIDS2_it138 = qBIDS2.begin();
    map<double,double>::iterator qBIDS2_end137 = qBIDS2.end();
    for (; qBIDS2_it138 != qBIDS2_end137; ++qBIDS2_it138)
    {
        double bigsum_A__P = qBIDS2_it138->first;
        qBIDS2[P] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it136 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end135 = 
            ASKS.end();
        for (; ASKS_it136 != ASKS_end135; ++ASKS_it136)
        {
            double A2__T = get<0>(*ASKS_it136);
            int A2__ID = get<1>(*ASKS_it136);
            int A2__BROKER_ID = get<2>(*ASKS_it136);
            double A2__P = get<3>(*ASKS_it136);
            double A2__V = get<4>(*ASKS_it136);
            double var15 = 0;
            var15 += P;
            double var16 = 0;
            var16 += A2__P;
            if ( var15 < var16 )
            {
                qBIDS2[P] += A2__V;
            }
        }
    }
    qBIDS3 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_it140 = 
        ASKS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_end139 = 
        ASKS.end();
    for (; ASKS_it140 != ASKS_end139; ++ASKS_it140)
    {
        double A1__T = get<0>(*ASKS_it140);
        int A1__ID = get<1>(*ASKS_it140);
        int A1__BROKER_ID = get<2>(*ASKS_it140);
        double A1__P = get<3>(*ASKS_it140);
        double A1__V = get<4>(*ASKS_it140);
        qBIDS3 += A1__V;
    }
    map<double,double>::iterator qBIDS7_it144 = qBIDS7.begin();
    map<double,double>::iterator qBIDS7_end143 = qBIDS7.end();
    for (; qBIDS7_it144 != qBIDS7_end143; ++qBIDS7_it144)
    {
        double bigsum_A__P = qBIDS7_it144->first;
        qBIDS7[P] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it142 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end141 = 
            ASKS.end();
        for (; ASKS_it142 != ASKS_end141; ++ASKS_it142)
        {
            double protect_A__T = get<0>(*ASKS_it142);
            int protect_A__ID = get<1>(*ASKS_it142);
            int protect_A__BROKER_ID = get<2>(*ASKS_it142);
            double protect_A__P = get<3>(*ASKS_it142);
            double protect_A__V = get<4>(*ASKS_it142);
            double var17 = 0;
            var17 += P;
            double var18 = 0;
            var18 += protect_A__P;
            if ( var17 == var18 )
            {
                qBIDS7[P] += P;
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_ASKS_sec_span, on_insert_ASKS_usec_span);
}

void on_delete_BIDS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    BIDS.erase(make_tuple(T,ID,BROKER_ID,P,V));
    q451 = 0.0;
    set<double>::iterator bigsum_A__P_dom_it146 = bigsum_A__P_dom.begin();
    set<double>::iterator bigsum_A__P_dom_end145 = bigsum_A__P_dom.end();
    for (
        ; bigsum_A__P_dom_it146 != bigsum_A__P_dom_end145; ++bigsum_A__P_dom_it146)
    {
        double bigsum_A__P = *bigsum_A__P_dom_it146;
        q451 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
             ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
    }
    q452 = 0;
    set<double>::iterator bigsum_B__P_dom_it148 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end147 = bigsum_B__P_dom.end();
    for (
        ; bigsum_B__P_dom_it148 != bigsum_B__P_dom_end147; ++bigsum_B__P_dom_it148)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it148;
        q452 += ( ( qBIDS4[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+-1*0.25*V )? ( ( ( P == bigsum_B__P )?
             ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q[BROKER_ID] += -1*q451*q452;
    q574 = 0.0;
    set<double>::iterator bigsum_A__P_dom_it150 = bigsum_A__P_dom.begin();
    set<double>::iterator bigsum_A__P_dom_end149 = bigsum_A__P_dom.end();
    for (
        ; bigsum_A__P_dom_it150 != bigsum_A__P_dom_end149; ++bigsum_A__P_dom_it150)
    {
        double bigsum_A__P = *bigsum_A__P_dom_it150;
        q574 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
             ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
    }
    q576 = 0.0;
    set<double>::iterator bigsum_B__P_dom_it152 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end151 = bigsum_B__P_dom.end();
    for (
        ; bigsum_B__P_dom_it152 != bigsum_B__P_dom_end151; ++bigsum_B__P_dom_it152)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it152;
        q576 += ( ( qBIDS4[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+-1*0.25*V )? ( ( ( P == bigsum_B__P )?
             ( P ) : ( 0 ) )*V ) : ( 0 ) );
    }
    q[BROKER_ID] += -1*-1*q574*q576;
    map<int,double>::iterator q_it158 = q.begin();
    map<int,double>::iterator q_end157 = q.end();
    for (; q_it158 != q_end157; ++q_it158)
    {
        int BROKER_ID = q_it158->first;
        q477 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it154 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end153 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it154 != bigsum_A__P_dom_end153; ++bigsum_A__P_dom_it154)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it154;
            q477 += ( ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) && (
                 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q478 = 0;
        set<double>::iterator bigsum_B__P_dom_it156 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end155 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it156 != bigsum_B__P_dom_end155; ++bigsum_B__P_dom_it156)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it156;
            q478 += ( ( qBIDS4[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+-1*0.25*V )?
                 ( qBIDS6[make_tuple(BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*-1*q477*q478;
    }
    map<int,double>::iterator q_it164 = q.begin();
    map<int,double>::iterator q_end163 = q.end();
    for (; q_it164 != q_end163; ++q_it164)
    {
        int BROKER_ID = q_it164->first;
        q505 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it160 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end159 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it160 != bigsum_A__P_dom_end159; ++bigsum_A__P_dom_it160)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it160;
            q505 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q506 = 0;
        set<double>::iterator bigsum_B__P_dom_it162 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end161 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it162 != bigsum_B__P_dom_end161; ++bigsum_B__P_dom_it162)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it162;
            q506 += ( ( ( qBIDS4[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+-1*0.25*V ) && (
                 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) )?
                 ( qBIDS6[make_tuple(BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*-1*q505*q506;
    }
    map<int,double>::iterator q_it170 = q.begin();
    map<int,double>::iterator q_end169 = q.end();
    for (; q_it170 != q_end169; ++q_it170)
    {
        int BROKER_ID = q_it170->first;
        q532 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it166 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end165 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it166 != bigsum_A__P_dom_end165; ++bigsum_A__P_dom_it166)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it166;
            q532 += ( ( ( 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) && (
                 qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q533 = 0;
        set<double>::iterator bigsum_B__P_dom_it168 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end167 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it168 != bigsum_B__P_dom_end167; ++bigsum_B__P_dom_it168)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it168;
            q533 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS6[make_tuple(BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*q532*q533;
    }
    map<int,double>::iterator q_it176 = q.begin();
    map<int,double>::iterator q_end175 = q.end();
    for (; q_it176 != q_end175; ++q_it176)
    {
        int BROKER_ID = q_it176->first;
        q547 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it172 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end171 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it172 != bigsum_A__P_dom_end171; ++bigsum_A__P_dom_it172)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it172;
            q547 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q548 = 0;
        set<double>::iterator bigsum_B__P_dom_it174 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end173 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it174 != bigsum_B__P_dom_end173; ++bigsum_B__P_dom_it174)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it174;
            q548 += ( ( ( 0.25*qBIDS5+-1*0.25*V <= qBIDS4[bigsum_B__P]+-1*V*( (
                 bigsum_B__P < P )? ( 1 ) : ( 0 ) ) ) && (
                 qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) )?
                 ( qBIDS6[make_tuple(BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*q547*q548;
    }
    map<int,double>::iterator q_it182 = q.begin();
    map<int,double>::iterator q_end181 = q.end();
    for (; q_it182 != q_end181; ++q_it182)
    {
        int BROKER_ID = q_it182->first;
        q603 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it178 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end177 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it178 != bigsum_A__P_dom_end177; ++bigsum_A__P_dom_it178)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it178;
            q603 += ( ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) && (
                 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q604 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it180 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end179 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it180 != bigsum_B__P_dom_end179; ++bigsum_B__P_dom_it180)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it180;
            q604 += ( ( qBIDS4[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+-1*0.25*V )?
                 ( qBIDS8[make_tuple(bigsum_B__P,BROKER_ID)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*-1*q603*q604;
    }
    map<int,double>::iterator q_it188 = q.begin();
    map<int,double>::iterator q_end187 = q.end();
    for (; q_it188 != q_end187; ++q_it188)
    {
        int BROKER_ID = q_it188->first;
        q631 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it184 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end183 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it184 != bigsum_A__P_dom_end183; ++bigsum_A__P_dom_it184)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it184;
            q631 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q632 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it186 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end185 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it186 != bigsum_B__P_dom_end185; ++bigsum_B__P_dom_it186)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it186;
            q632 += ( ( ( qBIDS4[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+-1*0.25*V ) && (
                 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) )?
                 ( qBIDS8[make_tuple(bigsum_B__P,BROKER_ID)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*-1*q631*q632;
    }
    map<int,double>::iterator q_it194 = q.begin();
    map<int,double>::iterator q_end193 = q.end();
    for (; q_it194 != q_end193; ++q_it194)
    {
        int BROKER_ID = q_it194->first;
        q658 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it190 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end189 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it190 != bigsum_A__P_dom_end189; ++bigsum_A__P_dom_it190)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it190;
            q658 += ( ( ( 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) && (
                 qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q660 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it192 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end191 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it192 != bigsum_B__P_dom_end191; ++bigsum_B__P_dom_it192)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it192;
            q660 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS8[make_tuple(bigsum_B__P,BROKER_ID)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*-1*q658*q660;
    }
    map<int,double>::iterator q_it200 = q.begin();
    map<int,double>::iterator q_end199 = q.end();
    for (; q_it200 != q_end199; ++q_it200)
    {
        int BROKER_ID = q_it200->first;
        q674 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it196 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end195 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it196 != bigsum_A__P_dom_end195; ++bigsum_A__P_dom_it196)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it196;
            q674 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q676 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it198 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end197 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it198 != bigsum_B__P_dom_end197; ++bigsum_B__P_dom_it198)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it198;
            q676 += ( ( ( 0.25*qBIDS5+-1*0.25*V <= qBIDS4[bigsum_B__P]+-1*V*( (
                 bigsum_B__P < P )? ( 1 ) : ( 0 ) ) ) && (
                 qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) )?
                 ( qBIDS8[make_tuple(bigsum_B__P,BROKER_ID)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*-1*q674*q676;
    }
    map<double,double>::iterator qBIDS4_it204 = qBIDS4.begin();
    map<double,double>::iterator qBIDS4_end203 = qBIDS4.end();
    for (; qBIDS4_it204 != qBIDS4_end203; ++qBIDS4_it204)
    {
        double bigsum_B__P = qBIDS4_it204->first;
        qBIDS4[P] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it202 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end201 = 
            BIDS.end();
        for (; BIDS_it202 != BIDS_end201; ++BIDS_it202)
        {
            double B2__T = get<0>(*BIDS_it202);
            int B2__ID = get<1>(*BIDS_it202);
            int B2__BROKER_ID = get<2>(*BIDS_it202);
            double B2__P = get<3>(*BIDS_it202);
            double B2__V = get<4>(*BIDS_it202);
            double var19 = 0;
            var19 += P;
            double var20 = 0;
            var20 += B2__P;
            if ( var19 < var20 )
            {
                qBIDS4[P] += B2__V;
            }
        }
    }
    qBIDS5 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_it206 = 
        BIDS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_end205 = 
        BIDS.end();
    for (; BIDS_it206 != BIDS_end205; ++BIDS_it206)
    {
        double B1__T = get<0>(*BIDS_it206);
        int B1__ID = get<1>(*BIDS_it206);
        int B1__BROKER_ID = get<2>(*BIDS_it206);
        double B1__P = get<3>(*BIDS_it206);
        double B1__V = get<4>(*BIDS_it206);
        qBIDS5 += B1__V;
    }
    map<tuple<int,double>,int>::iterator qBIDS6_it210 = qBIDS6.begin();
    map<tuple<int,double>,int>::iterator qBIDS6_end209 = qBIDS6.end();
    for (; qBIDS6_it210 != qBIDS6_end209; ++qBIDS6_it210)
    {
        int BROKER_ID = get<0>(qBIDS6_it210->first);
        double bigsum_B__P = get<1>(qBIDS6_it210->first);
        qBIDS6[make_tuple(BROKER_ID,P)] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it208 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end207 = 
            BIDS.end();
        for (; BIDS_it208 != BIDS_end207; ++BIDS_it208)
        {
            double protect_B__T = get<0>(*BIDS_it208);
            int protect_B__ID = get<1>(*BIDS_it208);
            int protect_B__BROKER_ID = get<2>(*BIDS_it208);
            double protect_B__P = get<3>(*BIDS_it208);
            double protect_B__V = get<4>(*BIDS_it208);
            double var23 = 0;
            var23 += P;
            double var24 = 0;
            var24 += protect_B__P;
            if ( var23 == var24 )
            {
                int var21 = 0;
                var21 += BROKER_ID;
                int var22 = 0;
                var22 += protect_B__BROKER_ID;
                if ( var21 == var22 )
                {
                    qBIDS6[make_tuple(BROKER_ID,P)] += 1;
                }
            }
        }
    }
    map<tuple<double,int>,double>::iterator qBIDS8_it214 = qBIDS8.begin();
    map<tuple<double,int>,double>::iterator qBIDS8_end213 = qBIDS8.end();
    for (; qBIDS8_it214 != qBIDS8_end213; ++qBIDS8_it214)
    {
        double bigsum_B__P = get<0>(qBIDS8_it214->first);
        int BROKER_ID = get<1>(qBIDS8_it214->first);
        qBIDS8[make_tuple(P,BROKER_ID)] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it212 = 
            BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end211 = 
            BIDS.end();
        for (; BIDS_it212 != BIDS_end211; ++BIDS_it212)
        {
            double protect_B__T = get<0>(*BIDS_it212);
            int protect_B__ID = get<1>(*BIDS_it212);
            int protect_B__BROKER_ID = get<2>(*BIDS_it212);
            double protect_B__P = get<3>(*BIDS_it212);
            double protect_B__V = get<4>(*BIDS_it212);
            double var28 = 0;
            var28 += P;
            double var29 = 0;
            var29 += protect_B__P;
            if ( var28 == var29 )
            {
                int var26 = 0;
                var26 += BROKER_ID;
                int var27 = 0;
                var27 += protect_B__BROKER_ID;
                if ( var26 == var27 )
                {
                    double var25 = 1;
                    var25 *= P;
                    var25 *= protect_B__V;
                    qBIDS8[make_tuple(P,BROKER_ID)] += var25;
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_BIDS_sec_span, on_delete_BIDS_usec_span);
}

void on_delete_ASKS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    ASKS.erase(make_tuple(T,ID,BROKER_ID,P,V));
    map<int,double>::iterator q_it220 = q.begin();
    map<int,double>::iterator q_end219 = q.end();
    for (; q_it220 != q_end219; ++q_it220)
    {
        int B__BROKER_ID = q_it220->first;
        q702 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it216 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end215 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it216 != bigsum_A__P_dom_end215; ++bigsum_A__P_dom_it216)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it216;
            q702 += ( ( qBIDS2[bigsum_A__P]+-1*V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+-1*0.25*V )? ( ( ( P == bigsum_A__P )?
                 ( P ) : ( 0 ) )*V ) : ( 0 ) );
        }
        q703 = 0;
        set<double>::iterator bigsum_B__P_dom_it218 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end217 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it218 != bigsum_B__P_dom_end217; ++bigsum_B__P_dom_it218)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it218;
            q703 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q702*q703;
    }
    map<int,double>::iterator q_it226 = q.begin();
    map<int,double>::iterator q_end225 = q.end();
    for (; q_it226 != q_end225; ++q_it226)
    {
        int B__BROKER_ID = q_it226->first;
        q730 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it222 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end221 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it222 != bigsum_A__P_dom_end221; ++bigsum_A__P_dom_it222)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it222;
            q730 += ( ( ( qBIDS2[bigsum_A__P]+-1*V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+-1*0.25*V ) && (
                 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q731 = 0;
        set<double>::iterator bigsum_B__P_dom_it224 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end223 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it224 != bigsum_B__P_dom_end223; ++bigsum_B__P_dom_it224)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it224;
            q731 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*-1*q730*q731;
    }
    map<int,double>::iterator q_it232 = q.begin();
    map<int,double>::iterator q_end231 = q.end();
    for (; q_it232 != q_end231; ++q_it232)
    {
        int B__BROKER_ID = q_it232->first;
        q758 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it228 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end227 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it228 != bigsum_A__P_dom_end227; ++bigsum_A__P_dom_it228)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it228;
            q758 += ( ( qBIDS2[bigsum_A__P]+-1*V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+-1*0.25*V )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q759 = 0;
        set<double>::iterator bigsum_B__P_dom_it230 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end229 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it230 != bigsum_B__P_dom_end229; ++bigsum_B__P_dom_it230)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it230;
            q759 += ( ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) && (
                 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*-1*q758*q759;
    }
    map<int,double>::iterator q_it238 = q.begin();
    map<int,double>::iterator q_end237 = q.end();
    for (; q_it238 != q_end237; ++q_it238)
    {
        int B__BROKER_ID = q_it238->first;
        q785 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it234 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end233 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it234 != bigsum_A__P_dom_end233; ++bigsum_A__P_dom_it234)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it234;
            q785 += ( ( ( 0.25*qBIDS3+-1*0.25*V <= qBIDS2[bigsum_A__P]+-1*V*( (
                 bigsum_A__P < P )? ( 1 ) : ( 0 ) ) ) && (
                 qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q786 = 0;
        set<double>::iterator bigsum_B__P_dom_it236 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end235 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it236 != bigsum_B__P_dom_end235; ++bigsum_B__P_dom_it236)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it236;
            q786 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q785*q786;
    }
    map<int,double>::iterator q_it244 = q.begin();
    map<int,double>::iterator q_end243 = q.end();
    for (; q_it244 != q_end243; ++q_it244)
    {
        int B__BROKER_ID = q_it244->first;
        q812 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it240 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end239 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it240 != bigsum_A__P_dom_end239; ++bigsum_A__P_dom_it240)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it240;
            q812 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q813 = 0;
        set<double>::iterator bigsum_B__P_dom_it242 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end241 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it242 != bigsum_B__P_dom_end241; ++bigsum_B__P_dom_it242)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it242;
            q813 += ( ( ( 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) && (
                 qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q812*q813;
    }
    map<int,double>::iterator q_it250 = q.begin();
    map<int,double>::iterator q_end249 = q.end();
    for (; q_it250 != q_end249; ++q_it250)
    {
        int B__BROKER_ID = q_it250->first;
        q827 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it246 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end245 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it246 != bigsum_A__P_dom_end245; ++bigsum_A__P_dom_it246)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it246;
            q827 += ( ( qBIDS2[bigsum_A__P]+-1*V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+-1*0.25*V )? ( ( ( P == bigsum_A__P )?
                 ( P ) : ( 0 ) ) ) : ( 0 ) );
        }
        q829 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it248 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end247 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it248 != bigsum_B__P_dom_end247; ++bigsum_B__P_dom_it248)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it248;
            q829 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*-1*q827*q829;
    }
    map<int,double>::iterator q_it256 = q.begin();
    map<int,double>::iterator q_end255 = q.end();
    for (; q_it256 != q_end255; ++q_it256)
    {
        int B__BROKER_ID = q_it256->first;
        q854 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it252 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end251 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it252 != bigsum_A__P_dom_end251; ++bigsum_A__P_dom_it252)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it252;
            q854 += ( ( ( qBIDS2[bigsum_A__P]+-1*V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+-1*0.25*V ) && (
                 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q855 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it254 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end253 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it254 != bigsum_B__P_dom_end253; ++bigsum_B__P_dom_it254)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it254;
            q855 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*-1*q854*q855;
    }
    map<int,double>::iterator q_it262 = q.begin();
    map<int,double>::iterator q_end261 = q.end();
    for (; q_it262 != q_end261; ++q_it262)
    {
        int B__BROKER_ID = q_it262->first;
        q882 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it258 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end257 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it258 != bigsum_A__P_dom_end257; ++bigsum_A__P_dom_it258)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it258;
            q882 += ( ( qBIDS2[bigsum_A__P]+-1*V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+-1*0.25*V )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q883 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it260 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end259 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it260 != bigsum_B__P_dom_end259; ++bigsum_B__P_dom_it260)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it260;
            q883 += ( ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) && (
                 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*-1*q882*q883;
    }
    map<int,double>::iterator q_it268 = q.begin();
    map<int,double>::iterator q_end267 = q.end();
    for (; q_it268 != q_end267; ++q_it268)
    {
        int B__BROKER_ID = q_it268->first;
        q909 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it264 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end263 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it264 != bigsum_A__P_dom_end263; ++bigsum_A__P_dom_it264)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it264;
            q909 += ( ( ( 0.25*qBIDS3+-1*0.25*V <= qBIDS2[bigsum_A__P]+-1*V*( (
                 bigsum_A__P < P )? ( 1 ) : ( 0 ) ) ) && (
                 qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q911 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it266 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end265 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it266 != bigsum_B__P_dom_end265; ++bigsum_B__P_dom_it266)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it266;
            q911 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*-1*q909*q911;
    }
    map<int,double>::iterator q_it274 = q.begin();
    map<int,double>::iterator q_end273 = q.end();
    for (; q_it274 != q_end273; ++q_it274)
    {
        int B__BROKER_ID = q_it274->first;
        q937 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it270 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end269 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it270 != bigsum_A__P_dom_end269; ++bigsum_A__P_dom_it270)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it270;
            q937 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q939 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it272 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end271 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it272 != bigsum_B__P_dom_end271; ++bigsum_B__P_dom_it272)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it272;
            q939 += ( ( ( 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) && (
                 qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*-1*q937*q939;
    }
    map<double,double>::iterator qBIDS1_it278 = qBIDS1.begin();
    map<double,double>::iterator qBIDS1_end277 = qBIDS1.end();
    for (; qBIDS1_it278 != qBIDS1_end277; ++qBIDS1_it278)
    {
        double bigsum_A__P = qBIDS1_it278->first;
        qBIDS1[P] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it276 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end275 = 
            ASKS.end();
        for (; ASKS_it276 != ASKS_end275; ++ASKS_it276)
        {
            double protect_A__T = get<0>(*ASKS_it276);
            int protect_A__ID = get<1>(*ASKS_it276);
            int protect_A__BROKER_ID = get<2>(*ASKS_it276);
            double protect_A__P = get<3>(*ASKS_it276);
            double protect_A__V = get<4>(*ASKS_it276);
            double var31 = 0;
            var31 += P;
            double var32 = 0;
            var32 += protect_A__P;
            if ( var31 == var32 )
            {
                double var30 = 1;
                var30 *= P;
                var30 *= protect_A__V;
                qBIDS1[P] += var30;
            }
        }
    }
    map<double,double>::iterator qBIDS2_it282 = qBIDS2.begin();
    map<double,double>::iterator qBIDS2_end281 = qBIDS2.end();
    for (; qBIDS2_it282 != qBIDS2_end281; ++qBIDS2_it282)
    {
        double bigsum_A__P = qBIDS2_it282->first;
        qBIDS2[P] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it280 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end279 = 
            ASKS.end();
        for (; ASKS_it280 != ASKS_end279; ++ASKS_it280)
        {
            double A2__T = get<0>(*ASKS_it280);
            int A2__ID = get<1>(*ASKS_it280);
            int A2__BROKER_ID = get<2>(*ASKS_it280);
            double A2__P = get<3>(*ASKS_it280);
            double A2__V = get<4>(*ASKS_it280);
            double var33 = 0;
            var33 += P;
            double var34 = 0;
            var34 += A2__P;
            if ( var33 < var34 )
            {
                qBIDS2[P] += A2__V;
            }
        }
    }
    qBIDS3 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_it284 = 
        ASKS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator ASKS_end283 = 
        ASKS.end();
    for (; ASKS_it284 != ASKS_end283; ++ASKS_it284)
    {
        double A1__T = get<0>(*ASKS_it284);
        int A1__ID = get<1>(*ASKS_it284);
        int A1__BROKER_ID = get<2>(*ASKS_it284);
        double A1__P = get<3>(*ASKS_it284);
        double A1__V = get<4>(*ASKS_it284);
        qBIDS3 += A1__V;
    }
    map<double,double>::iterator qBIDS7_it288 = qBIDS7.begin();
    map<double,double>::iterator qBIDS7_end287 = qBIDS7.end();
    for (; qBIDS7_it288 != qBIDS7_end287; ++qBIDS7_it288)
    {
        double bigsum_A__P = qBIDS7_it288->first;
        qBIDS7[P] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_it286 = 
            ASKS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator ASKS_end285 = 
            ASKS.end();
        for (; ASKS_it286 != ASKS_end285; ++ASKS_it286)
        {
            double protect_A__T = get<0>(*ASKS_it286);
            int protect_A__ID = get<1>(*ASKS_it286);
            int protect_A__BROKER_ID = get<2>(*ASKS_it286);
            double protect_A__P = get<3>(*ASKS_it286);
            double protect_A__V = get<4>(*ASKS_it286);
            double var35 = 0;
            var35 += P;
            double var36 = 0;
            var36 += protect_A__P;
            if ( var35 == var36 )
            {
                qBIDS7[P] += P;
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_ASKS_sec_span, on_delete_ASKS_usec_span);
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