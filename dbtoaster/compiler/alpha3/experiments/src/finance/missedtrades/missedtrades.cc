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
double q832;
int q833;
double q497;
double q323;
double q46;
int q498;
int q324;
int q47;
double q110;
double q623;
double q750;
double q624;
double q112;
int q751;
double q411;
double q412;
set<double> bigsum_A__P_dom;
set<double> bigsum_B__P_dom;
double q929;
double q201;
double q249;
double q202;
double q160;
double q162;
map<double,double> qBIDS1;
map<double,double> qBIDS2;
double qBIDS3;
double q847;
double q678;
map<double,double> qBIDS4;
double qBIDS5;
double q849;
map<tuple<int,double>,int> qBIDS6;
double q337;
map<double,double> qBIDS7;
double qBIDS8996;
double q931;
map<tuple<double,int>,double> qBIDS8;
double q339;
int q250;
double q805;
double q594;
double q298;
map<int,double> q;
int q806;
double qBIDS1460;
int q299;
double q596;
double q552;
double q722;
double q680;
int q553;
int q723;
double q385;
double q0;
double qBIDS71001;
int q1;
double q387;
double q471;
int q472;
double q22;
int q23;
double q135;
double q137;
double q778;
double q436;
int q779;
double q902;
double q437;
double q71;
double q903;
int q72;
double q694;
double q567;
double q225;
int q568;
int q226;
double q696;
double q186;
double q651;
double q187;
double q652;
double q525;
int q526;
double qBIDS1991;
double q273;
int q274;
double q957;
double qBIDS8464;
double q959;
double q360;
double q874;
double q362;
double q875;
double q86;
int q87;

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
    map<double,double>::iterator qBIDS4_it58 = qBIDS4.begin();
    map<double,double>::iterator qBIDS4_end57 = qBIDS4.end();
    for (; qBIDS4_it58 != qBIDS4_end57; ++qBIDS4_it58)
    {
        double bigsum_B__P = qBIDS4_it58->first;
        qBIDS4[bigsum_B__P] += V*( ( bigsum_B__P < P )? ( 1 ) : ( 0 ) );
    }
    qBIDS5 += V;
    map<tuple<int,double>,int>::iterator qBIDS6_it60 = qBIDS6.begin();
    map<tuple<int,double>,int>::iterator qBIDS6_end59 = qBIDS6.end();
    for (; qBIDS6_it60 != qBIDS6_end59; ++qBIDS6_it60)
    {
        double bigsum_B__P = get<1>(qBIDS6_it60->first);
        qBIDS6[make_tuple(BROKER_ID,bigsum_B__P)] += ( ( bigsum_B__P == P )?
             ( 1 ) : ( 0 ) );
    }
    map<tuple<double,int>,double>::iterator qBIDS8_it64 = qBIDS8.begin();
    map<tuple<double,int>,double>::iterator qBIDS8_end63 = qBIDS8.end();
    for (; qBIDS8_it64 != qBIDS8_end63; ++qBIDS8_it64)
    {
        double bigsum_B__P = get<0>(qBIDS8_it64->first);
        qBIDS8464 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it62 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end61 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it62 != bigsum_B__P_dom_end61; ++bigsum_B__P_dom_it62)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it62;
            qBIDS8464 += ( ( bigsum_B__P == P )? ( bigsum_B__P ) : ( 0 ) );
        }
        qBIDS8[make_tuple(bigsum_B__P,BROKER_ID)] += V*qBIDS8464;
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
    map<int,double>::iterator q_it70 = q.begin();
    map<int,double>::iterator q_end69 = q.end();
    for (; q_it70 != q_end69; ++q_it70)
    {
        int B__BROKER_ID = q_it70->first;
        q225 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it66 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end65 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it66 != bigsum_A__P_dom_end65; ++bigsum_A__P_dom_it66)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it66;
            q225 += ( ( qBIDS2[bigsum_A__P]+V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+0.25*V )? ( ( ( P == bigsum_A__P )?
                 ( P ) : ( 0 ) )*V ) : ( 0 ) );
        }
        q226 = 0;
        set<double>::iterator bigsum_B__P_dom_it68 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end67 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it68 != bigsum_B__P_dom_end67; ++bigsum_B__P_dom_it68)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it68;
            q226 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += q225*q226;
    }
    map<int,double>::iterator q_it76 = q.begin();
    map<int,double>::iterator q_end75 = q.end();
    for (; q_it76 != q_end75; ++q_it76)
    {
        int B__BROKER_ID = q_it76->first;
        q249 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it72 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end71 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it72 != bigsum_A__P_dom_end71; ++bigsum_A__P_dom_it72)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it72;
            q249 += ( ( ( qBIDS2[bigsum_A__P]+V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+0.25*V ) && (
                 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q250 = 0;
        set<double>::iterator bigsum_B__P_dom_it74 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end73 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it74 != bigsum_B__P_dom_end73; ++bigsum_B__P_dom_it74)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it74;
            q250 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += q249*q250;
    }
    map<int,double>::iterator q_it82 = q.begin();
    map<int,double>::iterator q_end81 = q.end();
    for (; q_it82 != q_end81; ++q_it82)
    {
        int B__BROKER_ID = q_it82->first;
        q273 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it78 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end77 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it78 != bigsum_A__P_dom_end77; ++bigsum_A__P_dom_it78)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it78;
            q273 += ( ( qBIDS2[bigsum_A__P]+V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+0.25*V )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q274 = 0;
        set<double>::iterator bigsum_B__P_dom_it80 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end79 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it80 != bigsum_B__P_dom_end79; ++bigsum_B__P_dom_it80)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it80;
            q274 += ( ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) && (
                 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += q273*q274;
    }
    map<int,double>::iterator q_it88 = q.begin();
    map<int,double>::iterator q_end87 = q.end();
    for (; q_it88 != q_end87; ++q_it88)
    {
        int B__BROKER_ID = q_it88->first;
        q298 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it84 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end83 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it84 != bigsum_A__P_dom_end83; ++bigsum_A__P_dom_it84)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it84;
            q298 += ( ( ( 0.25*qBIDS3+0.25*V <= qBIDS2[bigsum_A__P]+V*( (
                 bigsum_A__P < P )? ( 1 ) : ( 0 ) ) ) && (
                 qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q299 = 0;
        set<double>::iterator bigsum_B__P_dom_it86 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end85 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it86 != bigsum_B__P_dom_end85; ++bigsum_B__P_dom_it86)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it86;
            q299 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q298*q299;
    }
    map<int,double>::iterator q_it94 = q.begin();
    map<int,double>::iterator q_end93 = q.end();
    for (; q_it94 != q_end93; ++q_it94)
    {
        int B__BROKER_ID = q_it94->first;
        q323 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it90 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end89 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it90 != bigsum_A__P_dom_end89; ++bigsum_A__P_dom_it90)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it90;
            q323 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q324 = 0;
        set<double>::iterator bigsum_B__P_dom_it92 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end91 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it92 != bigsum_B__P_dom_end91; ++bigsum_B__P_dom_it92)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it92;
            q324 += ( ( ( 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) && (
                 qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q323*q324;
    }
    map<int,double>::iterator q_it100 = q.begin();
    map<int,double>::iterator q_end99 = q.end();
    for (; q_it100 != q_end99; ++q_it100)
    {
        int B__BROKER_ID = q_it100->first;
        q337 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it96 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end95 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it96 != bigsum_A__P_dom_end95; ++bigsum_A__P_dom_it96)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it96;
            q337 += ( ( qBIDS2[bigsum_A__P]+V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+0.25*V )? ( ( ( P == bigsum_A__P )?
                 ( P ) : ( 0 ) ) ) : ( 0 ) );
        }
        q339 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it98 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end97 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it98 != bigsum_B__P_dom_end97; ++bigsum_B__P_dom_it98)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it98;
            q339 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q337*q339;
    }
    map<int,double>::iterator q_it106 = q.begin();
    map<int,double>::iterator q_end105 = q.end();
    for (; q_it106 != q_end105; ++q_it106)
    {
        int B__BROKER_ID = q_it106->first;
        q360 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it102 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end101 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it102 != bigsum_A__P_dom_end101; ++bigsum_A__P_dom_it102)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it102;
            q360 += ( ( ( qBIDS2[bigsum_A__P]+V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+0.25*V ) && (
                 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q362 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it104 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end103 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it104 != bigsum_B__P_dom_end103; ++bigsum_B__P_dom_it104)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it104;
            q362 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q360*q362;
    }
    map<int,double>::iterator q_it112 = q.begin();
    map<int,double>::iterator q_end111 = q.end();
    for (; q_it112 != q_end111; ++q_it112)
    {
        int B__BROKER_ID = q_it112->first;
        q385 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it108 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end107 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it108 != bigsum_A__P_dom_end107; ++bigsum_A__P_dom_it108)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it108;
            q385 += ( ( qBIDS2[bigsum_A__P]+V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+0.25*V )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q387 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it110 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end109 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it110 != bigsum_B__P_dom_end109; ++bigsum_B__P_dom_it110)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it110;
            q387 += ( ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) && (
                 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q385*q387;
    }
    map<int,double>::iterator q_it118 = q.begin();
    map<int,double>::iterator q_end117 = q.end();
    for (; q_it118 != q_end117; ++q_it118)
    {
        int B__BROKER_ID = q_it118->first;
        q411 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it114 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end113 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it114 != bigsum_A__P_dom_end113; ++bigsum_A__P_dom_it114)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it114;
            q411 += ( ( ( 0.25*qBIDS3+0.25*V <= qBIDS2[bigsum_A__P]+V*( (
                 bigsum_A__P < P )? ( 1 ) : ( 0 ) ) ) && (
                 qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q412 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it116 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end115 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it116 != bigsum_B__P_dom_end115; ++bigsum_B__P_dom_it116)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it116;
            q412 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q411*q412;
    }
    map<int,double>::iterator q_it124 = q.begin();
    map<int,double>::iterator q_end123 = q.end();
    for (; q_it124 != q_end123; ++q_it124)
    {
        int B__BROKER_ID = q_it124->first;
        q436 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it120 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end119 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it120 != bigsum_A__P_dom_end119; ++bigsum_A__P_dom_it120)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it120;
            q436 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q437 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it122 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end121 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it122 != bigsum_B__P_dom_end121; ++bigsum_B__P_dom_it122)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it122;
            q437 += ( ( ( 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) && (
                 qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q436*q437;
    }
    map<double,double>::iterator qBIDS1_it128 = qBIDS1.begin();
    map<double,double>::iterator qBIDS1_end127 = qBIDS1.end();
    for (; qBIDS1_it128 != qBIDS1_end127; ++qBIDS1_it128)
    {
        double bigsum_A__P = qBIDS1_it128->first;
        qBIDS1460 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it126 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end125 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it126 != bigsum_A__P_dom_end125; ++bigsum_A__P_dom_it126)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it126;
            qBIDS1460 += ( ( bigsum_A__P == P )? ( bigsum_A__P ) : ( 0 ) );
        }
        qBIDS1[bigsum_A__P] += V*qBIDS1460;
    }
    map<double,double>::iterator qBIDS2_it130 = qBIDS2.begin();
    map<double,double>::iterator qBIDS2_end129 = qBIDS2.end();
    for (; qBIDS2_it130 != qBIDS2_end129; ++qBIDS2_it130)
    {
        double bigsum_A__P = qBIDS2_it130->first;
        qBIDS2[bigsum_A__P] += V*( ( bigsum_A__P < P )? ( 1 ) : ( 0 ) );
    }
    qBIDS3 += V;
    map<double,double>::iterator qBIDS7_it134 = qBIDS7.begin();
    map<double,double>::iterator qBIDS7_end133 = qBIDS7.end();
    for (; qBIDS7_it134 != qBIDS7_end133; ++qBIDS7_it134)
    {
        double bigsum_A__P = qBIDS7_it134->first;
        set<double>::iterator bigsum_A__P_dom_it132 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end131 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it132 != bigsum_A__P_dom_end131; ++bigsum_A__P_dom_it132)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it132;
            qBIDS7[bigsum_A__P] += ( ( bigsum_A__P == P )?
                 ( bigsum_A__P ) : ( 0 ) );
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
    q471 = 0.0;
    set<double>::iterator bigsum_A__P_dom_it136 = bigsum_A__P_dom.begin();
    set<double>::iterator bigsum_A__P_dom_end135 = bigsum_A__P_dom.end();
    for (
        ; bigsum_A__P_dom_it136 != bigsum_A__P_dom_end135; ++bigsum_A__P_dom_it136)
    {
        double bigsum_A__P = *bigsum_A__P_dom_it136;
        q471 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
             ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
    }
    q472 = 0;
    set<double>::iterator bigsum_B__P_dom_it138 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end137 = bigsum_B__P_dom.end();
    for (
        ; bigsum_B__P_dom_it138 != bigsum_B__P_dom_end137; ++bigsum_B__P_dom_it138)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it138;
        q472 += ( ( qBIDS4[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+-1*0.25*V )? ( ( ( P == bigsum_B__P )?
             ( 1 ) : ( 0 ) ) ) : ( 0 ) );
    }
    q[BROKER_ID] += -1*q471*q472;
    q594 = 0.0;
    set<double>::iterator bigsum_A__P_dom_it140 = bigsum_A__P_dom.begin();
    set<double>::iterator bigsum_A__P_dom_end139 = bigsum_A__P_dom.end();
    for (
        ; bigsum_A__P_dom_it140 != bigsum_A__P_dom_end139; ++bigsum_A__P_dom_it140)
    {
        double bigsum_A__P = *bigsum_A__P_dom_it140;
        q594 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
             ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
    }
    q596 = 0.0;
    set<double>::iterator bigsum_B__P_dom_it142 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end141 = bigsum_B__P_dom.end();
    for (
        ; bigsum_B__P_dom_it142 != bigsum_B__P_dom_end141; ++bigsum_B__P_dom_it142)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it142;
        q596 += ( ( qBIDS4[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+-1*0.25*V )? ( ( ( P == bigsum_B__P )?
             ( P ) : ( 0 ) )*V ) : ( 0 ) );
    }
    q[BROKER_ID] += -1*-1*q594*q596;
    map<int,double>::iterator q_it148 = q.begin();
    map<int,double>::iterator q_end147 = q.end();
    for (; q_it148 != q_end147; ++q_it148)
    {
        int BROKER_ID = q_it148->first;
        q497 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it144 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end143 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it144 != bigsum_A__P_dom_end143; ++bigsum_A__P_dom_it144)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it144;
            q497 += ( ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) && (
                 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q498 = 0;
        set<double>::iterator bigsum_B__P_dom_it146 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end145 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it146 != bigsum_B__P_dom_end145; ++bigsum_B__P_dom_it146)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it146;
            q498 += ( ( qBIDS4[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+-1*0.25*V )?
                 ( qBIDS6[make_tuple(BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*-1*q497*q498;
    }
    map<int,double>::iterator q_it154 = q.begin();
    map<int,double>::iterator q_end153 = q.end();
    for (; q_it154 != q_end153; ++q_it154)
    {
        int BROKER_ID = q_it154->first;
        q525 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it150 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end149 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it150 != bigsum_A__P_dom_end149; ++bigsum_A__P_dom_it150)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it150;
            q525 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q526 = 0;
        set<double>::iterator bigsum_B__P_dom_it152 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end151 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it152 != bigsum_B__P_dom_end151; ++bigsum_B__P_dom_it152)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it152;
            q526 += ( ( ( qBIDS4[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+-1*0.25*V ) && (
                 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) )?
                 ( qBIDS6[make_tuple(BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*-1*q525*q526;
    }
    map<int,double>::iterator q_it160 = q.begin();
    map<int,double>::iterator q_end159 = q.end();
    for (; q_it160 != q_end159; ++q_it160)
    {
        int BROKER_ID = q_it160->first;
        q552 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it156 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end155 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it156 != bigsum_A__P_dom_end155; ++bigsum_A__P_dom_it156)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it156;
            q552 += ( ( ( 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) && (
                 qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q553 = 0;
        set<double>::iterator bigsum_B__P_dom_it158 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end157 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it158 != bigsum_B__P_dom_end157; ++bigsum_B__P_dom_it158)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it158;
            q553 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS6[make_tuple(BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*q552*q553;
    }
    map<int,double>::iterator q_it166 = q.begin();
    map<int,double>::iterator q_end165 = q.end();
    for (; q_it166 != q_end165; ++q_it166)
    {
        int BROKER_ID = q_it166->first;
        q567 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it162 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end161 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it162 != bigsum_A__P_dom_end161; ++bigsum_A__P_dom_it162)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it162;
            q567 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q568 = 0;
        set<double>::iterator bigsum_B__P_dom_it164 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end163 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it164 != bigsum_B__P_dom_end163; ++bigsum_B__P_dom_it164)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it164;
            q568 += ( ( ( 0.25*qBIDS5+-1*0.25*V <= qBIDS4[bigsum_B__P]+-1*V*( (
                 bigsum_B__P < P )? ( 1 ) : ( 0 ) ) ) && (
                 qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) )?
                 ( qBIDS6[make_tuple(BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*q567*q568;
    }
    map<int,double>::iterator q_it172 = q.begin();
    map<int,double>::iterator q_end171 = q.end();
    for (; q_it172 != q_end171; ++q_it172)
    {
        int BROKER_ID = q_it172->first;
        q623 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it168 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end167 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it168 != bigsum_A__P_dom_end167; ++bigsum_A__P_dom_it168)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it168;
            q623 += ( ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) && (
                 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q624 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it170 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end169 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it170 != bigsum_B__P_dom_end169; ++bigsum_B__P_dom_it170)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it170;
            q624 += ( ( qBIDS4[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+-1*0.25*V )?
                 ( qBIDS8[make_tuple(bigsum_B__P,BROKER_ID)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*-1*q623*q624;
    }
    map<int,double>::iterator q_it178 = q.begin();
    map<int,double>::iterator q_end177 = q.end();
    for (; q_it178 != q_end177; ++q_it178)
    {
        int BROKER_ID = q_it178->first;
        q651 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it174 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end173 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it174 != bigsum_A__P_dom_end173; ++bigsum_A__P_dom_it174)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it174;
            q651 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q652 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it176 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end175 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it176 != bigsum_B__P_dom_end175; ++bigsum_B__P_dom_it176)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it176;
            q652 += ( ( ( qBIDS4[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS5+-1*0.25*V ) && (
                 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) )?
                 ( qBIDS8[make_tuple(bigsum_B__P,BROKER_ID)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*-1*q651*q652;
    }
    map<int,double>::iterator q_it184 = q.begin();
    map<int,double>::iterator q_end183 = q.end();
    for (; q_it184 != q_end183; ++q_it184)
    {
        int BROKER_ID = q_it184->first;
        q678 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it180 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end179 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it180 != bigsum_A__P_dom_end179; ++bigsum_A__P_dom_it180)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it180;
            q678 += ( ( ( 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) && (
                 qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q680 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it182 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end181 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it182 != bigsum_B__P_dom_end181; ++bigsum_B__P_dom_it182)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it182;
            q680 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS8[make_tuple(bigsum_B__P,BROKER_ID)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*-1*q678*q680;
    }
    map<int,double>::iterator q_it190 = q.begin();
    map<int,double>::iterator q_end189 = q.end();
    for (; q_it190 != q_end189; ++q_it190)
    {
        int BROKER_ID = q_it190->first;
        q694 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it186 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end185 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it186 != bigsum_A__P_dom_end185; ++bigsum_A__P_dom_it186)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it186;
            q694 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q696 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it188 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end187 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it188 != bigsum_B__P_dom_end187; ++bigsum_B__P_dom_it188)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it188;
            q696 += ( ( ( 0.25*qBIDS5+-1*0.25*V <= qBIDS4[bigsum_B__P]+-1*V*( (
                 bigsum_B__P < P )? ( 1 ) : ( 0 ) ) ) && (
                 qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) )?
                 ( qBIDS8[make_tuple(bigsum_B__P,BROKER_ID)] ) : ( 0 ) );
        }
        q[BROKER_ID] += -1*-1*q694*q696;
    }
    map<double,double>::iterator qBIDS4_it192 = qBIDS4.begin();
    map<double,double>::iterator qBIDS4_end191 = qBIDS4.end();
    for (; qBIDS4_it192 != qBIDS4_end191; ++qBIDS4_it192)
    {
        double bigsum_B__P = qBIDS4_it192->first;
        qBIDS4[bigsum_B__P] += -1*V*( ( bigsum_B__P < P )? ( 1 ) : ( 0 ) );
    }
    qBIDS5 += -1*V;
    map<tuple<int,double>,int>::iterator qBIDS6_it194 = qBIDS6.begin();
    map<tuple<int,double>,int>::iterator qBIDS6_end193 = qBIDS6.end();
    for (; qBIDS6_it194 != qBIDS6_end193; ++qBIDS6_it194)
    {
        double bigsum_B__P = get<1>(qBIDS6_it194->first);
        qBIDS6[make_tuple(BROKER_ID,bigsum_B__P)] += -1*( ( bigsum_B__P == P )?
             ( 1 ) : ( 0 ) );
    }
    map<tuple<double,int>,double>::iterator qBIDS8_it198 = qBIDS8.begin();
    map<tuple<double,int>,double>::iterator qBIDS8_end197 = qBIDS8.end();
    for (; qBIDS8_it198 != qBIDS8_end197; ++qBIDS8_it198)
    {
        double bigsum_B__P = get<0>(qBIDS8_it198->first);
        qBIDS8996 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it196 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end195 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it196 != bigsum_B__P_dom_end195; ++bigsum_B__P_dom_it196)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it196;
            qBIDS8996 += ( ( bigsum_B__P == P )? ( bigsum_B__P ) : ( 0 ) );
        }
        qBIDS8[make_tuple(bigsum_B__P,BROKER_ID)] += -1*V*qBIDS8996;
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
    map<int,double>::iterator q_it204 = q.begin();
    map<int,double>::iterator q_end203 = q.end();
    for (; q_it204 != q_end203; ++q_it204)
    {
        int B__BROKER_ID = q_it204->first;
        q722 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it200 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end199 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it200 != bigsum_A__P_dom_end199; ++bigsum_A__P_dom_it200)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it200;
            q722 += ( ( qBIDS2[bigsum_A__P]+-1*V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+-1*0.25*V )? ( ( ( P == bigsum_A__P )?
                 ( P ) : ( 0 ) )*V ) : ( 0 ) );
        }
        q723 = 0;
        set<double>::iterator bigsum_B__P_dom_it202 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end201 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it202 != bigsum_B__P_dom_end201; ++bigsum_B__P_dom_it202)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it202;
            q723 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q722*q723;
    }
    map<int,double>::iterator q_it210 = q.begin();
    map<int,double>::iterator q_end209 = q.end();
    for (; q_it210 != q_end209; ++q_it210)
    {
        int B__BROKER_ID = q_it210->first;
        q750 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it206 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end205 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it206 != bigsum_A__P_dom_end205; ++bigsum_A__P_dom_it206)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it206;
            q750 += ( ( ( qBIDS2[bigsum_A__P]+-1*V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+-1*0.25*V ) && (
                 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q751 = 0;
        set<double>::iterator bigsum_B__P_dom_it208 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end207 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it208 != bigsum_B__P_dom_end207; ++bigsum_B__P_dom_it208)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it208;
            q751 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*-1*q750*q751;
    }
    map<int,double>::iterator q_it216 = q.begin();
    map<int,double>::iterator q_end215 = q.end();
    for (; q_it216 != q_end215; ++q_it216)
    {
        int B__BROKER_ID = q_it216->first;
        q778 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it212 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end211 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it212 != bigsum_A__P_dom_end211; ++bigsum_A__P_dom_it212)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it212;
            q778 += ( ( qBIDS2[bigsum_A__P]+-1*V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+-1*0.25*V )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q779 = 0;
        set<double>::iterator bigsum_B__P_dom_it214 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end213 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it214 != bigsum_B__P_dom_end213; ++bigsum_B__P_dom_it214)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it214;
            q779 += ( ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) && (
                 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*-1*q778*q779;
    }
    map<int,double>::iterator q_it222 = q.begin();
    map<int,double>::iterator q_end221 = q.end();
    for (; q_it222 != q_end221; ++q_it222)
    {
        int B__BROKER_ID = q_it222->first;
        q805 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it218 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end217 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it218 != bigsum_A__P_dom_end217; ++bigsum_A__P_dom_it218)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it218;
            q805 += ( ( ( 0.25*qBIDS3+-1*0.25*V <= qBIDS2[bigsum_A__P]+-1*V*( (
                 bigsum_A__P < P )? ( 1 ) : ( 0 ) ) ) && (
                 qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) )? ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q806 = 0;
        set<double>::iterator bigsum_B__P_dom_it220 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end219 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it220 != bigsum_B__P_dom_end219; ++bigsum_B__P_dom_it220)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it220;
            q806 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q805*q806;
    }
    map<int,double>::iterator q_it228 = q.begin();
    map<int,double>::iterator q_end227 = q.end();
    for (; q_it228 != q_end227; ++q_it228)
    {
        int B__BROKER_ID = q_it228->first;
        q832 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it224 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end223 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it224 != bigsum_A__P_dom_end223; ++bigsum_A__P_dom_it224)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it224;
            q832 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS1[bigsum_A__P] ) : ( 0 ) );
        }
        q833 = 0;
        set<double>::iterator bigsum_B__P_dom_it226 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end225 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it226 != bigsum_B__P_dom_end225; ++bigsum_B__P_dom_it226)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it226;
            q833 += ( ( ( 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) && (
                 qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) )?
                 ( qBIDS6[make_tuple(B__BROKER_ID,bigsum_B__P)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*q832*q833;
    }
    map<int,double>::iterator q_it234 = q.begin();
    map<int,double>::iterator q_end233 = q.end();
    for (; q_it234 != q_end233; ++q_it234)
    {
        int B__BROKER_ID = q_it234->first;
        q847 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it230 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end229 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it230 != bigsum_A__P_dom_end229; ++bigsum_A__P_dom_it230)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it230;
            q847 += ( ( qBIDS2[bigsum_A__P]+-1*V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+-1*0.25*V )? ( ( ( P == bigsum_A__P )?
                 ( P ) : ( 0 ) ) ) : ( 0 ) );
        }
        q849 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it232 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end231 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it232 != bigsum_B__P_dom_end231; ++bigsum_B__P_dom_it232)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it232;
            q849 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*-1*q847*q849;
    }
    map<int,double>::iterator q_it240 = q.begin();
    map<int,double>::iterator q_end239 = q.end();
    for (; q_it240 != q_end239; ++q_it240)
    {
        int B__BROKER_ID = q_it240->first;
        q874 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it236 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end235 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it236 != bigsum_A__P_dom_end235; ++bigsum_A__P_dom_it236)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it236;
            q874 += ( ( ( qBIDS2[bigsum_A__P]+-1*V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+-1*0.25*V ) && (
                 0.25*qBIDS3 <= qBIDS2[bigsum_A__P] ) )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q875 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it238 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end237 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it238 != bigsum_B__P_dom_end237; ++bigsum_B__P_dom_it238)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it238;
            q875 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*-1*q874*q875;
    }
    map<int,double>::iterator q_it246 = q.begin();
    map<int,double>::iterator q_end245 = q.end();
    for (; q_it246 != q_end245; ++q_it246)
    {
        int B__BROKER_ID = q_it246->first;
        q902 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it242 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end241 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it242 != bigsum_A__P_dom_end241; ++bigsum_A__P_dom_it242)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it242;
            q902 += ( ( qBIDS2[bigsum_A__P]+-1*V*( ( bigsum_A__P < P )?
                 ( 1 ) : ( 0 ) ) < 0.25*qBIDS3+-1*0.25*V )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q903 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it244 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end243 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it244 != bigsum_B__P_dom_end243; ++bigsum_B__P_dom_it244)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it244;
            q903 += ( ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) && (
                 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*-1*q902*q903;
    }
    map<int,double>::iterator q_it252 = q.begin();
    map<int,double>::iterator q_end251 = q.end();
    for (; q_it252 != q_end251; ++q_it252)
    {
        int B__BROKER_ID = q_it252->first;
        q929 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it248 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end247 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it248 != bigsum_A__P_dom_end247; ++bigsum_A__P_dom_it248)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it248;
            q929 += ( ( ( 0.25*qBIDS3+-1*0.25*V <= qBIDS2[bigsum_A__P]+-1*V*( (
                 bigsum_A__P < P )? ( 1 ) : ( 0 ) ) ) && (
                 qBIDS2[bigsum_A__P] < 0.25*qBIDS3 ) )? ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q931 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it250 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end249 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it250 != bigsum_B__P_dom_end249; ++bigsum_B__P_dom_it250)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it250;
            q931 += ( ( qBIDS4[bigsum_B__P] < 0.25*qBIDS5 )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*-1*q929*q931;
    }
    map<int,double>::iterator q_it258 = q.begin();
    map<int,double>::iterator q_end257 = q.end();
    for (; q_it258 != q_end257; ++q_it258)
    {
        int B__BROKER_ID = q_it258->first;
        q957 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it254 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end253 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it254 != bigsum_A__P_dom_end253; ++bigsum_A__P_dom_it254)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it254;
            q957 += ( ( qBIDS2[bigsum_A__P] < 0.25*qBIDS3 )?
                 ( qBIDS7[bigsum_A__P] ) : ( 0 ) );
        }
        q959 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it256 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end255 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it256 != bigsum_B__P_dom_end255; ++bigsum_B__P_dom_it256)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it256;
            q959 += ( ( ( 0.25*qBIDS5 <= qBIDS4[bigsum_B__P] ) && (
                 qBIDS4[bigsum_B__P] < 0.25*qBIDS5 ) )?
                 ( qBIDS8[make_tuple(bigsum_B__P,B__BROKER_ID)] ) : ( 0 ) );
        }
        q[B__BROKER_ID] += -1*-1*q957*q959;
    }
    map<double,double>::iterator qBIDS1_it262 = qBIDS1.begin();
    map<double,double>::iterator qBIDS1_end261 = qBIDS1.end();
    for (; qBIDS1_it262 != qBIDS1_end261; ++qBIDS1_it262)
    {
        double bigsum_A__P = qBIDS1_it262->first;
        qBIDS1991 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it260 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end259 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it260 != bigsum_A__P_dom_end259; ++bigsum_A__P_dom_it260)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it260;
            qBIDS1991 += ( ( bigsum_A__P == P )? ( bigsum_A__P ) : ( 0 ) );
        }
        qBIDS1[bigsum_A__P] += -1*V*qBIDS1991;
    }
    map<double,double>::iterator qBIDS2_it264 = qBIDS2.begin();
    map<double,double>::iterator qBIDS2_end263 = qBIDS2.end();
    for (; qBIDS2_it264 != qBIDS2_end263; ++qBIDS2_it264)
    {
        double bigsum_A__P = qBIDS2_it264->first;
        qBIDS2[bigsum_A__P] += -1*V*( ( bigsum_A__P < P )? ( 1 ) : ( 0 ) );
    }
    qBIDS3 += -1*V;
    map<double,double>::iterator qBIDS7_it268 = qBIDS7.begin();
    map<double,double>::iterator qBIDS7_end267 = qBIDS7.end();
    for (; qBIDS7_it268 != qBIDS7_end267; ++qBIDS7_it268)
    {
        double bigsum_A__P = qBIDS7_it268->first;
        qBIDS71001 = 0.0;
        set<double>::iterator bigsum_A__P_dom_it266 = bigsum_A__P_dom.begin();
        set<double>::iterator bigsum_A__P_dom_end265 = bigsum_A__P_dom.end();
        for (
            ; bigsum_A__P_dom_it266 != bigsum_A__P_dom_end265; ++bigsum_A__P_dom_it266)
        {
            double bigsum_A__P = *bigsum_A__P_dom_it266;
            qBIDS71001 += ( ( bigsum_A__P == P )? ( bigsum_A__P ) : ( 0 ) );
        }
        qBIDS7[bigsum_A__P] += -1*qBIDS71001;
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