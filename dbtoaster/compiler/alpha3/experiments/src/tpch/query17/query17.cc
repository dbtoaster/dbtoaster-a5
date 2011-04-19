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
map<tuple<int64_t,double,int64_t>,double> qLINEITEM3PARTS1;
double q118;
map<tuple<int64_t,int64_t>,int> qLINEITEM3LINEITEM1;
set<int64_t> bigsum_P__PARTKEY_dom;
map<tuple<int64_t,double,int64_t>,double> qPARTS1;
double q61;
map<tuple<int64_t,int64_t>,int> qLINEITEM1;
double q171;
map<int64_t,double> qLINEITEM2;
double q161;
map<tuple<int64_t,double>,double> qLINEITEM3;
double q98;
double q32;
double q;
double q154;
set<double> bigsum_L__QUANTITY_dom;
double q136;

double on_insert_PARTS_sec_span = 0.0;
double on_insert_PARTS_usec_span = 0.0;
double on_insert_LINEITEM_sec_span = 0.0;
double on_insert_LINEITEM_usec_span = 0.0;
double on_delete_PARTS_sec_span = 0.0;
double on_delete_PARTS_usec_span = 0.0;
double on_delete_LINEITEM_sec_span = 0.0;
double on_delete_LINEITEM_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
   cout << "qLINEITEM3PARTS1 size: " << (((sizeof(map<tuple<int64_t,double,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,double,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM3PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,double,int64_t>,double>::key_type, map<tuple<int64_t,double,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,double,int64_t>,double>::value_type>, map<tuple<int64_t,double,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM3PARTS1" << "," << (((sizeof(map<tuple<int64_t,double,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,double,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM3PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,double,int64_t>,double>::key_type, map<tuple<int64_t,double,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,double,int64_t>,double>::value_type>, map<tuple<int64_t,double,int64_t>,double>::key_compare>))) << endl;

   cout << "qLINEITEM3LINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM3LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM3LINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM3LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "bigsum_P__PARTKEY_dom size: " << (((sizeof(set<int64_t>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_P__PARTKEY_dom.size())  + (sizeof(struct _Rb_tree<set<int64_t>::key_type, set<int64_t>::value_type, _Identity<set<int64_t>::value_type>, set<int64_t>::key_compare>))) << endl;

   (*stats) << "m," << "bigsum_P__PARTKEY_dom" << "," << (((sizeof(set<int64_t>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_P__PARTKEY_dom.size())  + (sizeof(struct _Rb_tree<set<int64_t>::key_type, set<int64_t>::value_type, _Identity<set<int64_t>::value_type>, set<int64_t>::key_compare>))) << endl;

   cout << "qPARTS1 size: " << (((sizeof(map<tuple<int64_t,double,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,double,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,double,int64_t>,double>::key_type, map<tuple<int64_t,double,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,double,int64_t>,double>::value_type>, map<tuple<int64_t,double,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS1" << "," << (((sizeof(map<tuple<int64_t,double,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,double,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,double,int64_t>,double>::key_type, map<tuple<int64_t,double,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,double,int64_t>,double>::value_type>, map<tuple<int64_t,double,int64_t>,double>::key_compare>))) << endl;

   cout << "qLINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qLINEITEM2 size: " << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM2.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM2" << "," << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM2.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   cout << "qLINEITEM3 size: " << (((sizeof(map<tuple<int64_t,double>,double>::key_type)
       + sizeof(map<tuple<int64_t,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,double>,double>::key_type, map<tuple<int64_t,double>,double>::value_type, _Select1st<map<tuple<int64_t,double>,double>::value_type>, map<tuple<int64_t,double>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM3" << "," << (((sizeof(map<tuple<int64_t,double>,double>::key_type)
       + sizeof(map<tuple<int64_t,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,double>,double>::key_type, map<tuple<int64_t,double>,double>::value_type, _Select1st<map<tuple<int64_t,double>,double>::value_type>, map<tuple<int64_t,double>,double>::key_compare>))) << endl;

   cout << "bigsum_L__QUANTITY_dom size: " << (((sizeof(set<double>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_L__QUANTITY_dom.size())  + (sizeof(struct _Rb_tree<set<double>::key_type, set<double>::value_type, _Identity<set<double>::value_type>, set<double>::key_compare>))) << endl;

   (*stats) << "m," << "bigsum_L__QUANTITY_dom" << "," << (((sizeof(set<double>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_L__QUANTITY_dom.size())  + (sizeof(struct _Rb_tree<set<double>::key_type, set<double>::value_type, _Identity<set<double>::value_type>, set<double>::key_compare>))) << endl;

}

void analyse_handler_usage(ofstream* stats)
{
   cout << "on_insert_PARTS cost: " << (on_insert_PARTS_sec_span + (on_insert_PARTS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_PARTS" << "," << (on_insert_PARTS_sec_span + (on_insert_PARTS_usec_span / 1000000.0)) << endl;
   cout << "on_insert_LINEITEM cost: " << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_LINEITEM" << "," << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_delete_PARTS cost: " << (on_delete_PARTS_sec_span + (on_delete_PARTS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_PARTS" << "," << (on_delete_PARTS_sec_span + (on_delete_PARTS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_LINEITEM cost: " << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_LINEITEM" << "," << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
}


void on_insert_PARTS(
    ofstream* results, ofstream* log, ofstream* stats, int64_t PARTKEY,string 
    NAME,string MFGR,string BRAND,string TYPE,int SIZE,string CONTAINER,double 
    RETAILPRICE,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    bigsum_P__PARTKEY_dom.insert(PARTKEY);
    q61 = 0.0;
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it4 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end3 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it4 != bigsum_P__PARTKEY_dom_end3; 
        ++bigsum_P__PARTKEY_dom_it4)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it4;
        set<double>::iterator bigsum_L__QUANTITY_dom_it2 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end1 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it2 != bigsum_L__QUANTITY_dom_end1; 
            ++bigsum_L__QUANTITY_dom_it2)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it2;
            q61 += ( ( (
                 0.005*qLINEITEM2[bigsum_P__PARTKEY] <= bigsum_L__QUANTITY ) && (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY] ) )?
                 ( qLINEITEM3[make_tuple(bigsum_P__PARTKEY,bigsum_L__QUANTITY)] ) : ( 0 ) );
        }
    }
    q += -1*q61;
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it8 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end7 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it8 != bigsum_P__PARTKEY_dom_end7; 
        ++bigsum_P__PARTKEY_dom_it8)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it8;
        set<double>::iterator bigsum_L__QUANTITY_dom_it6 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end5 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it6 != bigsum_L__QUANTITY_dom_end5; 
            ++bigsum_L__QUANTITY_dom_it6)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it6;
            q += ( ( bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY] )?
                 ( qPARTS1[make_tuple(PARTKEY,bigsum_L__QUANTITY,bigsum_P__PARTKEY)] ) : (
                 0 ) );
        }
    }
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it12 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end11 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it12 != bigsum_P__PARTKEY_dom_end11; 
        ++bigsum_P__PARTKEY_dom_it12)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it12;
        set<double>::iterator bigsum_L__QUANTITY_dom_it10 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end9 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it10 != bigsum_L__QUANTITY_dom_end9; 
            ++bigsum_L__QUANTITY_dom_it10)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it10;
            q += ( ( (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY] ) && (
                 0.005*qLINEITEM2[bigsum_P__PARTKEY] <= bigsum_L__QUANTITY ) )?
                 ( qLINEITEM3[make_tuple(bigsum_P__PARTKEY,bigsum_L__QUANTITY)] ) : ( 0 ) );
        }
    }
    qLINEITEM1[make_tuple(PARTKEY,PARTKEY)] += 1;
    map<tuple<int64_t,double>,double>::iterator qLINEITEM3_it14 = 
        qLINEITEM3.begin();
    map<tuple<int64_t,double>,double>::iterator qLINEITEM3_end13 = 
        qLINEITEM3.end();
    for (; qLINEITEM3_it14 != qLINEITEM3_end13; ++qLINEITEM3_it14)
    {
        int64_t bigsum_P__PARTKEY = get<0>(qLINEITEM3_it14->first);
        double bigsum_L__QUANTITY = get<1>(qLINEITEM3_it14->first);
        qLINEITEM3[make_tuple(
            bigsum_P__PARTKEY,bigsum_L__QUANTITY)] += qLINEITEM3PARTS1[make_tuple(
            bigsum_P__PARTKEY,bigsum_L__QUANTITY,PARTKEY)];
    }
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3LINEITEM1_it16 = 
        qLINEITEM3LINEITEM1.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3LINEITEM1_end15 = 
        qLINEITEM3LINEITEM1.end();
    for (
        ; qLINEITEM3LINEITEM1_it16 != qLINEITEM3LINEITEM1_end15; 
        ++qLINEITEM3LINEITEM1_it16)
    {
        int64_t bigsum_P__PARTKEY = get<0>(qLINEITEM3LINEITEM1_it16->first);
        qLINEITEM3LINEITEM1[make_tuple(bigsum_P__PARTKEY,bigsum_P__PARTKEY)] += 
            ( ( bigsum_P__PARTKEY == PARTKEY )? ( 1 ) : ( 0 ) );
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_PARTS_sec_span, on_insert_PARTS_usec_span);
    (*results) << "on_insert_PARTS" << "," << q << endl;
}

void on_insert_LINEITEM(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    PARTKEY,int64_t SUPPKEY,int LINENUMBER,double QUANTITY,double 
    EXTENDEDPRICE,double DISCOUNT,double TAX)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    bigsum_L__QUANTITY_dom.insert(QUANTITY);
    q32 = 0.0;
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it20 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end19 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it20 != bigsum_P__PARTKEY_dom_end19; 
        ++bigsum_P__PARTKEY_dom_it20)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it20;
        set<double>::iterator bigsum_L__QUANTITY_dom_it18 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end17 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it18 != bigsum_L__QUANTITY_dom_end17; 
            ++bigsum_L__QUANTITY_dom_it18)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it18;
            q32 += ( ( ( 0.005*qLINEITEM2[bigsum_P__PARTKEY]+0.005*QUANTITY*( (
                 PARTKEY == bigsum_P__PARTKEY )?
                 ( 1 ) : ( 0 ) ) <= bigsum_L__QUANTITY ) && (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY] ) )?
                 ( qLINEITEM3[make_tuple(bigsum_P__PARTKEY,bigsum_L__QUANTITY)] ) : ( 0 ) );
        }
    }
    q += -1*q32;
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it24 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end23 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it24 != bigsum_P__PARTKEY_dom_end23; 
        ++bigsum_P__PARTKEY_dom_it24)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it24;
        set<double>::iterator bigsum_L__QUANTITY_dom_it22 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end21 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it22 != bigsum_L__QUANTITY_dom_end21; 
            ++bigsum_L__QUANTITY_dom_it22)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it22;
            q += ( (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY]+0.005*QUANTITY*( (
                 PARTKEY == bigsum_P__PARTKEY )? ( 1 ) : ( 0 ) ) )?
                 ( EXTENDEDPRICE*qLINEITEM1[make_tuple(PARTKEY,bigsum_P__PARTKEY)]*( (
                 QUANTITY == bigsum_L__QUANTITY )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
        }
    }
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it28 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end27 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it28 != bigsum_P__PARTKEY_dom_end27; 
        ++bigsum_P__PARTKEY_dom_it28)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it28;
        set<double>::iterator bigsum_L__QUANTITY_dom_it26 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end25 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it26 != bigsum_L__QUANTITY_dom_end25; 
            ++bigsum_L__QUANTITY_dom_it26)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it26;
            q += ( ( (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY]+0.005*QUANTITY*( (
                 PARTKEY == bigsum_P__PARTKEY )?
                 ( 1 ) : ( 0 ) ) ) && (
                 0.005*qLINEITEM2[bigsum_P__PARTKEY] <= bigsum_L__QUANTITY ) )?
                 ( qLINEITEM3[make_tuple(bigsum_P__PARTKEY,bigsum_L__QUANTITY)] ) : ( 0 ) );
        }
    }
    map<int64_t,double>::iterator qLINEITEM2_it30 = qLINEITEM2.begin();
    map<int64_t,double>::iterator qLINEITEM2_end29 = qLINEITEM2.end();
    for (; qLINEITEM2_it30 != qLINEITEM2_end29; ++qLINEITEM2_it30)
    {
        int64_t bigsum_P__PARTKEY = qLINEITEM2_it30->first;
        qLINEITEM2[bigsum_P__PARTKEY] += QUANTITY*( (
             bigsum_P__PARTKEY == PARTKEY )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int64_t,double>,double>::iterator qLINEITEM3_it32 = 
        qLINEITEM3.begin();
    map<tuple<int64_t,double>,double>::iterator qLINEITEM3_end31 = 
        qLINEITEM3.end();
    for (; qLINEITEM3_it32 != qLINEITEM3_end31; ++qLINEITEM3_it32)
    {
        int64_t bigsum_P__PARTKEY = get<0>(qLINEITEM3_it32->first);
        double bigsum_L__QUANTITY = get<1>(qLINEITEM3_it32->first);
        qLINEITEM3[make_tuple(
            bigsum_P__PARTKEY,bigsum_L__QUANTITY)] += 
            EXTENDEDPRICE*qLINEITEM3LINEITEM1[make_tuple(bigsum_P__PARTKEY,PARTKEY)]*( (
             bigsum_L__QUANTITY == QUANTITY )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int64_t,double,int64_t>,double>::iterator qLINEITEM3PARTS1_it34 = 
        qLINEITEM3PARTS1.begin();
    map<tuple<int64_t,double,int64_t>,
        double>::iterator qLINEITEM3PARTS1_end33 = qLINEITEM3PARTS1.end();
    for (
        ; qLINEITEM3PARTS1_it34 != qLINEITEM3PARTS1_end33; ++qLINEITEM3PARTS1_it34)
    {
        int64_t bigsum_P__PARTKEY = get<0>(qLINEITEM3PARTS1_it34->first);
        double bigsum_L__QUANTITY = get<1>(qLINEITEM3PARTS1_it34->first);
        qLINEITEM3PARTS1[make_tuple(
            bigsum_P__PARTKEY,bigsum_L__QUANTITY,bigsum_P__PARTKEY)] += EXTENDEDPRICE*( (
             bigsum_P__PARTKEY == PARTKEY )?
             ( 1 ) : ( 0 ) )*( ( bigsum_L__QUANTITY == QUANTITY )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int64_t,double,int64_t>,double>::iterator qPARTS1_it36 = 
        qPARTS1.begin();
    map<tuple<int64_t,double,int64_t>,double>::iterator qPARTS1_end35 = 
        qPARTS1.end();
    for (; qPARTS1_it36 != qPARTS1_end35; ++qPARTS1_it36)
    {
        double bigsum_L__QUANTITY = get<1>(qPARTS1_it36->first);
        qPARTS1[make_tuple(
            PARTKEY,bigsum_L__QUANTITY,PARTKEY)] += EXTENDEDPRICE*
            ( ( bigsum_L__QUANTITY == QUANTITY )? ( 1 ) : ( 0 ) );
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_LINEITEM_sec_span, on_insert_LINEITEM_usec_span);
    (*results) << "on_insert_LINEITEM" << "," << q << endl;
}

void on_delete_PARTS(
    ofstream* results, ofstream* log, ofstream* stats, int64_t PARTKEY,string 
    NAME,string MFGR,string BRAND,string TYPE,int SIZE,string CONTAINER,double 
    RETAILPRICE,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    q154 = 0.0;
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it40 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end39 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it40 != bigsum_P__PARTKEY_dom_end39; 
        ++bigsum_P__PARTKEY_dom_it40)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it40;
        set<double>::iterator bigsum_L__QUANTITY_dom_it38 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end37 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it38 != bigsum_L__QUANTITY_dom_end37; 
            ++bigsum_L__QUANTITY_dom_it38)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it38;
            q154 += ( (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY] )?
                 ( qPARTS1[make_tuple(PARTKEY,bigsum_L__QUANTITY,bigsum_P__PARTKEY)] ) : (
                 0 ) );
        }
    }
    q += -1*q154;
    q161 = 0.0;
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it44 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end43 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it44 != bigsum_P__PARTKEY_dom_end43; 
        ++bigsum_P__PARTKEY_dom_it44)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it44;
        set<double>::iterator bigsum_L__QUANTITY_dom_it42 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end41 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it42 != bigsum_L__QUANTITY_dom_end41; 
            ++bigsum_L__QUANTITY_dom_it42)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it42;
            q161 += ( ( (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY] ) && (
                 0.005*qLINEITEM2[bigsum_P__PARTKEY] <= bigsum_L__QUANTITY ) )?
                 ( qLINEITEM3[make_tuple(bigsum_P__PARTKEY,bigsum_L__QUANTITY)] ) : ( 0 ) );
        }
    }
    q += -1*-1*q161;
    q171 = 0.0;
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it48 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end47 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it48 != bigsum_P__PARTKEY_dom_end47; 
        ++bigsum_P__PARTKEY_dom_it48)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it48;
        set<double>::iterator bigsum_L__QUANTITY_dom_it46 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end45 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it46 != bigsum_L__QUANTITY_dom_end45; 
            ++bigsum_L__QUANTITY_dom_it46)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it46;
            q171 += ( ( (
                 0.005*qLINEITEM2[bigsum_P__PARTKEY] <= bigsum_L__QUANTITY ) && (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY] ) )?
                 ( qLINEITEM3[make_tuple(bigsum_P__PARTKEY,bigsum_L__QUANTITY)] ) : ( 0 ) );
        }
    }
    q += -1*q171;
    qLINEITEM1[make_tuple(PARTKEY,PARTKEY)] += -1;
    map<tuple<int64_t,double>,double>::iterator qLINEITEM3_it50 = 
        qLINEITEM3.begin();
    map<tuple<int64_t,double>,double>::iterator qLINEITEM3_end49 = 
        qLINEITEM3.end();
    for (; qLINEITEM3_it50 != qLINEITEM3_end49; ++qLINEITEM3_it50)
    {
        int64_t bigsum_P__PARTKEY = get<0>(qLINEITEM3_it50->first);
        double bigsum_L__QUANTITY = get<1>(qLINEITEM3_it50->first);
        qLINEITEM3[make_tuple(
            bigsum_P__PARTKEY,bigsum_L__QUANTITY)] += -1*qLINEITEM3PARTS1[make_tuple(
            bigsum_P__PARTKEY,bigsum_L__QUANTITY,PARTKEY)];
    }
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3LINEITEM1_it52 = 
        qLINEITEM3LINEITEM1.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3LINEITEM1_end51 = 
        qLINEITEM3LINEITEM1.end();
    for (
        ; qLINEITEM3LINEITEM1_it52 != qLINEITEM3LINEITEM1_end51; 
        ++qLINEITEM3LINEITEM1_it52)
    {
        int64_t bigsum_P__PARTKEY = get<0>(qLINEITEM3LINEITEM1_it52->first);
        qLINEITEM3LINEITEM1[make_tuple(
            bigsum_P__PARTKEY,bigsum_P__PARTKEY)] += -1*( ( bigsum_P__PARTKEY == PARTKEY )?
             ( 1 ) : ( 0 ) );
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_PARTS_sec_span, on_delete_PARTS_usec_span);
    (*results) << "on_delete_PARTS" << "," << q << endl;
}

void on_delete_LINEITEM(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    PARTKEY,int64_t SUPPKEY,int LINENUMBER,double QUANTITY,double 
    EXTENDEDPRICE,double DISCOUNT,double TAX)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    q118 = 0.0;
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it56 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end55 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it56 != bigsum_P__PARTKEY_dom_end55; 
        ++bigsum_P__PARTKEY_dom_it56)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it56;
        set<double>::iterator bigsum_L__QUANTITY_dom_it54 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end53 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it54 != bigsum_L__QUANTITY_dom_end53; 
            ++bigsum_L__QUANTITY_dom_it54)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it54;
            q118 += ( ( (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY]+-1*0.005*QUANTITY*( (
                 PARTKEY == bigsum_P__PARTKEY )?
                 ( 1 ) : ( 0 ) ) ) && (
                 0.005*qLINEITEM2[bigsum_P__PARTKEY] <= bigsum_L__QUANTITY ) )?
                 ( qLINEITEM3[make_tuple(bigsum_P__PARTKEY,bigsum_L__QUANTITY)] ) : ( 0 ) );
        }
    }
    q += -1*-1*q118;
    q136 = 0.0;
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it60 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end59 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it60 != bigsum_P__PARTKEY_dom_end59; 
        ++bigsum_P__PARTKEY_dom_it60)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it60;
        set<double>::iterator bigsum_L__QUANTITY_dom_it58 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end57 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it58 != bigsum_L__QUANTITY_dom_end57; 
            ++bigsum_L__QUANTITY_dom_it58)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it58;
            q136 += ( ( ( 
                0.005*qLINEITEM2[bigsum_P__PARTKEY]+-1*0.005*QUANTITY*( (
                 PARTKEY == bigsum_P__PARTKEY )?
                 ( 1 ) : ( 0 ) ) <= bigsum_L__QUANTITY ) && (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY] ) )?
                 ( qLINEITEM3[make_tuple(bigsum_P__PARTKEY,bigsum_L__QUANTITY)] ) : ( 0 ) );
        }
    }
    q += -1*q136;
    q98 = 0.0;
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it64 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end63 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it64 != bigsum_P__PARTKEY_dom_end63; 
        ++bigsum_P__PARTKEY_dom_it64)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it64;
        set<double>::iterator bigsum_L__QUANTITY_dom_it62 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end61 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it62 != bigsum_L__QUANTITY_dom_end61; 
            ++bigsum_L__QUANTITY_dom_it62)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it62;
            q98 += ( (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY]+-1*0.005*QUANTITY*( (
                 PARTKEY == bigsum_P__PARTKEY )? ( 1 ) : ( 0 ) ) )?
                 ( EXTENDEDPRICE*qLINEITEM1[make_tuple(PARTKEY,bigsum_P__PARTKEY)]*( (
                 QUANTITY == bigsum_L__QUANTITY )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
        }
    }
    q += -1*q98;
    map<int64_t,double>::iterator qLINEITEM2_it66 = qLINEITEM2.begin();
    map<int64_t,double>::iterator qLINEITEM2_end65 = qLINEITEM2.end();
    for (; qLINEITEM2_it66 != qLINEITEM2_end65; ++qLINEITEM2_it66)
    {
        int64_t bigsum_P__PARTKEY = qLINEITEM2_it66->first;
        qLINEITEM2[bigsum_P__PARTKEY] += -1*QUANTITY*( (
             bigsum_P__PARTKEY == PARTKEY )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int64_t,double>,double>::iterator qLINEITEM3_it68 = 
        qLINEITEM3.begin();
    map<tuple<int64_t,double>,double>::iterator qLINEITEM3_end67 = 
        qLINEITEM3.end();
    for (; qLINEITEM3_it68 != qLINEITEM3_end67; ++qLINEITEM3_it68)
    {
        int64_t bigsum_P__PARTKEY = get<0>(qLINEITEM3_it68->first);
        double bigsum_L__QUANTITY = get<1>(qLINEITEM3_it68->first);
        qLINEITEM3[make_tuple(
            bigsum_P__PARTKEY,bigsum_L__QUANTITY)] += 
            -1*EXTENDEDPRICE*qLINEITEM3LINEITEM1[make_tuple(bigsum_P__PARTKEY,PARTKEY)]*( (
             bigsum_L__QUANTITY == QUANTITY )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int64_t,double,int64_t>,double>::iterator qLINEITEM3PARTS1_it70 = 
        qLINEITEM3PARTS1.begin();
    map<tuple<int64_t,double,int64_t>,
        double>::iterator qLINEITEM3PARTS1_end69 = qLINEITEM3PARTS1.end();
    for (
        ; qLINEITEM3PARTS1_it70 != qLINEITEM3PARTS1_end69; ++qLINEITEM3PARTS1_it70)
    {
        int64_t bigsum_P__PARTKEY = get<0>(qLINEITEM3PARTS1_it70->first);
        double bigsum_L__QUANTITY = get<1>(qLINEITEM3PARTS1_it70->first);
        qLINEITEM3PARTS1[make_tuple(
            bigsum_P__PARTKEY,bigsum_L__QUANTITY,bigsum_P__PARTKEY)] += -1*EXTENDEDPRICE*( (
             bigsum_P__PARTKEY == PARTKEY )?
             ( 1 ) : ( 0 ) )*( ( bigsum_L__QUANTITY == QUANTITY )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int64_t,double,int64_t>,double>::iterator qPARTS1_it72 = 
        qPARTS1.begin();
    map<tuple<int64_t,double,int64_t>,double>::iterator qPARTS1_end71 = 
        qPARTS1.end();
    for (; qPARTS1_it72 != qPARTS1_end71; ++qPARTS1_it72)
    {
        double bigsum_L__QUANTITY = get<1>(qPARTS1_it72->first);
        qPARTS1[make_tuple(
            PARTKEY,bigsum_L__QUANTITY,PARTKEY)] += -1*EXTENDEDPRICE*
            ( ( bigsum_L__QUANTITY == QUANTITY )? ( 1 ) : ( 0 ) );
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_LINEITEM_sec_span, on_delete_LINEITEM_usec_span);
    (*results) << "on_delete_LINEITEM" << "," << q << endl;
}

DBToaster::DemoDatasets::PartStream SSBParts("/home/yanif/datasets/tpch/sf1/singlefile/part.tbl.a",&DBToaster::DemoDatasets::parsePartField,9,2100000,512);

boost::shared_ptr<DBToaster::DemoDatasets::PartTupleAdaptor> SSBParts_adaptor(new DBToaster::DemoDatasets::PartTupleAdaptor());
static int streamSSBPartsId = 0;

struct on_insert_PARTS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::PartTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::PartTupleAdaptor::Result>(data); 
        on_insert_PARTS(results, log, stats, input.partkey,input.name,input.mfgr,input.brand,input.type,input.size,input.container,input.retailprice,input.comment);
    }
};

on_insert_PARTS_fun_obj fo_on_insert_PARTS_0;

DBToaster::DemoDatasets::LineitemStream SSBLineitem("/home/yanif/datasets/tpch/sf1/singlefile/lineitem.tbl.a",&DBToaster::DemoDatasets::parseLineitemField,16,65000000,512);

boost::shared_ptr<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor> SSBLineitem_adaptor(new DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor());
static int streamSSBLineitemId = 1;

struct on_insert_LINEITEM_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result>(data); 
        on_insert_LINEITEM(results, log, stats, input.orderkey,input.partkey,input.suppkey,input.linenumber,input.quantity,input.extendedprice,input.discount,input.tax);
    }
};

on_insert_LINEITEM_fun_obj fo_on_insert_LINEITEM_1;

struct on_delete_PARTS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::PartTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::PartTupleAdaptor::Result>(data); 
        on_delete_PARTS(results, log, stats, input.partkey,input.name,input.mfgr,input.brand,input.type,input.size,input.container,input.retailprice,input.comment);
    }
};

on_delete_PARTS_fun_obj fo_on_delete_PARTS_2;

struct on_delete_LINEITEM_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result>(data); 
        on_delete_LINEITEM(results, log, stats, input.orderkey,input.partkey,input.suppkey,input.linenumber,input.quantity,input.extendedprice,input.discount,input.tax);
    }
};

on_delete_LINEITEM_fun_obj fo_on_delete_LINEITEM_3;


void init(DBToaster::StandaloneEngine::FileMultiplexer& sources,
    DBToaster::StandaloneEngine::FileStreamDispatcher& router, 
    ofstream* results, ofstream* log, ofstream* stats)
{
    router.setFiles(results, log, stats);
    sources.addStream<DBToaster::DemoDatasets::part>(&SSBParts, boost::ref(*SSBParts_adaptor), streamSSBPartsId);
    router.addHandler(streamSSBPartsId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_PARTS_0));
    sources.addStream<DBToaster::DemoDatasets::lineitem>(&SSBLineitem, boost::ref(*SSBLineitem_adaptor), streamSSBLineitemId);
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_LINEITEM_1));
    router.addHandler(streamSSBPartsId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_PARTS_2));
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_LINEITEM_3));
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
