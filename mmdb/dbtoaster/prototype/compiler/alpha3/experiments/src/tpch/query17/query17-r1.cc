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
double q127;
multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> > 
    LINEITEM;
set<int64_t> bigsum_P__PARTKEY_dom;
double q109;
double q91;
map<tuple<int64_t,double,int64_t>,double> qPARTS1;
double q71;
double q61;
map<tuple<int64_t,int64_t>,int> qLINEITEM1;
map<int64_t,double> qLINEITEM2;
map<tuple<int64_t,double>,double> qLINEITEM3;
double q32;
double q;
double q144;
multiset<tuple<int64_t,string,string,string,string,int,string,double,string> > 
    PARTS;
double q134;
set<double> bigsum_L__QUANTITY_dom;

double on_insert_LINEITEM_sec_span = 0.0;
double on_insert_LINEITEM_usec_span = 0.0;
double on_insert_PARTS_sec_span = 0.0;
double on_insert_PARTS_usec_span = 0.0;
double on_delete_LINEITEM_sec_span = 0.0;
double on_delete_LINEITEM_usec_span = 0.0;
double on_delete_PARTS_sec_span = 0.0;
double on_delete_PARTS_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
   cout << "LINEITEM size: " << (((sizeof(multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * LINEITEM.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_type, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type>, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_compare>))) << endl;

   (*stats) << "m," << "LINEITEM" << "," << (((sizeof(multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * LINEITEM.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_type, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type>, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_compare>))) << endl;

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

   cout << "PARTS size: " << (((sizeof(multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * PARTS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::key_type, multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type>, multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::key_compare>))) << endl;

   (*stats) << "m," << "PARTS" << "," << (((sizeof(multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * PARTS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::key_type, multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type>, multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::key_compare>))) << endl;

   cout << "bigsum_L__QUANTITY_dom size: " << (((sizeof(set<double>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_L__QUANTITY_dom.size())  + (sizeof(struct _Rb_tree<set<double>::key_type, set<double>::value_type, _Identity<set<double>::value_type>, set<double>::key_compare>))) << endl;

   (*stats) << "m," << "bigsum_L__QUANTITY_dom" << "," << (((sizeof(set<double>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_L__QUANTITY_dom.size())  + (sizeof(struct _Rb_tree<set<double>::key_type, set<double>::value_type, _Identity<set<double>::value_type>, set<double>::key_compare>))) << endl;

}

void analyse_handler_usage(ofstream* stats)
{
   cout << "on_insert_LINEITEM cost: " << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_LINEITEM" << "," << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_insert_PARTS cost: " << (on_insert_PARTS_sec_span + (on_insert_PARTS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_PARTS" << "," << (on_insert_PARTS_sec_span + (on_insert_PARTS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_LINEITEM cost: " << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_LINEITEM" << "," << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_delete_PARTS cost: " << (on_delete_PARTS_sec_span + (on_delete_PARTS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_PARTS" << "," << (on_delete_PARTS_sec_span + (on_delete_PARTS_usec_span / 1000000.0)) << endl;
}


void on_insert_LINEITEM(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    PARTKEY,int64_t SUPPKEY,int LINENUMBER,double QUANTITY,double 
    EXTENDEDPRICE,double DISCOUNT,double TAX)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    bigsum_L__QUANTITY_dom.insert(QUANTITY);
    LINEITEM.insert(make_tuple(
        ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDEDPRICE,DISCOUNT,TAX));
    q32 = 0.0;
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
            q32 += ( ( ( 0.005*qLINEITEM2[bigsum_P__PARTKEY]+0.005*QUANTITY*( (
                 PARTKEY == bigsum_P__PARTKEY )?
                 ( 1 ) : ( 0 ) ) <= bigsum_L__QUANTITY ) && (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY] ) )?
                 ( qLINEITEM3[make_tuple(bigsum_P__PARTKEY,bigsum_L__QUANTITY)] ) : ( 0 ) );
        }
    }
    q += -1*q32;
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
            q += ( (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY]+0.005*QUANTITY*( (
                 PARTKEY == bigsum_P__PARTKEY )? ( 1 ) : ( 0 ) ) )?
                 ( EXTENDEDPRICE*qLINEITEM1[make_tuple(PARTKEY,bigsum_P__PARTKEY)]*( (
                 QUANTITY == bigsum_L__QUANTITY )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
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
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY]+0.005*QUANTITY*( (
                 PARTKEY == bigsum_P__PARTKEY )?
                 ( 1 ) : ( 0 ) ) ) && (
                 0.005*qLINEITEM2[bigsum_P__PARTKEY] <= bigsum_L__QUANTITY ) )?
                 ( qLINEITEM3[make_tuple(bigsum_P__PARTKEY,bigsum_L__QUANTITY)] ) : ( 0 ) );
        }
    }
    map<int64_t,double>::iterator qLINEITEM2_it16 = qLINEITEM2.begin();
    map<int64_t,double>::iterator qLINEITEM2_end15 = qLINEITEM2.end();
    for (; qLINEITEM2_it16 != qLINEITEM2_end15; ++qLINEITEM2_it16)
    {
        int64_t P__PARTKEY = qLINEITEM2_it16->first;
        qLINEITEM2[P__PARTKEY] = 0;
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_it14 = LINEITEM.begin();
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_end13 = LINEITEM.end();
        for (; LINEITEM_it14 != LINEITEM_end13; ++LINEITEM_it14)
        {
            int64_t L2__ORDERKEY = get<0>(*LINEITEM_it14);
            int64_t protect_P__PARTKEY = get<1>(*LINEITEM_it14);
            int64_t L2__SUPPKEY = get<2>(*LINEITEM_it14);
            int L2__LINENUMBER = get<3>(*LINEITEM_it14);
            double L2__QUANTITY = get<4>(*LINEITEM_it14);
            double L2__EXTENDEDPRICE = get<5>(*LINEITEM_it14);
            double L2__DISCOUNT = get<6>(*LINEITEM_it14);
            double L2__TAX = get<7>(*LINEITEM_it14);
            int64_t var1 = 0;
            var1 += P__PARTKEY;
            int64_t var2 = 0;
            var2 += protect_P__PARTKEY;
            if ( var1 == var2 )
            {
                qLINEITEM2[P__PARTKEY] += L2__QUANTITY;
            }
        }
    }
    map<tuple<int64_t,double>,double>::iterator qLINEITEM3_it22 = 
        qLINEITEM3.begin();
    map<tuple<int64_t,double>,double>::iterator qLINEITEM3_end21 = 
        qLINEITEM3.end();
    for (; qLINEITEM3_it22 != qLINEITEM3_end21; ++qLINEITEM3_it22)
    {
        int64_t P__PARTKEY = get<0>(qLINEITEM3_it22->first);
        double bigsum_L__QUANTITY = get<1>(qLINEITEM3_it22->first);
        qLINEITEM3[make_tuple(P__PARTKEY,QUANTITY)] = 0;
        
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
            >::iterator PARTS_it20 = PARTS.begin();
        
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
            >::iterator PARTS_end19 = PARTS.end();
        for (; PARTS_it20 != PARTS_end19; ++PARTS_it20)
        {
            int64_t protect_P__PARTKEY = get<0>(*PARTS_it20);
            string protect_P__NAME = get<1>(*PARTS_it20);
            string protect_P__MFGR = get<2>(*PARTS_it20);
            string protect_P__BRAND = get<3>(*PARTS_it20);
            string protect_P__TYPE = get<4>(*PARTS_it20);
            int protect_P__SIZE = get<5>(*PARTS_it20);
            string protect_P__CONTAINER = get<6>(*PARTS_it20);
            double protect_P__RETAILPRICE = get<7>(*PARTS_it20);
            string protect_P__COMMENT = get<8>(*PARTS_it20);
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_it18 = LINEITEM.begin();
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_end17 = LINEITEM.end();
            for (; LINEITEM_it18 != LINEITEM_end17; ++LINEITEM_it18)
            {
                int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it18);
                int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it18);
                int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it18);
                int protect_L__LINENUMBER = get<3>(*LINEITEM_it18);
                double protect_L__QUANTITY = get<4>(*LINEITEM_it18);
                double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it18);
                double protect_L__DISCOUNT = get<6>(*LINEITEM_it18);
                double protect_L__TAX = get<7>(*LINEITEM_it18);
                int64_t var9 = 0;
                var9 += protect_P__PARTKEY;
                int64_t var10 = 0;
                var10 += protect_L__PARTKEY;
                if ( var9 == var10 )
                {
                    int64_t var7 = 0;
                    var7 += P__PARTKEY;
                    int64_t var8 = 0;
                    var8 += protect_P__PARTKEY;
                    if ( var7 == var8 )
                    {
                        double var5 = 0;
                        var5 += QUANTITY;
                        double var6 = 0;
                        var6 += protect_L__QUANTITY;
                        if ( var5 == var6 )
                        {
                            int64_t var3 = 0;
                            var3 += P__PARTKEY;
                            int64_t var4 = 0;
                            var4 += protect_P__PARTKEY;
                            if ( var3 == var4 )
                            {
                                qLINEITEM3[make_tuple(
                                    P__PARTKEY,QUANTITY)] += protect_L__EXTENDEDPRICE;
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,double,int64_t>,double>::iterator qPARTS1_it26 = 
        qPARTS1.begin();
    map<tuple<int64_t,double,int64_t>,double>::iterator qPARTS1_end25 = 
        qPARTS1.end();
    for (; qPARTS1_it26 != qPARTS1_end25; ++qPARTS1_it26)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it26->first);
        double bigsum_L__QUANTITY = get<1>(qPARTS1_it26->first);
        int64_t P__PARTKEY = get<2>(qPARTS1_it26->first);
        qPARTS1[make_tuple(x_qPARTS_P__PARTKEY,QUANTITY,P__PARTKEY)] = 0;
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_it24 = LINEITEM.begin();
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_end23 = LINEITEM.end();
        for (; LINEITEM_it24 != LINEITEM_end23; ++LINEITEM_it24)
        {
            int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it24);
            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it24);
            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it24);
            int protect_L__LINENUMBER = get<3>(*LINEITEM_it24);
            double protect_L__QUANTITY = get<4>(*LINEITEM_it24);
            double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it24);
            double protect_L__DISCOUNT = get<6>(*LINEITEM_it24);
            double protect_L__TAX = get<7>(*LINEITEM_it24);
            int64_t var15 = 0;
            var15 += x_qPARTS_P__PARTKEY;
            int64_t var16 = 0;
            var16 += P__PARTKEY;
            if ( var15 == var16 )
            {
                double var13 = 0;
                var13 += QUANTITY;
                double var14 = 0;
                var14 += protect_L__QUANTITY;
                if ( var13 == var14 )
                {
                    int64_t var11 = 0;
                    var11 += x_qPARTS_P__PARTKEY;
                    int64_t var12 = 0;
                    var12 += protect_L__PARTKEY;
                    if ( var11 == var12 )
                    {
                        qPARTS1[make_tuple(
                            x_qPARTS_P__PARTKEY,QUANTITY,P__PARTKEY)] += protect_L__EXTENDEDPRICE;
                    }
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_LINEITEM_sec_span, on_insert_LINEITEM_usec_span);
    (*results) << "on_insert_LINEITEM" << "," << q << endl;
}

void on_insert_PARTS(
    ofstream* results, ofstream* log, ofstream* stats, int64_t PARTKEY,string 
    NAME,string MFGR,string BRAND,string TYPE,int SIZE,string CONTAINER,double 
    RETAILPRICE,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    bigsum_P__PARTKEY_dom.insert(PARTKEY);
    PARTS.insert(make_tuple(
        PARTKEY,NAME,MFGR,BRAND,TYPE,SIZE,CONTAINER,RETAILPRICE,COMMENT));
    q61 = 0.0;
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it30 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end29 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it30 != bigsum_P__PARTKEY_dom_end29; 
        ++bigsum_P__PARTKEY_dom_it30)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it30;
        set<double>::iterator bigsum_L__QUANTITY_dom_it28 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end27 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it28 != bigsum_L__QUANTITY_dom_end27; 
            ++bigsum_L__QUANTITY_dom_it28)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it28;
            q61 += ( ( (
                 0.005*qLINEITEM2[bigsum_P__PARTKEY] <= bigsum_L__QUANTITY ) && (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY] ) )?
                 ( qLINEITEM3[make_tuple(bigsum_P__PARTKEY,bigsum_L__QUANTITY)] ) : ( 0 ) );
        }
    }
    q += -1*q61;
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it34 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end33 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it34 != bigsum_P__PARTKEY_dom_end33; 
        ++bigsum_P__PARTKEY_dom_it34)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it34;
        set<double>::iterator bigsum_L__QUANTITY_dom_it32 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end31 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it32 != bigsum_L__QUANTITY_dom_end31; 
            ++bigsum_L__QUANTITY_dom_it32)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it32;
            q += ( ( bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY] )?
                 ( qPARTS1[make_tuple(PARTKEY,bigsum_L__QUANTITY,bigsum_P__PARTKEY)] ) : (
                 0 ) );
        }
    }
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it38 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end37 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it38 != bigsum_P__PARTKEY_dom_end37; 
        ++bigsum_P__PARTKEY_dom_it38)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it38;
        set<double>::iterator bigsum_L__QUANTITY_dom_it36 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end35 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it36 != bigsum_L__QUANTITY_dom_end35; 
            ++bigsum_L__QUANTITY_dom_it36)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it36;
            q += ( ( (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY] ) && (
                 0.005*qLINEITEM2[bigsum_P__PARTKEY] <= bigsum_L__QUANTITY ) )?
                 ( qLINEITEM3[make_tuple(bigsum_P__PARTKEY,bigsum_L__QUANTITY)] ) : ( 0 ) );
        }
    }
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1_it42 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1_end41 = qLINEITEM1.end(
        );
    for (; qLINEITEM1_it42 != qLINEITEM1_end41; ++qLINEITEM1_it42)
    {
        int64_t x_qLINEITEM_L__PARTKEY = get<0>(qLINEITEM1_it42->first);
        int64_t bigsum_P__PARTKEY = get<1>(qLINEITEM1_it42->first);
        qLINEITEM1[make_tuple(x_qLINEITEM_L__PARTKEY,PARTKEY)] = 0;
        
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
            >::iterator PARTS_it40 = PARTS.begin();
        
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
            >::iterator PARTS_end39 = PARTS.end();
        for (; PARTS_it40 != PARTS_end39; ++PARTS_it40)
        {
            int64_t protect_P__PARTKEY = get<0>(*PARTS_it40);
            string protect_P__NAME = get<1>(*PARTS_it40);
            string protect_P__MFGR = get<2>(*PARTS_it40);
            string protect_P__BRAND = get<3>(*PARTS_it40);
            string protect_P__TYPE = get<4>(*PARTS_it40);
            int protect_P__SIZE = get<5>(*PARTS_it40);
            string protect_P__CONTAINER = get<6>(*PARTS_it40);
            double protect_P__RETAILPRICE = get<7>(*PARTS_it40);
            string protect_P__COMMENT = get<8>(*PARTS_it40);
            int64_t var19 = 0;
            var19 += x_qLINEITEM_L__PARTKEY;
            int64_t var20 = 0;
            var20 += PARTKEY;
            if ( var19 == var20 )
            {
                int64_t var17 = 0;
                var17 += x_qLINEITEM_L__PARTKEY;
                int64_t var18 = 0;
                var18 += protect_P__PARTKEY;
                if ( var17 == var18 )
                {
                    qLINEITEM1[make_tuple(x_qLINEITEM_L__PARTKEY,PARTKEY)] += 1;
                }
            }
        }
    }
    map<tuple<int64_t,double>,double>::iterator qLINEITEM3_it48 = 
        qLINEITEM3.begin();
    map<tuple<int64_t,double>,double>::iterator qLINEITEM3_end47 = 
        qLINEITEM3.end();
    for (; qLINEITEM3_it48 != qLINEITEM3_end47; ++qLINEITEM3_it48)
    {
        int64_t bigsum_P__PARTKEY = get<0>(qLINEITEM3_it48->first);
        double L__QUANTITY = get<1>(qLINEITEM3_it48->first);
        qLINEITEM3[make_tuple(PARTKEY,L__QUANTITY)] = 0;
        
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
            >::iterator PARTS_it46 = PARTS.begin();
        
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
            >::iterator PARTS_end45 = PARTS.end();
        for (; PARTS_it46 != PARTS_end45; ++PARTS_it46)
        {
            int64_t protect_P__PARTKEY = get<0>(*PARTS_it46);
            string protect_P__NAME = get<1>(*PARTS_it46);
            string protect_P__MFGR = get<2>(*PARTS_it46);
            string protect_P__BRAND = get<3>(*PARTS_it46);
            string protect_P__TYPE = get<4>(*PARTS_it46);
            int protect_P__SIZE = get<5>(*PARTS_it46);
            string protect_P__CONTAINER = get<6>(*PARTS_it46);
            double protect_P__RETAILPRICE = get<7>(*PARTS_it46);
            string protect_P__COMMENT = get<8>(*PARTS_it46);
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_it44 = LINEITEM.begin();
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_end43 = LINEITEM.end();
            for (; LINEITEM_it44 != LINEITEM_end43; ++LINEITEM_it44)
            {
                int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it44);
                int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it44);
                int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it44);
                int protect_L__LINENUMBER = get<3>(*LINEITEM_it44);
                double protect_L__QUANTITY = get<4>(*LINEITEM_it44);
                double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it44);
                double protect_L__DISCOUNT = get<6>(*LINEITEM_it44);
                double protect_L__TAX = get<7>(*LINEITEM_it44);
                int64_t var27 = 0;
                var27 += protect_P__PARTKEY;
                int64_t var28 = 0;
                var28 += protect_L__PARTKEY;
                if ( var27 == var28 )
                {
                    int64_t var25 = 0;
                    var25 += PARTKEY;
                    int64_t var26 = 0;
                    var26 += protect_P__PARTKEY;
                    if ( var25 == var26 )
                    {
                        double var23 = 0;
                        var23 += L__QUANTITY;
                        double var24 = 0;
                        var24 += protect_L__QUANTITY;
                        if ( var23 == var24 )
                        {
                            int64_t var21 = 0;
                            var21 += PARTKEY;
                            int64_t var22 = 0;
                            var22 += protect_P__PARTKEY;
                            if ( var21 == var22 )
                            {
                                qLINEITEM3[make_tuple(
                                    PARTKEY,L__QUANTITY)] += protect_L__EXTENDEDPRICE;
                            }
                        }
                    }
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_PARTS_sec_span, on_insert_PARTS_usec_span);
    (*results) << "on_insert_PARTS" << "," << q << endl;
}

void on_delete_LINEITEM(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    PARTKEY,int64_t SUPPKEY,int LINENUMBER,double QUANTITY,double 
    EXTENDEDPRICE,double DISCOUNT,double TAX)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    LINEITEM.erase(make_tuple(
        ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDEDPRICE,DISCOUNT,TAX));
    q109 = 0.0;
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it52 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end51 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it52 != bigsum_P__PARTKEY_dom_end51; 
        ++bigsum_P__PARTKEY_dom_it52)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it52;
        set<double>::iterator bigsum_L__QUANTITY_dom_it50 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end49 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it50 != bigsum_L__QUANTITY_dom_end49; 
            ++bigsum_L__QUANTITY_dom_it50)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it50;
            q109 += ( ( ( 
                0.005*qLINEITEM2[bigsum_P__PARTKEY]+-1*0.005*QUANTITY*( (
                 PARTKEY == bigsum_P__PARTKEY )?
                 ( 1 ) : ( 0 ) ) <= bigsum_L__QUANTITY ) && (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY] ) )?
                 ( qLINEITEM3[make_tuple(bigsum_P__PARTKEY,bigsum_L__QUANTITY)] ) : ( 0 ) );
        }
    }
    q += -1*q109;
    q71 = 0.0;
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
            q71 += ( (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY]+-1*0.005*QUANTITY*( (
                 PARTKEY == bigsum_P__PARTKEY )? ( 1 ) : ( 0 ) ) )?
                 ( EXTENDEDPRICE*qLINEITEM1[make_tuple(PARTKEY,bigsum_P__PARTKEY)]*( (
                 QUANTITY == bigsum_L__QUANTITY )? ( 1 ) : ( 0 ) ) ) : ( 0 ) );
        }
    }
    q += -1*q71;
    q91 = 0.0;
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
            q91 += ( ( (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY]+-1*0.005*QUANTITY*( (
                 PARTKEY == bigsum_P__PARTKEY )?
                 ( 1 ) : ( 0 ) ) ) && (
                 0.005*qLINEITEM2[bigsum_P__PARTKEY] <= bigsum_L__QUANTITY ) )?
                 ( qLINEITEM3[make_tuple(bigsum_P__PARTKEY,bigsum_L__QUANTITY)] ) : ( 0 ) );
        }
    }
    q += -1*-1*q91;
    map<int64_t,double>::iterator qLINEITEM2_it64 = qLINEITEM2.begin();
    map<int64_t,double>::iterator qLINEITEM2_end63 = qLINEITEM2.end();
    for (; qLINEITEM2_it64 != qLINEITEM2_end63; ++qLINEITEM2_it64)
    {
        int64_t P__PARTKEY = qLINEITEM2_it64->first;
        qLINEITEM2[P__PARTKEY] = 0;
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_it62 = LINEITEM.begin();
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_end61 = LINEITEM.end();
        for (; LINEITEM_it62 != LINEITEM_end61; ++LINEITEM_it62)
        {
            int64_t L2__ORDERKEY = get<0>(*LINEITEM_it62);
            int64_t protect_P__PARTKEY = get<1>(*LINEITEM_it62);
            int64_t L2__SUPPKEY = get<2>(*LINEITEM_it62);
            int L2__LINENUMBER = get<3>(*LINEITEM_it62);
            double L2__QUANTITY = get<4>(*LINEITEM_it62);
            double L2__EXTENDEDPRICE = get<5>(*LINEITEM_it62);
            double L2__DISCOUNT = get<6>(*LINEITEM_it62);
            double L2__TAX = get<7>(*LINEITEM_it62);
            int64_t var29 = 0;
            var29 += P__PARTKEY;
            int64_t var30 = 0;
            var30 += protect_P__PARTKEY;
            if ( var29 == var30 )
            {
                qLINEITEM2[P__PARTKEY] += L2__QUANTITY;
            }
        }
    }
    map<tuple<int64_t,double>,double>::iterator qLINEITEM3_it70 = 
        qLINEITEM3.begin();
    map<tuple<int64_t,double>,double>::iterator qLINEITEM3_end69 = 
        qLINEITEM3.end();
    for (; qLINEITEM3_it70 != qLINEITEM3_end69; ++qLINEITEM3_it70)
    {
        int64_t P__PARTKEY = get<0>(qLINEITEM3_it70->first);
        double bigsum_L__QUANTITY = get<1>(qLINEITEM3_it70->first);
        qLINEITEM3[make_tuple(P__PARTKEY,QUANTITY)] = 0;
        
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
            >::iterator PARTS_it68 = PARTS.begin();
        
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
            >::iterator PARTS_end67 = PARTS.end();
        for (; PARTS_it68 != PARTS_end67; ++PARTS_it68)
        {
            int64_t protect_P__PARTKEY = get<0>(*PARTS_it68);
            string protect_P__NAME = get<1>(*PARTS_it68);
            string protect_P__MFGR = get<2>(*PARTS_it68);
            string protect_P__BRAND = get<3>(*PARTS_it68);
            string protect_P__TYPE = get<4>(*PARTS_it68);
            int protect_P__SIZE = get<5>(*PARTS_it68);
            string protect_P__CONTAINER = get<6>(*PARTS_it68);
            double protect_P__RETAILPRICE = get<7>(*PARTS_it68);
            string protect_P__COMMENT = get<8>(*PARTS_it68);
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_it66 = LINEITEM.begin();
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_end65 = LINEITEM.end();
            for (; LINEITEM_it66 != LINEITEM_end65; ++LINEITEM_it66)
            {
                int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it66);
                int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it66);
                int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it66);
                int protect_L__LINENUMBER = get<3>(*LINEITEM_it66);
                double protect_L__QUANTITY = get<4>(*LINEITEM_it66);
                double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it66);
                double protect_L__DISCOUNT = get<6>(*LINEITEM_it66);
                double protect_L__TAX = get<7>(*LINEITEM_it66);
                int64_t var37 = 0;
                var37 += protect_P__PARTKEY;
                int64_t var38 = 0;
                var38 += protect_L__PARTKEY;
                if ( var37 == var38 )
                {
                    int64_t var35 = 0;
                    var35 += P__PARTKEY;
                    int64_t var36 = 0;
                    var36 += protect_P__PARTKEY;
                    if ( var35 == var36 )
                    {
                        double var33 = 0;
                        var33 += QUANTITY;
                        double var34 = 0;
                        var34 += protect_L__QUANTITY;
                        if ( var33 == var34 )
                        {
                            int64_t var31 = 0;
                            var31 += P__PARTKEY;
                            int64_t var32 = 0;
                            var32 += protect_P__PARTKEY;
                            if ( var31 == var32 )
                            {
                                qLINEITEM3[make_tuple(
                                    P__PARTKEY,QUANTITY)] += protect_L__EXTENDEDPRICE;
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,double,int64_t>,double>::iterator qPARTS1_it74 = 
        qPARTS1.begin();
    map<tuple<int64_t,double,int64_t>,double>::iterator qPARTS1_end73 = 
        qPARTS1.end();
    for (; qPARTS1_it74 != qPARTS1_end73; ++qPARTS1_it74)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it74->first);
        double bigsum_L__QUANTITY = get<1>(qPARTS1_it74->first);
        int64_t P__PARTKEY = get<2>(qPARTS1_it74->first);
        qPARTS1[make_tuple(x_qPARTS_P__PARTKEY,QUANTITY,P__PARTKEY)] = 0;
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_it72 = LINEITEM.begin();
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_end71 = LINEITEM.end();
        for (; LINEITEM_it72 != LINEITEM_end71; ++LINEITEM_it72)
        {
            int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it72);
            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it72);
            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it72);
            int protect_L__LINENUMBER = get<3>(*LINEITEM_it72);
            double protect_L__QUANTITY = get<4>(*LINEITEM_it72);
            double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it72);
            double protect_L__DISCOUNT = get<6>(*LINEITEM_it72);
            double protect_L__TAX = get<7>(*LINEITEM_it72);
            int64_t var43 = 0;
            var43 += x_qPARTS_P__PARTKEY;
            int64_t var44 = 0;
            var44 += P__PARTKEY;
            if ( var43 == var44 )
            {
                double var41 = 0;
                var41 += QUANTITY;
                double var42 = 0;
                var42 += protect_L__QUANTITY;
                if ( var41 == var42 )
                {
                    int64_t var39 = 0;
                    var39 += x_qPARTS_P__PARTKEY;
                    int64_t var40 = 0;
                    var40 += protect_L__PARTKEY;
                    if ( var39 == var40 )
                    {
                        qPARTS1[make_tuple(
                            x_qPARTS_P__PARTKEY,QUANTITY,P__PARTKEY)] += protect_L__EXTENDEDPRICE;
                    }
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_LINEITEM_sec_span, on_delete_LINEITEM_usec_span);
    (*results) << "on_delete_LINEITEM" << "," << q << endl;
}

void on_delete_PARTS(
    ofstream* results, ofstream* log, ofstream* stats, int64_t PARTKEY,string 
    NAME,string MFGR,string BRAND,string TYPE,int SIZE,string CONTAINER,double 
    RETAILPRICE,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    PARTS.erase(make_tuple(
        PARTKEY,NAME,MFGR,BRAND,TYPE,SIZE,CONTAINER,RETAILPRICE,COMMENT));
    q127 = 0.0;
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it78 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end77 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it78 != bigsum_P__PARTKEY_dom_end77; 
        ++bigsum_P__PARTKEY_dom_it78)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it78;
        set<double>::iterator bigsum_L__QUANTITY_dom_it76 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end75 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it76 != bigsum_L__QUANTITY_dom_end75; 
            ++bigsum_L__QUANTITY_dom_it76)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it76;
            q127 += ( (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY] )?
                 ( qPARTS1[make_tuple(PARTKEY,bigsum_L__QUANTITY,bigsum_P__PARTKEY)] ) : (
                 0 ) );
        }
    }
    q += -1*q127;
    q134 = 0.0;
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it82 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end81 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it82 != bigsum_P__PARTKEY_dom_end81; 
        ++bigsum_P__PARTKEY_dom_it82)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it82;
        set<double>::iterator bigsum_L__QUANTITY_dom_it80 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end79 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it80 != bigsum_L__QUANTITY_dom_end79; 
            ++bigsum_L__QUANTITY_dom_it80)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it80;
            q134 += ( ( (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY] ) && (
                 0.005*qLINEITEM2[bigsum_P__PARTKEY] <= bigsum_L__QUANTITY ) )?
                 ( qLINEITEM3[make_tuple(bigsum_P__PARTKEY,bigsum_L__QUANTITY)] ) : ( 0 ) );
        }
    }
    q += -1*-1*q134;
    q144 = 0.0;
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_it86 = 
        bigsum_P__PARTKEY_dom.begin();
    set<int64_t>::iterator bigsum_P__PARTKEY_dom_end85 = 
        bigsum_P__PARTKEY_dom.end();
    for (
        ; bigsum_P__PARTKEY_dom_it86 != bigsum_P__PARTKEY_dom_end85; 
        ++bigsum_P__PARTKEY_dom_it86)
    {
        int64_t bigsum_P__PARTKEY = *bigsum_P__PARTKEY_dom_it86;
        set<double>::iterator bigsum_L__QUANTITY_dom_it84 = 
            bigsum_L__QUANTITY_dom.begin();
        set<double>::iterator bigsum_L__QUANTITY_dom_end83 = 
            bigsum_L__QUANTITY_dom.end();
        for (
            ; bigsum_L__QUANTITY_dom_it84 != bigsum_L__QUANTITY_dom_end83; 
            ++bigsum_L__QUANTITY_dom_it84)
        {
            double bigsum_L__QUANTITY = *bigsum_L__QUANTITY_dom_it84;
            q144 += ( ( (
                 0.005*qLINEITEM2[bigsum_P__PARTKEY] <= bigsum_L__QUANTITY ) && (
                 bigsum_L__QUANTITY < 0.005*qLINEITEM2[bigsum_P__PARTKEY] ) )?
                 ( qLINEITEM3[make_tuple(bigsum_P__PARTKEY,bigsum_L__QUANTITY)] ) : ( 0 ) );
        }
    }
    q += -1*q144;
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1_it90 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1_end89 = qLINEITEM1.end(
        );
    for (; qLINEITEM1_it90 != qLINEITEM1_end89; ++qLINEITEM1_it90)
    {
        int64_t x_qLINEITEM_L__PARTKEY = get<0>(qLINEITEM1_it90->first);
        int64_t bigsum_P__PARTKEY = get<1>(qLINEITEM1_it90->first);
        qLINEITEM1[make_tuple(x_qLINEITEM_L__PARTKEY,PARTKEY)] = 0;
        
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
            >::iterator PARTS_it88 = PARTS.begin();
        
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
            >::iterator PARTS_end87 = PARTS.end();
        for (; PARTS_it88 != PARTS_end87; ++PARTS_it88)
        {
            int64_t protect_P__PARTKEY = get<0>(*PARTS_it88);
            string protect_P__NAME = get<1>(*PARTS_it88);
            string protect_P__MFGR = get<2>(*PARTS_it88);
            string protect_P__BRAND = get<3>(*PARTS_it88);
            string protect_P__TYPE = get<4>(*PARTS_it88);
            int protect_P__SIZE = get<5>(*PARTS_it88);
            string protect_P__CONTAINER = get<6>(*PARTS_it88);
            double protect_P__RETAILPRICE = get<7>(*PARTS_it88);
            string protect_P__COMMENT = get<8>(*PARTS_it88);
            int64_t var47 = 0;
            var47 += x_qLINEITEM_L__PARTKEY;
            int64_t var48 = 0;
            var48 += PARTKEY;
            if ( var47 == var48 )
            {
                int64_t var45 = 0;
                var45 += x_qLINEITEM_L__PARTKEY;
                int64_t var46 = 0;
                var46 += protect_P__PARTKEY;
                if ( var45 == var46 )
                {
                    qLINEITEM1[make_tuple(x_qLINEITEM_L__PARTKEY,PARTKEY)] += 1;
                }
            }
        }
    }
    map<tuple<int64_t,double>,double>::iterator qLINEITEM3_it96 = 
        qLINEITEM3.begin();
    map<tuple<int64_t,double>,double>::iterator qLINEITEM3_end95 = 
        qLINEITEM3.end();
    for (; qLINEITEM3_it96 != qLINEITEM3_end95; ++qLINEITEM3_it96)
    {
        int64_t bigsum_P__PARTKEY = get<0>(qLINEITEM3_it96->first);
        double L__QUANTITY = get<1>(qLINEITEM3_it96->first);
        qLINEITEM3[make_tuple(PARTKEY,L__QUANTITY)] = 0;
        
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
            >::iterator PARTS_it94 = PARTS.begin();
        
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
            >::iterator PARTS_end93 = PARTS.end();
        for (; PARTS_it94 != PARTS_end93; ++PARTS_it94)
        {
            int64_t protect_P__PARTKEY = get<0>(*PARTS_it94);
            string protect_P__NAME = get<1>(*PARTS_it94);
            string protect_P__MFGR = get<2>(*PARTS_it94);
            string protect_P__BRAND = get<3>(*PARTS_it94);
            string protect_P__TYPE = get<4>(*PARTS_it94);
            int protect_P__SIZE = get<5>(*PARTS_it94);
            string protect_P__CONTAINER = get<6>(*PARTS_it94);
            double protect_P__RETAILPRICE = get<7>(*PARTS_it94);
            string protect_P__COMMENT = get<8>(*PARTS_it94);
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_it92 = LINEITEM.begin();
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_end91 = LINEITEM.end();
            for (; LINEITEM_it92 != LINEITEM_end91; ++LINEITEM_it92)
            {
                int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it92);
                int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it92);
                int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it92);
                int protect_L__LINENUMBER = get<3>(*LINEITEM_it92);
                double protect_L__QUANTITY = get<4>(*LINEITEM_it92);
                double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it92);
                double protect_L__DISCOUNT = get<6>(*LINEITEM_it92);
                double protect_L__TAX = get<7>(*LINEITEM_it92);
                int64_t var55 = 0;
                var55 += protect_P__PARTKEY;
                int64_t var56 = 0;
                var56 += protect_L__PARTKEY;
                if ( var55 == var56 )
                {
                    int64_t var53 = 0;
                    var53 += PARTKEY;
                    int64_t var54 = 0;
                    var54 += protect_P__PARTKEY;
                    if ( var53 == var54 )
                    {
                        double var51 = 0;
                        var51 += L__QUANTITY;
                        double var52 = 0;
                        var52 += protect_L__QUANTITY;
                        if ( var51 == var52 )
                        {
                            int64_t var49 = 0;
                            var49 += PARTKEY;
                            int64_t var50 = 0;
                            var50 += protect_P__PARTKEY;
                            if ( var49 == var50 )
                            {
                                qLINEITEM3[make_tuple(
                                    PARTKEY,L__QUANTITY)] += protect_L__EXTENDEDPRICE;
                            }
                        }
                    }
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_PARTS_sec_span, on_delete_PARTS_usec_span);
    (*results) << "on_delete_PARTS" << "," << q << endl;
}

DBToaster::DemoDatasets::LineitemStream SSBLineitem("/home/yanif/datasets/tpch/sf1/singlefile/lineitem.tbl.a",&DBToaster::DemoDatasets::parseLineitemField,16,65000000,512);

boost::shared_ptr<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor> SSBLineitem_adaptor(new DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor());
static int streamSSBLineitemId = 0;

struct on_insert_LINEITEM_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result>(data); 
        on_insert_LINEITEM(results, log, stats, input.orderkey,input.partkey,input.suppkey,input.linenumber,input.quantity,input.extendedprice,input.discount,input.tax);
    }
};

on_insert_LINEITEM_fun_obj fo_on_insert_LINEITEM_0;

DBToaster::DemoDatasets::PartStream SSBParts("/home/yanif/datasets/tpch/sf1/singlefile/part.tbl.a",&DBToaster::DemoDatasets::parsePartField,9,2100000,512);

boost::shared_ptr<DBToaster::DemoDatasets::PartTupleAdaptor> SSBParts_adaptor(new DBToaster::DemoDatasets::PartTupleAdaptor());
static int streamSSBPartsId = 1;

struct on_insert_PARTS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::PartTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::PartTupleAdaptor::Result>(data); 
        on_insert_PARTS(results, log, stats, input.partkey,input.name,input.mfgr,input.brand,input.type,input.size,input.container,input.retailprice,input.comment);
    }
};

on_insert_PARTS_fun_obj fo_on_insert_PARTS_1;

struct on_delete_LINEITEM_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result>(data); 
        on_delete_LINEITEM(results, log, stats, input.orderkey,input.partkey,input.suppkey,input.linenumber,input.quantity,input.extendedprice,input.discount,input.tax);
    }
};

on_delete_LINEITEM_fun_obj fo_on_delete_LINEITEM_2;

struct on_delete_PARTS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::PartTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::PartTupleAdaptor::Result>(data); 
        on_delete_PARTS(results, log, stats, input.partkey,input.name,input.mfgr,input.brand,input.type,input.size,input.container,input.retailprice,input.comment);
    }
};

on_delete_PARTS_fun_obj fo_on_delete_PARTS_3;


void init(DBToaster::StandaloneEngine::FileMultiplexer& sources,
    DBToaster::StandaloneEngine::FileStreamDispatcher& router, 
    ofstream* results, ofstream* log, ofstream* stats)
{
    router.setFiles(results, log, stats);
    sources.addStream<DBToaster::DemoDatasets::lineitem>(&SSBLineitem, boost::ref(*SSBLineitem_adaptor), streamSSBLineitemId);
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_LINEITEM_0));
    sources.addStream<DBToaster::DemoDatasets::part>(&SSBParts, boost::ref(*SSBParts_adaptor), streamSSBPartsId);
    router.addHandler(streamSSBPartsId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_PARTS_1));
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_LINEITEM_2));
    router.addHandler(streamSSBPartsId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_PARTS_3));
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
