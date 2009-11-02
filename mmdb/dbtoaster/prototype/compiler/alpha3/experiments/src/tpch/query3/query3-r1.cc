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
multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> > 
    LINEITEM;
map<int64_t,double> qORDERS1;
map<int64_t,int> qORDERS2;
multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> > 
    ORDERS;
map<tuple<int64_t,int>,int> qLINEITEM1;
map<tuple<int,int64_t>,double> q;
multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> > 
    CUSTOMER;
map<tuple<int64_t,int64_t,int>,double> qCUSTOMER1;

double on_insert_LINEITEM_sec_span = 0.0;
double on_insert_LINEITEM_usec_span = 0.0;
double on_insert_ORDERS_sec_span = 0.0;
double on_insert_ORDERS_usec_span = 0.0;
double on_insert_CUSTOMER_sec_span = 0.0;
double on_insert_CUSTOMER_usec_span = 0.0;
double on_delete_LINEITEM_sec_span = 0.0;
double on_delete_LINEITEM_usec_span = 0.0;
double on_delete_ORDERS_sec_span = 0.0;
double on_delete_ORDERS_usec_span = 0.0;
double on_delete_CUSTOMER_sec_span = 0.0;
double on_delete_CUSTOMER_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
   cout << "LINEITEM size: " << (((sizeof(multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * LINEITEM.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_type, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type>, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_compare>))) << endl;

   (*stats) << "m," << "LINEITEM" << "," << (((sizeof(multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * LINEITEM.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_type, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type>, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_compare>))) << endl;

   cout << "qORDERS1 size: " << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1" << "," << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   cout << "qORDERS2 size: " << (((sizeof(map<int64_t,int>::key_type)
       + sizeof(map<int64_t,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS2.size())  + (sizeof(struct _Rb_tree<map<int64_t,int>::key_type, map<int64_t,int>::value_type, _Select1st<map<int64_t,int>::value_type>, map<int64_t,int>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS2" << "," << (((sizeof(map<int64_t,int>::key_type)
       + sizeof(map<int64_t,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS2.size())  + (sizeof(struct _Rb_tree<map<int64_t,int>::key_type, map<int64_t,int>::value_type, _Select1st<map<int64_t,int>::value_type>, map<int64_t,int>::key_compare>))) << endl;

   cout << "ORDERS size: " << (((sizeof(multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * ORDERS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_type, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type>, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_compare>))) << endl;

   (*stats) << "m," << "ORDERS" << "," << (((sizeof(multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * ORDERS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_type, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type>, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_compare>))) << endl;

   cout << "qLINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int>,int>::key_type)
       + sizeof(map<tuple<int64_t,int>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int>,int>::key_type, map<tuple<int64_t,int>,int>::value_type, _Select1st<map<tuple<int64_t,int>,int>::value_type>, map<tuple<int64_t,int>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int>,int>::key_type)
       + sizeof(map<tuple<int64_t,int>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int>,int>::key_type, map<tuple<int64_t,int>,int>::value_type, _Select1st<map<tuple<int64_t,int>,int>::value_type>, map<tuple<int64_t,int>,int>::key_compare>))) << endl;

   cout << "q size: " << (((sizeof(map<tuple<int,int64_t>,double>::key_type)
       + sizeof(map<tuple<int,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<tuple<int,int64_t>,double>::key_type, map<tuple<int,int64_t>,double>::value_type, _Select1st<map<tuple<int,int64_t>,double>::value_type>, map<tuple<int,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "q" << "," << (((sizeof(map<tuple<int,int64_t>,double>::key_type)
       + sizeof(map<tuple<int,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<tuple<int,int64_t>,double>::key_type, map<tuple<int,int64_t>,double>::value_type, _Select1st<map<tuple<int,int64_t>,double>::value_type>, map<tuple<int,int64_t>,double>::key_compare>))) << endl;

   cout << "CUSTOMER size: " << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * CUSTOMER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_compare>))) << endl;

   (*stats) << "m," << "CUSTOMER" << "," << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * CUSTOMER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_compare>))) << endl;

   cout << "qCUSTOMER1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int>,double>::key_type, map<tuple<int64_t,int64_t,int>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int>,double>::value_type>, map<tuple<int64_t,int64_t,int>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int>,double>::key_type, map<tuple<int64_t,int64_t,int>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int>,double>::value_type>, map<tuple<int64_t,int64_t,int>,double>::key_compare>))) << endl;

}

void analyse_handler_usage(ofstream* stats)
{
   cout << "on_insert_LINEITEM cost: " << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_LINEITEM" << "," << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_insert_ORDERS cost: " << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_ORDERS" << "," << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   cout << "on_insert_CUSTOMER cost: " << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_CUSTOMER" << "," << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   cout << "on_delete_LINEITEM cost: " << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_LINEITEM" << "," << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_delete_ORDERS cost: " << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_ORDERS" << "," << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_CUSTOMER cost: " << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_CUSTOMER" << "," << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
}


void on_insert_LINEITEM(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    PARTKEY,int64_t SUPPKEY,int LINENUMBER,double QUANTITY,double 
    EXTENDEDPRICE,double DISCOUNT,double TAX)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    LINEITEM.insert(make_tuple(
        ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDEDPRICE,DISCOUNT,TAX));
    map<tuple<int,int64_t>,double>::iterator q_it2 = q.begin();
    map<tuple<int,int64_t>,double>::iterator q_end1 = q.end();
    for (; q_it2 != q_end1; ++q_it2)
    {
        int O__SHIPPRIORITY = get<0>(q_it2->first);
        q[make_tuple(
            O__SHIPPRIORITY,ORDERKEY)] += EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,O__SHIPPRIORITY)];
    }
    map<tuple<int64_t,int64_t,int>,double>::iterator qCUSTOMER1_it8 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,int>,double>::iterator qCUSTOMER1_end7 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it8 != qCUSTOMER1_end7; ++qCUSTOMER1_it8)
    {
        int64_t ORDERKEY = get<0>(qCUSTOMER1_it8->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1_it8->first);
        int O__SHIPPRIORITY = get<2>(qCUSTOMER1_it8->first);
        qCUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY,O__SHIPPRIORITY)] = 0;
        
            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
            >::iterator ORDERS_it6 = ORDERS.begin();
        
            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
            >::iterator ORDERS_end5 = ORDERS.end();
        for (; ORDERS_it6 != ORDERS_end5; ++ORDERS_it6)
        {
            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it6);
            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it6);
            string protect_O__ORDERSTATUS = get<2>(*ORDERS_it6);
            double protect_O__TOTALPRICE = get<3>(*ORDERS_it6);
            string protect_O__ORDERDATE = get<4>(*ORDERS_it6);
            string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it6);
            string protect_O__CLERK = get<6>(*ORDERS_it6);
            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it6);
            string protect_O__COMMENT = get<8>(*ORDERS_it6);
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_it4 = LINEITEM.begin();
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_end3 = LINEITEM.end();
            for (; LINEITEM_it4 != LINEITEM_end3; ++LINEITEM_it4)
            {
                int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it4);
                int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it4);
                int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it4);
                int protect_L__LINENUMBER = get<3>(*LINEITEM_it4);
                double protect_L__QUANTITY = get<4>(*LINEITEM_it4);
                double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it4);
                double protect_L__DISCOUNT = get<6>(*LINEITEM_it4);
                double protect_L__TAX = get<7>(*LINEITEM_it4);
                int var7 = 0;
                var7 += O__SHIPPRIORITY;
                int var8 = 0;
                var8 += protect_O__SHIPPRIORITY;
                if ( var7 == var8 )
                {
                    int64_t var5 = 0;
                    var5 += x_qCUSTOMER_C__CUSTKEY;
                    int64_t var6 = 0;
                    var6 += protect_O__CUSTKEY;
                    if ( var5 == var6 )
                    {
                        int64_t var3 = 0;
                        var3 += ORDERKEY;
                        int64_t var4 = 0;
                        var4 += protect_O__ORDERKEY;
                        if ( var3 == var4 )
                        {
                            int64_t var1 = 0;
                            var1 += ORDERKEY;
                            int64_t var2 = 0;
                            var2 += protect_L__ORDERKEY;
                            if ( var1 == var2 )
                            {
                                qCUSTOMER1[make_tuple(
                                    ORDERKEY,x_qCUSTOMER_C__CUSTKEY,O__SHIPPRIORITY)] += protect_L__EXTENDEDPRICE;
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qORDERS1_it12 = qORDERS1.begin();
    map<int64_t,double>::iterator qORDERS1_end11 = qORDERS1.end();
    for (; qORDERS1_it12 != qORDERS1_end11; ++qORDERS1_it12)
    {
        int64_t x_qORDERS_O__ORDERKEY = qORDERS1_it12->first;
        qORDERS1[x_qORDERS_O__ORDERKEY] = 0;
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_it10 = LINEITEM.begin();
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_end9 = LINEITEM.end();
        for (; LINEITEM_it10 != LINEITEM_end9; ++LINEITEM_it10)
        {
            int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it10);
            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it10);
            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it10);
            int protect_L__LINENUMBER = get<3>(*LINEITEM_it10);
            double protect_L__QUANTITY = get<4>(*LINEITEM_it10);
            double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it10);
            double protect_L__DISCOUNT = get<6>(*LINEITEM_it10);
            double protect_L__TAX = get<7>(*LINEITEM_it10);
            int64_t var9 = 0;
            var9 += x_qORDERS_O__ORDERKEY;
            int64_t var10 = 0;
            var10 += protect_L__ORDERKEY;
            if ( var9 == var10 )
            {
                qORDERS1[x_qORDERS_O__ORDERKEY] += protect_L__EXTENDEDPRICE;
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_LINEITEM_sec_span, on_insert_LINEITEM_usec_span);
}

void on_insert_ORDERS(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    CUSTKEY,string ORDERSTATUS,double TOTALPRICE,string ORDERDATE,string 
    ORDERPRIORITY,string CLERK,int SHIPPRIORITY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    ORDERS.insert(make_tuple(
        ORDERKEY,CUSTKEY,ORDERSTATUS,TOTALPRICE,ORDERDATE,ORDERPRIORITY,CLERK,
        SHIPPRIORITY,COMMENT));
    q[make_tuple(
        SHIPPRIORITY,ORDERKEY)] += qORDERS1[ORDERKEY]*qORDERS2[CUSTKEY];
    map<tuple<int64_t,int64_t,int>,double>::iterator qCUSTOMER1_it18 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,int>,double>::iterator qCUSTOMER1_end17 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it18 != qCUSTOMER1_end17; ++qCUSTOMER1_it18)
    {
        int64_t L__ORDERKEY = get<0>(qCUSTOMER1_it18->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1_it18->first);
        int SHIPPRIORITY = get<2>(qCUSTOMER1_it18->first);
        qCUSTOMER1[make_tuple(
            L__ORDERKEY,x_qCUSTOMER_C__CUSTKEY,SHIPPRIORITY)] = 0;
        
            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
            >::iterator ORDERS_it16 = ORDERS.begin();
        
            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
            >::iterator ORDERS_end15 = ORDERS.end();
        for (; ORDERS_it16 != ORDERS_end15; ++ORDERS_it16)
        {
            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it16);
            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it16);
            string protect_O__ORDERSTATUS = get<2>(*ORDERS_it16);
            double protect_O__TOTALPRICE = get<3>(*ORDERS_it16);
            string protect_O__ORDERDATE = get<4>(*ORDERS_it16);
            string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it16);
            string protect_O__CLERK = get<6>(*ORDERS_it16);
            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it16);
            string protect_O__COMMENT = get<8>(*ORDERS_it16);
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_it14 = LINEITEM.begin();
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_end13 = LINEITEM.end();
            for (; LINEITEM_it14 != LINEITEM_end13; ++LINEITEM_it14)
            {
                int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it14);
                int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it14);
                int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it14);
                int protect_L__LINENUMBER = get<3>(*LINEITEM_it14);
                double protect_L__QUANTITY = get<4>(*LINEITEM_it14);
                double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it14);
                double protect_L__DISCOUNT = get<6>(*LINEITEM_it14);
                double protect_L__TAX = get<7>(*LINEITEM_it14);
                int var17 = 0;
                var17 += SHIPPRIORITY;
                int var18 = 0;
                var18 += protect_O__SHIPPRIORITY;
                if ( var17 == var18 )
                {
                    int64_t var15 = 0;
                    var15 += x_qCUSTOMER_C__CUSTKEY;
                    int64_t var16 = 0;
                    var16 += protect_O__CUSTKEY;
                    if ( var15 == var16 )
                    {
                        int64_t var13 = 0;
                        var13 += L__ORDERKEY;
                        int64_t var14 = 0;
                        var14 += protect_O__ORDERKEY;
                        if ( var13 == var14 )
                        {
                            int64_t var11 = 0;
                            var11 += L__ORDERKEY;
                            int64_t var12 = 0;
                            var12 += protect_L__ORDERKEY;
                            if ( var11 == var12 )
                            {
                                qCUSTOMER1[make_tuple(
                                    L__ORDERKEY,x_qCUSTOMER_C__CUSTKEY,SHIPPRIORITY)] += protect_L__EXTENDEDPRICE;
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int>,int>::iterator qLINEITEM1_it24 = qLINEITEM1.begin();
    map<tuple<int64_t,int>,int>::iterator qLINEITEM1_end23 = qLINEITEM1.end();
    for (; qLINEITEM1_it24 != qLINEITEM1_end23; ++qLINEITEM1_it24)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it24->first);
        int SHIPPRIORITY = get<1>(qLINEITEM1_it24->first);
        qLINEITEM1[make_tuple(x_qLINEITEM_L__ORDERKEY,SHIPPRIORITY)] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it22 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end21 = CUSTOMER.end();
        for (; CUSTOMER_it22 != CUSTOMER_end21; ++CUSTOMER_it22)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it22);
            string protect_C__NAME = get<1>(*CUSTOMER_it22);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it22);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it22);
            string protect_C__PHONE = get<4>(*CUSTOMER_it22);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it22);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it22);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it22);
            
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_it20 = ORDERS.begin();
            
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_end19 = ORDERS.end();
            for (; ORDERS_it20 != ORDERS_end19; ++ORDERS_it20)
            {
                int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it20);
                int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it20);
                string protect_O__ORDERSTATUS = get<2>(*ORDERS_it20);
                double protect_O__TOTALPRICE = get<3>(*ORDERS_it20);
                string protect_O__ORDERDATE = get<4>(*ORDERS_it20);
                string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it20);
                string protect_O__CLERK = get<6>(*ORDERS_it20);
                int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it20);
                string protect_O__COMMENT = get<8>(*ORDERS_it20);
                int64_t var23 = 0;
                var23 += protect_C__CUSTKEY;
                int64_t var24 = 0;
                var24 += protect_O__CUSTKEY;
                if ( var23 == var24 )
                {
                    int var21 = 0;
                    var21 += SHIPPRIORITY;
                    int var22 = 0;
                    var22 += protect_O__SHIPPRIORITY;
                    if ( var21 == var22 )
                    {
                        int64_t var19 = 0;
                        var19 += x_qLINEITEM_L__ORDERKEY;
                        int64_t var20 = 0;
                        var20 += protect_O__ORDERKEY;
                        if ( var19 == var20 )
                        {
                            qLINEITEM1[make_tuple(
                                x_qLINEITEM_L__ORDERKEY,SHIPPRIORITY)] += 1;
                        }
                    }
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_ORDERS_sec_span, on_insert_ORDERS_usec_span);
}

void on_insert_CUSTOMER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t CUSTKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    MKTSEGMENT,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    CUSTOMER.insert(make_tuple(
        CUSTKEY,NAME,ADDRESS,NATIONKEY,PHONE,ACCTBAL,MKTSEGMENT,COMMENT));
    map<tuple<int,int64_t>,double>::iterator q_it26 = q.begin();
    map<tuple<int,int64_t>,double>::iterator q_end25 = q.end();
    for (; q_it26 != q_end25; ++q_it26)
    {
        int O__SHIPPRIORITY = get<0>(q_it26->first);
        int64_t L__ORDERKEY = get<1>(q_it26->first);
        q[make_tuple(O__SHIPPRIORITY,L__ORDERKEY)] += qCUSTOMER1[make_tuple(
            L__ORDERKEY,CUSTKEY,O__SHIPPRIORITY)];
    }
    map<tuple<int64_t,int>,int>::iterator qLINEITEM1_it32 = qLINEITEM1.begin();
    map<tuple<int64_t,int>,int>::iterator qLINEITEM1_end31 = qLINEITEM1.end();
    for (; qLINEITEM1_it32 != qLINEITEM1_end31; ++qLINEITEM1_it32)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it32->first);
        int O__SHIPPRIORITY = get<1>(qLINEITEM1_it32->first);
        qLINEITEM1[make_tuple(x_qLINEITEM_L__ORDERKEY,O__SHIPPRIORITY)] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it30 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end29 = CUSTOMER.end();
        for (; CUSTOMER_it30 != CUSTOMER_end29; ++CUSTOMER_it30)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it30);
            string protect_C__NAME = get<1>(*CUSTOMER_it30);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it30);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it30);
            string protect_C__PHONE = get<4>(*CUSTOMER_it30);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it30);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it30);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it30);
            
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_it28 = ORDERS.begin();
            
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_end27 = ORDERS.end();
            for (; ORDERS_it28 != ORDERS_end27; ++ORDERS_it28)
            {
                int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it28);
                int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it28);
                string protect_O__ORDERSTATUS = get<2>(*ORDERS_it28);
                double protect_O__TOTALPRICE = get<3>(*ORDERS_it28);
                string protect_O__ORDERDATE = get<4>(*ORDERS_it28);
                string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it28);
                string protect_O__CLERK = get<6>(*ORDERS_it28);
                int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it28);
                string protect_O__COMMENT = get<8>(*ORDERS_it28);
                int64_t var29 = 0;
                var29 += protect_C__CUSTKEY;
                int64_t var30 = 0;
                var30 += protect_O__CUSTKEY;
                if ( var29 == var30 )
                {
                    int var27 = 0;
                    var27 += O__SHIPPRIORITY;
                    int var28 = 0;
                    var28 += protect_O__SHIPPRIORITY;
                    if ( var27 == var28 )
                    {
                        int64_t var25 = 0;
                        var25 += x_qLINEITEM_L__ORDERKEY;
                        int64_t var26 = 0;
                        var26 += protect_O__ORDERKEY;
                        if ( var25 == var26 )
                        {
                            qLINEITEM1[make_tuple(
                                x_qLINEITEM_L__ORDERKEY,O__SHIPPRIORITY)] += 1;
                        }
                    }
                }
            }
        }
    }
    map<int64_t,int>::iterator qORDERS2_it36 = qORDERS2.begin();
    map<int64_t,int>::iterator qORDERS2_end35 = qORDERS2.end();
    for (; qORDERS2_it36 != qORDERS2_end35; ++qORDERS2_it36)
    {
        int64_t x_qORDERS_O__CUSTKEY = qORDERS2_it36->first;
        qORDERS2[x_qORDERS_O__CUSTKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it34 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end33 = CUSTOMER.end();
        for (; CUSTOMER_it34 != CUSTOMER_end33; ++CUSTOMER_it34)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it34);
            string protect_C__NAME = get<1>(*CUSTOMER_it34);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it34);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it34);
            string protect_C__PHONE = get<4>(*CUSTOMER_it34);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it34);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it34);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it34);
            int64_t var31 = 0;
            var31 += x_qORDERS_O__CUSTKEY;
            int64_t var32 = 0;
            var32 += protect_C__CUSTKEY;
            if ( var31 == var32 )
            {
                qORDERS2[x_qORDERS_O__CUSTKEY] += 1;
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_CUSTOMER_sec_span, on_insert_CUSTOMER_usec_span);
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
    map<tuple<int,int64_t>,double>::iterator q_it38 = q.begin();
    map<tuple<int,int64_t>,double>::iterator q_end37 = q.end();
    for (; q_it38 != q_end37; ++q_it38)
    {
        int O__SHIPPRIORITY = get<0>(q_it38->first);
        q[make_tuple(
            O__SHIPPRIORITY,ORDERKEY)] += -1*EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,O__SHIPPRIORITY)];
    }
    map<tuple<int64_t,int64_t,int>,double>::iterator qCUSTOMER1_it44 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,int>,double>::iterator qCUSTOMER1_end43 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it44 != qCUSTOMER1_end43; ++qCUSTOMER1_it44)
    {
        int64_t ORDERKEY = get<0>(qCUSTOMER1_it44->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1_it44->first);
        int O__SHIPPRIORITY = get<2>(qCUSTOMER1_it44->first);
        qCUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY,O__SHIPPRIORITY)] = 0;
        
            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
            >::iterator ORDERS_it42 = ORDERS.begin();
        
            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
            >::iterator ORDERS_end41 = ORDERS.end();
        for (; ORDERS_it42 != ORDERS_end41; ++ORDERS_it42)
        {
            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it42);
            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it42);
            string protect_O__ORDERSTATUS = get<2>(*ORDERS_it42);
            double protect_O__TOTALPRICE = get<3>(*ORDERS_it42);
            string protect_O__ORDERDATE = get<4>(*ORDERS_it42);
            string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it42);
            string protect_O__CLERK = get<6>(*ORDERS_it42);
            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it42);
            string protect_O__COMMENT = get<8>(*ORDERS_it42);
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_it40 = LINEITEM.begin();
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_end39 = LINEITEM.end();
            for (; LINEITEM_it40 != LINEITEM_end39; ++LINEITEM_it40)
            {
                int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it40);
                int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it40);
                int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it40);
                int protect_L__LINENUMBER = get<3>(*LINEITEM_it40);
                double protect_L__QUANTITY = get<4>(*LINEITEM_it40);
                double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it40);
                double protect_L__DISCOUNT = get<6>(*LINEITEM_it40);
                double protect_L__TAX = get<7>(*LINEITEM_it40);
                int var39 = 0;
                var39 += O__SHIPPRIORITY;
                int var40 = 0;
                var40 += protect_O__SHIPPRIORITY;
                if ( var39 == var40 )
                {
                    int64_t var37 = 0;
                    var37 += x_qCUSTOMER_C__CUSTKEY;
                    int64_t var38 = 0;
                    var38 += protect_O__CUSTKEY;
                    if ( var37 == var38 )
                    {
                        int64_t var35 = 0;
                        var35 += ORDERKEY;
                        int64_t var36 = 0;
                        var36 += protect_O__ORDERKEY;
                        if ( var35 == var36 )
                        {
                            int64_t var33 = 0;
                            var33 += ORDERKEY;
                            int64_t var34 = 0;
                            var34 += protect_L__ORDERKEY;
                            if ( var33 == var34 )
                            {
                                qCUSTOMER1[make_tuple(
                                    ORDERKEY,x_qCUSTOMER_C__CUSTKEY,O__SHIPPRIORITY)] += protect_L__EXTENDEDPRICE;
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qORDERS1_it48 = qORDERS1.begin();
    map<int64_t,double>::iterator qORDERS1_end47 = qORDERS1.end();
    for (; qORDERS1_it48 != qORDERS1_end47; ++qORDERS1_it48)
    {
        int64_t x_qORDERS_O__ORDERKEY = qORDERS1_it48->first;
        qORDERS1[x_qORDERS_O__ORDERKEY] = 0;
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_it46 = LINEITEM.begin();
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_end45 = LINEITEM.end();
        for (; LINEITEM_it46 != LINEITEM_end45; ++LINEITEM_it46)
        {
            int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it46);
            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it46);
            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it46);
            int protect_L__LINENUMBER = get<3>(*LINEITEM_it46);
            double protect_L__QUANTITY = get<4>(*LINEITEM_it46);
            double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it46);
            double protect_L__DISCOUNT = get<6>(*LINEITEM_it46);
            double protect_L__TAX = get<7>(*LINEITEM_it46);
            int64_t var41 = 0;
            var41 += x_qORDERS_O__ORDERKEY;
            int64_t var42 = 0;
            var42 += protect_L__ORDERKEY;
            if ( var41 == var42 )
            {
                qORDERS1[x_qORDERS_O__ORDERKEY] += protect_L__EXTENDEDPRICE;
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_LINEITEM_sec_span, on_delete_LINEITEM_usec_span);
}

void on_delete_ORDERS(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    CUSTKEY,string ORDERSTATUS,double TOTALPRICE,string ORDERDATE,string 
    ORDERPRIORITY,string CLERK,int SHIPPRIORITY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    ORDERS.erase(make_tuple(
        ORDERKEY,CUSTKEY,ORDERSTATUS,TOTALPRICE,ORDERDATE,ORDERPRIORITY,CLERK,
        SHIPPRIORITY,COMMENT));
    q[make_tuple(
        SHIPPRIORITY,ORDERKEY)] += -1*qORDERS1[ORDERKEY]*qORDERS2[CUSTKEY];
    map<tuple<int64_t,int64_t,int>,double>::iterator qCUSTOMER1_it54 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,int>,double>::iterator qCUSTOMER1_end53 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it54 != qCUSTOMER1_end53; ++qCUSTOMER1_it54)
    {
        int64_t L__ORDERKEY = get<0>(qCUSTOMER1_it54->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1_it54->first);
        int SHIPPRIORITY = get<2>(qCUSTOMER1_it54->first);
        qCUSTOMER1[make_tuple(
            L__ORDERKEY,x_qCUSTOMER_C__CUSTKEY,SHIPPRIORITY)] = 0;
        
            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
            >::iterator ORDERS_it52 = ORDERS.begin();
        
            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
            >::iterator ORDERS_end51 = ORDERS.end();
        for (; ORDERS_it52 != ORDERS_end51; ++ORDERS_it52)
        {
            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it52);
            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it52);
            string protect_O__ORDERSTATUS = get<2>(*ORDERS_it52);
            double protect_O__TOTALPRICE = get<3>(*ORDERS_it52);
            string protect_O__ORDERDATE = get<4>(*ORDERS_it52);
            string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it52);
            string protect_O__CLERK = get<6>(*ORDERS_it52);
            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it52);
            string protect_O__COMMENT = get<8>(*ORDERS_it52);
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_it50 = LINEITEM.begin();
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_end49 = LINEITEM.end();
            for (; LINEITEM_it50 != LINEITEM_end49; ++LINEITEM_it50)
            {
                int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it50);
                int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it50);
                int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it50);
                int protect_L__LINENUMBER = get<3>(*LINEITEM_it50);
                double protect_L__QUANTITY = get<4>(*LINEITEM_it50);
                double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it50);
                double protect_L__DISCOUNT = get<6>(*LINEITEM_it50);
                double protect_L__TAX = get<7>(*LINEITEM_it50);
                int var49 = 0;
                var49 += SHIPPRIORITY;
                int var50 = 0;
                var50 += protect_O__SHIPPRIORITY;
                if ( var49 == var50 )
                {
                    int64_t var47 = 0;
                    var47 += x_qCUSTOMER_C__CUSTKEY;
                    int64_t var48 = 0;
                    var48 += protect_O__CUSTKEY;
                    if ( var47 == var48 )
                    {
                        int64_t var45 = 0;
                        var45 += L__ORDERKEY;
                        int64_t var46 = 0;
                        var46 += protect_O__ORDERKEY;
                        if ( var45 == var46 )
                        {
                            int64_t var43 = 0;
                            var43 += L__ORDERKEY;
                            int64_t var44 = 0;
                            var44 += protect_L__ORDERKEY;
                            if ( var43 == var44 )
                            {
                                qCUSTOMER1[make_tuple(
                                    L__ORDERKEY,x_qCUSTOMER_C__CUSTKEY,SHIPPRIORITY)] += protect_L__EXTENDEDPRICE;
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int>,int>::iterator qLINEITEM1_it60 = qLINEITEM1.begin();
    map<tuple<int64_t,int>,int>::iterator qLINEITEM1_end59 = qLINEITEM1.end();
    for (; qLINEITEM1_it60 != qLINEITEM1_end59; ++qLINEITEM1_it60)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it60->first);
        int SHIPPRIORITY = get<1>(qLINEITEM1_it60->first);
        qLINEITEM1[make_tuple(x_qLINEITEM_L__ORDERKEY,SHIPPRIORITY)] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it58 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end57 = CUSTOMER.end();
        for (; CUSTOMER_it58 != CUSTOMER_end57; ++CUSTOMER_it58)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it58);
            string protect_C__NAME = get<1>(*CUSTOMER_it58);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it58);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it58);
            string protect_C__PHONE = get<4>(*CUSTOMER_it58);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it58);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it58);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it58);
            
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_it56 = ORDERS.begin();
            
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_end55 = ORDERS.end();
            for (; ORDERS_it56 != ORDERS_end55; ++ORDERS_it56)
            {
                int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it56);
                int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it56);
                string protect_O__ORDERSTATUS = get<2>(*ORDERS_it56);
                double protect_O__TOTALPRICE = get<3>(*ORDERS_it56);
                string protect_O__ORDERDATE = get<4>(*ORDERS_it56);
                string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it56);
                string protect_O__CLERK = get<6>(*ORDERS_it56);
                int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it56);
                string protect_O__COMMENT = get<8>(*ORDERS_it56);
                int64_t var55 = 0;
                var55 += protect_C__CUSTKEY;
                int64_t var56 = 0;
                var56 += protect_O__CUSTKEY;
                if ( var55 == var56 )
                {
                    int var53 = 0;
                    var53 += SHIPPRIORITY;
                    int var54 = 0;
                    var54 += protect_O__SHIPPRIORITY;
                    if ( var53 == var54 )
                    {
                        int64_t var51 = 0;
                        var51 += x_qLINEITEM_L__ORDERKEY;
                        int64_t var52 = 0;
                        var52 += protect_O__ORDERKEY;
                        if ( var51 == var52 )
                        {
                            qLINEITEM1[make_tuple(
                                x_qLINEITEM_L__ORDERKEY,SHIPPRIORITY)] += 1;
                        }
                    }
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_ORDERS_sec_span, on_delete_ORDERS_usec_span);
}

void on_delete_CUSTOMER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t CUSTKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    MKTSEGMENT,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    CUSTOMER.erase(make_tuple(
        CUSTKEY,NAME,ADDRESS,NATIONKEY,PHONE,ACCTBAL,MKTSEGMENT,COMMENT));
    map<tuple<int,int64_t>,double>::iterator q_it62 = q.begin();
    map<tuple<int,int64_t>,double>::iterator q_end61 = q.end();
    for (; q_it62 != q_end61; ++q_it62)
    {
        int O__SHIPPRIORITY = get<0>(q_it62->first);
        int64_t L__ORDERKEY = get<1>(q_it62->first);
        q[make_tuple(O__SHIPPRIORITY,L__ORDERKEY)] += -1*qCUSTOMER1[make_tuple(
            L__ORDERKEY,CUSTKEY,O__SHIPPRIORITY)];
    }
    map<tuple<int64_t,int>,int>::iterator qLINEITEM1_it68 = qLINEITEM1.begin();
    map<tuple<int64_t,int>,int>::iterator qLINEITEM1_end67 = qLINEITEM1.end();
    for (; qLINEITEM1_it68 != qLINEITEM1_end67; ++qLINEITEM1_it68)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it68->first);
        int O__SHIPPRIORITY = get<1>(qLINEITEM1_it68->first);
        qLINEITEM1[make_tuple(x_qLINEITEM_L__ORDERKEY,O__SHIPPRIORITY)] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it66 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end65 = CUSTOMER.end();
        for (; CUSTOMER_it66 != CUSTOMER_end65; ++CUSTOMER_it66)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it66);
            string protect_C__NAME = get<1>(*CUSTOMER_it66);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it66);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it66);
            string protect_C__PHONE = get<4>(*CUSTOMER_it66);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it66);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it66);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it66);
            
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_it64 = ORDERS.begin();
            
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_end63 = ORDERS.end();
            for (; ORDERS_it64 != ORDERS_end63; ++ORDERS_it64)
            {
                int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it64);
                int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it64);
                string protect_O__ORDERSTATUS = get<2>(*ORDERS_it64);
                double protect_O__TOTALPRICE = get<3>(*ORDERS_it64);
                string protect_O__ORDERDATE = get<4>(*ORDERS_it64);
                string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it64);
                string protect_O__CLERK = get<6>(*ORDERS_it64);
                int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it64);
                string protect_O__COMMENT = get<8>(*ORDERS_it64);
                int64_t var61 = 0;
                var61 += protect_C__CUSTKEY;
                int64_t var62 = 0;
                var62 += protect_O__CUSTKEY;
                if ( var61 == var62 )
                {
                    int var59 = 0;
                    var59 += O__SHIPPRIORITY;
                    int var60 = 0;
                    var60 += protect_O__SHIPPRIORITY;
                    if ( var59 == var60 )
                    {
                        int64_t var57 = 0;
                        var57 += x_qLINEITEM_L__ORDERKEY;
                        int64_t var58 = 0;
                        var58 += protect_O__ORDERKEY;
                        if ( var57 == var58 )
                        {
                            qLINEITEM1[make_tuple(
                                x_qLINEITEM_L__ORDERKEY,O__SHIPPRIORITY)] += 1;
                        }
                    }
                }
            }
        }
    }
    map<int64_t,int>::iterator qORDERS2_it72 = qORDERS2.begin();
    map<int64_t,int>::iterator qORDERS2_end71 = qORDERS2.end();
    for (; qORDERS2_it72 != qORDERS2_end71; ++qORDERS2_it72)
    {
        int64_t x_qORDERS_O__CUSTKEY = qORDERS2_it72->first;
        qORDERS2[x_qORDERS_O__CUSTKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it70 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end69 = CUSTOMER.end();
        for (; CUSTOMER_it70 != CUSTOMER_end69; ++CUSTOMER_it70)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it70);
            string protect_C__NAME = get<1>(*CUSTOMER_it70);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it70);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it70);
            string protect_C__PHONE = get<4>(*CUSTOMER_it70);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it70);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it70);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it70);
            int64_t var63 = 0;
            var63 += x_qORDERS_O__CUSTKEY;
            int64_t var64 = 0;
            var64 += protect_C__CUSTKEY;
            if ( var63 == var64 )
            {
                qORDERS2[x_qORDERS_O__CUSTKEY] += 1;
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_CUSTOMER_sec_span, on_delete_CUSTOMER_usec_span);
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

DBToaster::DemoDatasets::OrderStream SSBOrder("/home/yanif/datasets/tpch/sf1/singlefile/orders.tbl.a",&DBToaster::DemoDatasets::parseOrderField,9,17000000,512);

boost::shared_ptr<DBToaster::DemoDatasets::OrderTupleAdaptor> SSBOrder_adaptor(new DBToaster::DemoDatasets::OrderTupleAdaptor());
static int streamSSBOrderId = 1;

struct on_insert_ORDERS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::OrderTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::OrderTupleAdaptor::Result>(data); 
        on_insert_ORDERS(results, log, stats, input.orderkey,input.custkey,input.orderstatus,input.totalprice,input.orderdate,input.orderpriority,input.clerk,input.shippriority,input.comment);
    }
};

on_insert_ORDERS_fun_obj fo_on_insert_ORDERS_1;

DBToaster::DemoDatasets::CustomerStream SSBCustomer("/home/yanif/datasets/tpch/sf1/singlefile/customer.tbl.a",&DBToaster::DemoDatasets::parseCustomerField,8,1600000, 512);

boost::shared_ptr<DBToaster::DemoDatasets::CustomerTupleAdaptor> SSBCustomer_adaptor(new DBToaster::DemoDatasets::CustomerTupleAdaptor());
static int streamSSBCustomerId = 2;

struct on_insert_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_insert_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_insert_CUSTOMER_fun_obj fo_on_insert_CUSTOMER_2;

struct on_delete_LINEITEM_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result>(data); 
        on_delete_LINEITEM(results, log, stats, input.orderkey,input.partkey,input.suppkey,input.linenumber,input.quantity,input.extendedprice,input.discount,input.tax);
    }
};

on_delete_LINEITEM_fun_obj fo_on_delete_LINEITEM_3;

struct on_delete_ORDERS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::OrderTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::OrderTupleAdaptor::Result>(data); 
        on_delete_ORDERS(results, log, stats, input.orderkey,input.custkey,input.orderstatus,input.totalprice,input.orderdate,input.orderpriority,input.clerk,input.shippriority,input.comment);
    }
};

on_delete_ORDERS_fun_obj fo_on_delete_ORDERS_4;

struct on_delete_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_delete_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_delete_CUSTOMER_fun_obj fo_on_delete_CUSTOMER_5;


void init(DBToaster::StandaloneEngine::FileMultiplexer& sources,
    DBToaster::StandaloneEngine::FileStreamDispatcher& router, 
    ofstream* results, ofstream* log, ofstream* stats)
{
    router.setFiles(results, log, stats);
    sources.addStream<DBToaster::DemoDatasets::lineitem>(&SSBLineitem, boost::ref(*SSBLineitem_adaptor), streamSSBLineitemId);
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_LINEITEM_0));
    sources.addStream<DBToaster::DemoDatasets::order>(&SSBOrder, boost::ref(*SSBOrder_adaptor), streamSSBOrderId);
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_ORDERS_1));
    sources.addStream<DBToaster::DemoDatasets::customer>(&SSBCustomer, boost::ref(*SSBCustomer_adaptor), streamSSBCustomerId);
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_CUSTOMER_2));
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_LINEITEM_3));
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_ORDERS_4));
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_CUSTOMER_5));
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