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
multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> > 
    ORDERS;
map<tuple<int,int64_t>,double> q;
multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> > 
    CUSTOMER;

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

   cout << "ORDERS size: " << (((sizeof(multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * ORDERS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_type, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type>, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_compare>))) << endl;

   (*stats) << "m," << "ORDERS" << "," << (((sizeof(multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * ORDERS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_type, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type>, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_compare>))) << endl;

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
    map<tuple<int,int64_t>,double>::iterator q_it8 = q.begin();
    map<tuple<int,int64_t>,double>::iterator q_end7 = q.end();
    for (; q_it8 != q_end7; ++q_it8)
    {
        int O__SHIPPRIORITY = get<0>(q_it8->first);
        int64_t ORDERKEY = get<1>(q_it8->first);
        q[make_tuple(O__SHIPPRIORITY,ORDERKEY)] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it6 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end5 = CUSTOMER.end();
        for (; CUSTOMER_it6 != CUSTOMER_end5; ++CUSTOMER_it6)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it6);
            string protect_C__NAME = get<1>(*CUSTOMER_it6);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it6);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it6);
            string protect_C__PHONE = get<4>(*CUSTOMER_it6);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it6);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it6);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it6);
            
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_it4 = ORDERS.begin();
            
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_end3 = ORDERS.end();
            for (; ORDERS_it4 != ORDERS_end3; ++ORDERS_it4)
            {
                int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it4);
                int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it4);
                string protect_O__ORDERSTATUS = get<2>(*ORDERS_it4);
                double protect_O__TOTALPRICE = get<3>(*ORDERS_it4);
                string protect_O__ORDERDATE = get<4>(*ORDERS_it4);
                string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it4);
                string protect_O__CLERK = get<6>(*ORDERS_it4);
                int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it4);
                string protect_O__COMMENT = get<8>(*ORDERS_it4);
                
                    multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                    >::iterator LINEITEM_it2 = LINEITEM.begin();
                
                    multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                    >::iterator LINEITEM_end1 = LINEITEM.end();
                for (; LINEITEM_it2 != LINEITEM_end1; ++LINEITEM_it2)
                {
                    int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it2);
                    int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it2);
                    int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it2);
                    int protect_L__LINENUMBER = get<3>(*LINEITEM_it2);
                    double protect_L__QUANTITY = get<4>(*LINEITEM_it2);
                    double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it2);
                    double protect_L__DISCOUNT = get<6>(*LINEITEM_it2);
                    double protect_L__TAX = get<7>(*LINEITEM_it2);
                    int64_t var7 = 0;
                    var7 += protect_C__CUSTKEY;
                    int64_t var8 = 0;
                    var8 += protect_O__CUSTKEY;
                    if ( var7 == var8 )
                    {
                        int var5 = 0;
                        var5 += O__SHIPPRIORITY;
                        int var6 = 0;
                        var6 += protect_O__SHIPPRIORITY;
                        if ( var5 == var6 )
                        {
                            int64_t var3 = 0;
                            var3 += protect_L__ORDERKEY;
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
                                    q[make_tuple(
                                        O__SHIPPRIORITY,ORDERKEY)] += protect_L__EXTENDEDPRICE;
                                }
                            }
                        }
                    }
                }
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
    map<tuple<int,int64_t>,double>::iterator q_it16 = q.begin();
    map<tuple<int,int64_t>,double>::iterator q_end15 = q.end();
    for (; q_it16 != q_end15; ++q_it16)
    {
        int SHIPPRIORITY = get<0>(q_it16->first);
        int64_t L__ORDERKEY = get<1>(q_it16->first);
        q[make_tuple(SHIPPRIORITY,L__ORDERKEY)] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it14 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end13 = CUSTOMER.end();
        for (; CUSTOMER_it14 != CUSTOMER_end13; ++CUSTOMER_it14)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it14);
            string protect_C__NAME = get<1>(*CUSTOMER_it14);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it14);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it14);
            string protect_C__PHONE = get<4>(*CUSTOMER_it14);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it14);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it14);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it14);
            
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_it12 = ORDERS.begin();
            
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_end11 = ORDERS.end();
            for (; ORDERS_it12 != ORDERS_end11; ++ORDERS_it12)
            {
                int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it12);
                int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it12);
                string protect_O__ORDERSTATUS = get<2>(*ORDERS_it12);
                double protect_O__TOTALPRICE = get<3>(*ORDERS_it12);
                string protect_O__ORDERDATE = get<4>(*ORDERS_it12);
                string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it12);
                string protect_O__CLERK = get<6>(*ORDERS_it12);
                int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it12);
                string protect_O__COMMENT = get<8>(*ORDERS_it12);
                
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
                    int64_t var15 = 0;
                    var15 += protect_C__CUSTKEY;
                    int64_t var16 = 0;
                    var16 += protect_O__CUSTKEY;
                    if ( var15 == var16 )
                    {
                        int var13 = 0;
                        var13 += SHIPPRIORITY;
                        int var14 = 0;
                        var14 += protect_O__SHIPPRIORITY;
                        if ( var13 == var14 )
                        {
                            int64_t var11 = 0;
                            var11 += protect_L__ORDERKEY;
                            int64_t var12 = 0;
                            var12 += protect_O__ORDERKEY;
                            if ( var11 == var12 )
                            {
                                int64_t var9 = 0;
                                var9 += L__ORDERKEY;
                                int64_t var10 = 0;
                                var10 += protect_L__ORDERKEY;
                                if ( var9 == var10 )
                                {
                                    q[make_tuple(
                                        SHIPPRIORITY,L__ORDERKEY)] += protect_L__EXTENDEDPRICE;
                                }
                            }
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
    map<tuple<int,int64_t>,double>::iterator q_it24 = q.begin();
    map<tuple<int,int64_t>,double>::iterator q_end23 = q.end();
    for (; q_it24 != q_end23; ++q_it24)
    {
        int O__SHIPPRIORITY = get<0>(q_it24->first);
        int64_t L__ORDERKEY = get<1>(q_it24->first);
        q[make_tuple(O__SHIPPRIORITY,L__ORDERKEY)] = 0;
        
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
                    int64_t var23 = 0;
                    var23 += protect_C__CUSTKEY;
                    int64_t var24 = 0;
                    var24 += protect_O__CUSTKEY;
                    if ( var23 == var24 )
                    {
                        int var21 = 0;
                        var21 += O__SHIPPRIORITY;
                        int var22 = 0;
                        var22 += protect_O__SHIPPRIORITY;
                        if ( var21 == var22 )
                        {
                            int64_t var19 = 0;
                            var19 += protect_L__ORDERKEY;
                            int64_t var20 = 0;
                            var20 += protect_O__ORDERKEY;
                            if ( var19 == var20 )
                            {
                                int64_t var17 = 0;
                                var17 += L__ORDERKEY;
                                int64_t var18 = 0;
                                var18 += protect_L__ORDERKEY;
                                if ( var17 == var18 )
                                {
                                    q[make_tuple(
                                        O__SHIPPRIORITY,L__ORDERKEY)] += protect_L__EXTENDEDPRICE;
                                }
                            }
                        }
                    }
                }
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
    map<tuple<int,int64_t>,double>::iterator q_it32 = q.begin();
    map<tuple<int,int64_t>,double>::iterator q_end31 = q.end();
    for (; q_it32 != q_end31; ++q_it32)
    {
        int O__SHIPPRIORITY = get<0>(q_it32->first);
        int64_t ORDERKEY = get<1>(q_it32->first);
        q[make_tuple(O__SHIPPRIORITY,ORDERKEY)] = 0;
        
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
                
                    multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                    >::iterator LINEITEM_it26 = LINEITEM.begin();
                
                    multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                    >::iterator LINEITEM_end25 = LINEITEM.end();
                for (; LINEITEM_it26 != LINEITEM_end25; ++LINEITEM_it26)
                {
                    int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it26);
                    int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it26);
                    int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it26);
                    int protect_L__LINENUMBER = get<3>(*LINEITEM_it26);
                    double protect_L__QUANTITY = get<4>(*LINEITEM_it26);
                    double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it26);
                    double protect_L__DISCOUNT = get<6>(*LINEITEM_it26);
                    double protect_L__TAX = get<7>(*LINEITEM_it26);
                    int64_t var31 = 0;
                    var31 += protect_C__CUSTKEY;
                    int64_t var32 = 0;
                    var32 += protect_O__CUSTKEY;
                    if ( var31 == var32 )
                    {
                        int var29 = 0;
                        var29 += O__SHIPPRIORITY;
                        int var30 = 0;
                        var30 += protect_O__SHIPPRIORITY;
                        if ( var29 == var30 )
                        {
                            int64_t var27 = 0;
                            var27 += protect_L__ORDERKEY;
                            int64_t var28 = 0;
                            var28 += protect_O__ORDERKEY;
                            if ( var27 == var28 )
                            {
                                int64_t var25 = 0;
                                var25 += ORDERKEY;
                                int64_t var26 = 0;
                                var26 += protect_L__ORDERKEY;
                                if ( var25 == var26 )
                                {
                                    q[make_tuple(
                                        O__SHIPPRIORITY,ORDERKEY)] += protect_L__EXTENDEDPRICE;
                                }
                            }
                        }
                    }
                }
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
    map<tuple<int,int64_t>,double>::iterator q_it40 = q.begin();
    map<tuple<int,int64_t>,double>::iterator q_end39 = q.end();
    for (; q_it40 != q_end39; ++q_it40)
    {
        int SHIPPRIORITY = get<0>(q_it40->first);
        int64_t L__ORDERKEY = get<1>(q_it40->first);
        q[make_tuple(SHIPPRIORITY,L__ORDERKEY)] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it38 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end37 = CUSTOMER.end();
        for (; CUSTOMER_it38 != CUSTOMER_end37; ++CUSTOMER_it38)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it38);
            string protect_C__NAME = get<1>(*CUSTOMER_it38);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it38);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it38);
            string protect_C__PHONE = get<4>(*CUSTOMER_it38);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it38);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it38);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it38);
            
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_it36 = ORDERS.begin();
            
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_end35 = ORDERS.end();
            for (; ORDERS_it36 != ORDERS_end35; ++ORDERS_it36)
            {
                int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it36);
                int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it36);
                string protect_O__ORDERSTATUS = get<2>(*ORDERS_it36);
                double protect_O__TOTALPRICE = get<3>(*ORDERS_it36);
                string protect_O__ORDERDATE = get<4>(*ORDERS_it36);
                string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it36);
                string protect_O__CLERK = get<6>(*ORDERS_it36);
                int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it36);
                string protect_O__COMMENT = get<8>(*ORDERS_it36);
                
                    multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                    >::iterator LINEITEM_it34 = LINEITEM.begin();
                
                    multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                    >::iterator LINEITEM_end33 = LINEITEM.end();
                for (; LINEITEM_it34 != LINEITEM_end33; ++LINEITEM_it34)
                {
                    int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it34);
                    int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it34);
                    int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it34);
                    int protect_L__LINENUMBER = get<3>(*LINEITEM_it34);
                    double protect_L__QUANTITY = get<4>(*LINEITEM_it34);
                    double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it34);
                    double protect_L__DISCOUNT = get<6>(*LINEITEM_it34);
                    double protect_L__TAX = get<7>(*LINEITEM_it34);
                    int64_t var39 = 0;
                    var39 += protect_C__CUSTKEY;
                    int64_t var40 = 0;
                    var40 += protect_O__CUSTKEY;
                    if ( var39 == var40 )
                    {
                        int var37 = 0;
                        var37 += SHIPPRIORITY;
                        int var38 = 0;
                        var38 += protect_O__SHIPPRIORITY;
                        if ( var37 == var38 )
                        {
                            int64_t var35 = 0;
                            var35 += protect_L__ORDERKEY;
                            int64_t var36 = 0;
                            var36 += protect_O__ORDERKEY;
                            if ( var35 == var36 )
                            {
                                int64_t var33 = 0;
                                var33 += L__ORDERKEY;
                                int64_t var34 = 0;
                                var34 += protect_L__ORDERKEY;
                                if ( var33 == var34 )
                                {
                                    q[make_tuple(
                                        SHIPPRIORITY,L__ORDERKEY)] += protect_L__EXTENDEDPRICE;
                                }
                            }
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
    map<tuple<int,int64_t>,double>::iterator q_it48 = q.begin();
    map<tuple<int,int64_t>,double>::iterator q_end47 = q.end();
    for (; q_it48 != q_end47; ++q_it48)
    {
        int O__SHIPPRIORITY = get<0>(q_it48->first);
        int64_t L__ORDERKEY = get<1>(q_it48->first);
        q[make_tuple(O__SHIPPRIORITY,L__ORDERKEY)] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it46 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end45 = CUSTOMER.end();
        for (; CUSTOMER_it46 != CUSTOMER_end45; ++CUSTOMER_it46)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it46);
            string protect_C__NAME = get<1>(*CUSTOMER_it46);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it46);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it46);
            string protect_C__PHONE = get<4>(*CUSTOMER_it46);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it46);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it46);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it46);
            
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_it44 = ORDERS.begin();
            
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_end43 = ORDERS.end();
            for (; ORDERS_it44 != ORDERS_end43; ++ORDERS_it44)
            {
                int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it44);
                int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it44);
                string protect_O__ORDERSTATUS = get<2>(*ORDERS_it44);
                double protect_O__TOTALPRICE = get<3>(*ORDERS_it44);
                string protect_O__ORDERDATE = get<4>(*ORDERS_it44);
                string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it44);
                string protect_O__CLERK = get<6>(*ORDERS_it44);
                int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it44);
                string protect_O__COMMENT = get<8>(*ORDERS_it44);
                
                    multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                    >::iterator LINEITEM_it42 = LINEITEM.begin();
                
                    multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                    >::iterator LINEITEM_end41 = LINEITEM.end();
                for (; LINEITEM_it42 != LINEITEM_end41; ++LINEITEM_it42)
                {
                    int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it42);
                    int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it42);
                    int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it42);
                    int protect_L__LINENUMBER = get<3>(*LINEITEM_it42);
                    double protect_L__QUANTITY = get<4>(*LINEITEM_it42);
                    double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it42);
                    double protect_L__DISCOUNT = get<6>(*LINEITEM_it42);
                    double protect_L__TAX = get<7>(*LINEITEM_it42);
                    int64_t var47 = 0;
                    var47 += protect_C__CUSTKEY;
                    int64_t var48 = 0;
                    var48 += protect_O__CUSTKEY;
                    if ( var47 == var48 )
                    {
                        int var45 = 0;
                        var45 += O__SHIPPRIORITY;
                        int var46 = 0;
                        var46 += protect_O__SHIPPRIORITY;
                        if ( var45 == var46 )
                        {
                            int64_t var43 = 0;
                            var43 += protect_L__ORDERKEY;
                            int64_t var44 = 0;
                            var44 += protect_O__ORDERKEY;
                            if ( var43 == var44 )
                            {
                                int64_t var41 = 0;
                                var41 += L__ORDERKEY;
                                int64_t var42 = 0;
                                var42 += protect_L__ORDERKEY;
                                if ( var41 == var42 )
                                {
                                    q[make_tuple(
                                        O__SHIPPRIORITY,L__ORDERKEY)] += protect_L__EXTENDEDPRICE;
                                }
                            }
                        }
                    }
                }
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
