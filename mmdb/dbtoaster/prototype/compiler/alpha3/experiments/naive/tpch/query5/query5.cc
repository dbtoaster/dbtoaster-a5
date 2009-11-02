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
multiset<tuple<int64_t,string,string,int64_t,string,double,string> > SUPPLIER;
multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> > 
    LINEITEM;
multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> > 
    ORDERS;
multiset<tuple<int64_t,string,string> > REGION;
map<string,double> q;
multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> > 
    CUSTOMER;
multiset<tuple<int64_t,string,int64_t,string> > NATION;

double on_insert_LINEITEM_sec_span = 0.0;
double on_insert_LINEITEM_usec_span = 0.0;
double on_insert_ORDERS_sec_span = 0.0;
double on_insert_ORDERS_usec_span = 0.0;
double on_insert_SUPPLIER_sec_span = 0.0;
double on_insert_SUPPLIER_usec_span = 0.0;
double on_insert_CUSTOMER_sec_span = 0.0;
double on_insert_CUSTOMER_usec_span = 0.0;
double on_insert_NATION_sec_span = 0.0;
double on_insert_NATION_usec_span = 0.0;
double on_insert_REGION_sec_span = 0.0;
double on_insert_REGION_usec_span = 0.0;
double on_delete_LINEITEM_sec_span = 0.0;
double on_delete_LINEITEM_usec_span = 0.0;
double on_delete_ORDERS_sec_span = 0.0;
double on_delete_ORDERS_usec_span = 0.0;
double on_delete_SUPPLIER_sec_span = 0.0;
double on_delete_SUPPLIER_usec_span = 0.0;
double on_delete_CUSTOMER_sec_span = 0.0;
double on_delete_CUSTOMER_usec_span = 0.0;
double on_delete_NATION_sec_span = 0.0;
double on_delete_NATION_usec_span = 0.0;
double on_delete_REGION_sec_span = 0.0;
double on_delete_REGION_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
   cout << "SUPPLIER size: " << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * SUPPLIER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::key_compare>))) << endl;

   (*stats) << "m," << "SUPPLIER" << "," << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * SUPPLIER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::key_compare>))) << endl;

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

   cout << "REGION size: " << (((sizeof(multiset<tuple<int64_t,string,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * REGION.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string> >::key_type, multiset<tuple<int64_t,string,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string> >::value_type>, multiset<tuple<int64_t,string,string> >::key_compare>))) << endl;

   (*stats) << "m," << "REGION" << "," << (((sizeof(multiset<tuple<int64_t,string,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * REGION.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string> >::key_type, multiset<tuple<int64_t,string,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string> >::value_type>, multiset<tuple<int64_t,string,string> >::key_compare>))) << endl;

   cout << "q size: " << (((sizeof(map<string,double>::key_type)
       + sizeof(map<string,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<string,double>::key_type, map<string,double>::value_type, _Select1st<map<string,double>::value_type>, map<string,double>::key_compare>))) << endl;

   (*stats) << "m," << "q" << "," << (((sizeof(map<string,double>::key_type)
       + sizeof(map<string,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<string,double>::key_type, map<string,double>::value_type, _Select1st<map<string,double>::value_type>, map<string,double>::key_compare>))) << endl;

   cout << "CUSTOMER size: " << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * CUSTOMER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_compare>))) << endl;

   (*stats) << "m," << "CUSTOMER" << "," << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * CUSTOMER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_compare>))) << endl;

   cout << "NATION size: " << (((sizeof(multiset<tuple<int64_t,string,int64_t,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * NATION.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,int64_t,string> >::key_type, multiset<tuple<int64_t,string,int64_t,string> >::value_type, _Identity<multiset<tuple<int64_t,string,int64_t,string> >::value_type>, multiset<tuple<int64_t,string,int64_t,string> >::key_compare>))) << endl;

   (*stats) << "m," << "NATION" << "," << (((sizeof(multiset<tuple<int64_t,string,int64_t,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * NATION.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,int64_t,string> >::key_type, multiset<tuple<int64_t,string,int64_t,string> >::value_type, _Identity<multiset<tuple<int64_t,string,int64_t,string> >::value_type>, multiset<tuple<int64_t,string,int64_t,string> >::key_compare>))) << endl;

}

void analyse_handler_usage(ofstream* stats)
{
   cout << "on_insert_LINEITEM cost: " << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_LINEITEM" << "," << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_insert_ORDERS cost: " << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_ORDERS" << "," << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   cout << "on_insert_SUPPLIER cost: " << (on_insert_SUPPLIER_sec_span + (on_insert_SUPPLIER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_SUPPLIER" << "," << (on_insert_SUPPLIER_sec_span + (on_insert_SUPPLIER_usec_span / 1000000.0)) << endl;
   cout << "on_insert_CUSTOMER cost: " << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_CUSTOMER" << "," << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   cout << "on_insert_NATION cost: " << (on_insert_NATION_sec_span + (on_insert_NATION_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_NATION" << "," << (on_insert_NATION_sec_span + (on_insert_NATION_usec_span / 1000000.0)) << endl;
   cout << "on_insert_REGION cost: " << (on_insert_REGION_sec_span + (on_insert_REGION_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_REGION" << "," << (on_insert_REGION_sec_span + (on_insert_REGION_usec_span / 1000000.0)) << endl;
   cout << "on_delete_LINEITEM cost: " << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_LINEITEM" << "," << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_delete_ORDERS cost: " << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_ORDERS" << "," << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_SUPPLIER cost: " << (on_delete_SUPPLIER_sec_span + (on_delete_SUPPLIER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_SUPPLIER" << "," << (on_delete_SUPPLIER_sec_span + (on_delete_SUPPLIER_usec_span / 1000000.0)) << endl;
   cout << "on_delete_CUSTOMER cost: " << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_CUSTOMER" << "," << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
   cout << "on_delete_NATION cost: " << (on_delete_NATION_sec_span + (on_delete_NATION_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_NATION" << "," << (on_delete_NATION_sec_span + (on_delete_NATION_usec_span / 1000000.0)) << endl;
   cout << "on_delete_REGION cost: " << (on_delete_REGION_sec_span + (on_delete_REGION_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_REGION" << "," << (on_delete_REGION_sec_span + (on_delete_REGION_usec_span / 1000000.0)) << endl;
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
    map<string,double>::iterator q_it26 = q.begin();
    map<string,double>::iterator q_end25 = q.end();
    for (; q_it26 != q_end25; ++q_it26)
    {
        string N__NAME = q_it26->first;
        q[N__NAME] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it12 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end11 = 
            REGION.end();
        for (; REGION_it12 != REGION_end11; ++REGION_it12)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it12);
            string protect_R__NAME = get<1>(*REGION_it12);
            string protect_R__COMMENT = get<2>(*REGION_it12);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it10 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end9 = NATION.end();
            for (; NATION_it10 != NATION_end9; ++NATION_it10)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it10);
                string protect_N__NAME = get<1>(*NATION_it10);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it10);
                string protect_N__COMMENT = get<3>(*NATION_it10);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it8 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end7 = CUSTOMER.end();
                for (; CUSTOMER_it8 != CUSTOMER_end7; ++CUSTOMER_it8)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it8);
                    string protect_C__NAME = get<1>(*CUSTOMER_it8);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it8);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it8);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it8);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it8);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it8);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it8);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it6 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end5 = SUPPLIER.end();
                    for (; SUPPLIER_it6 != SUPPLIER_end5; ++SUPPLIER_it6)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it6);
                        string protect_S__NAME = get<1>(*SUPPLIER_it6);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it6);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it6);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it6);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it6);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it6);
                        
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
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it4);
                            string protect_O__CLERK = get<6>(*ORDERS_it4);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it4);
                            string protect_O__COMMENT = get<8>(*ORDERS_it4);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it2 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end1 = LINEITEM.end();
                            for (
                                ; LINEITEM_it2 != LINEITEM_end1; ++LINEITEM_it2)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it2);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it2);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it2);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it2);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it2);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it2);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it2);
                                double protect_L__TAX = get<7>(*LINEITEM_it2);
                                int64_t var13 = 0;
                                var13 += protect_N__REGIONKEY;
                                int64_t var14 = 0;
                                var14 += protect_R__REGIONKEY;
                                if ( var13 == var14 )
                                {
                                    string var11 = 0;
                                    var11 += N__NAME;
                                    string var12 = 0;
                                    var12 += protect_N__NAME;
                                    if ( var11 == var12 )
                                    {
                                        int64_t var9 = 0;
                                        var9 += protect_S__NATIONKEY;
                                        int64_t var10 = 0;
                                        var10 += protect_N__NATIONKEY;
                                        if ( var9 == var10 )
                                        {
                                            int64_t var7 = 0;
                                            var7 += protect_C__NATIONKEY;
                                            int64_t var8 = 0;
                                            var8 += protect_S__NATIONKEY;
                                            if ( var7 == var8 )
                                            {
                                                int64_t var5 = 0;
                                                var5 += protect_C__CUSTKEY;
                                                int64_t var6 = 0;
                                                var6 += protect_O__CUSTKEY;
                                                if ( var5 == var6 )
                                                {
                                                    int64_t var3 = 0;
                                                    var3 += protect_L__SUPPKEY;
                                                    int64_t var4 = 0;
                                                    var4 += protect_S__SUPPKEY;
                                                    if ( var3 == var4 )
                                                    {
                                                        int64_t var1 = 0;
                                                        var1 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var2 = 0;
                                                        var2 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var1 == var2 )
                                                        {
                                                            q[N__NAME] += 
                                                                protect_L__EXTENDEDPRICE;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        double var15 = 1;
        double var16 = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it24 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end23 = 
            REGION.end();
        for (; REGION_it24 != REGION_end23; ++REGION_it24)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it24);
            string protect_R__NAME = get<1>(*REGION_it24);
            string protect_R__COMMENT = get<2>(*REGION_it24);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it22 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end21 = NATION.end();
            for (; NATION_it22 != NATION_end21; ++NATION_it22)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it22);
                string protect_N__NAME = get<1>(*NATION_it22);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it22);
                string protect_N__COMMENT = get<3>(*NATION_it22);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it20 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end19 = CUSTOMER.end();
                for (; CUSTOMER_it20 != CUSTOMER_end19; ++CUSTOMER_it20)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it20);
                    string protect_C__NAME = get<1>(*CUSTOMER_it20);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it20);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it20);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it20);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it20);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it20);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it20);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it18 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end17 = SUPPLIER.end();
                    for (; SUPPLIER_it18 != SUPPLIER_end17; ++SUPPLIER_it18)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it18);
                        string protect_S__NAME = get<1>(*SUPPLIER_it18);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it18);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it18);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it18);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it18);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it18);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it16 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end15 = ORDERS.end();
                        for (; ORDERS_it16 != ORDERS_end15; ++ORDERS_it16)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it16);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it16);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it16);
                            double protect_O__TOTALPRICE = get<3>(*ORDERS_it16);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it16);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it16);
                            string protect_O__CLERK = get<6>(*ORDERS_it16);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it16);
                            string protect_O__COMMENT = get<8>(*ORDERS_it16);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it14 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end13 = LINEITEM.end();
                            for (
                                ; LINEITEM_it14 != LINEITEM_end13; ++LINEITEM_it14)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it14);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it14);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it14);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it14);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it14);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it14);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it14);
                                double protect_L__TAX = get<7>(*LINEITEM_it14);
                                int64_t var30 = 0;
                                var30 += protect_N__REGIONKEY;
                                int64_t var31 = 0;
                                var31 += protect_R__REGIONKEY;
                                if ( var30 == var31 )
                                {
                                    string var28 = 0;
                                    var28 += N__NAME;
                                    string var29 = 0;
                                    var29 += protect_N__NAME;
                                    if ( var28 == var29 )
                                    {
                                        int64_t var26 = 0;
                                        var26 += protect_S__NATIONKEY;
                                        int64_t var27 = 0;
                                        var27 += protect_N__NATIONKEY;
                                        if ( var26 == var27 )
                                        {
                                            int64_t var24 = 0;
                                            var24 += protect_C__NATIONKEY;
                                            int64_t var25 = 0;
                                            var25 += protect_S__NATIONKEY;
                                            if ( var24 == var25 )
                                            {
                                                int64_t var22 = 0;
                                                var22 += protect_C__CUSTKEY;
                                                int64_t var23 = 0;
                                                var23 += protect_O__CUSTKEY;
                                                if ( var22 == var23 )
                                                {
                                                    int64_t var20 = 0;
                                                    var20 += protect_L__SUPPKEY;
                                                    int64_t var21 = 0;
                                                    var21 += protect_S__SUPPKEY;
                                                    if ( var20 == var21 )
                                                    {
                                                        int64_t var18 = 0;
                                                        var18 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var19 = 0;
                                                        var19 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var18 == var19 )
                                                        {
                                                            double var17 = 1;
                                                            var17 *= 
                                                                protect_L__EXTENDEDPRICE;
                                                            var17 *= 
                                                                protect_L__DISCOUNT;
                                                            var16 += var17;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        var15 *= var16;
        var15 *= -1;
        q[N__NAME] += var15;
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
    map<string,double>::iterator q_it52 = q.begin();
    map<string,double>::iterator q_end51 = q.end();
    for (; q_it52 != q_end51; ++q_it52)
    {
        string N__NAME = q_it52->first;
        q[N__NAME] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it38 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end37 = 
            REGION.end();
        for (; REGION_it38 != REGION_end37; ++REGION_it38)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it38);
            string protect_R__NAME = get<1>(*REGION_it38);
            string protect_R__COMMENT = get<2>(*REGION_it38);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it36 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end35 = NATION.end();
            for (; NATION_it36 != NATION_end35; ++NATION_it36)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it36);
                string protect_N__NAME = get<1>(*NATION_it36);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it36);
                string protect_N__COMMENT = get<3>(*NATION_it36);
                
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
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it32 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end31 = SUPPLIER.end();
                    for (; SUPPLIER_it32 != SUPPLIER_end31; ++SUPPLIER_it32)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it32);
                        string protect_S__NAME = get<1>(*SUPPLIER_it32);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it32);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it32);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it32);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it32);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it32);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it30 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end29 = ORDERS.end();
                        for (; ORDERS_it30 != ORDERS_end29; ++ORDERS_it30)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it30);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it30);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it30);
                            double protect_O__TOTALPRICE = get<3>(*ORDERS_it30);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it30);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it30);
                            string protect_O__CLERK = get<6>(*ORDERS_it30);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it30);
                            string protect_O__COMMENT = get<8>(*ORDERS_it30);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it28 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end27 = LINEITEM.end();
                            for (
                                ; LINEITEM_it28 != LINEITEM_end27; ++LINEITEM_it28)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it28);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it28);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it28);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it28);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it28);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it28);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it28);
                                double protect_L__TAX = get<7>(*LINEITEM_it28);
                                int64_t var44 = 0;
                                var44 += protect_N__REGIONKEY;
                                int64_t var45 = 0;
                                var45 += protect_R__REGIONKEY;
                                if ( var44 == var45 )
                                {
                                    string var42 = 0;
                                    var42 += N__NAME;
                                    string var43 = 0;
                                    var43 += protect_N__NAME;
                                    if ( var42 == var43 )
                                    {
                                        int64_t var40 = 0;
                                        var40 += protect_S__NATIONKEY;
                                        int64_t var41 = 0;
                                        var41 += protect_N__NATIONKEY;
                                        if ( var40 == var41 )
                                        {
                                            int64_t var38 = 0;
                                            var38 += protect_C__NATIONKEY;
                                            int64_t var39 = 0;
                                            var39 += protect_S__NATIONKEY;
                                            if ( var38 == var39 )
                                            {
                                                int64_t var36 = 0;
                                                var36 += protect_C__CUSTKEY;
                                                int64_t var37 = 0;
                                                var37 += protect_O__CUSTKEY;
                                                if ( var36 == var37 )
                                                {
                                                    int64_t var34 = 0;
                                                    var34 += protect_L__SUPPKEY;
                                                    int64_t var35 = 0;
                                                    var35 += protect_S__SUPPKEY;
                                                    if ( var34 == var35 )
                                                    {
                                                        int64_t var32 = 0;
                                                        var32 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var33 = 0;
                                                        var33 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var32 == var33 )
                                                        {
                                                            q[N__NAME] += 
                                                                protect_L__EXTENDEDPRICE;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        double var46 = 1;
        double var47 = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it50 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end49 = 
            REGION.end();
        for (; REGION_it50 != REGION_end49; ++REGION_it50)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it50);
            string protect_R__NAME = get<1>(*REGION_it50);
            string protect_R__COMMENT = get<2>(*REGION_it50);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it48 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end47 = NATION.end();
            for (; NATION_it48 != NATION_end47; ++NATION_it48)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it48);
                string protect_N__NAME = get<1>(*NATION_it48);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it48);
                string protect_N__COMMENT = get<3>(*NATION_it48);
                
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
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it44 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end43 = SUPPLIER.end();
                    for (; SUPPLIER_it44 != SUPPLIER_end43; ++SUPPLIER_it44)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it44);
                        string protect_S__NAME = get<1>(*SUPPLIER_it44);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it44);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it44);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it44);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it44);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it44);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it42 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end41 = ORDERS.end();
                        for (; ORDERS_it42 != ORDERS_end41; ++ORDERS_it42)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it42);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it42);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it42);
                            double protect_O__TOTALPRICE = get<3>(*ORDERS_it42);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it42);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it42);
                            string protect_O__CLERK = get<6>(*ORDERS_it42);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it42);
                            string protect_O__COMMENT = get<8>(*ORDERS_it42);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it40 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end39 = LINEITEM.end();
                            for (
                                ; LINEITEM_it40 != LINEITEM_end39; ++LINEITEM_it40)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it40);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it40);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it40);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it40);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it40);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it40);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it40);
                                double protect_L__TAX = get<7>(*LINEITEM_it40);
                                int64_t var61 = 0;
                                var61 += protect_N__REGIONKEY;
                                int64_t var62 = 0;
                                var62 += protect_R__REGIONKEY;
                                if ( var61 == var62 )
                                {
                                    string var59 = 0;
                                    var59 += N__NAME;
                                    string var60 = 0;
                                    var60 += protect_N__NAME;
                                    if ( var59 == var60 )
                                    {
                                        int64_t var57 = 0;
                                        var57 += protect_S__NATIONKEY;
                                        int64_t var58 = 0;
                                        var58 += protect_N__NATIONKEY;
                                        if ( var57 == var58 )
                                        {
                                            int64_t var55 = 0;
                                            var55 += protect_C__NATIONKEY;
                                            int64_t var56 = 0;
                                            var56 += protect_S__NATIONKEY;
                                            if ( var55 == var56 )
                                            {
                                                int64_t var53 = 0;
                                                var53 += protect_C__CUSTKEY;
                                                int64_t var54 = 0;
                                                var54 += protect_O__CUSTKEY;
                                                if ( var53 == var54 )
                                                {
                                                    int64_t var51 = 0;
                                                    var51 += protect_L__SUPPKEY;
                                                    int64_t var52 = 0;
                                                    var52 += protect_S__SUPPKEY;
                                                    if ( var51 == var52 )
                                                    {
                                                        int64_t var49 = 0;
                                                        var49 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var50 = 0;
                                                        var50 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var49 == var50 )
                                                        {
                                                            double var48 = 1;
                                                            var48 *= 
                                                                protect_L__EXTENDEDPRICE;
                                                            var48 *= 
                                                                protect_L__DISCOUNT;
                                                            var47 += var48;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        var46 *= var47;
        var46 *= -1;
        q[N__NAME] += var46;
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_ORDERS_sec_span, on_insert_ORDERS_usec_span);
}

void on_insert_SUPPLIER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t SUPPKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    SUPPLIER.insert(make_tuple(
        SUPPKEY,NAME,ADDRESS,NATIONKEY,PHONE,ACCTBAL,COMMENT));
    map<string,double>::iterator q_it78 = q.begin();
    map<string,double>::iterator q_end77 = q.end();
    for (; q_it78 != q_end77; ++q_it78)
    {
        string N__NAME = q_it78->first;
        q[N__NAME] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it64 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end63 = 
            REGION.end();
        for (; REGION_it64 != REGION_end63; ++REGION_it64)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it64);
            string protect_R__NAME = get<1>(*REGION_it64);
            string protect_R__COMMENT = get<2>(*REGION_it64);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it62 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end61 = NATION.end();
            for (; NATION_it62 != NATION_end61; ++NATION_it62)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it62);
                string protect_N__NAME = get<1>(*NATION_it62);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it62);
                string protect_N__COMMENT = get<3>(*NATION_it62);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it60 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end59 = CUSTOMER.end();
                for (; CUSTOMER_it60 != CUSTOMER_end59; ++CUSTOMER_it60)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it60);
                    string protect_C__NAME = get<1>(*CUSTOMER_it60);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it60);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it60);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it60);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it60);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it60);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it60);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it58 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end57 = SUPPLIER.end();
                    for (; SUPPLIER_it58 != SUPPLIER_end57; ++SUPPLIER_it58)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it58);
                        string protect_S__NAME = get<1>(*SUPPLIER_it58);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it58);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it58);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it58);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it58);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it58);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it56 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end55 = ORDERS.end();
                        for (; ORDERS_it56 != ORDERS_end55; ++ORDERS_it56)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it56);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it56);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it56);
                            double protect_O__TOTALPRICE = get<3>(*ORDERS_it56);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it56);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it56);
                            string protect_O__CLERK = get<6>(*ORDERS_it56);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it56);
                            string protect_O__COMMENT = get<8>(*ORDERS_it56);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it54 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end53 = LINEITEM.end();
                            for (
                                ; LINEITEM_it54 != LINEITEM_end53; ++LINEITEM_it54)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it54);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it54);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it54);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it54);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it54);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it54);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it54);
                                double protect_L__TAX = get<7>(*LINEITEM_it54);
                                int64_t var75 = 0;
                                var75 += protect_N__REGIONKEY;
                                int64_t var76 = 0;
                                var76 += protect_R__REGIONKEY;
                                if ( var75 == var76 )
                                {
                                    string var73 = 0;
                                    var73 += N__NAME;
                                    string var74 = 0;
                                    var74 += protect_N__NAME;
                                    if ( var73 == var74 )
                                    {
                                        int64_t var71 = 0;
                                        var71 += protect_S__NATIONKEY;
                                        int64_t var72 = 0;
                                        var72 += protect_N__NATIONKEY;
                                        if ( var71 == var72 )
                                        {
                                            int64_t var69 = 0;
                                            var69 += protect_C__NATIONKEY;
                                            int64_t var70 = 0;
                                            var70 += protect_S__NATIONKEY;
                                            if ( var69 == var70 )
                                            {
                                                int64_t var67 = 0;
                                                var67 += protect_C__CUSTKEY;
                                                int64_t var68 = 0;
                                                var68 += protect_O__CUSTKEY;
                                                if ( var67 == var68 )
                                                {
                                                    int64_t var65 = 0;
                                                    var65 += protect_L__SUPPKEY;
                                                    int64_t var66 = 0;
                                                    var66 += protect_S__SUPPKEY;
                                                    if ( var65 == var66 )
                                                    {
                                                        int64_t var63 = 0;
                                                        var63 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var64 = 0;
                                                        var64 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var63 == var64 )
                                                        {
                                                            q[N__NAME] += 
                                                                protect_L__EXTENDEDPRICE;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        double var77 = 1;
        double var78 = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it76 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end75 = 
            REGION.end();
        for (; REGION_it76 != REGION_end75; ++REGION_it76)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it76);
            string protect_R__NAME = get<1>(*REGION_it76);
            string protect_R__COMMENT = get<2>(*REGION_it76);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it74 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end73 = NATION.end();
            for (; NATION_it74 != NATION_end73; ++NATION_it74)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it74);
                string protect_N__NAME = get<1>(*NATION_it74);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it74);
                string protect_N__COMMENT = get<3>(*NATION_it74);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it72 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end71 = CUSTOMER.end();
                for (; CUSTOMER_it72 != CUSTOMER_end71; ++CUSTOMER_it72)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it72);
                    string protect_C__NAME = get<1>(*CUSTOMER_it72);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it72);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it72);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it72);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it72);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it72);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it72);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it70 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end69 = SUPPLIER.end();
                    for (; SUPPLIER_it70 != SUPPLIER_end69; ++SUPPLIER_it70)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it70);
                        string protect_S__NAME = get<1>(*SUPPLIER_it70);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it70);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it70);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it70);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it70);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it70);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it68 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end67 = ORDERS.end();
                        for (; ORDERS_it68 != ORDERS_end67; ++ORDERS_it68)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it68);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it68);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it68);
                            double protect_O__TOTALPRICE = get<3>(*ORDERS_it68);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it68);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it68);
                            string protect_O__CLERK = get<6>(*ORDERS_it68);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it68);
                            string protect_O__COMMENT = get<8>(*ORDERS_it68);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it66 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end65 = LINEITEM.end();
                            for (
                                ; LINEITEM_it66 != LINEITEM_end65; ++LINEITEM_it66)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it66);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it66);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it66);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it66);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it66);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it66);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it66);
                                double protect_L__TAX = get<7>(*LINEITEM_it66);
                                int64_t var92 = 0;
                                var92 += protect_N__REGIONKEY;
                                int64_t var93 = 0;
                                var93 += protect_R__REGIONKEY;
                                if ( var92 == var93 )
                                {
                                    string var90 = 0;
                                    var90 += N__NAME;
                                    string var91 = 0;
                                    var91 += protect_N__NAME;
                                    if ( var90 == var91 )
                                    {
                                        int64_t var88 = 0;
                                        var88 += protect_S__NATIONKEY;
                                        int64_t var89 = 0;
                                        var89 += protect_N__NATIONKEY;
                                        if ( var88 == var89 )
                                        {
                                            int64_t var86 = 0;
                                            var86 += protect_C__NATIONKEY;
                                            int64_t var87 = 0;
                                            var87 += protect_S__NATIONKEY;
                                            if ( var86 == var87 )
                                            {
                                                int64_t var84 = 0;
                                                var84 += protect_C__CUSTKEY;
                                                int64_t var85 = 0;
                                                var85 += protect_O__CUSTKEY;
                                                if ( var84 == var85 )
                                                {
                                                    int64_t var82 = 0;
                                                    var82 += protect_L__SUPPKEY;
                                                    int64_t var83 = 0;
                                                    var83 += protect_S__SUPPKEY;
                                                    if ( var82 == var83 )
                                                    {
                                                        int64_t var80 = 0;
                                                        var80 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var81 = 0;
                                                        var81 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var80 == var81 )
                                                        {
                                                            double var79 = 1;
                                                            var79 *= 
                                                                protect_L__EXTENDEDPRICE;
                                                            var79 *= 
                                                                protect_L__DISCOUNT;
                                                            var78 += var79;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        var77 *= var78;
        var77 *= -1;
        q[N__NAME] += var77;
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_SUPPLIER_sec_span, on_insert_SUPPLIER_usec_span);
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
    map<string,double>::iterator q_it104 = q.begin();
    map<string,double>::iterator q_end103 = q.end();
    for (; q_it104 != q_end103; ++q_it104)
    {
        string N__NAME = q_it104->first;
        q[N__NAME] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it90 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end89 = 
            REGION.end();
        for (; REGION_it90 != REGION_end89; ++REGION_it90)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it90);
            string protect_R__NAME = get<1>(*REGION_it90);
            string protect_R__COMMENT = get<2>(*REGION_it90);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it88 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end87 = NATION.end();
            for (; NATION_it88 != NATION_end87; ++NATION_it88)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it88);
                string protect_N__NAME = get<1>(*NATION_it88);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it88);
                string protect_N__COMMENT = get<3>(*NATION_it88);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it86 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end85 = CUSTOMER.end();
                for (; CUSTOMER_it86 != CUSTOMER_end85; ++CUSTOMER_it86)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it86);
                    string protect_C__NAME = get<1>(*CUSTOMER_it86);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it86);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it86);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it86);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it86);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it86);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it86);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it84 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end83 = SUPPLIER.end();
                    for (; SUPPLIER_it84 != SUPPLIER_end83; ++SUPPLIER_it84)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it84);
                        string protect_S__NAME = get<1>(*SUPPLIER_it84);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it84);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it84);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it84);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it84);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it84);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it82 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end81 = ORDERS.end();
                        for (; ORDERS_it82 != ORDERS_end81; ++ORDERS_it82)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it82);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it82);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it82);
                            double protect_O__TOTALPRICE = get<3>(*ORDERS_it82);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it82);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it82);
                            string protect_O__CLERK = get<6>(*ORDERS_it82);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it82);
                            string protect_O__COMMENT = get<8>(*ORDERS_it82);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it80 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end79 = LINEITEM.end();
                            for (
                                ; LINEITEM_it80 != LINEITEM_end79; ++LINEITEM_it80)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it80);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it80);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it80);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it80);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it80);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it80);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it80);
                                double protect_L__TAX = get<7>(*LINEITEM_it80);
                                int64_t var106 = 0;
                                var106 += protect_N__REGIONKEY;
                                int64_t var107 = 0;
                                var107 += protect_R__REGIONKEY;
                                if ( var106 == var107 )
                                {
                                    string var104 = 0;
                                    var104 += N__NAME;
                                    string var105 = 0;
                                    var105 += protect_N__NAME;
                                    if ( var104 == var105 )
                                    {
                                        int64_t var102 = 0;
                                        var102 += protect_S__NATIONKEY;
                                        int64_t var103 = 0;
                                        var103 += protect_N__NATIONKEY;
                                        if ( var102 == var103 )
                                        {
                                            int64_t var100 = 0;
                                            var100 += protect_C__NATIONKEY;
                                            int64_t var101 = 0;
                                            var101 += protect_S__NATIONKEY;
                                            if ( var100 == var101 )
                                            {
                                                int64_t var98 = 0;
                                                var98 += protect_C__CUSTKEY;
                                                int64_t var99 = 0;
                                                var99 += protect_O__CUSTKEY;
                                                if ( var98 == var99 )
                                                {
                                                    int64_t var96 = 0;
                                                    var96 += protect_L__SUPPKEY;
                                                    int64_t var97 = 0;
                                                    var97 += protect_S__SUPPKEY;
                                                    if ( var96 == var97 )
                                                    {
                                                        int64_t var94 = 0;
                                                        var94 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var95 = 0;
                                                        var95 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var94 == var95 )
                                                        {
                                                            q[N__NAME] += 
                                                                protect_L__EXTENDEDPRICE;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        double var108 = 1;
        double var109 = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it102 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end101 = 
            REGION.end();
        for (; REGION_it102 != REGION_end101; ++REGION_it102)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it102);
            string protect_R__NAME = get<1>(*REGION_it102);
            string protect_R__COMMENT = get<2>(*REGION_it102);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it100 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end99 = NATION.end();
            for (; NATION_it100 != NATION_end99; ++NATION_it100)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it100);
                string protect_N__NAME = get<1>(*NATION_it100);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it100);
                string protect_N__COMMENT = get<3>(*NATION_it100);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it98 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end97 = CUSTOMER.end();
                for (; CUSTOMER_it98 != CUSTOMER_end97; ++CUSTOMER_it98)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it98);
                    string protect_C__NAME = get<1>(*CUSTOMER_it98);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it98);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it98);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it98);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it98);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it98);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it98);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it96 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end95 = SUPPLIER.end();
                    for (; SUPPLIER_it96 != SUPPLIER_end95; ++SUPPLIER_it96)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it96);
                        string protect_S__NAME = get<1>(*SUPPLIER_it96);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it96);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it96);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it96);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it96);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it96);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it94 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end93 = ORDERS.end();
                        for (; ORDERS_it94 != ORDERS_end93; ++ORDERS_it94)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it94);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it94);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it94);
                            double protect_O__TOTALPRICE = get<3>(*ORDERS_it94);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it94);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it94);
                            string protect_O__CLERK = get<6>(*ORDERS_it94);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it94);
                            string protect_O__COMMENT = get<8>(*ORDERS_it94);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it92 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end91 = LINEITEM.end();
                            for (
                                ; LINEITEM_it92 != LINEITEM_end91; ++LINEITEM_it92)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it92);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it92);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it92);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it92);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it92);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it92);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it92);
                                double protect_L__TAX = get<7>(*LINEITEM_it92);
                                int64_t var123 = 0;
                                var123 += protect_N__REGIONKEY;
                                int64_t var124 = 0;
                                var124 += protect_R__REGIONKEY;
                                if ( var123 == var124 )
                                {
                                    string var121 = 0;
                                    var121 += N__NAME;
                                    string var122 = 0;
                                    var122 += protect_N__NAME;
                                    if ( var121 == var122 )
                                    {
                                        int64_t var119 = 0;
                                        var119 += protect_S__NATIONKEY;
                                        int64_t var120 = 0;
                                        var120 += protect_N__NATIONKEY;
                                        if ( var119 == var120 )
                                        {
                                            int64_t var117 = 0;
                                            var117 += protect_C__NATIONKEY;
                                            int64_t var118 = 0;
                                            var118 += protect_S__NATIONKEY;
                                            if ( var117 == var118 )
                                            {
                                                int64_t var115 = 0;
                                                var115 += protect_C__CUSTKEY;
                                                int64_t var116 = 0;
                                                var116 += protect_O__CUSTKEY;
                                                if ( var115 == var116 )
                                                {
                                                    int64_t var113 = 0;
                                                    var113 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var114 = 0;
                                                    var114 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var113 == var114 )
                                                    {
                                                        int64_t var111 = 0;
                                                        var111 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var112 = 0;
                                                        var112 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var111 == var112 )
                                                        {
                                                            double var110 = 1;
                                                            var110 *= 
                                                                protect_L__EXTENDEDPRICE;
                                                            var110 *= 
                                                                protect_L__DISCOUNT;
                                                            var109 += var110;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        var108 *= var109;
        var108 *= -1;
        q[N__NAME] += var108;
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_CUSTOMER_sec_span, on_insert_CUSTOMER_usec_span);
}

void on_insert_NATION(
    ofstream* results, ofstream* log, ofstream* stats, int64_t NATIONKEY,string 
    NAME,int64_t REGIONKEY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    NATION.insert(make_tuple(NATIONKEY,NAME,REGIONKEY,COMMENT));
    map<string,double>::iterator q_it130 = q.begin();
    map<string,double>::iterator q_end129 = q.end();
    for (; q_it130 != q_end129; ++q_it130)
    {
        string NAME = q_it130->first;
        q[NAME] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it116 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end115 = 
            REGION.end();
        for (; REGION_it116 != REGION_end115; ++REGION_it116)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it116);
            string protect_R__NAME = get<1>(*REGION_it116);
            string protect_R__COMMENT = get<2>(*REGION_it116);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it114 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end113 = NATION.end();
            for (; NATION_it114 != NATION_end113; ++NATION_it114)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it114);
                string protect_N__NAME = get<1>(*NATION_it114);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it114);
                string protect_N__COMMENT = get<3>(*NATION_it114);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it112 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end111 = CUSTOMER.end();
                for (; CUSTOMER_it112 != CUSTOMER_end111; ++CUSTOMER_it112)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it112);
                    string protect_C__NAME = get<1>(*CUSTOMER_it112);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it112);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it112);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it112);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it112);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it112);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it112);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it110 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end109 = SUPPLIER.end();
                    for (; SUPPLIER_it110 != SUPPLIER_end109; ++SUPPLIER_it110)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it110);
                        string protect_S__NAME = get<1>(*SUPPLIER_it110);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it110);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it110);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it110);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it110);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it110);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it108 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end107 = ORDERS.end();
                        for (; ORDERS_it108 != ORDERS_end107; ++ORDERS_it108)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it108);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it108);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it108);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it108);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it108);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it108);
                            string protect_O__CLERK = get<6>(*ORDERS_it108);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it108);
                            string protect_O__COMMENT = get<8>(*ORDERS_it108);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it106 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end105 = LINEITEM.end();
                            for (
                                ; LINEITEM_it106 != LINEITEM_end105; ++LINEITEM_it106)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it106);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it106);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it106);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it106);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it106);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it106);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it106);
                                double protect_L__TAX = get<7>(*LINEITEM_it106);
                                int64_t var137 = 0;
                                var137 += protect_N__REGIONKEY;
                                int64_t var138 = 0;
                                var138 += protect_R__REGIONKEY;
                                if ( var137 == var138 )
                                {
                                    string var135 = 0;
                                    var135 += NAME;
                                    string var136 = 0;
                                    var136 += protect_N__NAME;
                                    if ( var135 == var136 )
                                    {
                                        int64_t var133 = 0;
                                        var133 += protect_S__NATIONKEY;
                                        int64_t var134 = 0;
                                        var134 += protect_N__NATIONKEY;
                                        if ( var133 == var134 )
                                        {
                                            int64_t var131 = 0;
                                            var131 += protect_C__NATIONKEY;
                                            int64_t var132 = 0;
                                            var132 += protect_S__NATIONKEY;
                                            if ( var131 == var132 )
                                            {
                                                int64_t var129 = 0;
                                                var129 += protect_C__CUSTKEY;
                                                int64_t var130 = 0;
                                                var130 += protect_O__CUSTKEY;
                                                if ( var129 == var130 )
                                                {
                                                    int64_t var127 = 0;
                                                    var127 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var128 = 0;
                                                    var128 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var127 == var128 )
                                                    {
                                                        int64_t var125 = 0;
                                                        var125 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var126 = 0;
                                                        var126 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var125 == var126 )
                                                        {
                                                            q[NAME] += 
                                                                protect_L__EXTENDEDPRICE;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        double var139 = 1;
        double var140 = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it128 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end127 = 
            REGION.end();
        for (; REGION_it128 != REGION_end127; ++REGION_it128)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it128);
            string protect_R__NAME = get<1>(*REGION_it128);
            string protect_R__COMMENT = get<2>(*REGION_it128);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it126 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end125 = NATION.end();
            for (; NATION_it126 != NATION_end125; ++NATION_it126)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it126);
                string protect_N__NAME = get<1>(*NATION_it126);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it126);
                string protect_N__COMMENT = get<3>(*NATION_it126);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it124 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end123 = CUSTOMER.end();
                for (; CUSTOMER_it124 != CUSTOMER_end123; ++CUSTOMER_it124)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it124);
                    string protect_C__NAME = get<1>(*CUSTOMER_it124);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it124);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it124);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it124);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it124);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it124);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it124);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it122 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end121 = SUPPLIER.end();
                    for (; SUPPLIER_it122 != SUPPLIER_end121; ++SUPPLIER_it122)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it122);
                        string protect_S__NAME = get<1>(*SUPPLIER_it122);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it122);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it122);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it122);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it122);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it122);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it120 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end119 = ORDERS.end();
                        for (; ORDERS_it120 != ORDERS_end119; ++ORDERS_it120)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it120);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it120);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it120);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it120);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it120);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it120);
                            string protect_O__CLERK = get<6>(*ORDERS_it120);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it120);
                            string protect_O__COMMENT = get<8>(*ORDERS_it120);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it118 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end117 = LINEITEM.end();
                            for (
                                ; LINEITEM_it118 != LINEITEM_end117; ++LINEITEM_it118)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it118);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it118);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it118);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it118);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it118);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it118);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it118);
                                double protect_L__TAX = get<7>(*LINEITEM_it118);
                                int64_t var154 = 0;
                                var154 += protect_N__REGIONKEY;
                                int64_t var155 = 0;
                                var155 += protect_R__REGIONKEY;
                                if ( var154 == var155 )
                                {
                                    string var152 = 0;
                                    var152 += NAME;
                                    string var153 = 0;
                                    var153 += protect_N__NAME;
                                    if ( var152 == var153 )
                                    {
                                        int64_t var150 = 0;
                                        var150 += protect_S__NATIONKEY;
                                        int64_t var151 = 0;
                                        var151 += protect_N__NATIONKEY;
                                        if ( var150 == var151 )
                                        {
                                            int64_t var148 = 0;
                                            var148 += protect_C__NATIONKEY;
                                            int64_t var149 = 0;
                                            var149 += protect_S__NATIONKEY;
                                            if ( var148 == var149 )
                                            {
                                                int64_t var146 = 0;
                                                var146 += protect_C__CUSTKEY;
                                                int64_t var147 = 0;
                                                var147 += protect_O__CUSTKEY;
                                                if ( var146 == var147 )
                                                {
                                                    int64_t var144 = 0;
                                                    var144 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var145 = 0;
                                                    var145 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var144 == var145 )
                                                    {
                                                        int64_t var142 = 0;
                                                        var142 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var143 = 0;
                                                        var143 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var142 == var143 )
                                                        {
                                                            double var141 = 1;
                                                            var141 *= 
                                                                protect_L__EXTENDEDPRICE;
                                                            var141 *= 
                                                                protect_L__DISCOUNT;
                                                            var140 += var141;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        var139 *= var140;
        var139 *= -1;
        q[NAME] += var139;
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_NATION_sec_span, on_insert_NATION_usec_span);
}

void on_insert_REGION(
    ofstream* results, ofstream* log, ofstream* stats, int64_t REGIONKEY,string 
    NAME,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    REGION.insert(make_tuple(REGIONKEY,NAME,COMMENT));
    map<string,double>::iterator q_it156 = q.begin();
    map<string,double>::iterator q_end155 = q.end();
    for (; q_it156 != q_end155; ++q_it156)
    {
        string N__NAME = q_it156->first;
        q[N__NAME] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it142 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end141 = 
            REGION.end();
        for (; REGION_it142 != REGION_end141; ++REGION_it142)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it142);
            string protect_R__NAME = get<1>(*REGION_it142);
            string protect_R__COMMENT = get<2>(*REGION_it142);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it140 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end139 = NATION.end();
            for (; NATION_it140 != NATION_end139; ++NATION_it140)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it140);
                string protect_N__NAME = get<1>(*NATION_it140);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it140);
                string protect_N__COMMENT = get<3>(*NATION_it140);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it138 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end137 = CUSTOMER.end();
                for (; CUSTOMER_it138 != CUSTOMER_end137; ++CUSTOMER_it138)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it138);
                    string protect_C__NAME = get<1>(*CUSTOMER_it138);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it138);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it138);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it138);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it138);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it138);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it138);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it136 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end135 = SUPPLIER.end();
                    for (; SUPPLIER_it136 != SUPPLIER_end135; ++SUPPLIER_it136)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it136);
                        string protect_S__NAME = get<1>(*SUPPLIER_it136);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it136);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it136);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it136);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it136);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it136);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it134 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end133 = ORDERS.end();
                        for (; ORDERS_it134 != ORDERS_end133; ++ORDERS_it134)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it134);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it134);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it134);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it134);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it134);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it134);
                            string protect_O__CLERK = get<6>(*ORDERS_it134);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it134);
                            string protect_O__COMMENT = get<8>(*ORDERS_it134);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it132 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end131 = LINEITEM.end();
                            for (
                                ; LINEITEM_it132 != LINEITEM_end131; ++LINEITEM_it132)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it132);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it132);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it132);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it132);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it132);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it132);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it132);
                                double protect_L__TAX = get<7>(*LINEITEM_it132);
                                int64_t var168 = 0;
                                var168 += protect_N__REGIONKEY;
                                int64_t var169 = 0;
                                var169 += protect_R__REGIONKEY;
                                if ( var168 == var169 )
                                {
                                    string var166 = 0;
                                    var166 += N__NAME;
                                    string var167 = 0;
                                    var167 += protect_N__NAME;
                                    if ( var166 == var167 )
                                    {
                                        int64_t var164 = 0;
                                        var164 += protect_S__NATIONKEY;
                                        int64_t var165 = 0;
                                        var165 += protect_N__NATIONKEY;
                                        if ( var164 == var165 )
                                        {
                                            int64_t var162 = 0;
                                            var162 += protect_C__NATIONKEY;
                                            int64_t var163 = 0;
                                            var163 += protect_S__NATIONKEY;
                                            if ( var162 == var163 )
                                            {
                                                int64_t var160 = 0;
                                                var160 += protect_C__CUSTKEY;
                                                int64_t var161 = 0;
                                                var161 += protect_O__CUSTKEY;
                                                if ( var160 == var161 )
                                                {
                                                    int64_t var158 = 0;
                                                    var158 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var159 = 0;
                                                    var159 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var158 == var159 )
                                                    {
                                                        int64_t var156 = 0;
                                                        var156 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var157 = 0;
                                                        var157 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var156 == var157 )
                                                        {
                                                            q[N__NAME] += 
                                                                protect_L__EXTENDEDPRICE;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        double var170 = 1;
        double var171 = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it154 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end153 = 
            REGION.end();
        for (; REGION_it154 != REGION_end153; ++REGION_it154)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it154);
            string protect_R__NAME = get<1>(*REGION_it154);
            string protect_R__COMMENT = get<2>(*REGION_it154);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it152 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end151 = NATION.end();
            for (; NATION_it152 != NATION_end151; ++NATION_it152)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it152);
                string protect_N__NAME = get<1>(*NATION_it152);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it152);
                string protect_N__COMMENT = get<3>(*NATION_it152);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it150 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end149 = CUSTOMER.end();
                for (; CUSTOMER_it150 != CUSTOMER_end149; ++CUSTOMER_it150)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it150);
                    string protect_C__NAME = get<1>(*CUSTOMER_it150);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it150);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it150);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it150);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it150);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it150);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it150);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it148 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end147 = SUPPLIER.end();
                    for (; SUPPLIER_it148 != SUPPLIER_end147; ++SUPPLIER_it148)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it148);
                        string protect_S__NAME = get<1>(*SUPPLIER_it148);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it148);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it148);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it148);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it148);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it148);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it146 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end145 = ORDERS.end();
                        for (; ORDERS_it146 != ORDERS_end145; ++ORDERS_it146)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it146);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it146);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it146);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it146);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it146);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it146);
                            string protect_O__CLERK = get<6>(*ORDERS_it146);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it146);
                            string protect_O__COMMENT = get<8>(*ORDERS_it146);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it144 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end143 = LINEITEM.end();
                            for (
                                ; LINEITEM_it144 != LINEITEM_end143; ++LINEITEM_it144)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it144);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it144);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it144);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it144);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it144);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it144);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it144);
                                double protect_L__TAX = get<7>(*LINEITEM_it144);
                                int64_t var185 = 0;
                                var185 += protect_N__REGIONKEY;
                                int64_t var186 = 0;
                                var186 += protect_R__REGIONKEY;
                                if ( var185 == var186 )
                                {
                                    string var183 = 0;
                                    var183 += N__NAME;
                                    string var184 = 0;
                                    var184 += protect_N__NAME;
                                    if ( var183 == var184 )
                                    {
                                        int64_t var181 = 0;
                                        var181 += protect_S__NATIONKEY;
                                        int64_t var182 = 0;
                                        var182 += protect_N__NATIONKEY;
                                        if ( var181 == var182 )
                                        {
                                            int64_t var179 = 0;
                                            var179 += protect_C__NATIONKEY;
                                            int64_t var180 = 0;
                                            var180 += protect_S__NATIONKEY;
                                            if ( var179 == var180 )
                                            {
                                                int64_t var177 = 0;
                                                var177 += protect_C__CUSTKEY;
                                                int64_t var178 = 0;
                                                var178 += protect_O__CUSTKEY;
                                                if ( var177 == var178 )
                                                {
                                                    int64_t var175 = 0;
                                                    var175 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var176 = 0;
                                                    var176 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var175 == var176 )
                                                    {
                                                        int64_t var173 = 0;
                                                        var173 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var174 = 0;
                                                        var174 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var173 == var174 )
                                                        {
                                                            double var172 = 1;
                                                            var172 *= 
                                                                protect_L__EXTENDEDPRICE;
                                                            var172 *= 
                                                                protect_L__DISCOUNT;
                                                            var171 += var172;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        var170 *= var171;
        var170 *= -1;
        q[N__NAME] += var170;
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_REGION_sec_span, on_insert_REGION_usec_span);
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
    map<string,double>::iterator q_it182 = q.begin();
    map<string,double>::iterator q_end181 = q.end();
    for (; q_it182 != q_end181; ++q_it182)
    {
        string N__NAME = q_it182->first;
        q[N__NAME] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it168 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end167 = 
            REGION.end();
        for (; REGION_it168 != REGION_end167; ++REGION_it168)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it168);
            string protect_R__NAME = get<1>(*REGION_it168);
            string protect_R__COMMENT = get<2>(*REGION_it168);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it166 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end165 = NATION.end();
            for (; NATION_it166 != NATION_end165; ++NATION_it166)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it166);
                string protect_N__NAME = get<1>(*NATION_it166);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it166);
                string protect_N__COMMENT = get<3>(*NATION_it166);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it164 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end163 = CUSTOMER.end();
                for (; CUSTOMER_it164 != CUSTOMER_end163; ++CUSTOMER_it164)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it164);
                    string protect_C__NAME = get<1>(*CUSTOMER_it164);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it164);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it164);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it164);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it164);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it164);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it164);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it162 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end161 = SUPPLIER.end();
                    for (; SUPPLIER_it162 != SUPPLIER_end161; ++SUPPLIER_it162)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it162);
                        string protect_S__NAME = get<1>(*SUPPLIER_it162);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it162);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it162);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it162);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it162);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it162);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it160 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end159 = ORDERS.end();
                        for (; ORDERS_it160 != ORDERS_end159; ++ORDERS_it160)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it160);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it160);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it160);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it160);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it160);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it160);
                            string protect_O__CLERK = get<6>(*ORDERS_it160);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it160);
                            string protect_O__COMMENT = get<8>(*ORDERS_it160);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it158 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end157 = LINEITEM.end();
                            for (
                                ; LINEITEM_it158 != LINEITEM_end157; ++LINEITEM_it158)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it158);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it158);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it158);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it158);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it158);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it158);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it158);
                                double protect_L__TAX = get<7>(*LINEITEM_it158);
                                int64_t var199 = 0;
                                var199 += protect_N__REGIONKEY;
                                int64_t var200 = 0;
                                var200 += protect_R__REGIONKEY;
                                if ( var199 == var200 )
                                {
                                    string var197 = 0;
                                    var197 += N__NAME;
                                    string var198 = 0;
                                    var198 += protect_N__NAME;
                                    if ( var197 == var198 )
                                    {
                                        int64_t var195 = 0;
                                        var195 += protect_S__NATIONKEY;
                                        int64_t var196 = 0;
                                        var196 += protect_N__NATIONKEY;
                                        if ( var195 == var196 )
                                        {
                                            int64_t var193 = 0;
                                            var193 += protect_C__NATIONKEY;
                                            int64_t var194 = 0;
                                            var194 += protect_S__NATIONKEY;
                                            if ( var193 == var194 )
                                            {
                                                int64_t var191 = 0;
                                                var191 += protect_C__CUSTKEY;
                                                int64_t var192 = 0;
                                                var192 += protect_O__CUSTKEY;
                                                if ( var191 == var192 )
                                                {
                                                    int64_t var189 = 0;
                                                    var189 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var190 = 0;
                                                    var190 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var189 == var190 )
                                                    {
                                                        int64_t var187 = 0;
                                                        var187 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var188 = 0;
                                                        var188 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var187 == var188 )
                                                        {
                                                            q[N__NAME] += 
                                                                protect_L__EXTENDEDPRICE;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        double var201 = 1;
        double var202 = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it180 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end179 = 
            REGION.end();
        for (; REGION_it180 != REGION_end179; ++REGION_it180)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it180);
            string protect_R__NAME = get<1>(*REGION_it180);
            string protect_R__COMMENT = get<2>(*REGION_it180);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it178 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end177 = NATION.end();
            for (; NATION_it178 != NATION_end177; ++NATION_it178)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it178);
                string protect_N__NAME = get<1>(*NATION_it178);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it178);
                string protect_N__COMMENT = get<3>(*NATION_it178);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it176 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end175 = CUSTOMER.end();
                for (; CUSTOMER_it176 != CUSTOMER_end175; ++CUSTOMER_it176)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it176);
                    string protect_C__NAME = get<1>(*CUSTOMER_it176);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it176);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it176);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it176);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it176);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it176);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it176);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it174 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end173 = SUPPLIER.end();
                    for (; SUPPLIER_it174 != SUPPLIER_end173; ++SUPPLIER_it174)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it174);
                        string protect_S__NAME = get<1>(*SUPPLIER_it174);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it174);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it174);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it174);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it174);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it174);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it172 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end171 = ORDERS.end();
                        for (; ORDERS_it172 != ORDERS_end171; ++ORDERS_it172)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it172);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it172);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it172);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it172);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it172);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it172);
                            string protect_O__CLERK = get<6>(*ORDERS_it172);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it172);
                            string protect_O__COMMENT = get<8>(*ORDERS_it172);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it170 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end169 = LINEITEM.end();
                            for (
                                ; LINEITEM_it170 != LINEITEM_end169; ++LINEITEM_it170)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it170);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it170);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it170);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it170);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it170);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it170);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it170);
                                double protect_L__TAX = get<7>(*LINEITEM_it170);
                                int64_t var216 = 0;
                                var216 += protect_N__REGIONKEY;
                                int64_t var217 = 0;
                                var217 += protect_R__REGIONKEY;
                                if ( var216 == var217 )
                                {
                                    string var214 = 0;
                                    var214 += N__NAME;
                                    string var215 = 0;
                                    var215 += protect_N__NAME;
                                    if ( var214 == var215 )
                                    {
                                        int64_t var212 = 0;
                                        var212 += protect_S__NATIONKEY;
                                        int64_t var213 = 0;
                                        var213 += protect_N__NATIONKEY;
                                        if ( var212 == var213 )
                                        {
                                            int64_t var210 = 0;
                                            var210 += protect_C__NATIONKEY;
                                            int64_t var211 = 0;
                                            var211 += protect_S__NATIONKEY;
                                            if ( var210 == var211 )
                                            {
                                                int64_t var208 = 0;
                                                var208 += protect_C__CUSTKEY;
                                                int64_t var209 = 0;
                                                var209 += protect_O__CUSTKEY;
                                                if ( var208 == var209 )
                                                {
                                                    int64_t var206 = 0;
                                                    var206 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var207 = 0;
                                                    var207 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var206 == var207 )
                                                    {
                                                        int64_t var204 = 0;
                                                        var204 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var205 = 0;
                                                        var205 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var204 == var205 )
                                                        {
                                                            double var203 = 1;
                                                            var203 *= 
                                                                protect_L__EXTENDEDPRICE;
                                                            var203 *= 
                                                                protect_L__DISCOUNT;
                                                            var202 += var203;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        var201 *= var202;
        var201 *= -1;
        q[N__NAME] += var201;
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
    map<string,double>::iterator q_it208 = q.begin();
    map<string,double>::iterator q_end207 = q.end();
    for (; q_it208 != q_end207; ++q_it208)
    {
        string N__NAME = q_it208->first;
        q[N__NAME] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it194 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end193 = 
            REGION.end();
        for (; REGION_it194 != REGION_end193; ++REGION_it194)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it194);
            string protect_R__NAME = get<1>(*REGION_it194);
            string protect_R__COMMENT = get<2>(*REGION_it194);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it192 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end191 = NATION.end();
            for (; NATION_it192 != NATION_end191; ++NATION_it192)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it192);
                string protect_N__NAME = get<1>(*NATION_it192);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it192);
                string protect_N__COMMENT = get<3>(*NATION_it192);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it190 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end189 = CUSTOMER.end();
                for (; CUSTOMER_it190 != CUSTOMER_end189; ++CUSTOMER_it190)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it190);
                    string protect_C__NAME = get<1>(*CUSTOMER_it190);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it190);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it190);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it190);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it190);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it190);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it190);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it188 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end187 = SUPPLIER.end();
                    for (; SUPPLIER_it188 != SUPPLIER_end187; ++SUPPLIER_it188)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it188);
                        string protect_S__NAME = get<1>(*SUPPLIER_it188);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it188);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it188);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it188);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it188);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it188);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it186 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end185 = ORDERS.end();
                        for (; ORDERS_it186 != ORDERS_end185; ++ORDERS_it186)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it186);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it186);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it186);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it186);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it186);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it186);
                            string protect_O__CLERK = get<6>(*ORDERS_it186);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it186);
                            string protect_O__COMMENT = get<8>(*ORDERS_it186);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it184 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end183 = LINEITEM.end();
                            for (
                                ; LINEITEM_it184 != LINEITEM_end183; ++LINEITEM_it184)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it184);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it184);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it184);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it184);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it184);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it184);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it184);
                                double protect_L__TAX = get<7>(*LINEITEM_it184);
                                int64_t var230 = 0;
                                var230 += protect_N__REGIONKEY;
                                int64_t var231 = 0;
                                var231 += protect_R__REGIONKEY;
                                if ( var230 == var231 )
                                {
                                    string var228 = 0;
                                    var228 += N__NAME;
                                    string var229 = 0;
                                    var229 += protect_N__NAME;
                                    if ( var228 == var229 )
                                    {
                                        int64_t var226 = 0;
                                        var226 += protect_S__NATIONKEY;
                                        int64_t var227 = 0;
                                        var227 += protect_N__NATIONKEY;
                                        if ( var226 == var227 )
                                        {
                                            int64_t var224 = 0;
                                            var224 += protect_C__NATIONKEY;
                                            int64_t var225 = 0;
                                            var225 += protect_S__NATIONKEY;
                                            if ( var224 == var225 )
                                            {
                                                int64_t var222 = 0;
                                                var222 += protect_C__CUSTKEY;
                                                int64_t var223 = 0;
                                                var223 += protect_O__CUSTKEY;
                                                if ( var222 == var223 )
                                                {
                                                    int64_t var220 = 0;
                                                    var220 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var221 = 0;
                                                    var221 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var220 == var221 )
                                                    {
                                                        int64_t var218 = 0;
                                                        var218 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var219 = 0;
                                                        var219 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var218 == var219 )
                                                        {
                                                            q[N__NAME] += 
                                                                protect_L__EXTENDEDPRICE;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        double var232 = 1;
        double var233 = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it206 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end205 = 
            REGION.end();
        for (; REGION_it206 != REGION_end205; ++REGION_it206)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it206);
            string protect_R__NAME = get<1>(*REGION_it206);
            string protect_R__COMMENT = get<2>(*REGION_it206);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it204 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end203 = NATION.end();
            for (; NATION_it204 != NATION_end203; ++NATION_it204)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it204);
                string protect_N__NAME = get<1>(*NATION_it204);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it204);
                string protect_N__COMMENT = get<3>(*NATION_it204);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it202 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end201 = CUSTOMER.end();
                for (; CUSTOMER_it202 != CUSTOMER_end201; ++CUSTOMER_it202)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it202);
                    string protect_C__NAME = get<1>(*CUSTOMER_it202);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it202);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it202);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it202);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it202);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it202);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it202);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it200 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end199 = SUPPLIER.end();
                    for (; SUPPLIER_it200 != SUPPLIER_end199; ++SUPPLIER_it200)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it200);
                        string protect_S__NAME = get<1>(*SUPPLIER_it200);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it200);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it200);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it200);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it200);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it200);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it198 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end197 = ORDERS.end();
                        for (; ORDERS_it198 != ORDERS_end197; ++ORDERS_it198)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it198);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it198);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it198);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it198);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it198);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it198);
                            string protect_O__CLERK = get<6>(*ORDERS_it198);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it198);
                            string protect_O__COMMENT = get<8>(*ORDERS_it198);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it196 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end195 = LINEITEM.end();
                            for (
                                ; LINEITEM_it196 != LINEITEM_end195; ++LINEITEM_it196)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it196);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it196);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it196);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it196);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it196);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it196);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it196);
                                double protect_L__TAX = get<7>(*LINEITEM_it196);
                                int64_t var247 = 0;
                                var247 += protect_N__REGIONKEY;
                                int64_t var248 = 0;
                                var248 += protect_R__REGIONKEY;
                                if ( var247 == var248 )
                                {
                                    string var245 = 0;
                                    var245 += N__NAME;
                                    string var246 = 0;
                                    var246 += protect_N__NAME;
                                    if ( var245 == var246 )
                                    {
                                        int64_t var243 = 0;
                                        var243 += protect_S__NATIONKEY;
                                        int64_t var244 = 0;
                                        var244 += protect_N__NATIONKEY;
                                        if ( var243 == var244 )
                                        {
                                            int64_t var241 = 0;
                                            var241 += protect_C__NATIONKEY;
                                            int64_t var242 = 0;
                                            var242 += protect_S__NATIONKEY;
                                            if ( var241 == var242 )
                                            {
                                                int64_t var239 = 0;
                                                var239 += protect_C__CUSTKEY;
                                                int64_t var240 = 0;
                                                var240 += protect_O__CUSTKEY;
                                                if ( var239 == var240 )
                                                {
                                                    int64_t var237 = 0;
                                                    var237 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var238 = 0;
                                                    var238 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var237 == var238 )
                                                    {
                                                        int64_t var235 = 0;
                                                        var235 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var236 = 0;
                                                        var236 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var235 == var236 )
                                                        {
                                                            double var234 = 1;
                                                            var234 *= 
                                                                protect_L__EXTENDEDPRICE;
                                                            var234 *= 
                                                                protect_L__DISCOUNT;
                                                            var233 += var234;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        var232 *= var233;
        var232 *= -1;
        q[N__NAME] += var232;
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_ORDERS_sec_span, on_delete_ORDERS_usec_span);
}

void on_delete_SUPPLIER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t SUPPKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    SUPPLIER.erase(make_tuple(
        SUPPKEY,NAME,ADDRESS,NATIONKEY,PHONE,ACCTBAL,COMMENT));
    map<string,double>::iterator q_it234 = q.begin();
    map<string,double>::iterator q_end233 = q.end();
    for (; q_it234 != q_end233; ++q_it234)
    {
        string N__NAME = q_it234->first;
        q[N__NAME] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it220 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end219 = 
            REGION.end();
        for (; REGION_it220 != REGION_end219; ++REGION_it220)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it220);
            string protect_R__NAME = get<1>(*REGION_it220);
            string protect_R__COMMENT = get<2>(*REGION_it220);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it218 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end217 = NATION.end();
            for (; NATION_it218 != NATION_end217; ++NATION_it218)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it218);
                string protect_N__NAME = get<1>(*NATION_it218);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it218);
                string protect_N__COMMENT = get<3>(*NATION_it218);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it216 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end215 = CUSTOMER.end();
                for (; CUSTOMER_it216 != CUSTOMER_end215; ++CUSTOMER_it216)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it216);
                    string protect_C__NAME = get<1>(*CUSTOMER_it216);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it216);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it216);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it216);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it216);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it216);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it216);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it214 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end213 = SUPPLIER.end();
                    for (; SUPPLIER_it214 != SUPPLIER_end213; ++SUPPLIER_it214)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it214);
                        string protect_S__NAME = get<1>(*SUPPLIER_it214);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it214);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it214);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it214);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it214);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it214);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it212 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end211 = ORDERS.end();
                        for (; ORDERS_it212 != ORDERS_end211; ++ORDERS_it212)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it212);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it212);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it212);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it212);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it212);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it212);
                            string protect_O__CLERK = get<6>(*ORDERS_it212);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it212);
                            string protect_O__COMMENT = get<8>(*ORDERS_it212);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it210 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end209 = LINEITEM.end();
                            for (
                                ; LINEITEM_it210 != LINEITEM_end209; ++LINEITEM_it210)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it210);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it210);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it210);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it210);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it210);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it210);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it210);
                                double protect_L__TAX = get<7>(*LINEITEM_it210);
                                int64_t var261 = 0;
                                var261 += protect_N__REGIONKEY;
                                int64_t var262 = 0;
                                var262 += protect_R__REGIONKEY;
                                if ( var261 == var262 )
                                {
                                    string var259 = 0;
                                    var259 += N__NAME;
                                    string var260 = 0;
                                    var260 += protect_N__NAME;
                                    if ( var259 == var260 )
                                    {
                                        int64_t var257 = 0;
                                        var257 += protect_S__NATIONKEY;
                                        int64_t var258 = 0;
                                        var258 += protect_N__NATIONKEY;
                                        if ( var257 == var258 )
                                        {
                                            int64_t var255 = 0;
                                            var255 += protect_C__NATIONKEY;
                                            int64_t var256 = 0;
                                            var256 += protect_S__NATIONKEY;
                                            if ( var255 == var256 )
                                            {
                                                int64_t var253 = 0;
                                                var253 += protect_C__CUSTKEY;
                                                int64_t var254 = 0;
                                                var254 += protect_O__CUSTKEY;
                                                if ( var253 == var254 )
                                                {
                                                    int64_t var251 = 0;
                                                    var251 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var252 = 0;
                                                    var252 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var251 == var252 )
                                                    {
                                                        int64_t var249 = 0;
                                                        var249 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var250 = 0;
                                                        var250 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var249 == var250 )
                                                        {
                                                            q[N__NAME] += 
                                                                protect_L__EXTENDEDPRICE;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        double var263 = 1;
        double var264 = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it232 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end231 = 
            REGION.end();
        for (; REGION_it232 != REGION_end231; ++REGION_it232)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it232);
            string protect_R__NAME = get<1>(*REGION_it232);
            string protect_R__COMMENT = get<2>(*REGION_it232);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it230 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end229 = NATION.end();
            for (; NATION_it230 != NATION_end229; ++NATION_it230)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it230);
                string protect_N__NAME = get<1>(*NATION_it230);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it230);
                string protect_N__COMMENT = get<3>(*NATION_it230);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it228 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end227 = CUSTOMER.end();
                for (; CUSTOMER_it228 != CUSTOMER_end227; ++CUSTOMER_it228)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it228);
                    string protect_C__NAME = get<1>(*CUSTOMER_it228);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it228);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it228);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it228);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it228);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it228);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it228);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it226 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end225 = SUPPLIER.end();
                    for (; SUPPLIER_it226 != SUPPLIER_end225; ++SUPPLIER_it226)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it226);
                        string protect_S__NAME = get<1>(*SUPPLIER_it226);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it226);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it226);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it226);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it226);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it226);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it224 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end223 = ORDERS.end();
                        for (; ORDERS_it224 != ORDERS_end223; ++ORDERS_it224)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it224);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it224);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it224);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it224);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it224);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it224);
                            string protect_O__CLERK = get<6>(*ORDERS_it224);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it224);
                            string protect_O__COMMENT = get<8>(*ORDERS_it224);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it222 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end221 = LINEITEM.end();
                            for (
                                ; LINEITEM_it222 != LINEITEM_end221; ++LINEITEM_it222)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it222);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it222);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it222);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it222);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it222);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it222);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it222);
                                double protect_L__TAX = get<7>(*LINEITEM_it222);
                                int64_t var278 = 0;
                                var278 += protect_N__REGIONKEY;
                                int64_t var279 = 0;
                                var279 += protect_R__REGIONKEY;
                                if ( var278 == var279 )
                                {
                                    string var276 = 0;
                                    var276 += N__NAME;
                                    string var277 = 0;
                                    var277 += protect_N__NAME;
                                    if ( var276 == var277 )
                                    {
                                        int64_t var274 = 0;
                                        var274 += protect_S__NATIONKEY;
                                        int64_t var275 = 0;
                                        var275 += protect_N__NATIONKEY;
                                        if ( var274 == var275 )
                                        {
                                            int64_t var272 = 0;
                                            var272 += protect_C__NATIONKEY;
                                            int64_t var273 = 0;
                                            var273 += protect_S__NATIONKEY;
                                            if ( var272 == var273 )
                                            {
                                                int64_t var270 = 0;
                                                var270 += protect_C__CUSTKEY;
                                                int64_t var271 = 0;
                                                var271 += protect_O__CUSTKEY;
                                                if ( var270 == var271 )
                                                {
                                                    int64_t var268 = 0;
                                                    var268 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var269 = 0;
                                                    var269 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var268 == var269 )
                                                    {
                                                        int64_t var266 = 0;
                                                        var266 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var267 = 0;
                                                        var267 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var266 == var267 )
                                                        {
                                                            double var265 = 1;
                                                            var265 *= 
                                                                protect_L__EXTENDEDPRICE;
                                                            var265 *= 
                                                                protect_L__DISCOUNT;
                                                            var264 += var265;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        var263 *= var264;
        var263 *= -1;
        q[N__NAME] += var263;
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_SUPPLIER_sec_span, on_delete_SUPPLIER_usec_span);
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
    map<string,double>::iterator q_it260 = q.begin();
    map<string,double>::iterator q_end259 = q.end();
    for (; q_it260 != q_end259; ++q_it260)
    {
        string N__NAME = q_it260->first;
        q[N__NAME] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it246 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end245 = 
            REGION.end();
        for (; REGION_it246 != REGION_end245; ++REGION_it246)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it246);
            string protect_R__NAME = get<1>(*REGION_it246);
            string protect_R__COMMENT = get<2>(*REGION_it246);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it244 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end243 = NATION.end();
            for (; NATION_it244 != NATION_end243; ++NATION_it244)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it244);
                string protect_N__NAME = get<1>(*NATION_it244);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it244);
                string protect_N__COMMENT = get<3>(*NATION_it244);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it242 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end241 = CUSTOMER.end();
                for (; CUSTOMER_it242 != CUSTOMER_end241; ++CUSTOMER_it242)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it242);
                    string protect_C__NAME = get<1>(*CUSTOMER_it242);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it242);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it242);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it242);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it242);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it242);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it242);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it240 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end239 = SUPPLIER.end();
                    for (; SUPPLIER_it240 != SUPPLIER_end239; ++SUPPLIER_it240)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it240);
                        string protect_S__NAME = get<1>(*SUPPLIER_it240);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it240);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it240);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it240);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it240);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it240);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it238 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end237 = ORDERS.end();
                        for (; ORDERS_it238 != ORDERS_end237; ++ORDERS_it238)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it238);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it238);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it238);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it238);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it238);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it238);
                            string protect_O__CLERK = get<6>(*ORDERS_it238);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it238);
                            string protect_O__COMMENT = get<8>(*ORDERS_it238);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it236 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end235 = LINEITEM.end();
                            for (
                                ; LINEITEM_it236 != LINEITEM_end235; ++LINEITEM_it236)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it236);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it236);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it236);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it236);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it236);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it236);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it236);
                                double protect_L__TAX = get<7>(*LINEITEM_it236);
                                int64_t var292 = 0;
                                var292 += protect_N__REGIONKEY;
                                int64_t var293 = 0;
                                var293 += protect_R__REGIONKEY;
                                if ( var292 == var293 )
                                {
                                    string var290 = 0;
                                    var290 += N__NAME;
                                    string var291 = 0;
                                    var291 += protect_N__NAME;
                                    if ( var290 == var291 )
                                    {
                                        int64_t var288 = 0;
                                        var288 += protect_S__NATIONKEY;
                                        int64_t var289 = 0;
                                        var289 += protect_N__NATIONKEY;
                                        if ( var288 == var289 )
                                        {
                                            int64_t var286 = 0;
                                            var286 += protect_C__NATIONKEY;
                                            int64_t var287 = 0;
                                            var287 += protect_S__NATIONKEY;
                                            if ( var286 == var287 )
                                            {
                                                int64_t var284 = 0;
                                                var284 += protect_C__CUSTKEY;
                                                int64_t var285 = 0;
                                                var285 += protect_O__CUSTKEY;
                                                if ( var284 == var285 )
                                                {
                                                    int64_t var282 = 0;
                                                    var282 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var283 = 0;
                                                    var283 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var282 == var283 )
                                                    {
                                                        int64_t var280 = 0;
                                                        var280 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var281 = 0;
                                                        var281 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var280 == var281 )
                                                        {
                                                            q[N__NAME] += 
                                                                protect_L__EXTENDEDPRICE;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        double var294 = 1;
        double var295 = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it258 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end257 = 
            REGION.end();
        for (; REGION_it258 != REGION_end257; ++REGION_it258)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it258);
            string protect_R__NAME = get<1>(*REGION_it258);
            string protect_R__COMMENT = get<2>(*REGION_it258);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it256 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end255 = NATION.end();
            for (; NATION_it256 != NATION_end255; ++NATION_it256)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it256);
                string protect_N__NAME = get<1>(*NATION_it256);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it256);
                string protect_N__COMMENT = get<3>(*NATION_it256);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it254 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end253 = CUSTOMER.end();
                for (; CUSTOMER_it254 != CUSTOMER_end253; ++CUSTOMER_it254)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it254);
                    string protect_C__NAME = get<1>(*CUSTOMER_it254);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it254);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it254);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it254);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it254);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it254);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it254);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it252 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end251 = SUPPLIER.end();
                    for (; SUPPLIER_it252 != SUPPLIER_end251; ++SUPPLIER_it252)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it252);
                        string protect_S__NAME = get<1>(*SUPPLIER_it252);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it252);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it252);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it252);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it252);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it252);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it250 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end249 = ORDERS.end();
                        for (; ORDERS_it250 != ORDERS_end249; ++ORDERS_it250)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it250);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it250);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it250);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it250);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it250);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it250);
                            string protect_O__CLERK = get<6>(*ORDERS_it250);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it250);
                            string protect_O__COMMENT = get<8>(*ORDERS_it250);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it248 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end247 = LINEITEM.end();
                            for (
                                ; LINEITEM_it248 != LINEITEM_end247; ++LINEITEM_it248)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it248);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it248);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it248);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it248);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it248);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it248);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it248);
                                double protect_L__TAX = get<7>(*LINEITEM_it248);
                                int64_t var309 = 0;
                                var309 += protect_N__REGIONKEY;
                                int64_t var310 = 0;
                                var310 += protect_R__REGIONKEY;
                                if ( var309 == var310 )
                                {
                                    string var307 = 0;
                                    var307 += N__NAME;
                                    string var308 = 0;
                                    var308 += protect_N__NAME;
                                    if ( var307 == var308 )
                                    {
                                        int64_t var305 = 0;
                                        var305 += protect_S__NATIONKEY;
                                        int64_t var306 = 0;
                                        var306 += protect_N__NATIONKEY;
                                        if ( var305 == var306 )
                                        {
                                            int64_t var303 = 0;
                                            var303 += protect_C__NATIONKEY;
                                            int64_t var304 = 0;
                                            var304 += protect_S__NATIONKEY;
                                            if ( var303 == var304 )
                                            {
                                                int64_t var301 = 0;
                                                var301 += protect_C__CUSTKEY;
                                                int64_t var302 = 0;
                                                var302 += protect_O__CUSTKEY;
                                                if ( var301 == var302 )
                                                {
                                                    int64_t var299 = 0;
                                                    var299 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var300 = 0;
                                                    var300 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var299 == var300 )
                                                    {
                                                        int64_t var297 = 0;
                                                        var297 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var298 = 0;
                                                        var298 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var297 == var298 )
                                                        {
                                                            double var296 = 1;
                                                            var296 *= 
                                                                protect_L__EXTENDEDPRICE;
                                                            var296 *= 
                                                                protect_L__DISCOUNT;
                                                            var295 += var296;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        var294 *= var295;
        var294 *= -1;
        q[N__NAME] += var294;
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_CUSTOMER_sec_span, on_delete_CUSTOMER_usec_span);
}

void on_delete_NATION(
    ofstream* results, ofstream* log, ofstream* stats, int64_t NATIONKEY,string 
    NAME,int64_t REGIONKEY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    NATION.erase(make_tuple(NATIONKEY,NAME,REGIONKEY,COMMENT));
    map<string,double>::iterator q_it286 = q.begin();
    map<string,double>::iterator q_end285 = q.end();
    for (; q_it286 != q_end285; ++q_it286)
    {
        string NAME = q_it286->first;
        q[NAME] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it272 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end271 = 
            REGION.end();
        for (; REGION_it272 != REGION_end271; ++REGION_it272)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it272);
            string protect_R__NAME = get<1>(*REGION_it272);
            string protect_R__COMMENT = get<2>(*REGION_it272);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it270 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end269 = NATION.end();
            for (; NATION_it270 != NATION_end269; ++NATION_it270)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it270);
                string protect_N__NAME = get<1>(*NATION_it270);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it270);
                string protect_N__COMMENT = get<3>(*NATION_it270);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it268 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end267 = CUSTOMER.end();
                for (; CUSTOMER_it268 != CUSTOMER_end267; ++CUSTOMER_it268)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it268);
                    string protect_C__NAME = get<1>(*CUSTOMER_it268);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it268);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it268);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it268);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it268);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it268);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it268);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it266 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end265 = SUPPLIER.end();
                    for (; SUPPLIER_it266 != SUPPLIER_end265; ++SUPPLIER_it266)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it266);
                        string protect_S__NAME = get<1>(*SUPPLIER_it266);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it266);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it266);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it266);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it266);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it266);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it264 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end263 = ORDERS.end();
                        for (; ORDERS_it264 != ORDERS_end263; ++ORDERS_it264)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it264);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it264);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it264);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it264);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it264);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it264);
                            string protect_O__CLERK = get<6>(*ORDERS_it264);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it264);
                            string protect_O__COMMENT = get<8>(*ORDERS_it264);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it262 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end261 = LINEITEM.end();
                            for (
                                ; LINEITEM_it262 != LINEITEM_end261; ++LINEITEM_it262)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it262);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it262);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it262);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it262);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it262);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it262);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it262);
                                double protect_L__TAX = get<7>(*LINEITEM_it262);
                                int64_t var323 = 0;
                                var323 += protect_N__REGIONKEY;
                                int64_t var324 = 0;
                                var324 += protect_R__REGIONKEY;
                                if ( var323 == var324 )
                                {
                                    string var321 = 0;
                                    var321 += NAME;
                                    string var322 = 0;
                                    var322 += protect_N__NAME;
                                    if ( var321 == var322 )
                                    {
                                        int64_t var319 = 0;
                                        var319 += protect_S__NATIONKEY;
                                        int64_t var320 = 0;
                                        var320 += protect_N__NATIONKEY;
                                        if ( var319 == var320 )
                                        {
                                            int64_t var317 = 0;
                                            var317 += protect_C__NATIONKEY;
                                            int64_t var318 = 0;
                                            var318 += protect_S__NATIONKEY;
                                            if ( var317 == var318 )
                                            {
                                                int64_t var315 = 0;
                                                var315 += protect_C__CUSTKEY;
                                                int64_t var316 = 0;
                                                var316 += protect_O__CUSTKEY;
                                                if ( var315 == var316 )
                                                {
                                                    int64_t var313 = 0;
                                                    var313 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var314 = 0;
                                                    var314 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var313 == var314 )
                                                    {
                                                        int64_t var311 = 0;
                                                        var311 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var312 = 0;
                                                        var312 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var311 == var312 )
                                                        {
                                                            q[NAME] += 
                                                                protect_L__EXTENDEDPRICE;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        double var325 = 1;
        double var326 = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it284 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end283 = 
            REGION.end();
        for (; REGION_it284 != REGION_end283; ++REGION_it284)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it284);
            string protect_R__NAME = get<1>(*REGION_it284);
            string protect_R__COMMENT = get<2>(*REGION_it284);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it282 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end281 = NATION.end();
            for (; NATION_it282 != NATION_end281; ++NATION_it282)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it282);
                string protect_N__NAME = get<1>(*NATION_it282);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it282);
                string protect_N__COMMENT = get<3>(*NATION_it282);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it280 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end279 = CUSTOMER.end();
                for (; CUSTOMER_it280 != CUSTOMER_end279; ++CUSTOMER_it280)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it280);
                    string protect_C__NAME = get<1>(*CUSTOMER_it280);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it280);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it280);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it280);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it280);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it280);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it280);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it278 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end277 = SUPPLIER.end();
                    for (; SUPPLIER_it278 != SUPPLIER_end277; ++SUPPLIER_it278)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it278);
                        string protect_S__NAME = get<1>(*SUPPLIER_it278);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it278);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it278);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it278);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it278);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it278);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it276 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end275 = ORDERS.end();
                        for (; ORDERS_it276 != ORDERS_end275; ++ORDERS_it276)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it276);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it276);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it276);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it276);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it276);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it276);
                            string protect_O__CLERK = get<6>(*ORDERS_it276);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it276);
                            string protect_O__COMMENT = get<8>(*ORDERS_it276);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it274 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end273 = LINEITEM.end();
                            for (
                                ; LINEITEM_it274 != LINEITEM_end273; ++LINEITEM_it274)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it274);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it274);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it274);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it274);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it274);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it274);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it274);
                                double protect_L__TAX = get<7>(*LINEITEM_it274);
                                int64_t var340 = 0;
                                var340 += protect_N__REGIONKEY;
                                int64_t var341 = 0;
                                var341 += protect_R__REGIONKEY;
                                if ( var340 == var341 )
                                {
                                    string var338 = 0;
                                    var338 += NAME;
                                    string var339 = 0;
                                    var339 += protect_N__NAME;
                                    if ( var338 == var339 )
                                    {
                                        int64_t var336 = 0;
                                        var336 += protect_S__NATIONKEY;
                                        int64_t var337 = 0;
                                        var337 += protect_N__NATIONKEY;
                                        if ( var336 == var337 )
                                        {
                                            int64_t var334 = 0;
                                            var334 += protect_C__NATIONKEY;
                                            int64_t var335 = 0;
                                            var335 += protect_S__NATIONKEY;
                                            if ( var334 == var335 )
                                            {
                                                int64_t var332 = 0;
                                                var332 += protect_C__CUSTKEY;
                                                int64_t var333 = 0;
                                                var333 += protect_O__CUSTKEY;
                                                if ( var332 == var333 )
                                                {
                                                    int64_t var330 = 0;
                                                    var330 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var331 = 0;
                                                    var331 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var330 == var331 )
                                                    {
                                                        int64_t var328 = 0;
                                                        var328 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var329 = 0;
                                                        var329 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var328 == var329 )
                                                        {
                                                            double var327 = 1;
                                                            var327 *= 
                                                                protect_L__EXTENDEDPRICE;
                                                            var327 *= 
                                                                protect_L__DISCOUNT;
                                                            var326 += var327;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        var325 *= var326;
        var325 *= -1;
        q[NAME] += var325;
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_NATION_sec_span, on_delete_NATION_usec_span);
}

void on_delete_REGION(
    ofstream* results, ofstream* log, ofstream* stats, int64_t REGIONKEY,string 
    NAME,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    REGION.erase(make_tuple(REGIONKEY,NAME,COMMENT));
    map<string,double>::iterator q_it312 = q.begin();
    map<string,double>::iterator q_end311 = q.end();
    for (; q_it312 != q_end311; ++q_it312)
    {
        string N__NAME = q_it312->first;
        q[N__NAME] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it298 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end297 = 
            REGION.end();
        for (; REGION_it298 != REGION_end297; ++REGION_it298)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it298);
            string protect_R__NAME = get<1>(*REGION_it298);
            string protect_R__COMMENT = get<2>(*REGION_it298);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it296 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end295 = NATION.end();
            for (; NATION_it296 != NATION_end295; ++NATION_it296)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it296);
                string protect_N__NAME = get<1>(*NATION_it296);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it296);
                string protect_N__COMMENT = get<3>(*NATION_it296);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it294 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end293 = CUSTOMER.end();
                for (; CUSTOMER_it294 != CUSTOMER_end293; ++CUSTOMER_it294)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it294);
                    string protect_C__NAME = get<1>(*CUSTOMER_it294);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it294);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it294);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it294);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it294);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it294);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it294);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it292 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end291 = SUPPLIER.end();
                    for (; SUPPLIER_it292 != SUPPLIER_end291; ++SUPPLIER_it292)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it292);
                        string protect_S__NAME = get<1>(*SUPPLIER_it292);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it292);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it292);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it292);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it292);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it292);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it290 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end289 = ORDERS.end();
                        for (; ORDERS_it290 != ORDERS_end289; ++ORDERS_it290)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it290);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it290);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it290);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it290);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it290);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it290);
                            string protect_O__CLERK = get<6>(*ORDERS_it290);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it290);
                            string protect_O__COMMENT = get<8>(*ORDERS_it290);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it288 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end287 = LINEITEM.end();
                            for (
                                ; LINEITEM_it288 != LINEITEM_end287; ++LINEITEM_it288)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it288);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it288);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it288);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it288);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it288);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it288);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it288);
                                double protect_L__TAX = get<7>(*LINEITEM_it288);
                                int64_t var354 = 0;
                                var354 += protect_N__REGIONKEY;
                                int64_t var355 = 0;
                                var355 += protect_R__REGIONKEY;
                                if ( var354 == var355 )
                                {
                                    string var352 = 0;
                                    var352 += N__NAME;
                                    string var353 = 0;
                                    var353 += protect_N__NAME;
                                    if ( var352 == var353 )
                                    {
                                        int64_t var350 = 0;
                                        var350 += protect_S__NATIONKEY;
                                        int64_t var351 = 0;
                                        var351 += protect_N__NATIONKEY;
                                        if ( var350 == var351 )
                                        {
                                            int64_t var348 = 0;
                                            var348 += protect_C__NATIONKEY;
                                            int64_t var349 = 0;
                                            var349 += protect_S__NATIONKEY;
                                            if ( var348 == var349 )
                                            {
                                                int64_t var346 = 0;
                                                var346 += protect_C__CUSTKEY;
                                                int64_t var347 = 0;
                                                var347 += protect_O__CUSTKEY;
                                                if ( var346 == var347 )
                                                {
                                                    int64_t var344 = 0;
                                                    var344 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var345 = 0;
                                                    var345 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var344 == var345 )
                                                    {
                                                        int64_t var342 = 0;
                                                        var342 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var343 = 0;
                                                        var343 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var342 == var343 )
                                                        {
                                                            q[N__NAME] += 
                                                                protect_L__EXTENDEDPRICE;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        double var356 = 1;
        double var357 = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it310 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end309 = 
            REGION.end();
        for (; REGION_it310 != REGION_end309; ++REGION_it310)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it310);
            string protect_R__NAME = get<1>(*REGION_it310);
            string protect_R__COMMENT = get<2>(*REGION_it310);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it308 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end307 = NATION.end();
            for (; NATION_it308 != NATION_end307; ++NATION_it308)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it308);
                string protect_N__NAME = get<1>(*NATION_it308);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it308);
                string protect_N__COMMENT = get<3>(*NATION_it308);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it306 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end305 = CUSTOMER.end();
                for (; CUSTOMER_it306 != CUSTOMER_end305; ++CUSTOMER_it306)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it306);
                    string protect_C__NAME = get<1>(*CUSTOMER_it306);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it306);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it306);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it306);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it306);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it306);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it306);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it304 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end303 = SUPPLIER.end();
                    for (; SUPPLIER_it304 != SUPPLIER_end303; ++SUPPLIER_it304)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it304);
                        string protect_S__NAME = get<1>(*SUPPLIER_it304);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it304);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it304);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it304);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it304);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it304);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it302 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end301 = ORDERS.end();
                        for (; ORDERS_it302 != ORDERS_end301; ++ORDERS_it302)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it302);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it302);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it302);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it302);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it302);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it302);
                            string protect_O__CLERK = get<6>(*ORDERS_it302);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it302);
                            string protect_O__COMMENT = get<8>(*ORDERS_it302);
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it300 = LINEITEM.begin();
                            
                                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end299 = LINEITEM.end();
                            for (
                                ; LINEITEM_it300 != LINEITEM_end299; ++LINEITEM_it300)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(
                                    *LINEITEM_it300);
                                int64_t protect_L__PARTKEY = get<1>(
                                    *LINEITEM_it300);
                                int64_t protect_L__SUPPKEY = get<2>(
                                    *LINEITEM_it300);
                                int protect_L__LINENUMBER = get<3>(
                                    *LINEITEM_it300);
                                double protect_L__QUANTITY = get<4>(
                                    *LINEITEM_it300);
                                double protect_L__EXTENDEDPRICE = get<5>(
                                    *LINEITEM_it300);
                                double protect_L__DISCOUNT = get<6>(
                                    *LINEITEM_it300);
                                double protect_L__TAX = get<7>(*LINEITEM_it300);
                                int64_t var371 = 0;
                                var371 += protect_N__REGIONKEY;
                                int64_t var372 = 0;
                                var372 += protect_R__REGIONKEY;
                                if ( var371 == var372 )
                                {
                                    string var369 = 0;
                                    var369 += N__NAME;
                                    string var370 = 0;
                                    var370 += protect_N__NAME;
                                    if ( var369 == var370 )
                                    {
                                        int64_t var367 = 0;
                                        var367 += protect_S__NATIONKEY;
                                        int64_t var368 = 0;
                                        var368 += protect_N__NATIONKEY;
                                        if ( var367 == var368 )
                                        {
                                            int64_t var365 = 0;
                                            var365 += protect_C__NATIONKEY;
                                            int64_t var366 = 0;
                                            var366 += protect_S__NATIONKEY;
                                            if ( var365 == var366 )
                                            {
                                                int64_t var363 = 0;
                                                var363 += protect_C__CUSTKEY;
                                                int64_t var364 = 0;
                                                var364 += protect_O__CUSTKEY;
                                                if ( var363 == var364 )
                                                {
                                                    int64_t var361 = 0;
                                                    var361 += 
                                                        protect_L__SUPPKEY;
                                                    int64_t var362 = 0;
                                                    var362 += 
                                                        protect_S__SUPPKEY;
                                                    if ( var361 == var362 )
                                                    {
                                                        int64_t var359 = 0;
                                                        var359 += 
                                                            protect_L__ORDERKEY;
                                                        int64_t var360 = 0;
                                                        var360 += 
                                                            protect_O__ORDERKEY;
                                                        if ( var359 == var360 )
                                                        {
                                                            double var358 = 1;
                                                            var358 *= 
                                                                protect_L__EXTENDEDPRICE;
                                                            var358 *= 
                                                                protect_L__DISCOUNT;
                                                            var357 += var358;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        var356 *= var357;
        var356 *= -1;
        q[N__NAME] += var356;
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_REGION_sec_span, on_delete_REGION_usec_span);
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

DBToaster::DemoDatasets::SupplierStream SSBSupplier("/home/yanif/datasets/tpch/sf1/singlefile/supplier.tbl.a",&DBToaster::DemoDatasets::parseSupplierField,7,110000,512);

boost::shared_ptr<DBToaster::DemoDatasets::SupplierTupleAdaptor> SSBSupplier_adaptor(new DBToaster::DemoDatasets::SupplierTupleAdaptor());
static int streamSSBSupplierId = 2;

struct on_insert_SUPPLIER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::SupplierTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::SupplierTupleAdaptor::Result>(data); 
        on_insert_SUPPLIER(results, log, stats, input.suppkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.comment);
    }
};

on_insert_SUPPLIER_fun_obj fo_on_insert_SUPPLIER_2;

DBToaster::DemoDatasets::CustomerStream SSBCustomer("/home/yanif/datasets/tpch/sf1/singlefile/customer.tbl.a",&DBToaster::DemoDatasets::parseCustomerField,8,1600000, 512);

boost::shared_ptr<DBToaster::DemoDatasets::CustomerTupleAdaptor> SSBCustomer_adaptor(new DBToaster::DemoDatasets::CustomerTupleAdaptor());
static int streamSSBCustomerId = 3;

struct on_insert_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_insert_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_insert_CUSTOMER_fun_obj fo_on_insert_CUSTOMER_3;

DBToaster::DemoDatasets::NationStream SSBNation("/home/yanif/datasets/tpch/sf1/singlefile/nation.tbl",&DBToaster::DemoDatasets::parseNationField,4,100,512);

boost::shared_ptr<DBToaster::DemoDatasets::NationTupleAdaptor> SSBNation_adaptor(new DBToaster::DemoDatasets::NationTupleAdaptor());
static int streamSSBNationId = 4;

struct on_insert_NATION_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::NationTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::NationTupleAdaptor::Result>(data); 
        on_insert_NATION(results, log, stats, input.nationkey,input.name,input.regionkey,input.comment);
    }
};

on_insert_NATION_fun_obj fo_on_insert_NATION_4;

DBToaster::DemoDatasets::RegionStream SSBRegion("/home/yanif/datasets/tpch/sf1/singlefile/region.tbl",&DBToaster::DemoDatasets::parseRegionField,3,100,512);

boost::shared_ptr<DBToaster::DemoDatasets::RegionTupleAdaptor> SSBRegion_adaptor(new DBToaster::DemoDatasets::RegionTupleAdaptor());
static int streamSSBRegionId = 5;

struct on_insert_REGION_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::RegionTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::RegionTupleAdaptor::Result>(data); 
        on_insert_REGION(results, log, stats, input.regionkey,input.name,input.comment);
    }
};

on_insert_REGION_fun_obj fo_on_insert_REGION_5;

struct on_delete_LINEITEM_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result>(data); 
        on_delete_LINEITEM(results, log, stats, input.orderkey,input.partkey,input.suppkey,input.linenumber,input.quantity,input.extendedprice,input.discount,input.tax);
    }
};

on_delete_LINEITEM_fun_obj fo_on_delete_LINEITEM_6;

struct on_delete_ORDERS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::OrderTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::OrderTupleAdaptor::Result>(data); 
        on_delete_ORDERS(results, log, stats, input.orderkey,input.custkey,input.orderstatus,input.totalprice,input.orderdate,input.orderpriority,input.clerk,input.shippriority,input.comment);
    }
};

on_delete_ORDERS_fun_obj fo_on_delete_ORDERS_7;

struct on_delete_SUPPLIER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::SupplierTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::SupplierTupleAdaptor::Result>(data); 
        on_delete_SUPPLIER(results, log, stats, input.suppkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.comment);
    }
};

on_delete_SUPPLIER_fun_obj fo_on_delete_SUPPLIER_8;

struct on_delete_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_delete_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_delete_CUSTOMER_fun_obj fo_on_delete_CUSTOMER_9;

struct on_delete_NATION_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::NationTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::NationTupleAdaptor::Result>(data); 
        on_delete_NATION(results, log, stats, input.nationkey,input.name,input.regionkey,input.comment);
    }
};

on_delete_NATION_fun_obj fo_on_delete_NATION_10;

struct on_delete_REGION_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::RegionTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::RegionTupleAdaptor::Result>(data); 
        on_delete_REGION(results, log, stats, input.regionkey,input.name,input.comment);
    }
};

on_delete_REGION_fun_obj fo_on_delete_REGION_11;


void init(DBToaster::StandaloneEngine::FileMultiplexer& sources,
    DBToaster::StandaloneEngine::FileStreamDispatcher& router, 
    ofstream* results, ofstream* log, ofstream* stats)
{
    router.setFiles(results, log, stats);
    sources.addStream<DBToaster::DemoDatasets::lineitem>(&SSBLineitem, boost::ref(*SSBLineitem_adaptor), streamSSBLineitemId);
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_LINEITEM_0));
    sources.addStream<DBToaster::DemoDatasets::order>(&SSBOrder, boost::ref(*SSBOrder_adaptor), streamSSBOrderId);
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_ORDERS_1));
    sources.addStream<DBToaster::DemoDatasets::supplier>(&SSBSupplier, boost::ref(*SSBSupplier_adaptor), streamSSBSupplierId);
    router.addHandler(streamSSBSupplierId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_SUPPLIER_2));
    sources.addStream<DBToaster::DemoDatasets::customer>(&SSBCustomer, boost::ref(*SSBCustomer_adaptor), streamSSBCustomerId);
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_CUSTOMER_3));
    sources.addStream<DBToaster::DemoDatasets::nation>(&SSBNation, boost::ref(*SSBNation_adaptor), streamSSBNationId);
    router.addHandler(streamSSBNationId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_NATION_4));
    sources.addStream<DBToaster::DemoDatasets::region>(&SSBRegion, boost::ref(*SSBRegion_adaptor), streamSSBRegionId);
    router.addHandler(streamSSBRegionId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_REGION_5));
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_LINEITEM_6));
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_ORDERS_7));
    router.addHandler(streamSSBSupplierId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_SUPPLIER_8));
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_CUSTOMER_9));
    router.addHandler(streamSSBNationId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_NATION_10));
    router.addHandler(streamSSBRegionId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_REGION_11));
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