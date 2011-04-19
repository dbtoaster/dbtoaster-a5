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
double q;
multiset<tuple<int64_t,string,string,string,string,int,string,double,string> > 
    PARTS;

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

   cout << "PARTS size: " << (((sizeof(multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * PARTS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::key_type, multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type>, multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::key_compare>))) << endl;

   (*stats) << "m," << "PARTS" << "," << (((sizeof(multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * PARTS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::key_type, multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type>, multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::key_compare>))) << endl;

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
    LINEITEM.insert(make_tuple(
        ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDEDPRICE,DISCOUNT,TAX));
    q = 0;
    multiset<tuple<int64_t,string,string,string,string,int,string,double,
        string> >::iterator PARTS_it6 = PARTS.begin();
    multiset<tuple<int64_t,string,string,string,string,int,string,double,
        string> >::iterator PARTS_end5 = PARTS.end();
    for (; PARTS_it6 != PARTS_end5; ++PARTS_it6)
    {
        int64_t protect_P__PARTKEY = get<0>(*PARTS_it6);
        string protect_P__NAME = get<1>(*PARTS_it6);
        string protect_P__MFGR = get<2>(*PARTS_it6);
        string protect_P__BRAND = get<3>(*PARTS_it6);
        string protect_P__TYPE = get<4>(*PARTS_it6);
        int protect_P__SIZE = get<5>(*PARTS_it6);
        string protect_P__CONTAINER = get<6>(*PARTS_it6);
        double protect_P__RETAILPRICE = get<7>(*PARTS_it6);
        string protect_P__COMMENT = get<8>(*PARTS_it6);
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
            double var3 = 0;
            var3 += protect_L__QUANTITY;
            double var4 = 0;
            double var5 = 1;
            var5 *= 0.005;
            double var6 = 0;
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_it2 = LINEITEM.begin();
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_end1 = LINEITEM.end();
            for (; LINEITEM_it2 != LINEITEM_end1; ++LINEITEM_it2)
            {
                int64_t L2__ORDERKEY = get<0>(*LINEITEM_it2);
                int64_t L2__PARTKEY = get<1>(*LINEITEM_it2);
                int64_t L2__SUPPKEY = get<2>(*LINEITEM_it2);
                int L2__LINENUMBER = get<3>(*LINEITEM_it2);
                double L2__QUANTITY = get<4>(*LINEITEM_it2);
                double L2__EXTENDEDPRICE = get<5>(*LINEITEM_it2);
                double L2__DISCOUNT = get<6>(*LINEITEM_it2);
                double L2__TAX = get<7>(*LINEITEM_it2);
                int64_t var7 = 0;
                var7 += L2__PARTKEY;
                int64_t var8 = 0;
                var8 += protect_P__PARTKEY;
                if ( var7 == var8 )
                {
                    var6 += L2__QUANTITY;
                }
            }
            var5 *= var6;
            var4 += var5;
            if ( var3 < var4 )
            {
                int64_t var1 = 0;
                var1 += protect_P__PARTKEY;
                int64_t var2 = 0;
                var2 += protect_L__PARTKEY;
                if ( var1 == var2 )
                {
                    q += protect_L__EXTENDEDPRICE;
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
    PARTS.insert(make_tuple(
        PARTKEY,NAME,MFGR,BRAND,TYPE,SIZE,CONTAINER,RETAILPRICE,COMMENT));
    q = 0;
    multiset<tuple<int64_t,string,string,string,string,int,string,double,
        string> >::iterator PARTS_it12 = PARTS.begin();
    multiset<tuple<int64_t,string,string,string,string,int,string,double,
        string> >::iterator PARTS_end11 = PARTS.end();
    for (; PARTS_it12 != PARTS_end11; ++PARTS_it12)
    {
        int64_t protect_P__PARTKEY = get<0>(*PARTS_it12);
        string protect_P__NAME = get<1>(*PARTS_it12);
        string protect_P__MFGR = get<2>(*PARTS_it12);
        string protect_P__BRAND = get<3>(*PARTS_it12);
        string protect_P__TYPE = get<4>(*PARTS_it12);
        int protect_P__SIZE = get<5>(*PARTS_it12);
        string protect_P__CONTAINER = get<6>(*PARTS_it12);
        double protect_P__RETAILPRICE = get<7>(*PARTS_it12);
        string protect_P__COMMENT = get<8>(*PARTS_it12);
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
            double var11 = 0;
            var11 += protect_L__QUANTITY;
            double var12 = 0;
            double var13 = 1;
            var13 *= 0.005;
            double var14 = 0;
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_it8 = LINEITEM.begin();
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_end7 = LINEITEM.end();
            for (; LINEITEM_it8 != LINEITEM_end7; ++LINEITEM_it8)
            {
                int64_t L2__ORDERKEY = get<0>(*LINEITEM_it8);
                int64_t L2__PARTKEY = get<1>(*LINEITEM_it8);
                int64_t L2__SUPPKEY = get<2>(*LINEITEM_it8);
                int L2__LINENUMBER = get<3>(*LINEITEM_it8);
                double L2__QUANTITY = get<4>(*LINEITEM_it8);
                double L2__EXTENDEDPRICE = get<5>(*LINEITEM_it8);
                double L2__DISCOUNT = get<6>(*LINEITEM_it8);
                double L2__TAX = get<7>(*LINEITEM_it8);
                int64_t var15 = 0;
                var15 += L2__PARTKEY;
                int64_t var16 = 0;
                var16 += protect_P__PARTKEY;
                if ( var15 == var16 )
                {
                    var14 += L2__QUANTITY;
                }
            }
            var13 *= var14;
            var12 += var13;
            if ( var11 < var12 )
            {
                int64_t var9 = 0;
                var9 += protect_P__PARTKEY;
                int64_t var10 = 0;
                var10 += protect_L__PARTKEY;
                if ( var9 == var10 )
                {
                    q += protect_L__EXTENDEDPRICE;
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
    q = 0;
    multiset<tuple<int64_t,string,string,string,string,int,string,double,
        string> >::iterator PARTS_it18 = PARTS.begin();
    multiset<tuple<int64_t,string,string,string,string,int,string,double,
        string> >::iterator PARTS_end17 = PARTS.end();
    for (; PARTS_it18 != PARTS_end17; ++PARTS_it18)
    {
        int64_t protect_P__PARTKEY = get<0>(*PARTS_it18);
        string protect_P__NAME = get<1>(*PARTS_it18);
        string protect_P__MFGR = get<2>(*PARTS_it18);
        string protect_P__BRAND = get<3>(*PARTS_it18);
        string protect_P__TYPE = get<4>(*PARTS_it18);
        int protect_P__SIZE = get<5>(*PARTS_it18);
        string protect_P__CONTAINER = get<6>(*PARTS_it18);
        double protect_P__RETAILPRICE = get<7>(*PARTS_it18);
        string protect_P__COMMENT = get<8>(*PARTS_it18);
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_it16 = LINEITEM.begin();
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_end15 = LINEITEM.end();
        for (; LINEITEM_it16 != LINEITEM_end15; ++LINEITEM_it16)
        {
            int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it16);
            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it16);
            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it16);
            int protect_L__LINENUMBER = get<3>(*LINEITEM_it16);
            double protect_L__QUANTITY = get<4>(*LINEITEM_it16);
            double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it16);
            double protect_L__DISCOUNT = get<6>(*LINEITEM_it16);
            double protect_L__TAX = get<7>(*LINEITEM_it16);
            double var19 = 0;
            var19 += protect_L__QUANTITY;
            double var20 = 0;
            double var21 = 1;
            var21 *= 0.005;
            double var22 = 0;
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_it14 = LINEITEM.begin();
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_end13 = LINEITEM.end();
            for (; LINEITEM_it14 != LINEITEM_end13; ++LINEITEM_it14)
            {
                int64_t L2__ORDERKEY = get<0>(*LINEITEM_it14);
                int64_t L2__PARTKEY = get<1>(*LINEITEM_it14);
                int64_t L2__SUPPKEY = get<2>(*LINEITEM_it14);
                int L2__LINENUMBER = get<3>(*LINEITEM_it14);
                double L2__QUANTITY = get<4>(*LINEITEM_it14);
                double L2__EXTENDEDPRICE = get<5>(*LINEITEM_it14);
                double L2__DISCOUNT = get<6>(*LINEITEM_it14);
                double L2__TAX = get<7>(*LINEITEM_it14);
                int64_t var23 = 0;
                var23 += L2__PARTKEY;
                int64_t var24 = 0;
                var24 += protect_P__PARTKEY;
                if ( var23 == var24 )
                {
                    var22 += L2__QUANTITY;
                }
            }
            var21 *= var22;
            var20 += var21;
            if ( var19 < var20 )
            {
                int64_t var17 = 0;
                var17 += protect_P__PARTKEY;
                int64_t var18 = 0;
                var18 += protect_L__PARTKEY;
                if ( var17 == var18 )
                {
                    q += protect_L__EXTENDEDPRICE;
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
    q = 0;
    multiset<tuple<int64_t,string,string,string,string,int,string,double,
        string> >::iterator PARTS_it24 = PARTS.begin();
    multiset<tuple<int64_t,string,string,string,string,int,string,double,
        string> >::iterator PARTS_end23 = PARTS.end();
    for (; PARTS_it24 != PARTS_end23; ++PARTS_it24)
    {
        int64_t protect_P__PARTKEY = get<0>(*PARTS_it24);
        string protect_P__NAME = get<1>(*PARTS_it24);
        string protect_P__MFGR = get<2>(*PARTS_it24);
        string protect_P__BRAND = get<3>(*PARTS_it24);
        string protect_P__TYPE = get<4>(*PARTS_it24);
        int protect_P__SIZE = get<5>(*PARTS_it24);
        string protect_P__CONTAINER = get<6>(*PARTS_it24);
        double protect_P__RETAILPRICE = get<7>(*PARTS_it24);
        string protect_P__COMMENT = get<8>(*PARTS_it24);
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_it22 = LINEITEM.begin();
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_end21 = LINEITEM.end();
        for (; LINEITEM_it22 != LINEITEM_end21; ++LINEITEM_it22)
        {
            int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it22);
            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it22);
            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it22);
            int protect_L__LINENUMBER = get<3>(*LINEITEM_it22);
            double protect_L__QUANTITY = get<4>(*LINEITEM_it22);
            double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it22);
            double protect_L__DISCOUNT = get<6>(*LINEITEM_it22);
            double protect_L__TAX = get<7>(*LINEITEM_it22);
            double var27 = 0;
            var27 += protect_L__QUANTITY;
            double var28 = 0;
            double var29 = 1;
            var29 *= 0.005;
            double var30 = 0;
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_it20 = LINEITEM.begin();
            
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_end19 = LINEITEM.end();
            for (; LINEITEM_it20 != LINEITEM_end19; ++LINEITEM_it20)
            {
                int64_t L2__ORDERKEY = get<0>(*LINEITEM_it20);
                int64_t L2__PARTKEY = get<1>(*LINEITEM_it20);
                int64_t L2__SUPPKEY = get<2>(*LINEITEM_it20);
                int L2__LINENUMBER = get<3>(*LINEITEM_it20);
                double L2__QUANTITY = get<4>(*LINEITEM_it20);
                double L2__EXTENDEDPRICE = get<5>(*LINEITEM_it20);
                double L2__DISCOUNT = get<6>(*LINEITEM_it20);
                double L2__TAX = get<7>(*LINEITEM_it20);
                int64_t var31 = 0;
                var31 += L2__PARTKEY;
                int64_t var32 = 0;
                var32 += protect_P__PARTKEY;
                if ( var31 == var32 )
                {
                    var30 += L2__QUANTITY;
                }
            }
            var29 *= var30;
            var28 += var29;
            if ( var27 < var28 )
            {
                int64_t var25 = 0;
                var25 += protect_P__PARTKEY;
                int64_t var26 = 0;
                var26 += protect_L__PARTKEY;
                if ( var25 == var26 )
                {
                    q += protect_L__EXTENDEDPRICE;
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
