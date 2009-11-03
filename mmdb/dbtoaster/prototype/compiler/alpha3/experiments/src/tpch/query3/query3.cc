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
map<int64_t,double> qORDERS1;
map<int64_t,int> qORDERS2;
map<tuple<int64_t,int64_t,int>,int> qCUSTOMER1LINEITEM1;
map<tuple<int64_t,int>,int> qLINEITEM1;
map<tuple<int,int64_t>,double> q;
map<tuple<int64_t,int64_t,int>,double> qCUSTOMER1;

double on_insert_LINEITEM_sec_span = 0.0;
double on_insert_LINEITEM_usec_span = 0.0;
double on_insert_CUSTOMER_sec_span = 0.0;
double on_insert_CUSTOMER_usec_span = 0.0;
double on_insert_ORDERS_sec_span = 0.0;
double on_insert_ORDERS_usec_span = 0.0;
double on_delete_LINEITEM_sec_span = 0.0;
double on_delete_LINEITEM_usec_span = 0.0;
double on_delete_CUSTOMER_sec_span = 0.0;
double on_delete_CUSTOMER_usec_span = 0.0;
double on_delete_ORDERS_sec_span = 0.0;
double on_delete_ORDERS_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
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

   cout << "qCUSTOMER1LINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int>,int>::key_type, map<tuple<int64_t,int64_t,int>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int>,int>::value_type>, map<tuple<int64_t,int64_t,int>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1LINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int>,int>::key_type, map<tuple<int64_t,int64_t,int>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int>,int>::value_type>, map<tuple<int64_t,int64_t,int>,int>::key_compare>))) << endl;

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
   cout << "on_insert_CUSTOMER cost: " << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_CUSTOMER" << "," << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   cout << "on_insert_ORDERS cost: " << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_ORDERS" << "," << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_LINEITEM cost: " << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_LINEITEM" << "," << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_delete_CUSTOMER cost: " << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_CUSTOMER" << "," << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
   cout << "on_delete_ORDERS cost: " << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_ORDERS" << "," << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
}


void on_insert_LINEITEM(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    PARTKEY,int64_t SUPPKEY,int LINENUMBER,double QUANTITY,double 
    EXTENDEDPRICE,double DISCOUNT,double TAX)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<tuple<int,int64_t>,double>::iterator q_it2 = q.begin();
    map<tuple<int,int64_t>,double>::iterator q_end1 = q.end();
    for (; q_it2 != q_end1; ++q_it2)
    {
        int O__SHIPPRIORITY = get<0>(q_it2->first);
        q[make_tuple(
            O__SHIPPRIORITY,ORDERKEY)] += EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,O__SHIPPRIORITY)];
    }
    map<tuple<int64_t,int64_t,int>,double>::iterator qCUSTOMER1_it4 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,int>,double>::iterator qCUSTOMER1_end3 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it4 != qCUSTOMER1_end3; ++qCUSTOMER1_it4)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1_it4->first);
        int O__SHIPPRIORITY = get<2>(qCUSTOMER1_it4->first);
        qCUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY,O__SHIPPRIORITY)] += 
            EXTENDEDPRICE*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY,O__SHIPPRIORITY)];
    }
    qORDERS1[ORDERKEY] += EXTENDEDPRICE;
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_LINEITEM_sec_span, on_insert_LINEITEM_usec_span);
}

void on_insert_CUSTOMER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t CUSTKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    MKTSEGMENT,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<tuple<int,int64_t>,double>::iterator q_it6 = q.begin();
    map<tuple<int,int64_t>,double>::iterator q_end5 = q.end();
    for (; q_it6 != q_end5; ++q_it6)
    {
        int O__SHIPPRIORITY = get<0>(q_it6->first);
        int64_t L__ORDERKEY = get<1>(q_it6->first);
        q[make_tuple(O__SHIPPRIORITY,L__ORDERKEY)] += qCUSTOMER1[make_tuple(
            L__ORDERKEY,CUSTKEY,O__SHIPPRIORITY)];
    }
    map<tuple<int64_t,int>,int>::iterator qLINEITEM1_it8 = qLINEITEM1.begin();
    map<tuple<int64_t,int>,int>::iterator qLINEITEM1_end7 = qLINEITEM1.end();
    for (; qLINEITEM1_it8 != qLINEITEM1_end7; ++qLINEITEM1_it8)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it8->first);
        int O__SHIPPRIORITY = get<1>(qLINEITEM1_it8->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,O__SHIPPRIORITY)] += qCUSTOMER1LINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY,O__SHIPPRIORITY)];
    }
    qORDERS2[CUSTKEY] += 1;
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_CUSTOMER_sec_span, on_insert_CUSTOMER_usec_span);
}

void on_insert_ORDERS(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    CUSTKEY,string ORDERSTATUS,double TOTALPRICE,string ORDERDATE,string 
    ORDERPRIORITY,string CLERK,int SHIPPRIORITY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    q[make_tuple(
        SHIPPRIORITY,ORDERKEY)] += qORDERS1[ORDERKEY]*qORDERS2[CUSTKEY];
    qCUSTOMER1[make_tuple(ORDERKEY,CUSTKEY,SHIPPRIORITY)] += qORDERS1[ORDERKEY];
    qCUSTOMER1LINEITEM1[make_tuple(ORDERKEY,CUSTKEY,SHIPPRIORITY)] += 1;
    qLINEITEM1[make_tuple(ORDERKEY,SHIPPRIORITY)] += qORDERS2[CUSTKEY];
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_ORDERS_sec_span, on_insert_ORDERS_usec_span);
}

void on_delete_LINEITEM(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    PARTKEY,int64_t SUPPKEY,int LINENUMBER,double QUANTITY,double 
    EXTENDEDPRICE,double DISCOUNT,double TAX)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<tuple<int,int64_t>,double>::iterator q_it10 = q.begin();
    map<tuple<int,int64_t>,double>::iterator q_end9 = q.end();
    for (; q_it10 != q_end9; ++q_it10)
    {
        int O__SHIPPRIORITY = get<0>(q_it10->first);
        q[make_tuple(
            O__SHIPPRIORITY,ORDERKEY)] += -1*EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,O__SHIPPRIORITY)];
    }
    map<tuple<int64_t,int64_t,int>,double>::iterator qCUSTOMER1_it12 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,int>,double>::iterator qCUSTOMER1_end11 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it12 != qCUSTOMER1_end11; ++qCUSTOMER1_it12)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1_it12->first);
        int O__SHIPPRIORITY = get<2>(qCUSTOMER1_it12->first);
        qCUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY,O__SHIPPRIORITY)] += 
            -1*EXTENDEDPRICE*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY,O__SHIPPRIORITY)];
    }
    qORDERS1[ORDERKEY] += -1*EXTENDEDPRICE;
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_LINEITEM_sec_span, on_delete_LINEITEM_usec_span);
}

void on_delete_CUSTOMER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t CUSTKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    MKTSEGMENT,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<tuple<int,int64_t>,double>::iterator q_it14 = q.begin();
    map<tuple<int,int64_t>,double>::iterator q_end13 = q.end();
    for (; q_it14 != q_end13; ++q_it14)
    {
        int O__SHIPPRIORITY = get<0>(q_it14->first);
        int64_t L__ORDERKEY = get<1>(q_it14->first);
        q[make_tuple(O__SHIPPRIORITY,L__ORDERKEY)] += -1*qCUSTOMER1[make_tuple(
            L__ORDERKEY,CUSTKEY,O__SHIPPRIORITY)];
    }
    map<tuple<int64_t,int>,int>::iterator qLINEITEM1_it16 = qLINEITEM1.begin();
    map<tuple<int64_t,int>,int>::iterator qLINEITEM1_end15 = qLINEITEM1.end();
    for (; qLINEITEM1_it16 != qLINEITEM1_end15; ++qLINEITEM1_it16)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it16->first);
        int O__SHIPPRIORITY = get<1>(qLINEITEM1_it16->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,O__SHIPPRIORITY)] += -1*qCUSTOMER1LINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY,O__SHIPPRIORITY)];
    }
    qORDERS2[CUSTKEY] += -1;
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_CUSTOMER_sec_span, on_delete_CUSTOMER_usec_span);
}

void on_delete_ORDERS(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    CUSTKEY,string ORDERSTATUS,double TOTALPRICE,string ORDERDATE,string 
    ORDERPRIORITY,string CLERK,int SHIPPRIORITY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    q[make_tuple(
        SHIPPRIORITY,ORDERKEY)] += -1*qORDERS1[ORDERKEY]*qORDERS2[CUSTKEY];
    qCUSTOMER1[make_tuple(
        ORDERKEY,CUSTKEY,SHIPPRIORITY)] += -1*qORDERS1[ORDERKEY];
    qCUSTOMER1LINEITEM1[make_tuple(ORDERKEY,CUSTKEY,SHIPPRIORITY)] += -1;
    qLINEITEM1[make_tuple(ORDERKEY,SHIPPRIORITY)] += -1*qORDERS2[CUSTKEY];
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_ORDERS_sec_span, on_delete_ORDERS_usec_span);
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

DBToaster::DemoDatasets::CustomerStream SSBCustomer("/home/yanif/datasets/tpch/sf1/singlefile/customer.tbl.a",&DBToaster::DemoDatasets::parseCustomerField,8,1600000, 512);

boost::shared_ptr<DBToaster::DemoDatasets::CustomerTupleAdaptor> SSBCustomer_adaptor(new DBToaster::DemoDatasets::CustomerTupleAdaptor());
static int streamSSBCustomerId = 1;

struct on_insert_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_insert_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_insert_CUSTOMER_fun_obj fo_on_insert_CUSTOMER_1;

DBToaster::DemoDatasets::OrderStream SSBOrder("/home/yanif/datasets/tpch/sf1/singlefile/orders.tbl.a",&DBToaster::DemoDatasets::parseOrderField,9,17000000,512);

boost::shared_ptr<DBToaster::DemoDatasets::OrderTupleAdaptor> SSBOrder_adaptor(new DBToaster::DemoDatasets::OrderTupleAdaptor());
static int streamSSBOrderId = 2;

struct on_insert_ORDERS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::OrderTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::OrderTupleAdaptor::Result>(data); 
        on_insert_ORDERS(results, log, stats, input.orderkey,input.custkey,input.orderstatus,input.totalprice,input.orderdate,input.orderpriority,input.clerk,input.shippriority,input.comment);
    }
};

on_insert_ORDERS_fun_obj fo_on_insert_ORDERS_2;

struct on_delete_LINEITEM_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result>(data); 
        on_delete_LINEITEM(results, log, stats, input.orderkey,input.partkey,input.suppkey,input.linenumber,input.quantity,input.extendedprice,input.discount,input.tax);
    }
};

on_delete_LINEITEM_fun_obj fo_on_delete_LINEITEM_3;

struct on_delete_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_delete_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_delete_CUSTOMER_fun_obj fo_on_delete_CUSTOMER_4;

struct on_delete_ORDERS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::OrderTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::OrderTupleAdaptor::Result>(data); 
        on_delete_ORDERS(results, log, stats, input.orderkey,input.custkey,input.orderstatus,input.totalprice,input.orderdate,input.orderpriority,input.clerk,input.shippriority,input.comment);
    }
};

on_delete_ORDERS_fun_obj fo_on_delete_ORDERS_5;


void init(DBToaster::StandaloneEngine::FileMultiplexer& sources,
    DBToaster::StandaloneEngine::FileStreamDispatcher& router, 
    ofstream* results, ofstream* log, ofstream* stats)
{
    router.setFiles(results, log, stats);
    sources.addStream<DBToaster::DemoDatasets::lineitem>(&SSBLineitem, boost::ref(*SSBLineitem_adaptor), streamSSBLineitemId);
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_LINEITEM_0));
    sources.addStream<DBToaster::DemoDatasets::customer>(&SSBCustomer, boost::ref(*SSBCustomer_adaptor), streamSSBCustomerId);
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_CUSTOMER_1));
    sources.addStream<DBToaster::DemoDatasets::order>(&SSBOrder, boost::ref(*SSBOrder_adaptor), streamSSBOrderId);
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_ORDERS_2));
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_LINEITEM_3));
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_CUSTOMER_4));
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_ORDERS_5));
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
