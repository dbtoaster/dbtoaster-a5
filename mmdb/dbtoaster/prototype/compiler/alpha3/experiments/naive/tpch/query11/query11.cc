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
multiset<tuple<int64_t,int64_t,int,double,string> > PARTSUPP;

double b_q;
map<int64_t,double> q;

double on_insert_PARTSUPP_sec_span = 0.0;
double on_insert_PARTSUPP_usec_span = 0.0;
double on_insert_SUPPLIER_sec_span = 0.0;
double on_insert_SUPPLIER_usec_span = 0.0;
double on_delete_PARTSUPP_sec_span = 0.0;
double on_delete_PARTSUPP_usec_span = 0.0;
double on_delete_SUPPLIER_sec_span = 0.0;
double on_delete_SUPPLIER_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
   cout << "SUPPLIER size: " << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * SUPPLIER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::key_compare>))) << endl;

   (*stats) << "m," << "SUPPLIER" << "," << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * SUPPLIER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::key_compare>))) << endl;

   cout << "PARTSUPP size: " << (((sizeof(multiset<tuple<int64_t,int64_t,int,double,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * PARTSUPP.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,int,double,string> >::key_type, multiset<tuple<int64_t,int64_t,int,double,string> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,int,double,string> >::value_type>, multiset<tuple<int64_t,int64_t,int,double,string> >::key_compare>))) << endl;

   (*stats) << "m," << "PARTSUPP" << "," << (((sizeof(multiset<tuple<int64_t,int64_t,int,double,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * PARTSUPP.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,int,double,string> >::key_type, multiset<tuple<int64_t,int64_t,int,double,string> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,int,double,string> >::value_type>, multiset<tuple<int64_t,int64_t,int,double,string> >::key_compare>))) << endl;

   cout << "q size: " << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   (*stats) << "m," << "q" << "," << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

}

void analyse_handler_usage(ofstream* stats)
{
   cout << "on_insert_PARTSUPP cost: " << (on_insert_PARTSUPP_sec_span + (on_insert_PARTSUPP_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_PARTSUPP" << "," << (on_insert_PARTSUPP_sec_span + (on_insert_PARTSUPP_usec_span / 1000000.0)) << endl;
   cout << "on_insert_SUPPLIER cost: " << (on_insert_SUPPLIER_sec_span + (on_insert_SUPPLIER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_SUPPLIER" << "," << (on_insert_SUPPLIER_sec_span + (on_insert_SUPPLIER_usec_span / 1000000.0)) << endl;
   cout << "on_delete_PARTSUPP cost: " << (on_delete_PARTSUPP_sec_span + (on_delete_PARTSUPP_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_PARTSUPP" << "," << (on_delete_PARTSUPP_sec_span + (on_delete_PARTSUPP_usec_span / 1000000.0)) << endl;
   cout << "on_delete_SUPPLIER cost: " << (on_delete_SUPPLIER_sec_span + (on_delete_SUPPLIER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_SUPPLIER" << "," << (on_delete_SUPPLIER_sec_span + (on_delete_SUPPLIER_usec_span / 1000000.0)) << endl;
}

////////////////////////////////////////
//
// Part A plans

inline void recompute_partA()
{
    map<int64_t,double>::iterator q_it6 = q.begin();
    map<int64_t,double>::iterator q_end5 = q.end();
    for (; q_it6 != q_end5; ++q_it6)
    {
        int64_t PARTKEY = q_it6->first;
        q[PARTKEY] = 0;
        multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
            >::iterator SUPPLIER_it4 = SUPPLIER.begin();
        multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
            >::iterator SUPPLIER_end3 = SUPPLIER.end();
        for (; SUPPLIER_it4 != SUPPLIER_end3; ++SUPPLIER_it4)
        {
            int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it4);
            multiset<tuple<int64_t,int64_t,int,double,string> >::iterator 
                PARTSUPP_it2 = PARTSUPP.begin();
            multiset<tuple<int64_t,int64_t,int,double,string> >::iterator 
                PARTSUPP_end1 = PARTSUPP.end();
            for (; PARTSUPP_it2 != PARTSUPP_end1; ++PARTSUPP_it2)
            {
                int64_t protect_PS__PARTKEY = get<0>(*PARTSUPP_it2);
                int64_t protect_PS__SUPPKEY = get<1>(*PARTSUPP_it2);
                int protect_PS__AVAILQTY = get<2>(*PARTSUPP_it2);
                double protect_PS__SUPPLYCOST = get<3>(*PARTSUPP_it2);
                if ( protect_PS__SUPPKEY == protect_S__SUPPKEY
                     && PARTKEY == protect_PS__PARTKEY )
                {
                    q[PARTKEY] += (protect_PS__SUPPLYCOST*protect_PS__AVAILQTY);
                }
            }
        }
    }
}


////////////////////////////////////////
//
// Part B plans

inline void recompute_partB()
{
    b_q = 0;
    multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
        >::iterator SUPPLIER_it4 = SUPPLIER.begin();
    multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
        >::iterator SUPPLIER_end3 = SUPPLIER.end();
    for (; SUPPLIER_it4 != SUPPLIER_end3; ++SUPPLIER_it4)
    {
        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it4);
        multiset<tuple<int64_t,int64_t,int,double,string> >::iterator 
            PARTSUPP_it2 = PARTSUPP.begin();
        multiset<tuple<int64_t,int64_t,int,double,string> >::iterator 
            PARTSUPP_end1 = PARTSUPP.end();
        for (; PARTSUPP_it2 != PARTSUPP_end1; ++PARTSUPP_it2)
        {
            int64_t protect_PS__PARTKEY = get<0>(*PARTSUPP_it2);
            int64_t protect_PS__SUPPKEY = get<1>(*PARTSUPP_it2);
            int protect_PS__AVAILQTY = get<2>(*PARTSUPP_it2);
            double protect_PS__SUPPLYCOST = get<3>(*PARTSUPP_it2);
            if ( protect_PS__SUPPKEY == protect_S__SUPPKEY )
            {
                b_q += (protect_PS__SUPPLYCOST*protect_PS__AVAILQTY);
            }
        }
    }
}

////////////////////////////////////////
//
// Triggers

void on_insert_PARTSUPP(
    ofstream* results, ofstream* log, ofstream* stats, int64_t PARTKEY,int64_t 
    SUPPKEY,int AVAILQTY,double SUPPLYCOST,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    PARTSUPP.insert(make_tuple(PARTKEY,SUPPKEY,AVAILQTY,SUPPLYCOST,COMMENT));

    recompute_partA();
    recompute_partB();

    // TODO: output q[partkey] > b_q

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_PARTSUPP_sec_span, on_insert_PARTSUPP_usec_span);
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

    recompute_partA();
    recompute_partB();

    // TODO: output q[partkey] > b_q

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_SUPPLIER_sec_span, on_insert_SUPPLIER_usec_span);
}

void on_delete_PARTSUPP(
    ofstream* results, ofstream* log, ofstream* stats, int64_t PARTKEY,int64_t 
    SUPPKEY,int AVAILQTY,double SUPPLYCOST,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    PARTSUPP.erase(make_tuple(PARTKEY,SUPPKEY,AVAILQTY,SUPPLYCOST,COMMENT));

    recompute_partA();
    recompute_partB();

    // TODO: output q[partkey] > b_q

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_PARTSUPP_sec_span, on_delete_PARTSUPP_usec_span);
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

    recompute_partA();
    recompute_partB();

    // TODO: output q[partkey] > b_q

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_SUPPLIER_sec_span, on_delete_SUPPLIER_usec_span);
}

DBToaster::DemoDatasets::PartSuppStream SSBPartSupp("/home/yanif/datasets/tpch/sf1/singlefile/partsupp.tbl.a",&DBToaster::DemoDatasets::parsePartSuppField,5,820000,512);

boost::shared_ptr<DBToaster::DemoDatasets::PartSuppTupleAdaptor> SSBPartSupp_adaptor(new DBToaster::DemoDatasets::PartSuppTupleAdaptor());
static int streamSSBPartSuppId = 0;

struct on_insert_PARTSUPP_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::PartSuppTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::PartSuppTupleAdaptor::Result>(data); 
        on_insert_PARTSUPP(results, log, stats, input.partkey,input.suppkey,input.availqty,input.supplycost,input.comment);
    }
};

on_insert_PARTSUPP_fun_obj fo_on_insert_PARTSUPP_0;

DBToaster::DemoDatasets::SupplierStream SSBSupplier("/home/yanif/datasets/tpch/sf1/singlefile/supplier.tbl.a",&DBToaster::DemoDatasets::parseSupplierField,7,110000,512);

boost::shared_ptr<DBToaster::DemoDatasets::SupplierTupleAdaptor> SSBSupplier_adaptor(new DBToaster::DemoDatasets::SupplierTupleAdaptor());
static int streamSSBSupplierId = 1;

struct on_insert_SUPPLIER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::SupplierTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::SupplierTupleAdaptor::Result>(data); 
        on_insert_SUPPLIER(results, log, stats, input.suppkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.comment);
    }
};

on_insert_SUPPLIER_fun_obj fo_on_insert_SUPPLIER_1;

struct on_delete_PARTSUPP_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::PartSuppTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::PartSuppTupleAdaptor::Result>(data); 
        on_delete_PARTSUPP(results, log, stats, input.partkey,input.suppkey,input.availqty,input.supplycost,input.comment);
    }
};

on_delete_PARTSUPP_fun_obj fo_on_delete_PARTSUPP_2;

struct on_delete_SUPPLIER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::SupplierTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::SupplierTupleAdaptor::Result>(data); 
        on_delete_SUPPLIER(results, log, stats, input.suppkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.comment);
    }
};

on_delete_SUPPLIER_fun_obj fo_on_delete_SUPPLIER_3;


void init(DBToaster::StandaloneEngine::FileMultiplexer& sources,
    DBToaster::StandaloneEngine::FileStreamDispatcher& router, 
    ofstream* results, ofstream* log, ofstream* stats)
{
    router.setFiles(results, log, stats);
    sources.addStream<DBToaster::DemoDatasets::partsupp>(&SSBPartSupp, boost::ref(*SSBPartSupp_adaptor), streamSSBPartSuppId);
    router.addHandler(streamSSBPartSuppId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_PARTSUPP_0));
    sources.addStream<DBToaster::DemoDatasets::supplier>(&SSBSupplier, boost::ref(*SSBSupplier_adaptor), streamSSBSupplierId);
    router.addHandler(streamSSBSupplierId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_SUPPLIER_1));
    router.addHandler(streamSSBPartSuppId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_PARTSUPP_2));
    router.addHandler(streamSSBSupplierId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_SUPPLIER_3));
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
