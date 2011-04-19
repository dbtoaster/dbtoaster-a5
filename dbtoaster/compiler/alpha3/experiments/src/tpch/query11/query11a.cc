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
map<int64_t,double,std::less<int64_t 
    >,boost::pool_allocator<pair<int64_t,double> > > q;

map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
    >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > > qSUPPLIER1;

map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > > 
    qPARTSUPP1;

double on_insert_SUPPLIER_sec_span = 0.0;
double on_insert_SUPPLIER_usec_span = 0.0;
double on_insert_PARTSUPP_sec_span = 0.0;
double on_insert_PARTSUPP_usec_span = 0.0;
double on_delete_SUPPLIER_sec_span = 0.0;
double on_delete_SUPPLIER_usec_span = 0.0;
double on_delete_PARTSUPP_sec_span = 0.0;
double on_delete_PARTSUPP_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
   cout << "qSUPPLIER1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type, _Select1st<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type>, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type, _Select1st<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type>, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_compare>))) << endl;

   cout << "q size: " << (((sizeof(map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_type)
       + sizeof(map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_type, map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::value_type, _Select1st<map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::value_type>, map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_compare>))) << endl;

   (*stats) << "m," << "q" << "," << (((sizeof(map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_type)
       + sizeof(map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_type, map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::value_type, _Select1st<map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::value_type>, map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_compare>))) << endl;

   cout << "qPARTSUPP1 size: " << (((sizeof(map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_type)
       + sizeof(map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTSUPP1.size())  + (sizeof(struct _Rb_tree<map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_type, map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::value_type, _Select1st<map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::value_type>, map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_compare>))) << endl;

   (*stats) << "m," << "qPARTSUPP1" << "," << (((sizeof(map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_type)
       + sizeof(map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTSUPP1.size())  + (sizeof(struct _Rb_tree<map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_type, map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::value_type, _Select1st<map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::value_type>, map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_compare>))) << endl;

}

void analyse_handler_usage(ofstream* stats)
{
   cout << "on_insert_SUPPLIER cost: " << (on_insert_SUPPLIER_sec_span + (on_insert_SUPPLIER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_SUPPLIER" << "," << (on_insert_SUPPLIER_sec_span + (on_insert_SUPPLIER_usec_span / 1000000.0)) << endl;
   cout << "on_insert_PARTSUPP cost: " << (on_insert_PARTSUPP_sec_span + (on_insert_PARTSUPP_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_PARTSUPP" << "," << (on_insert_PARTSUPP_sec_span + (on_insert_PARTSUPP_usec_span / 1000000.0)) << endl;
   cout << "on_delete_SUPPLIER cost: " << (on_delete_SUPPLIER_sec_span + (on_delete_SUPPLIER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_SUPPLIER" << "," << (on_delete_SUPPLIER_sec_span + (on_delete_SUPPLIER_usec_span / 1000000.0)) << endl;
   cout << "on_delete_PARTSUPP cost: " << (on_delete_PARTSUPP_sec_span + (on_delete_PARTSUPP_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_PARTSUPP" << "," << (on_delete_PARTSUPP_sec_span + (on_delete_PARTSUPP_usec_span / 1000000.0)) << endl;
}


void on_insert_SUPPLIER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t SUPPKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it2 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end1 = q.end();
    for (; q_it2 != q_end1; ++q_it2)
    {
        int64_t PS__PARTKEY = q_it2->first;
        q[PS__PARTKEY] += qSUPPLIER1[make_tuple(PS__PARTKEY,SUPPKEY)];
    }
    qPARTSUPP1[SUPPKEY] += 1;
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_SUPPLIER_sec_span, on_insert_SUPPLIER_usec_span);
}

void on_insert_PARTSUPP(
    ofstream* results, ofstream* log, ofstream* stats, int64_t PARTKEY,int64_t 
    SUPPKEY,int AVAILQTY,double SUPPLYCOST,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    q[PARTKEY] += SUPPLYCOST*AVAILQTY*qPARTSUPP1[SUPPKEY];
    qSUPPLIER1[make_tuple(PARTKEY,SUPPKEY)] += SUPPLYCOST*AVAILQTY;
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_PARTSUPP_sec_span, on_insert_PARTSUPP_usec_span);
}

void on_delete_SUPPLIER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t SUPPKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it4 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end3 = q.end();
    for (; q_it4 != q_end3; ++q_it4)
    {
        int64_t PS__PARTKEY = q_it4->first;
        q[PS__PARTKEY] += -1*qSUPPLIER1[make_tuple(PS__PARTKEY,SUPPKEY)];
    }
    qPARTSUPP1[SUPPKEY] += -1;
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_SUPPLIER_sec_span, on_delete_SUPPLIER_usec_span);
}

void on_delete_PARTSUPP(
    ofstream* results, ofstream* log, ofstream* stats, int64_t PARTKEY,int64_t 
    SUPPKEY,int AVAILQTY,double SUPPLYCOST,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    q[PARTKEY] += -1*SUPPLYCOST*AVAILQTY*qPARTSUPP1[SUPPKEY];
    qSUPPLIER1[make_tuple(PARTKEY,SUPPKEY)] += -1*SUPPLYCOST*AVAILQTY;
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_PARTSUPP_sec_span, on_delete_PARTSUPP_usec_span);
}

DBToaster::DemoDatasets::SupplierStream SSBSupplier("/Users/yanif/datasets/tpch/sf1/singlefile/supplier.tbl.a",&DBToaster::DemoDatasets::parseSupplierField,7,110000,512);

boost::shared_ptr<DBToaster::DemoDatasets::SupplierTupleAdaptor> SSBSupplier_adaptor(new DBToaster::DemoDatasets::SupplierTupleAdaptor());
static int streamSSBSupplierId = 0;

struct on_insert_SUPPLIER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::SupplierTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::SupplierTupleAdaptor::Result>(data); 
        on_insert_SUPPLIER(results, log, stats, input.suppkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.comment);
    }
};

on_insert_SUPPLIER_fun_obj fo_on_insert_SUPPLIER_0;

DBToaster::DemoDatasets::PartSuppStream SSBPartSupp("/Users/yanif/datasets/tpch/sf1/singlefile/partsupp.tbl.a",&DBToaster::DemoDatasets::parsePartSuppField,5,820000,512);

boost::shared_ptr<DBToaster::DemoDatasets::PartSuppTupleAdaptor> SSBPartSupp_adaptor(new DBToaster::DemoDatasets::PartSuppTupleAdaptor());
static int streamSSBPartSuppId = 1;

struct on_insert_PARTSUPP_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::PartSuppTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::PartSuppTupleAdaptor::Result>(data); 
        on_insert_PARTSUPP(results, log, stats, input.partkey,input.suppkey,input.availqty,input.supplycost,input.comment);
    }
};

on_insert_PARTSUPP_fun_obj fo_on_insert_PARTSUPP_1;

struct on_delete_SUPPLIER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::SupplierTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::SupplierTupleAdaptor::Result>(data); 
        on_delete_SUPPLIER(results, log, stats, input.suppkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.comment);
    }
};

on_delete_SUPPLIER_fun_obj fo_on_delete_SUPPLIER_2;

struct on_delete_PARTSUPP_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::PartSuppTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::PartSuppTupleAdaptor::Result>(data); 
        on_delete_PARTSUPP(results, log, stats, input.partkey,input.suppkey,input.availqty,input.supplycost,input.comment);
    }
};

on_delete_PARTSUPP_fun_obj fo_on_delete_PARTSUPP_3;


void init(DBToaster::StandaloneEngine::FileMultiplexer& sources,
    DBToaster::StandaloneEngine::FileStreamDispatcher& router, 
    ofstream* results, ofstream* log, ofstream* stats)
{
    router.setFiles(results, log, stats);
    sources.addStream<DBToaster::DemoDatasets::supplier>(&SSBSupplier, boost::ref(*SSBSupplier_adaptor), streamSSBSupplierId);
    router.addHandler(streamSSBSupplierId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_SUPPLIER_0));
    sources.addStream<DBToaster::DemoDatasets::partsupp>(&SSBPartSupp, boost::ref(*SSBPartSupp_adaptor), streamSSBPartSuppId);
    router.addHandler(streamSSBPartSuppId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_PARTSUPP_1));
    router.addHandler(streamSSBSupplierId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_SUPPLIER_2));
    router.addHandler(streamSSBPartSuppId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_PARTSUPP_3));
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
