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
double qBIDS360;
set<double> bigsum_B__P_dom;
double q65;
double q88;
double q;
map<double,double> qBIDS1;
double q110;
double qBIDS2;
map<double,double> qBIDS3;
double q37;
double qBIDS3139;

double on_insert_BIDS_sec_span = 0.0;
double on_insert_BIDS_usec_span = 0.0;
double on_delete_BIDS_sec_span = 0.0;
double on_delete_BIDS_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
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

   cout << "qBIDS3 size: " << (((sizeof(map<double,double>::key_type)
       + sizeof(map<double,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS3.size())  + (sizeof(struct _Rb_tree<map<double,double>::key_type, map<double,double>::value_type, _Select1st<map<double,double>::value_type>, map<double,double>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS3" << "," << (((sizeof(map<double,double>::key_type)
       + sizeof(map<double,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS3.size())  + (sizeof(struct _Rb_tree<map<double,double>::key_type, map<double,double>::value_type, _Select1st<map<double,double>::value_type>, map<double,double>::key_compare>))) << endl;

}

void analyse_handler_usage(ofstream* stats)
{
   cout << "on_insert_BIDS cost: " << (on_insert_BIDS_sec_span + (on_insert_BIDS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_BIDS" << "," << (on_insert_BIDS_sec_span + (on_insert_BIDS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_BIDS cost: " << (on_delete_BIDS_sec_span + (on_delete_BIDS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_BIDS" << "," << (on_delete_BIDS_sec_span + (on_delete_BIDS_usec_span / 1000000.0)) << endl;
}


void on_insert_BIDS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    bigsum_B__P_dom.insert(P);
    q37 = 0.0;
    set<double>::iterator bigsum_B__P_dom_it2 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end1 = bigsum_B__P_dom.end();
    for (; bigsum_B__P_dom_it2 != bigsum_B__P_dom_end1; ++bigsum_B__P_dom_it2)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it2;
        q37 += ( ( ( 0.25*qBIDS2+0.25*V <= qBIDS1[bigsum_B__P]+V*( (
             bigsum_B__P < P )? ( 1 ) : ( 0 ) ) ) && (
             qBIDS1[bigsum_B__P] < 0.25*qBIDS2 ) )? ( qBIDS3[bigsum_B__P] ) : ( 0 ) );
    }
    q += -1*q37;
    set<double>::iterator bigsum_B__P_dom_it4 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end3 = bigsum_B__P_dom.end();
    for (; bigsum_B__P_dom_it4 != bigsum_B__P_dom_end3; ++bigsum_B__P_dom_it4)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it4;
        q += ( ( qBIDS1[bigsum_B__P]+V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS2+0.25*V )? ( ( ( P == bigsum_B__P )?
             ( P ) : ( 0 ) )*V ) : ( 0 ) );
    }
    set<double>::iterator bigsum_B__P_dom_it6 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end5 = bigsum_B__P_dom.end();
    for (; bigsum_B__P_dom_it6 != bigsum_B__P_dom_end5; ++bigsum_B__P_dom_it6)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it6;
        q += ( ( ( qBIDS1[bigsum_B__P]+V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS2+0.25*V ) && (
             0.25*qBIDS2 <= qBIDS1[bigsum_B__P] ) )? ( qBIDS3[bigsum_B__P] ) : ( 0 ) );
    }
    map<double,double>::iterator qBIDS1_it8 = qBIDS1.begin();
    map<double,double>::iterator qBIDS1_end7 = qBIDS1.end();
    for (; qBIDS1_it8 != qBIDS1_end7; ++qBIDS1_it8)
    {
        double bigsum_B__P = qBIDS1_it8->first;
        qBIDS1[bigsum_B__P] += V*( ( bigsum_B__P < P )? ( 1 ) : ( 0 ) );
    }
    qBIDS2 += V;
    map<double,double>::iterator qBIDS3_it12 = qBIDS3.begin();
    map<double,double>::iterator qBIDS3_end11 = qBIDS3.end();
    for (; qBIDS3_it12 != qBIDS3_end11; ++qBIDS3_it12)
    {
        double bigsum_B__P = qBIDS3_it12->first;
        qBIDS360 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it10 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end9 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it10 != bigsum_B__P_dom_end9; ++bigsum_B__P_dom_it10)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it10;
            qBIDS360 += ( ( bigsum_B__P == P )? ( bigsum_B__P ) : ( 0 ) );
        }
        qBIDS3[bigsum_B__P] += V*qBIDS360;
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_BIDS_sec_span, on_insert_BIDS_usec_span);
    (*results) << "on_insert_BIDS" << "," << q << endl;
}

void on_delete_BIDS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    q110 = 0.0;
    set<double>::iterator bigsum_B__P_dom_it14 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end13 = bigsum_B__P_dom.end();
    for (
        ; bigsum_B__P_dom_it14 != bigsum_B__P_dom_end13; ++bigsum_B__P_dom_it14)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it14;
        q110 += ( ( ( 0.25*qBIDS2+-1*0.25*V <= qBIDS1[bigsum_B__P]+-1*V*( (
             bigsum_B__P < P )? ( 1 ) : ( 0 ) ) ) && (
             qBIDS1[bigsum_B__P] < 0.25*qBIDS2 ) )? ( qBIDS3[bigsum_B__P] ) : ( 0 ) );
    }
    q += -1*q110;
    q65 = 0.0;
    set<double>::iterator bigsum_B__P_dom_it16 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end15 = bigsum_B__P_dom.end();
    for (
        ; bigsum_B__P_dom_it16 != bigsum_B__P_dom_end15; ++bigsum_B__P_dom_it16)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it16;
        q65 += ( ( qBIDS1[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS2+-1*0.25*V )? ( ( ( P == bigsum_B__P )?
             ( P ) : ( 0 ) )*V ) : ( 0 ) );
    }
    q += -1*q65;
    q88 = 0.0;
    set<double>::iterator bigsum_B__P_dom_it18 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end17 = bigsum_B__P_dom.end();
    for (
        ; bigsum_B__P_dom_it18 != bigsum_B__P_dom_end17; ++bigsum_B__P_dom_it18)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it18;
        q88 += ( ( ( qBIDS1[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS2+-1*0.25*V ) && (
             0.25*qBIDS2 <= qBIDS1[bigsum_B__P] ) )? ( qBIDS3[bigsum_B__P] ) : ( 0 ) );
    }
    q += -1*-1*q88;
    map<double,double>::iterator qBIDS1_it20 = qBIDS1.begin();
    map<double,double>::iterator qBIDS1_end19 = qBIDS1.end();
    for (; qBIDS1_it20 != qBIDS1_end19; ++qBIDS1_it20)
    {
        double bigsum_B__P = qBIDS1_it20->first;
        qBIDS1[bigsum_B__P] += -1*V*( ( bigsum_B__P < P )? ( 1 ) : ( 0 ) );
    }
    qBIDS2 += -1*V;
    map<double,double>::iterator qBIDS3_it24 = qBIDS3.begin();
    map<double,double>::iterator qBIDS3_end23 = qBIDS3.end();
    for (; qBIDS3_it24 != qBIDS3_end23; ++qBIDS3_it24)
    {
        double bigsum_B__P = qBIDS3_it24->first;
        qBIDS3139 = 0.0;
        set<double>::iterator bigsum_B__P_dom_it22 = bigsum_B__P_dom.begin();
        set<double>::iterator bigsum_B__P_dom_end21 = bigsum_B__P_dom.end();
        for (
            ; bigsum_B__P_dom_it22 != bigsum_B__P_dom_end21; ++bigsum_B__P_dom_it22)
        {
            double bigsum_B__P = *bigsum_B__P_dom_it22;
            qBIDS3139 += ( ( bigsum_B__P == P )? ( bigsum_B__P ) : ( 0 ) );
        }
        qBIDS3[bigsum_B__P] += -1*V*qBIDS3139;
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_BIDS_sec_span, on_delete_BIDS_usec_span);
    (*results) << "on_delete_BIDS" << "," << q << endl;
}

DBToaster::DemoDatasets::OrderbookFileStream VwapBids("/home/yanif/tmp/data/cleanedData.csv",10000);

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

struct on_delete_BIDS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor::Result>(data); 
        on_delete_BIDS(results, log, stats, input.t,input.id,input.broker_id,input.price,input.volume);
    }
};

on_delete_BIDS_fun_obj fo_on_delete_BIDS_1;


void init(DBToaster::StandaloneEngine::FileMultiplexer& sources,
    DBToaster::StandaloneEngine::FileStreamDispatcher& router, 
    ofstream* results, ofstream* log, ofstream* stats)
{
    router.setFiles(results, log, stats);
    sources.addStream<DBToaster::DemoDatasets::OrderbookTuple>(&VwapBids, boost::ref(*VwapBids_adaptor), streamVwapBidsId);
    router.addHandler(streamVwapBidsId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_BIDS_0));
    router.addHandler(streamVwapBidsId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_BIDS_1));
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
