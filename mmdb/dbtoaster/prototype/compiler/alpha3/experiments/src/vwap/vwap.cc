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
multiset<tuple<double,int,int,double,double>, 
    std::less<tuple<double,int,int,double,double> >, 
    boost::pool_allocator<tuple<double,int,int,double,double> > > BIDS;
double qBIDS360;
set<double, std::less<double>, boost::pool_allocator<double> > bigsum_B__P_dom;
double q65;

double q;
map<double,double,std::less<double >,boost::pool_allocator<pair<double,
    double> > > qBIDS1;

double qBIDS2;
map<double,double,std::less<double >,boost::pool_allocator<pair<double,
    double> > > qBIDS3;

double qBIDS3139;

double on_insert_BIDS_sec_span = 0.0;
double on_insert_BIDS_usec_span = 0.0;
double on_delete_BIDS_sec_span = 0.0;
double on_delete_BIDS_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
   cout << "BIDS size: " << (((sizeof(multiset<tuple<double,int,int,double,double>, std::less<tuple<double,int,int,double,double> >, boost::pool_allocator<tuple<double,int,int,double,double> > >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * BIDS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<double,int,int,double,double>, std::less<tuple<double,int,int,double,double> >, boost::pool_allocator<tuple<double,int,int,double,double> > >::key_type, multiset<tuple<double,int,int,double,double>, std::less<tuple<double,int,int,double,double> >, boost::pool_allocator<tuple<double,int,int,double,double> > >::value_type, _Identity<multiset<tuple<double,int,int,double,double>, std::less<tuple<double,int,int,double,double> >, boost::pool_allocator<tuple<double,int,int,double,double> > >::value_type>, multiset<tuple<double,int,int,double,double>, std::less<tuple<double,int,int,double,double> >, boost::pool_allocator<tuple<double,int,int,double,double> > >::key_compare>))) << endl;

   (*stats) << "m," << "BIDS" << "," << (((sizeof(multiset<tuple<double,int,int,double,double>, std::less<tuple<double,int,int,double,double> >, boost::pool_allocator<tuple<double,int,int,double,double> > >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * BIDS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<double,int,int,double,double>, std::less<tuple<double,int,int,double,double> >, boost::pool_allocator<tuple<double,int,int,double,double> > >::key_type, multiset<tuple<double,int,int,double,double>, std::less<tuple<double,int,int,double,double> >, boost::pool_allocator<tuple<double,int,int,double,double> > >::value_type, _Identity<multiset<tuple<double,int,int,double,double>, std::less<tuple<double,int,int,double,double> >, boost::pool_allocator<tuple<double,int,int,double,double> > >::value_type>, multiset<tuple<double,int,int,double,double>, std::less<tuple<double,int,int,double,double> >, boost::pool_allocator<tuple<double,int,int,double,double> > >::key_compare>))) << endl;

   cout << "bigsum_B__P_dom size: " << (((sizeof(set<double, std::less<double>, boost::pool_allocator<double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_B__P_dom.size())  + (sizeof(struct _Rb_tree<set<double, std::less<double>, boost::pool_allocator<double> >::key_type, set<double, std::less<double>, boost::pool_allocator<double> >::value_type, _Identity<set<double, std::less<double>, boost::pool_allocator<double> >::value_type>, set<double, std::less<double>, boost::pool_allocator<double> >::key_compare>))) << endl;

   (*stats) << "m," << "bigsum_B__P_dom" << "," << (((sizeof(set<double, std::less<double>, boost::pool_allocator<double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_B__P_dom.size())  + (sizeof(struct _Rb_tree<set<double, std::less<double>, boost::pool_allocator<double> >::key_type, set<double, std::less<double>, boost::pool_allocator<double> >::value_type, _Identity<set<double, std::less<double>, boost::pool_allocator<double> >::value_type>, set<double, std::less<double>, boost::pool_allocator<double> >::key_compare>))) << endl;

   cout << "qBIDS1 size: " << (((sizeof(map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::key_type)
       + sizeof(map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS1.size())  + (sizeof(struct _Rb_tree<map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::key_type, map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::value_type, _Select1st<map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::value_type>, map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS1" << "," << (((sizeof(map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::key_type)
       + sizeof(map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS1.size())  + (sizeof(struct _Rb_tree<map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::key_type, map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::value_type, _Select1st<map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::value_type>, map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::key_compare>))) << endl;

   cout << "qBIDS3 size: " << (((sizeof(map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::key_type)
       + sizeof(map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS3.size())  + (sizeof(struct _Rb_tree<map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::key_type, map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::value_type, _Select1st<map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::value_type>, map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS3" << "," << (((sizeof(map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::key_type)
       + sizeof(map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS3.size())  + (sizeof(struct _Rb_tree<map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::key_type, map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::value_type, _Select1st<map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::value_type>, map<double,double,std::less<double >,boost::pool_allocator<pair<double,double> > >::key_compare>))) << endl;

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
    if ( qBIDS1.find(P) == qBIDS1.end() )
    {
        multiset<tuple<double,int,int,double,double>, 
            std::less<tuple<double,int,int,double,double> >, 
            boost::pool_allocator<tuple<double,int,int,double,
            double> > >::iterator BIDS_it2 = BIDS.begin();
        multiset<tuple<double,int,int,double,double>, 
            std::less<tuple<double,int,int,double,double> >, 
            boost::pool_allocator<tuple<double,int,int,double,double> > >::iterator 
            BIDS_end1 = BIDS.end();
        for (; BIDS_it2 != BIDS_end1; ++BIDS_it2)
        {
            double B2__P = get<3>(*BIDS_it2);
            double B2__V = get<4>(*BIDS_it2);
            if ( P < B2__P )
            {
                qBIDS1[P] += B2__V;
            }
        }
    }
    if ( qBIDS3.find(P) == qBIDS3.end() )
    {
        multiset<tuple<double,int,int,double,double>, 
            std::less<tuple<double,int,int,double,double> >, 
            boost::pool_allocator<tuple<double,int,int,double,
            double> > >::iterator BIDS_it4 = BIDS.begin();
        multiset<tuple<double,int,int,double,double>, 
            std::less<tuple<double,int,int,double,double> >, 
            boost::pool_allocator<tuple<double,int,int,double,double> > >::iterator 
            BIDS_end3 = BIDS.end();
        for (; BIDS_it4 != BIDS_end3; ++BIDS_it4)
        {
            double protect_B__P = get<3>(*BIDS_it4);
            double protect_B__V = get<4>(*BIDS_it4);
            if ( P == protect_B__P )
            {
                qBIDS3[P] += (P * protect_B__V);
            }
        }
    }
    bigsum_B__P_dom.insert(P);
    BIDS.insert(make_tuple(T,ID,BROKER_ID,P,V));

    set<double, std::less<double>, boost::pool_allocator<double> >::iterator 
        bigsum_B__P_dom_it6 = bigsum_B__P_dom.begin();
    set<double, std::less<double>, boost::pool_allocator<double> >::iterator 
        bigsum_B__P_dom_end5 = bigsum_B__P_dom.end();
    for (; bigsum_B__P_dom_it6 != bigsum_B__P_dom_end5; ++bigsum_B__P_dom_it6)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it6;
        q += -1*( ( ( 0.25*qBIDS2+0.25*V <= qBIDS1[bigsum_B__P]+V*( (
                 bigsum_B__P < P )? ( 1 ) : ( 0 ) ) ) && (
                 qBIDS1[bigsum_B__P] < 0.25*qBIDS2 ) )? ( qBIDS3[bigsum_B__P] ) : ( 0 ) );
    }

    set<double, std::less<double>, boost::pool_allocator<double> >::iterator 
        bigsum_B__P_dom_it8 = bigsum_B__P_dom.begin();
    set<double, std::less<double>, boost::pool_allocator<double> >::iterator 
        bigsum_B__P_dom_end7 = bigsum_B__P_dom.end();
    for (; bigsum_B__P_dom_it8 != bigsum_B__P_dom_end7; ++bigsum_B__P_dom_it8)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it8;
        q += ( ( qBIDS1[bigsum_B__P]+V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS2+0.25*V )? ( ( ( P == bigsum_B__P )?
             ( P ) : ( 0 ) )*V ) : ( 0 ) );
    }

    set<double, std::less<double>, boost::pool_allocator<double> >::iterator 
        bigsum_B__P_dom_it10 = bigsum_B__P_dom.begin();
    set<double, std::less<double>, boost::pool_allocator<double> >::iterator 
        bigsum_B__P_dom_end9 = bigsum_B__P_dom.end();
    for (; bigsum_B__P_dom_it10 != bigsum_B__P_dom_end9; ++bigsum_B__P_dom_it10)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it10;
        q += ( ( ( qBIDS1[bigsum_B__P]+V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS2+0.25*V ) && (
             0.25*qBIDS2 <= qBIDS1[bigsum_B__P] ) )? ( qBIDS3[bigsum_B__P] ) : ( 0 ) );
    }

    map<double,double,std::less<double 
        >,boost::pool_allocator<pair<double,double> > >::iterator qBIDS1_it12 = 
        qBIDS1.begin();
    map<double,double,std::less<double 
        >,boost::pool_allocator<pair<double,double> > >::iterator qBIDS1_end11 = 
        qBIDS1.end();
    for (; qBIDS1_it12 != qBIDS1_end11; ++qBIDS1_it12)
    {
        double bigsum_B__P = qBIDS1_it12->first;
        qBIDS1[bigsum_B__P] += V*( ( bigsum_B__P < P )? ( 1 ) : ( 0 ) );
    }

    qBIDS2 += V;

    map<double,double,std::less<double 
        >,boost::pool_allocator<pair<double,double> > >::iterator qBIDS3_it16 = 
        qBIDS3.begin();
    map<double,double,std::less<double 
        >,boost::pool_allocator<pair<double,double> > >::iterator qBIDS3_end15 = 
        qBIDS3.end();
    for (; qBIDS3_it16 != qBIDS3_end15; ++qBIDS3_it16)
    {
        double bigsum_B__P = qBIDS3_it16->first;
        qBIDS360 = 0.0;

        set<double, std::less<double>,
             boost::pool_allocator<double> >::iterator bigsum_B__P_dom_it14 = 
            bigsum_B__P_dom.find(P);

        if ( bigsum_B__P_dom_it14 != bigsum_B__P_dom.end() )
        {
            qBIDS3[bigsum_B__P] += (V*bigsum_B__P);
        }
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
    tuple<double, int, int, double, double> t = make_tuple(T,ID,BROKER_ID,P,V);
    BIDS.erase(t);
    // TODO: keep a count of P-values, i.e. select p,count(*) from bids group by p
    // For now we'll skip domain finalization
    /*
    if ( BIDS.find(t) == BIDS.end() )
    {
        bigsum_B__P_dom.erase(P);
        qBIDS1.erase(P);
        qBIDS3.erase(P);
    }
    */

    set<double, std::less<double>, boost::pool_allocator<double> >::iterator 
        bigsum_B__P_dom_it18 = bigsum_B__P_dom.begin();
    set<double, std::less<double>, boost::pool_allocator<double> >::iterator 
        bigsum_B__P_dom_end17 = bigsum_B__P_dom.end();
    for (
        ; bigsum_B__P_dom_it18 != bigsum_B__P_dom_end17; ++bigsum_B__P_dom_it18)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it18;
        q += -1*( ( ( 0.25*qBIDS2+-1*0.25*V <= qBIDS1[bigsum_B__P]+-1*V*( (
             bigsum_B__P < P )? ( 1 ) : ( 0 ) ) ) && (
             qBIDS1[bigsum_B__P] < 0.25*qBIDS2 ) )? ( qBIDS3[bigsum_B__P] ) : ( 0 ) );
    }

    set<double, std::less<double>, boost::pool_allocator<double> >::iterator 
        bigsum_B__P_dom_it20 = bigsum_B__P_dom.begin();
    set<double, std::less<double>, boost::pool_allocator<double> >::iterator 
        bigsum_B__P_dom_end19 = bigsum_B__P_dom.end();
    for (
        ; bigsum_B__P_dom_it20 != bigsum_B__P_dom_end19; ++bigsum_B__P_dom_it20)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it20;
        q += -1*( ( qBIDS1[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS2+-1*0.25*V )? ( ( ( P == bigsum_B__P )?
             ( P ) : ( 0 ) )*V ) : ( 0 ) );
    }

    set<double, std::less<double>, boost::pool_allocator<double> >::iterator 
        bigsum_B__P_dom_it22 = bigsum_B__P_dom.begin();
    set<double, std::less<double>, boost::pool_allocator<double> >::iterator 
        bigsum_B__P_dom_end21 = bigsum_B__P_dom.end();
    for (
        ; bigsum_B__P_dom_it22 != bigsum_B__P_dom_end21; ++bigsum_B__P_dom_it22)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it22;
        q += ( ( ( qBIDS1[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS2+-1*0.25*V ) && (
             0.25*qBIDS2 <= qBIDS1[bigsum_B__P] ) )? ( qBIDS3[bigsum_B__P] ) : ( 0 ) );
    }

    map<double,double,std::less<double 
        >,boost::pool_allocator<pair<double,double> > >::iterator qBIDS1_it24 = 
        qBIDS1.begin();
    map<double,double,std::less<double 
        >,boost::pool_allocator<pair<double,double> > >::iterator qBIDS1_end23 = 
        qBIDS1.end();
    for (; qBIDS1_it24 != qBIDS1_end23; ++qBIDS1_it24)
    {
        double bigsum_B__P = qBIDS1_it24->first;
        qBIDS1[bigsum_B__P] += -1*V*( ( bigsum_B__P < P )? ( 1 ) : ( 0 ) );
    }

    qBIDS2 += -1*V;

    map<double,double,std::less<double 
        >,boost::pool_allocator<pair<double,double> > >::iterator qBIDS3_it28 = 
        qBIDS3.begin();
    map<double,double,std::less<double 
        >,boost::pool_allocator<pair<double,double> > >::iterator qBIDS3_end27 = 
        qBIDS3.end();
    for (; qBIDS3_it28 != qBIDS3_end27; ++qBIDS3_it28)
    {
        double bigsum_B__P = qBIDS3_it28->first;

        set<double, std::less<double>,
             boost::pool_allocator<double> >::iterator bigsum_B__P_dom_it26 = 
            bigsum_B__P_dom.find(P);

        if ( bigsum_B__P_dom_it26 != bigsum_B__P_dom.end() ) {
            qBIDS3[bigsum_B__P] += (-P*V);
        }
    }

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_BIDS_sec_span, on_delete_BIDS_usec_span);
    (*results) << "on_delete_BIDS" << "," << q << endl;
}

DBToaster::DemoDatasets::OrderbookFileStream VwapBids("/home/yanif/datasets/orderbook/MSFT/20081201.csv",10000);

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
