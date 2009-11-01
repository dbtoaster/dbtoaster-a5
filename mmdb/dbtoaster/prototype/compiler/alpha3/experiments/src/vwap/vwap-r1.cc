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


using namespace DBToaster::Profiler;
double q;
map<double,double> qBIDS1;
double qBIDS2;
map<double,double> qBIDS3;
set<double> bigsum_B__P_dom;
multiset<tuple<double,int,int,double,double> > BIDS;

double q80;
double q57;
double q37;
double q102;

double on_insert_BIDS_sec_span = 0.0;
double on_insert_BIDS_usec_span = 0.0;
double on_delete_BIDS_sec_span = 0.0;
double on_delete_BIDS_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
   cout << "BIDS size: " << (((sizeof(multiset<tuple<double,int,int,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * BIDS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<double,int,int,double,double> >::key_type, multiset<tuple<double,int,int,double,double> >::value_type, _Identity<multiset<tuple<double,int,int,double,double> >::value_type>, multiset<tuple<double,int,int,double,double> >::key_compare>))) << endl;

   (*stats) << "m," << "BIDS" << "," << (((sizeof(multiset<tuple<double,int,int,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * BIDS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<double,int,int,double,double> >::key_type, multiset<tuple<double,int,int,double,double> >::value_type, _Identity<multiset<tuple<double,int,int,double,double> >::value_type>, multiset<tuple<double,int,int,double,double> >::key_compare>))) << endl;

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
    if ( qBIDS1.find(P) == qBIDS1.end() )
    {
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it2 = BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end1 = BIDS.end();
        for (; BIDS_it2 != BIDS_end1; ++BIDS_it2)
        {
            double B2__P = get<3>(*BIDS_it2);
            double B2__V = get<4>(*BIDS_it2);
            if ( P < B2__P )
                qBIDS1[P] += B2__V;
        }
    }

    if ( qBIDS3.find(P) == qBIDS3.end() )
    {
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it4 = BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end3 = BIDS.end();
        for (; BIDS_it4 != BIDS_end3; ++BIDS_it4)
        {
            double protect_B__P = get<3>(*BIDS_it4);
            double protect_B__V = get<4>(*BIDS_it4);
            if ( P == protect_B__P )
                qBIDS3[P] += P*protect_B__P;
        }
    }

    bigsum_B__P_dom.insert(P);
    BIDS.insert(make_tuple(T,ID,BROKER_ID,P,V));
    BIDS.insert(make_tuple(T,ID,BROKER_ID,P,V));

    q37 = 0.0;
    set<double>::iterator bigsum_B__P_dom_it6 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end5 = bigsum_B__P_dom.end();
    for (; bigsum_B__P_dom_it6 != bigsum_B__P_dom_end5; ++bigsum_B__P_dom_it6)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it6;
        q37 += ( ( ( 0.25*qBIDS2+0.25*V <= qBIDS1[bigsum_B__P]+V*( (
             bigsum_B__P < P )? ( 1 ) : ( 0 ) ) ) && (
             qBIDS1[bigsum_B__P] < 0.25*qBIDS2 ) )? ( qBIDS3[bigsum_B__P] ) : ( 0 ) );
    }
    q += -1*q37;

    set<double>::iterator bigsum_B__P_dom_it8 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end7 = bigsum_B__P_dom.end();
    for (; bigsum_B__P_dom_it8 != bigsum_B__P_dom_end7; ++bigsum_B__P_dom_it8)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it8;
        q += ( ( qBIDS1[bigsum_B__P]+V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS2+0.25*V )? ( ( ( P == bigsum_B__P )?
             ( P ) : ( 0 ) )*V ) : ( 0 ) );
    }

    set<double>::iterator bigsum_B__P_dom_it10 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end9 = bigsum_B__P_dom.end();
    for (; bigsum_B__P_dom_it10 != bigsum_B__P_dom_end9; ++bigsum_B__P_dom_it10)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it10;
        q += ( ( ( qBIDS1[bigsum_B__P]+V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS2+0.25*V ) && (
             0.25*qBIDS2 <= qBIDS1[bigsum_B__P] ) )? ( qBIDS3[bigsum_B__P] ) : ( 0 ) );
    }

    // TODO: reorder next three loops with a single scan over bids, and an
    // inner scan for each map.
    map<double,double>::iterator qBIDS1_it14 = qBIDS1.begin();
    map<double,double>::iterator qBIDS1_end13 = qBIDS1.end();
    for (; qBIDS1_it14 != qBIDS1_end13; ++qBIDS1_it14)
    {
        double bigsum_B__P = qBIDS1_it14->first;
        qBIDS1[P] = 0;

        // TODO: could be improved with an index over bids.
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it12 = BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end11 = BIDS.end();
        for (; BIDS_it12 != BIDS_end11; ++BIDS_it12)
        {
            double B2__P = get<3>(*BIDS_it12);
            double B2__V = get<4>(*BIDS_it12);
            if ( P < B2__P )
                qBIDS1[P] += B2__V;
        }
    }

    qBIDS2 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_it16 = BIDS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_end15 = BIDS.end();
    for (; BIDS_it16 != BIDS_end15; ++BIDS_it16)
    {
        double B1__V = get<4>(*BIDS_it16);
        qBIDS2 += B1__V;
    }

    map<double,double>::iterator qBIDS3_it20 = qBIDS3.begin();
    map<double,double>::iterator qBIDS3_end19 = qBIDS3.end();
    for (; qBIDS3_it20 != qBIDS3_end19; ++qBIDS3_it20)
    {
        double bigsum_B__P = qBIDS3_it20->first;
        qBIDS3[P] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it18 = BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end17 = BIDS.end();
        for (; BIDS_it18 != BIDS_end17; ++BIDS_it18)
        {
            double protect_B__P = get<3>(*BIDS_it18);
            double protect_B__V = get<4>(*BIDS_it18);
            if ( P == protect_B__P )
                qBIDS3[P] += P*protect_B__V;
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
    BIDS.erase(make_tuple(T,ID,BROKER_ID,P,V));
    // Commenting out domain deletion until we have count(*) from bids group by p
    /*
    if ( bigsum_B__P_dom.find(P) == bigsum_B__P_dom.end() )
    {
        qBIDS1.erase(P);
    }
    if ( bigsum_B__P_dom.find(P) == bigsum_B__P_dom.end() )
    {
        qBIDS3.erase(P);
    }
    */

    q102 = 0.0;
    set<double>::iterator bigsum_B__P_dom_it22 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end21 = bigsum_B__P_dom.end();
    for (
        ; bigsum_B__P_dom_it22 != bigsum_B__P_dom_end21; ++bigsum_B__P_dom_it22)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it22;
        q102 += ( ( ( 0.25*qBIDS2+-1*0.25*V <= qBIDS1[bigsum_B__P]+-1*V*( (
             bigsum_B__P < P )? ( 1 ) : ( 0 ) ) ) && (
             qBIDS1[bigsum_B__P] < 0.25*qBIDS2 ) )? ( qBIDS3[bigsum_B__P] ) : ( 0 ) );
    }
    q += -1*q102;

    q57 = 0.0;
    set<double>::iterator bigsum_B__P_dom_it24 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end23 = bigsum_B__P_dom.end();
    for (
        ; bigsum_B__P_dom_it24 != bigsum_B__P_dom_end23; ++bigsum_B__P_dom_it24)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it24;
        q57 += ( ( qBIDS1[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS2+-1*0.25*V )? ( ( ( P == bigsum_B__P )?
             ( P ) : ( 0 ) )*V ) : ( 0 ) );
    }
    q += -1*q57;

    q80 = 0.0;
    set<double>::iterator bigsum_B__P_dom_it26 = bigsum_B__P_dom.begin();
    set<double>::iterator bigsum_B__P_dom_end25 = bigsum_B__P_dom.end();
    for (
        ; bigsum_B__P_dom_it26 != bigsum_B__P_dom_end25; ++bigsum_B__P_dom_it26)
    {
        double bigsum_B__P = *bigsum_B__P_dom_it26;
        q80 += ( ( ( qBIDS1[bigsum_B__P]+-1*V*( ( bigsum_B__P < P )?
             ( 1 ) : ( 0 ) ) < 0.25*qBIDS2+-1*0.25*V ) && (
             0.25*qBIDS2 <= qBIDS1[bigsum_B__P] ) )? ( qBIDS3[bigsum_B__P] ) : ( 0 ) );
    }
    q += -1*-1*q80;

    // TODO: see note above about reordering.
    map<double,double>::iterator qBIDS1_it30 = qBIDS1.begin();
    map<double,double>::iterator qBIDS1_end29 = qBIDS1.end();
    for (; qBIDS1_it30 != qBIDS1_end29; ++qBIDS1_it30)
    {
        double bigsum_B__P = qBIDS1_it30->first;
        qBIDS1[P] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it28 = BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end27 = BIDS.end();
        for (; BIDS_it28 != BIDS_end27; ++BIDS_it28)
        {
            double B2__P = get<3>(*BIDS_it28);
            double B2__V = get<4>(*BIDS_it28);
            if ( P < B2__P )
                qBIDS1[P] += B2__V;
        }
    }

    qBIDS2 = 0;
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_it32 = BIDS.begin();
    multiset<tuple<double,int,int,double,double> >::iterator BIDS_end31 = BIDS.end();
    for (; BIDS_it32 != BIDS_end31; ++BIDS_it32)
    {
        double B1__V = get<4>(*BIDS_it32);
        qBIDS2 += B1__V;
    }

    map<double,double>::iterator qBIDS3_it36 = qBIDS3.begin();
    map<double,double>::iterator qBIDS3_end35 = qBIDS3.end();
    for (; qBIDS3_it36 != qBIDS3_end35; ++qBIDS3_it36)
    {
        double bigsum_B__P = qBIDS3_it36->first;
        qBIDS3[P] = 0;
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_it34 = BIDS.begin();
        multiset<tuple<double,int,int,double,double> >::iterator BIDS_end33 = BIDS.end();
        for (; BIDS_it34 != BIDS_end33; ++BIDS_it34)
        {
            double protect_B__P = get<3>(*BIDS_it34);
            double protect_B__V = get<4>(*BIDS_it34);
            if ( P == protect_B__P )
                qBIDS3[P] += P*protect_B__V;
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
