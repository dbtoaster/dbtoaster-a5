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
map<tuple<int,double>,int> qASKS1;
map<tuple<int,double>,double> qASKS2;
map<tuple<int,double>,int> qASKS3;
map<tuple<int,double>,double> qASKS4;
map<int,double> q;
map<tuple<int,double>,double> qBIDS1;
map<tuple<int,double>,int> qBIDS2;
map<tuple<int,double>,double> qBIDS3;
map<tuple<int,double>,int> qBIDS4;

double on_insert_ASKS_sec_span = 0.0;
double on_insert_ASKS_usec_span = 0.0;
double on_insert_BIDS_sec_span = 0.0;
double on_insert_BIDS_usec_span = 0.0;
double on_delete_ASKS_sec_span = 0.0;
double on_delete_ASKS_usec_span = 0.0;
double on_delete_BIDS_sec_span = 0.0;
double on_delete_BIDS_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
   cout << "qASKS1 size: " << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qASKS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qASKS1" << "," << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qASKS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

   cout << "qASKS2 size: " << (((sizeof(map<tuple<int,double>,double>::key_type)
       + sizeof(map<tuple<int,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qASKS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,double>::key_type, map<tuple<int,double>,double>::value_type, _Select1st<map<tuple<int,double>,double>::value_type>, map<tuple<int,double>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qASKS2" << "," << (((sizeof(map<tuple<int,double>,double>::key_type)
       + sizeof(map<tuple<int,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qASKS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,double>::key_type, map<tuple<int,double>,double>::value_type, _Select1st<map<tuple<int,double>,double>::value_type>, map<tuple<int,double>,double>::key_compare>))) << endl;

   cout << "qASKS3 size: " << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qASKS3.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qASKS3" << "," << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qASKS3.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

   cout << "qASKS4 size: " << (((sizeof(map<tuple<int,double>,double>::key_type)
       + sizeof(map<tuple<int,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qASKS4.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,double>::key_type, map<tuple<int,double>,double>::value_type, _Select1st<map<tuple<int,double>,double>::value_type>, map<tuple<int,double>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qASKS4" << "," << (((sizeof(map<tuple<int,double>,double>::key_type)
       + sizeof(map<tuple<int,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qASKS4.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,double>::key_type, map<tuple<int,double>,double>::value_type, _Select1st<map<tuple<int,double>,double>::value_type>, map<tuple<int,double>,double>::key_compare>))) << endl;

   cout << "q size: " << (((sizeof(map<int,double>::key_type)
       + sizeof(map<int,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int,double>::key_type, map<int,double>::value_type, _Select1st<map<int,double>::value_type>, map<int,double>::key_compare>))) << endl;

   (*stats) << "m," << "q" << "," << (((sizeof(map<int,double>::key_type)
       + sizeof(map<int,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int,double>::key_type, map<int,double>::value_type, _Select1st<map<int,double>::value_type>, map<int,double>::key_compare>))) << endl;

   cout << "qBIDS1 size: " << (((sizeof(map<tuple<int,double>,double>::key_type)
       + sizeof(map<tuple<int,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,double>::key_type, map<tuple<int,double>,double>::value_type, _Select1st<map<tuple<int,double>,double>::value_type>, map<tuple<int,double>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS1" << "," << (((sizeof(map<tuple<int,double>,double>::key_type)
       + sizeof(map<tuple<int,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,double>::key_type, map<tuple<int,double>,double>::value_type, _Select1st<map<tuple<int,double>,double>::value_type>, map<tuple<int,double>,double>::key_compare>))) << endl;

   cout << "qBIDS2 size: " << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS2" << "," << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

   cout << "qBIDS3 size: " << (((sizeof(map<tuple<int,double>,double>::key_type)
       + sizeof(map<tuple<int,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS3.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,double>::key_type, map<tuple<int,double>,double>::value_type, _Select1st<map<tuple<int,double>,double>::value_type>, map<tuple<int,double>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS3" << "," << (((sizeof(map<tuple<int,double>,double>::key_type)
       + sizeof(map<tuple<int,double>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS3.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,double>::key_type, map<tuple<int,double>,double>::value_type, _Select1st<map<tuple<int,double>,double>::value_type>, map<tuple<int,double>,double>::key_compare>))) << endl;

   cout << "qBIDS4 size: " << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS4.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qBIDS4" << "," << (((sizeof(map<tuple<int,double>,int>::key_type)
       + sizeof(map<tuple<int,double>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qBIDS4.size())  + (sizeof(struct _Rb_tree<map<tuple<int,double>,int>::key_type, map<tuple<int,double>,int>::value_type, _Select1st<map<tuple<int,double>,int>::value_type>, map<tuple<int,double>,int>::key_compare>))) << endl;

}

void analyse_handler_usage(ofstream* stats)
{
   cout << "on_insert_ASKS cost: " << (on_insert_ASKS_sec_span + (on_insert_ASKS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_ASKS" << "," << (on_insert_ASKS_sec_span + (on_insert_ASKS_usec_span / 1000000.0)) << endl;
   cout << "on_insert_BIDS cost: " << (on_insert_BIDS_sec_span + (on_insert_BIDS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_BIDS" << "," << (on_insert_BIDS_sec_span + (on_insert_BIDS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_ASKS cost: " << (on_delete_ASKS_sec_span + (on_delete_ASKS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_ASKS" << "," << (on_delete_ASKS_sec_span + (on_delete_ASKS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_BIDS cost: " << (on_delete_BIDS_sec_span + (on_delete_BIDS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_BIDS" << "," << (on_delete_BIDS_sec_span + (on_delete_BIDS_usec_span / 1000000.0)) << endl;
}


void on_insert_ASKS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    q[BROKER_ID] += -1*qASKS2[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*qASKS4[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += V*qASKS1[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += V*qASKS3[make_tuple(BROKER_ID,P)];
    map<tuple<int,double>,double>::iterator qBIDS1_it2 = qBIDS1.begin();
    map<tuple<int,double>,double>::iterator qBIDS1_end1 = qBIDS1.end();
    for (; qBIDS1_it2 != qBIDS1_end1; ++qBIDS1_it2)
    {
        double x_qBIDS_B__P = get<1>(qBIDS1_it2->first);
        qBIDS1[make_tuple(BROKER_ID,x_qBIDS_B__P)] += V*( (
             1000 < P+-1*x_qBIDS_B__P )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int,double>,int>::iterator qBIDS2_it4 = qBIDS2.begin();
    map<tuple<int,double>,int>::iterator qBIDS2_end3 = qBIDS2.end();
    for (; qBIDS2_it4 != qBIDS2_end3; ++qBIDS2_it4)
    {
        double x_qBIDS_B__P = get<1>(qBIDS2_it4->first);
        qBIDS2[make_tuple(BROKER_ID,x_qBIDS_B__P)] += ( (
             1000 < P+-1*x_qBIDS_B__P )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int,double>,double>::iterator qBIDS3_it6 = qBIDS3.begin();
    map<tuple<int,double>,double>::iterator qBIDS3_end5 = qBIDS3.end();
    for (; qBIDS3_it6 != qBIDS3_end5; ++qBIDS3_it6)
    {
        double x_qBIDS_B__P = get<1>(qBIDS3_it6->first);
        qBIDS3[make_tuple(BROKER_ID,x_qBIDS_B__P)] += V*( (
             1000 < x_qBIDS_B__P+-1*P )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int,double>,int>::iterator qBIDS4_it8 = qBIDS4.begin();
    map<tuple<int,double>,int>::iterator qBIDS4_end7 = qBIDS4.end();
    for (; qBIDS4_it8 != qBIDS4_end7; ++qBIDS4_it8)
    {
        double x_qBIDS_B__P = get<1>(qBIDS4_it8->first);
        qBIDS4[make_tuple(BROKER_ID,x_qBIDS_B__P)] += ( (
             1000 < x_qBIDS_B__P+-1*P )? ( 1 ) : ( 0 ) );
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_ASKS_sec_span, on_insert_ASKS_usec_span);
}

void on_insert_BIDS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    q[BROKER_ID] += qBIDS1[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += qBIDS3[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*V*qBIDS2[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*V*qBIDS4[make_tuple(BROKER_ID,P)];
    map<tuple<int,double>,int>::iterator qASKS1_it10 = qASKS1.begin();
    map<tuple<int,double>,int>::iterator qASKS1_end9 = qASKS1.end();
    for (; qASKS1_it10 != qASKS1_end9; ++qASKS1_it10)
    {
        double x_qASKS_A__P = get<1>(qASKS1_it10->first);
        qASKS1[make_tuple(BROKER_ID,x_qASKS_A__P)] += ( (
             1000 < x_qASKS_A__P+-1*P )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int,double>,double>::iterator qASKS2_it12 = qASKS2.begin();
    map<tuple<int,double>,double>::iterator qASKS2_end11 = qASKS2.end();
    for (; qASKS2_it12 != qASKS2_end11; ++qASKS2_it12)
    {
        double x_qASKS_A__P = get<1>(qASKS2_it12->first);
        qASKS2[make_tuple(BROKER_ID,x_qASKS_A__P)] += V*( (
             1000 < x_qASKS_A__P+-1*P )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int,double>,int>::iterator qASKS3_it14 = qASKS3.begin();
    map<tuple<int,double>,int>::iterator qASKS3_end13 = qASKS3.end();
    for (; qASKS3_it14 != qASKS3_end13; ++qASKS3_it14)
    {
        double x_qASKS_A__P = get<1>(qASKS3_it14->first);
        qASKS3[make_tuple(BROKER_ID,x_qASKS_A__P)] += ( (
             1000 < P+-1*x_qASKS_A__P )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int,double>,double>::iterator qASKS4_it16 = qASKS4.begin();
    map<tuple<int,double>,double>::iterator qASKS4_end15 = qASKS4.end();
    for (; qASKS4_it16 != qASKS4_end15; ++qASKS4_it16)
    {
        double x_qASKS_A__P = get<1>(qASKS4_it16->first);
        qASKS4[make_tuple(BROKER_ID,x_qASKS_A__P)] += V*( (
             1000 < P+-1*x_qASKS_A__P )? ( 1 ) : ( 0 ) );
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_BIDS_sec_span, on_insert_BIDS_usec_span);
}

void on_delete_ASKS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    q[BROKER_ID] += -1*-1*qASKS2[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*-1*qASKS4[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*V*qASKS1[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*V*qASKS3[make_tuple(BROKER_ID,P)];
    map<tuple<int,double>,double>::iterator qBIDS1_it18 = qBIDS1.begin();
    map<tuple<int,double>,double>::iterator qBIDS1_end17 = qBIDS1.end();
    for (; qBIDS1_it18 != qBIDS1_end17; ++qBIDS1_it18)
    {
        double x_qBIDS_B__P = get<1>(qBIDS1_it18->first);
        qBIDS1[make_tuple(BROKER_ID,x_qBIDS_B__P)] += -1*V*( (
             1000 < P+-1*x_qBIDS_B__P )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int,double>,int>::iterator qBIDS2_it20 = qBIDS2.begin();
    map<tuple<int,double>,int>::iterator qBIDS2_end19 = qBIDS2.end();
    for (; qBIDS2_it20 != qBIDS2_end19; ++qBIDS2_it20)
    {
        double x_qBIDS_B__P = get<1>(qBIDS2_it20->first);
        qBIDS2[make_tuple(BROKER_ID,x_qBIDS_B__P)] += -1*( (
             1000 < P+-1*x_qBIDS_B__P )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int,double>,double>::iterator qBIDS3_it22 = qBIDS3.begin();
    map<tuple<int,double>,double>::iterator qBIDS3_end21 = qBIDS3.end();
    for (; qBIDS3_it22 != qBIDS3_end21; ++qBIDS3_it22)
    {
        double x_qBIDS_B__P = get<1>(qBIDS3_it22->first);
        qBIDS3[make_tuple(BROKER_ID,x_qBIDS_B__P)] += -1*V*( (
             1000 < x_qBIDS_B__P+-1*P )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int,double>,int>::iterator qBIDS4_it24 = qBIDS4.begin();
    map<tuple<int,double>,int>::iterator qBIDS4_end23 = qBIDS4.end();
    for (; qBIDS4_it24 != qBIDS4_end23; ++qBIDS4_it24)
    {
        double x_qBIDS_B__P = get<1>(qBIDS4_it24->first);
        qBIDS4[make_tuple(BROKER_ID,x_qBIDS_B__P)] += -1*( (
             1000 < x_qBIDS_B__P+-1*P )? ( 1 ) : ( 0 ) );
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_ASKS_sec_span, on_delete_ASKS_usec_span);
}

void on_delete_BIDS(
    ofstream* results, ofstream* log, ofstream* stats, double T,int ID,int 
    BROKER_ID,double P,double V)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    q[BROKER_ID] += -1*-1*V*qBIDS2[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*-1*V*qBIDS4[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*qBIDS1[make_tuple(BROKER_ID,P)];
    q[BROKER_ID] += -1*qBIDS3[make_tuple(BROKER_ID,P)];
    map<tuple<int,double>,int>::iterator qASKS1_it26 = qASKS1.begin();
    map<tuple<int,double>,int>::iterator qASKS1_end25 = qASKS1.end();
    for (; qASKS1_it26 != qASKS1_end25; ++qASKS1_it26)
    {
        double x_qASKS_A__P = get<1>(qASKS1_it26->first);
        qASKS1[make_tuple(BROKER_ID,x_qASKS_A__P)] += -1*( (
             1000 < x_qASKS_A__P+-1*P )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int,double>,double>::iterator qASKS2_it28 = qASKS2.begin();
    map<tuple<int,double>,double>::iterator qASKS2_end27 = qASKS2.end();
    for (; qASKS2_it28 != qASKS2_end27; ++qASKS2_it28)
    {
        double x_qASKS_A__P = get<1>(qASKS2_it28->first);
        qASKS2[make_tuple(BROKER_ID,x_qASKS_A__P)] += -1*V*( (
             1000 < x_qASKS_A__P+-1*P )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int,double>,int>::iterator qASKS3_it30 = qASKS3.begin();
    map<tuple<int,double>,int>::iterator qASKS3_end29 = qASKS3.end();
    for (; qASKS3_it30 != qASKS3_end29; ++qASKS3_it30)
    {
        double x_qASKS_A__P = get<1>(qASKS3_it30->first);
        qASKS3[make_tuple(BROKER_ID,x_qASKS_A__P)] += -1*( (
             1000 < P+-1*x_qASKS_A__P )? ( 1 ) : ( 0 ) );
    }
    map<tuple<int,double>,double>::iterator qASKS4_it32 = qASKS4.begin();
    map<tuple<int,double>,double>::iterator qASKS4_end31 = qASKS4.end();
    for (; qASKS4_it32 != qASKS4_end31; ++qASKS4_it32)
    {
        double x_qASKS_A__P = get<1>(qASKS4_it32->first);
        qASKS4[make_tuple(BROKER_ID,x_qASKS_A__P)] += -1*V*( (
             1000 < P+-1*x_qASKS_A__P )? ( 1 ) : ( 0 ) );
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_BIDS_sec_span, on_delete_BIDS_usec_span);
}

DBToaster::DemoDatasets::OrderbookFileStream VwapAsks("/home/yanif/datasets/orderbook/MSFT/20081201.csv",10000);

boost::shared_ptr<DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor> VwapAsks_adaptor(new DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor());
static int streamVwapAsksId = 0;

struct on_insert_ASKS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor::Result>(data); 
        on_insert_ASKS(results, log, stats, input.t,input.id,input.broker_id,input.price,input.volume);
    }
};

on_insert_ASKS_fun_obj fo_on_insert_ASKS_0;

DBToaster::DemoDatasets::OrderbookFileStream VwapBids("/home/yanif/datasets/orderbook/MSFT/20081201.csv",10000);

boost::shared_ptr<DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor> VwapBids_adaptor(new DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor());
static int streamVwapBidsId = 1;

struct on_insert_BIDS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor::Result>(data); 
        on_insert_BIDS(results, log, stats, input.t,input.id,input.broker_id,input.price,input.volume);
    }
};

on_insert_BIDS_fun_obj fo_on_insert_BIDS_1;

struct on_delete_ASKS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor::Result>(data); 
        on_delete_ASKS(results, log, stats, input.t,input.id,input.broker_id,input.price,input.volume);
    }
};

on_delete_ASKS_fun_obj fo_on_delete_ASKS_2;

struct on_delete_BIDS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor::Result>(data); 
        on_delete_BIDS(results, log, stats, input.t,input.id,input.broker_id,input.price,input.volume);
    }
};

on_delete_BIDS_fun_obj fo_on_delete_BIDS_3;


void init(DBToaster::StandaloneEngine::FileMultiplexer& sources,
    DBToaster::StandaloneEngine::FileStreamDispatcher& router, 
    ofstream* results, ofstream* log, ofstream* stats)
{
    router.setFiles(results, log, stats);
    sources.addStream<DBToaster::DemoDatasets::OrderbookTuple>(&VwapAsks, boost::ref(*VwapAsks_adaptor), streamVwapAsksId);
    router.addHandler(streamVwapAsksId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_ASKS_0));
    sources.addStream<DBToaster::DemoDatasets::OrderbookTuple>(&VwapBids, boost::ref(*VwapBids_adaptor), streamVwapBidsId);
    router.addHandler(streamVwapBidsId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_BIDS_1));
    router.addHandler(streamVwapAsksId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_ASKS_2));
    router.addHandler(streamVwapBidsId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_BIDS_3));
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