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

map<int64_t,double,std::less<int64_t>, boost::pool_allocator<pair<int64_t,double> > > q;

set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> > bigsum_C1__CUSTKEY_dom;

set<double, std::less<double>, boost::pool_allocator<double> > bigsum_C1__ACCTBAL_dom;

multiset<tuple<int64_t,string,string,int64_t,string,double,string,string>, 
    std::less<tuple<int64_t,string,string,int64_t,string,double,string,string> >, 
    boost::pool_allocator<tuple<int64_t,string,string,int64_t,string,double,string,string> > >
    CUSTOMER;

double qCUSTOMER1;

map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > > qCUSTOMER2;

map<tuple<double,int64_t,int64_t>,double,std::less<tuple<double,int64_t,int64_t> >,
    boost::pool_allocator<pair<tuple<double,int64_t,int64_t>,double> > >
    qCUSTOMER3;

double on_insert_CUSTOMER_sec_span = 0.0;
double on_insert_CUSTOMER_usec_span = 0.0;
double on_insert_ORDERS_sec_span = 0.0;
double on_insert_ORDERS_usec_span = 0.0;
double on_delete_CUSTOMER_sec_span = 0.0;
double on_delete_CUSTOMER_usec_span = 0.0;
double on_delete_ORDERS_sec_span = 0.0;
double on_delete_ORDERS_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
   cout << "bigsum_C1__CUSTKEY_dom size: " << (((sizeof(set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_C1__CUSTKEY_dom.size())  + (sizeof(struct _Rb_tree<set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::key_type, set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type, _Identity<set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type>, set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::key_compare>))) << endl;

   (*stats) << "m," << "bigsum_C1__CUSTKEY_dom" << "," << (((sizeof(set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_C1__CUSTKEY_dom.size())  + (sizeof(struct _Rb_tree<set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::key_type, set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type, _Identity<set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type>, set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::key_compare>))) << endl;

   cout << "q size: " << (((sizeof(map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_type)
       + sizeof(map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_type, map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::value_type, _Select1st<map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::value_type>, map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_compare>))) << endl;

   (*stats) << "m," << "q" << "," << (((sizeof(map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_type)
       + sizeof(map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_type, map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::value_type, _Select1st<map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::value_type>, map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_compare>))) << endl;

   cout << "CUSTOMER size: " << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string,string>, std::less<tuple<int64_t,string,string,int64_t,string,double,string,string> >, boost::pool_allocator<tuple<int64_t,string,string,int64_t,string,double,string,string> > >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * CUSTOMER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string>, std::less<tuple<int64_t,string,string,int64_t,string,double,string,string> >, boost::pool_allocator<tuple<int64_t,string,string,int64_t,string,double,string,string> > >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string>, std::less<tuple<int64_t,string,string,int64_t,string,double,string,string> >, boost::pool_allocator<tuple<int64_t,string,string,int64_t,string,double,string,string> > >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string>, std::less<tuple<int64_t,string,string,int64_t,string,double,string,string> >, boost::pool_allocator<tuple<int64_t,string,string,int64_t,string,double,string,string> > >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string>, std::less<tuple<int64_t,string,string,int64_t,string,double,string,string> >, boost::pool_allocator<tuple<int64_t,string,string,int64_t,string,double,string,string> > >::key_compare>))) << endl;

   (*stats) << "m," << "CUSTOMER" << "," << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string,string>, std::less<tuple<int64_t,string,string,int64_t,string,double,string,string> >, boost::pool_allocator<tuple<int64_t,string,string,int64_t,string,double,string,string> > >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * CUSTOMER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string>, std::less<tuple<int64_t,string,string,int64_t,string,double,string,string> >, boost::pool_allocator<tuple<int64_t,string,string,int64_t,string,double,string,string> > >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string>, std::less<tuple<int64_t,string,string,int64_t,string,double,string,string> >, boost::pool_allocator<tuple<int64_t,string,string,int64_t,string,double,string,string> > >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string>, std::less<tuple<int64_t,string,string,int64_t,string,double,string,string> >, boost::pool_allocator<tuple<int64_t,string,string,int64_t,string,double,string,string> > >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string>, std::less<tuple<int64_t,string,string,int64_t,string,double,string,string> >, boost::pool_allocator<tuple<int64_t,string,string,int64_t,string,double,string,string> > >::key_compare>))) << endl;

   cout << "qCUSTOMER2 size: " << (((sizeof(map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_type)
       + sizeof(map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2.size())  + (sizeof(struct _Rb_tree<map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_type, map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::value_type, _Select1st<map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::value_type>, map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER2" << "," << (((sizeof(map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_type)
       + sizeof(map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2.size())  + (sizeof(struct _Rb_tree<map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_type, map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::value_type, _Select1st<map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::value_type>, map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_compare>))) << endl;

   cout << "bigsum_C1__ACCTBAL_dom size: " << (((sizeof(set<double, std::less<double>, boost::pool_allocator<double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_C1__ACCTBAL_dom.size())  + (sizeof(struct _Rb_tree<set<double, std::less<double>, boost::pool_allocator<double> >::key_type, set<double, std::less<double>, boost::pool_allocator<double> >::value_type, _Identity<set<double, std::less<double>, boost::pool_allocator<double> >::value_type>, set<double, std::less<double>, boost::pool_allocator<double> >::key_compare>))) << endl;

   (*stats) << "m," << "bigsum_C1__ACCTBAL_dom" << "," << (((sizeof(set<double, std::less<double>, boost::pool_allocator<double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_C1__ACCTBAL_dom.size())  + (sizeof(struct _Rb_tree<set<double, std::less<double>, boost::pool_allocator<double> >::key_type, set<double, std::less<double>, boost::pool_allocator<double> >::value_type, _Identity<set<double, std::less<double>, boost::pool_allocator<double> >::value_type>, set<double, std::less<double>, boost::pool_allocator<double> >::key_compare>))) << endl;

   cout << "qCUSTOMER3 size: " << (((sizeof(map<tuple<double,int64_t,int64_t>,double,std::less<tuple<double,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<double,int64_t,int64_t>,double> > >::key_type)
       + sizeof(map<tuple<double,int64_t,int64_t>,double,std::less<tuple<double,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<double,int64_t,int64_t>,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3.size())  + (sizeof(struct _Rb_tree<map<tuple<double,int64_t,int64_t>,double,std::less<tuple<double,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<double,int64_t,int64_t>,double> > >::key_type, map<tuple<double,int64_t,int64_t>,double,std::less<tuple<double,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<double,int64_t,int64_t>,double> > >::value_type, _Select1st<map<tuple<double,int64_t,int64_t>,double,std::less<tuple<double,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<double,int64_t,int64_t>,double> > >::value_type>, map<tuple<double,int64_t,int64_t>,double,std::less<tuple<double,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<double,int64_t,int64_t>,double> > >::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER3" << "," << (((sizeof(map<tuple<double,int64_t,int64_t>,double,std::less<tuple<double,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<double,int64_t,int64_t>,double> > >::key_type)
       + sizeof(map<tuple<double,int64_t,int64_t>,double,std::less<tuple<double,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<double,int64_t,int64_t>,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3.size())  + (sizeof(struct _Rb_tree<map<tuple<double,int64_t,int64_t>,double,std::less<tuple<double,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<double,int64_t,int64_t>,double> > >::key_type, map<tuple<double,int64_t,int64_t>,double,std::less<tuple<double,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<double,int64_t,int64_t>,double> > >::value_type, _Select1st<map<tuple<double,int64_t,int64_t>,double,std::less<tuple<double,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<double,int64_t,int64_t>,double> > >::value_type>, map<tuple<double,int64_t,int64_t>,double,std::less<tuple<double,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<double,int64_t,int64_t>,double> > >::key_compare>))) << endl;

}

void analyse_handler_usage(ofstream* stats)
{
   cout << "on_insert_CUSTOMER cost: " << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_CUSTOMER" << "," << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   cout << "on_insert_ORDERS cost: " << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_ORDERS" << "," << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_CUSTOMER cost: " << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_CUSTOMER" << "," << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
   cout << "on_delete_ORDERS cost: " << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_ORDERS" << "," << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
}


void on_insert_CUSTOMER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t CUSTKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    MKTSEGMENT,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    if ( qCUSTOMER3.find(make_tuple(ACCTBAL,CUSTKEY,NATIONKEY)) == qCUSTOMER3.end() )
    {
        
        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string>, 
            std::less<tuple<int64_t,string,string,int64_t,string,double,string,string> >, 
            boost::pool_allocator<tuple<int64_t,string,string,int64_t,string,double,string,
            string> > >::iterator CUSTOMER_it2 = CUSTOMER.begin();
        
        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string>, 
            std::less<tuple<int64_t,string,string,int64_t,string,double,string,string> >, 
            boost::pool_allocator<tuple<int64_t,string,string,int64_t,string,double,string,
            string> > >::iterator CUSTOMER_end1 = CUSTOMER.end();
        for (; CUSTOMER_it2 != CUSTOMER_end1; ++CUSTOMER_it2)
        {
            int64_t protect_C1__CUSTKEY = get<0>(*CUSTOMER_it2);
            int64_t protect_C1__NATIONKEY = get<3>(*CUSTOMER_it2);
            double protect_C1__ACCTBAL = get<5>(*CUSTOMER_it2);

            if ( (ACCTBAL == protect_C1__ACCTBAL) &&
                 (NATIONKEY == protect_C1__NATIONKEY) &&
                 (CUSTKEY == protect_C1__CUSTKEY) )
            {
                qCUSTOMER3[make_tuple(ACCTBAL,CUSTKEY,NATIONKEY)] += ACCTBAL;
            }
        }
    }

    bigsum_C1__ACCTBAL_dom.insert(ACCTBAL);
    bigsum_C1__CUSTKEY_dom.insert(CUSTKEY);
    CUSTOMER.insert(make_tuple(
        CUSTKEY,NAME,ADDRESS,NATIONKEY,PHONE,ACCTBAL,MKTSEGMENT,COMMENT));

    set<int64_t, std::less<int64_t>,
        boost::pool_allocator<int64_t> >::iterator bigsum_C1__CUSTKEY_dom_it6 =
        bigsum_C1__CUSTKEY_dom.find(CUSTKEY);

    set<double, std::less<double>,
        boost::pool_allocator<double> >::iterator bigsum_C1__ACCTBAL_dom_it4 = 
        bigsum_C1__ACCTBAL_dom.find(ACCTBAL);

    if ( !( bigsum_C1__CUSTKEY_dom_it6 == bigsum_C1__CUSTKEY_dom.end() ||
            bigsum_C1__ACCTBAL_dom_it4 == bigsum_C1__ACCTBAL_dom.end() ) )
    {
        q[NATIONKEY] += (
            ( ( ACCTBAL < qCUSTOMER1+( ( 0 < ACCTBAL )? ACCTBAL : 0 ) )
              && ( 0 == qCUSTOMER2[CUSTKEY] ) )?
            ACCTBAL : 0 );
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it12 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end11 = q.end();
    for (; q_it12 != q_end11; ++q_it12)
    {
        int64_t NATIONKEY = q_it12->first;

        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_it10 = bigsum_C1__CUSTKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_end9 = bigsum_C1__CUSTKEY_dom.end();
        for (
            ; bigsum_C1__CUSTKEY_dom_it10 != bigsum_C1__CUSTKEY_dom_end9; 
            ++bigsum_C1__CUSTKEY_dom_it10)
        {
            int64_t bigsum_C1__CUSTKEY = *bigsum_C1__CUSTKEY_dom_it10;
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_it8 = bigsum_C1__ACCTBAL_dom.begin();
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_end7 = bigsum_C1__ACCTBAL_dom.end();
            for (
                ; bigsum_C1__ACCTBAL_dom_it8 != bigsum_C1__ACCTBAL_dom_end7; 
                ++bigsum_C1__ACCTBAL_dom_it8)
            {
                double bigsum_C1__ACCTBAL = *bigsum_C1__ACCTBAL_dom_it8;
                q[NATIONKEY] +=
                    -1*( ( ( qCUSTOMER1+( ( 0 < ACCTBAL )? ACCTBAL : 0 ) <= bigsum_C1__ACCTBAL )
                         && ( bigsum_C1__ACCTBAL < qCUSTOMER1 )
                         && ( 0 == qCUSTOMER2[bigsum_C1__CUSTKEY] ) )?
                         qCUSTOMER3[make_tuple(bigsum_C1__ACCTBAL,bigsum_C1__CUSTKEY,NATIONKEY)] : 0 );
            }
        }
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it18 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end17 = q.end();
    for (; q_it18 != q_end17; ++q_it18)
    {
        int64_t NATIONKEY = q_it18->first;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_it16 = bigsum_C1__CUSTKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_end15 = bigsum_C1__CUSTKEY_dom.end();
        for (
            ; bigsum_C1__CUSTKEY_dom_it16 != bigsum_C1__CUSTKEY_dom_end15; 
            ++bigsum_C1__CUSTKEY_dom_it16)
        {
            int64_t bigsum_C1__CUSTKEY = *bigsum_C1__CUSTKEY_dom_it16;
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_it14 = bigsum_C1__ACCTBAL_dom.begin();
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_end13 = bigsum_C1__ACCTBAL_dom.end();
            for (
                ; bigsum_C1__ACCTBAL_dom_it14 != bigsum_C1__ACCTBAL_dom_end13; 
                ++bigsum_C1__ACCTBAL_dom_it14)
            {
                double bigsum_C1__ACCTBAL = *bigsum_C1__ACCTBAL_dom_it14;
                q[NATIONKEY] +=
                    -1*( ( ( qCUSTOMER2[bigsum_C1__CUSTKEY] != 0 )
                           && ( bigsum_C1__ACCTBAL < qCUSTOMER1 )
                           && ( 0 == qCUSTOMER2[bigsum_C1__CUSTKEY] ) )?
                         qCUSTOMER3[make_tuple(bigsum_C1__ACCTBAL,bigsum_C1__CUSTKEY,NATIONKEY)] : 0 );
            }
        }
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it24 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end23 = q.end();
    for (; q_it24 != q_end23; ++q_it24)
    {
        int64_t NATIONKEY = q_it24->first;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_it22 = bigsum_C1__CUSTKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_end21 = bigsum_C1__CUSTKEY_dom.end();
        for (
            ; bigsum_C1__CUSTKEY_dom_it22 != bigsum_C1__CUSTKEY_dom_end21; 
            ++bigsum_C1__CUSTKEY_dom_it22)
        {
            int64_t bigsum_C1__CUSTKEY = *bigsum_C1__CUSTKEY_dom_it22;
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_it20 = bigsum_C1__ACCTBAL_dom.begin();
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_end19 = bigsum_C1__ACCTBAL_dom.end();
            for (
                ; bigsum_C1__ACCTBAL_dom_it20 != bigsum_C1__ACCTBAL_dom_end19; 
                ++bigsum_C1__ACCTBAL_dom_it20)
            {
                double bigsum_C1__ACCTBAL = *bigsum_C1__ACCTBAL_dom_it20;
                q[NATIONKEY] += ( ( ( bigsum_C1__ACCTBAL < qCUSTOMER1+((0 < ACCTBAL)? ACCTBAL : 0) )
                                    && ( 0 == qCUSTOMER2[bigsum_C1__CUSTKEY] )
                                    && ( qCUSTOMER1 <= bigsum_C1__ACCTBAL ) )?
                     qCUSTOMER3[make_tuple(bigsum_C1__ACCTBAL,bigsum_C1__CUSTKEY,NATIONKEY)] : 0 );
            }
        }
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it30 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end29 = q.end();
    for (; q_it30 != q_end29; ++q_it30)
    {
        int64_t NATIONKEY = q_it30->first;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_it28 = bigsum_C1__CUSTKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_end27 = bigsum_C1__CUSTKEY_dom.end();
        for (
            ; bigsum_C1__CUSTKEY_dom_it28 != bigsum_C1__CUSTKEY_dom_end27; 
            ++bigsum_C1__CUSTKEY_dom_it28)
        {
            int64_t bigsum_C1__CUSTKEY = *bigsum_C1__CUSTKEY_dom_it28;
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_it26 = bigsum_C1__ACCTBAL_dom.begin();
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_end25 = bigsum_C1__ACCTBAL_dom.end();
            for (
                ; bigsum_C1__ACCTBAL_dom_it26 != bigsum_C1__ACCTBAL_dom_end25; 
                ++bigsum_C1__ACCTBAL_dom_it26)
            {
                double bigsum_C1__ACCTBAL = *bigsum_C1__ACCTBAL_dom_it26;
                q[NATIONKEY] += ( ( ( bigsum_C1__ACCTBAL < qCUSTOMER1+((0 < ACCTBAL)? ACCTBAL : 0) )
                                    && ( 0 == qCUSTOMER2[bigsum_C1__CUSTKEY] )
                                    && ( qCUSTOMER2[bigsum_C1__CUSTKEY] != 0 ) )?
                     qCUSTOMER3[make_tuple(bigsum_C1__ACCTBAL,bigsum_C1__CUSTKEY,NATIONKEY)] : 0 );
            }
        }
    }

    qCUSTOMER1 += (0 < ACCTBAL)? ACCTBAL : 0;

    map<tuple<double,int64_t,int64_t>,double,std::less<tuple<double,int64_t,
        int64_t> >,boost::pool_allocator<pair<tuple<double,int64_t,int64_t>,double> > 
        >::iterator qCUSTOMER3_it34 = qCUSTOMER3.find(make_tuple(ACCTBAL,CUSTKEY,NATIONKEY));

    set<double, std::less<double>,
        boost::pool_allocator<double> >::iterator bigsum_C1__ACCTBAL_dom_it32 = 
        bigsum_C1__ACCTBAL_dom.find(ACCTBAL);

    if ( !(qCUSTOMER3_it34 == qCUSTOMER3.end() ||
           bigsum_C1__ACCTBAL_dom_it32 == bigsum_C1__ACCTBAL_dom.end()) )
    {
        qCUSTOMER3[make_tuple(ACCTBAL,CUSTKEY,NATIONKEY)] += ACCTBAL;
    }
    

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
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it40 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end39 = q.end();
    for (; q_it40 != q_end39; ++q_it40)
    {
        int64_t C1__NATIONKEY = q_it40->first;

        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_it38 = bigsum_C1__CUSTKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_end37 = bigsum_C1__CUSTKEY_dom.end();
        for (
            ; bigsum_C1__CUSTKEY_dom_it38 != bigsum_C1__CUSTKEY_dom_end37; 
            ++bigsum_C1__CUSTKEY_dom_it38)
        {
            int64_t bigsum_C1__CUSTKEY = *bigsum_C1__CUSTKEY_dom_it38;
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_it36 = bigsum_C1__ACCTBAL_dom.begin();
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_end35 = bigsum_C1__ACCTBAL_dom.end();
            for (
                ; bigsum_C1__ACCTBAL_dom_it36 != bigsum_C1__ACCTBAL_dom_end35; 
                ++bigsum_C1__ACCTBAL_dom_it36)
            {
                double bigsum_C1__ACCTBAL = *bigsum_C1__ACCTBAL_dom_it36;
                q[C1__NATIONKEY] +=
                    -1*( ( ( qCUSTOMER1 <= bigsum_C1__ACCTBAL )
                           && ( bigsum_C1__ACCTBAL < qCUSTOMER1 )
                           && ( 0 == qCUSTOMER2[bigsum_C1__CUSTKEY] ) )?
                         qCUSTOMER3[make_tuple(bigsum_C1__ACCTBAL,bigsum_C1__CUSTKEY,C1__NATIONKEY)] : 0 );
            }
        }
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it46 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end45 = q.end();
    for (; q_it46 != q_end45; ++q_it46)
    {
        int64_t C1__NATIONKEY = q_it46->first;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_it44 = bigsum_C1__CUSTKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_end43 = bigsum_C1__CUSTKEY_dom.end();
        for (
            ; bigsum_C1__CUSTKEY_dom_it44 != bigsum_C1__CUSTKEY_dom_end43; 
            ++bigsum_C1__CUSTKEY_dom_it44)
        {
            int64_t bigsum_C1__CUSTKEY = *bigsum_C1__CUSTKEY_dom_it44;
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_it42 = bigsum_C1__ACCTBAL_dom.begin();
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_end41 = bigsum_C1__ACCTBAL_dom.end();
            for (
                ; bigsum_C1__ACCTBAL_dom_it42 != bigsum_C1__ACCTBAL_dom_end41; 
                ++bigsum_C1__ACCTBAL_dom_it42)
            {
                double bigsum_C1__ACCTBAL = *bigsum_C1__ACCTBAL_dom_it42;
                q[C1__NATIONKEY] +=
                    -1*( ( ( qCUSTOMER2[bigsum_C1__CUSTKEY]+((CUSTKEY == bigsum_C1__CUSTKEY )? 1 : 0) != 0 )
                           && ( bigsum_C1__ACCTBAL < qCUSTOMER1 )
                           && ( 0 == qCUSTOMER2[bigsum_C1__CUSTKEY] ) )?
                     qCUSTOMER3[make_tuple(bigsum_C1__ACCTBAL,bigsum_C1__CUSTKEY,C1__NATIONKEY)] : 0 );
            }
        }
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it52 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end51 = q.end();
    for (; q_it52 != q_end51; ++q_it52)
    {
        int64_t C1__NATIONKEY = q_it52->first;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_it50 = bigsum_C1__CUSTKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_end49 = bigsum_C1__CUSTKEY_dom.end();
        for (
            ; bigsum_C1__CUSTKEY_dom_it50 != bigsum_C1__CUSTKEY_dom_end49; 
            ++bigsum_C1__CUSTKEY_dom_it50)
        {
            int64_t bigsum_C1__CUSTKEY = *bigsum_C1__CUSTKEY_dom_it50;
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_it48 = bigsum_C1__ACCTBAL_dom.begin();
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_end47 = bigsum_C1__ACCTBAL_dom.end();
            for (
                ; bigsum_C1__ACCTBAL_dom_it48 != bigsum_C1__ACCTBAL_dom_end47; 
                ++bigsum_C1__ACCTBAL_dom_it48)
            {
                double bigsum_C1__ACCTBAL = *bigsum_C1__ACCTBAL_dom_it48;
                q[C1__NATIONKEY] += ( ( ( bigsum_C1__ACCTBAL < qCUSTOMER1 )
                     && ( 0 == qCUSTOMER2[bigsum_C1__CUSTKEY]+((CUSTKEY == bigsum_C1__CUSTKEY)? 1 : 0) )
                     && ( qCUSTOMER1 <= bigsum_C1__ACCTBAL ) )?
                     qCUSTOMER3[make_tuple(bigsum_C1__ACCTBAL,bigsum_C1__CUSTKEY,C1__NATIONKEY)] : 0 );
            }
        }
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it58 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end57 = q.end();
    for (; q_it58 != q_end57; ++q_it58)
    {
        int64_t C1__NATIONKEY = q_it58->first;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_it56 = bigsum_C1__CUSTKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_end55 = bigsum_C1__CUSTKEY_dom.end();
        for (
            ; bigsum_C1__CUSTKEY_dom_it56 != bigsum_C1__CUSTKEY_dom_end55; 
            ++bigsum_C1__CUSTKEY_dom_it56)
        {
            int64_t bigsum_C1__CUSTKEY = *bigsum_C1__CUSTKEY_dom_it56;
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_it54 = bigsum_C1__ACCTBAL_dom.begin();
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_end53 = bigsum_C1__ACCTBAL_dom.end();
            for (
                ; bigsum_C1__ACCTBAL_dom_it54 != bigsum_C1__ACCTBAL_dom_end53; 
                ++bigsum_C1__ACCTBAL_dom_it54)
            {
                double bigsum_C1__ACCTBAL = *bigsum_C1__ACCTBAL_dom_it54;
                q[C1__NATIONKEY] += ( ( ( bigsum_C1__ACCTBAL < qCUSTOMER1 )
                     && (0 == qCUSTOMER2[bigsum_C1__CUSTKEY]+((CUSTKEY == bigsum_C1__CUSTKEY)? 1 : 0))
                     && ( qCUSTOMER2[bigsum_C1__CUSTKEY] != 0 ) )?
                     qCUSTOMER3[make_tuple(bigsum_C1__ACCTBAL,bigsum_C1__CUSTKEY,C1__NATIONKEY)] : 0 );
            }
        }
    }

    map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> 
        > >::iterator qCUSTOMER2_it60 = qCUSTOMER2.find(CUSTKEY);

    if ( qCUSTOMER2_it60 != qCUSTOMER2.end() )
    {
        qCUSTOMER2[CUSTKEY] += 1;
    }

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_ORDERS_sec_span, on_insert_ORDERS_usec_span);
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
    if ( bigsum_C1__ACCTBAL_dom.find(ACCTBAL) == bigsum_C1__ACCTBAL_dom.end()
         && bigsum_C1__CUSTKEY_dom.find(CUSTKEY) == bigsum_C1__CUSTKEY_dom.end() )
    {
        qCUSTOMER3.erase(make_tuple(ACCTBAL,CUSTKEY,NATIONKEY));
    }

    set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::iterator 
        bigsum_C1__CUSTKEY_dom_it64 = bigsum_C1__CUSTKEY_dom.find(CUSTKEY);

    set<double, std::less<double>,
        boost::pool_allocator<double> >::iterator bigsum_C1__ACCTBAL_dom_it62 = 
        bigsum_C1__ACCTBAL_dom.find(ACCTBAL);

    if ( !(bigsum_C1__CUSTKEY_dom_it64 == bigsum_C1__CUSTKEY_dom.end() ||
           bigsum_C1__ACCTBAL_dom_it62 == bigsum_C1__ACCTBAL_dom.end()) )
    {
        q[NATIONKEY] += -1*( ( ( ACCTBAL < qCUSTOMER1+-1*((0<ACCTBAL)? ACCTBAL : 0) )
                && ( 0 == qCUSTOMER2[CUSTKEY] ) )?
                ACCTBAL : 0 );
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it70 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end69 = q.end();
    for (; q_it70 != q_end69; ++q_it70)
    {
        int64_t NATIONKEY = q_it70->first;

        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_it68 = bigsum_C1__CUSTKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_end67 = bigsum_C1__CUSTKEY_dom.end();
        for (
            ; bigsum_C1__CUSTKEY_dom_it68 != bigsum_C1__CUSTKEY_dom_end67; 
            ++bigsum_C1__CUSTKEY_dom_it68)
        {
            int64_t bigsum_C1__CUSTKEY = *bigsum_C1__CUSTKEY_dom_it68;
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_it66 = bigsum_C1__ACCTBAL_dom.begin();
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_end65 = bigsum_C1__ACCTBAL_dom.end();
            for (
                ; bigsum_C1__ACCTBAL_dom_it66 != bigsum_C1__ACCTBAL_dom_end65; 
                ++bigsum_C1__ACCTBAL_dom_it66)
            {
                double bigsum_C1__ACCTBAL = *bigsum_C1__ACCTBAL_dom_it66;
                q[NATIONKEY] += ( ( ( bigsum_C1__ACCTBAL < qCUSTOMER1+-1*(0 < ACCTBAL? ACCTBAL : 0) )
                            && ( 0 == qCUSTOMER2[bigsum_C1__CUSTKEY] )
                            && (qCUSTOMER1 <= bigsum_C1__ACCTBAL ) )?
                     qCUSTOMER3[make_tuple(bigsum_C1__ACCTBAL,bigsum_C1__CUSTKEY,NATIONKEY)] : 0 );
            }
        }
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it76 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end75 = q.end();
    for (; q_it76 != q_end75; ++q_it76)
    {
        int64_t NATIONKEY = q_it76->first;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_it74 = bigsum_C1__CUSTKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_end73 = bigsum_C1__CUSTKEY_dom.end();
        for (
            ; bigsum_C1__CUSTKEY_dom_it74 != bigsum_C1__CUSTKEY_dom_end73; 
            ++bigsum_C1__CUSTKEY_dom_it74)
        {
            int64_t bigsum_C1__CUSTKEY = *bigsum_C1__CUSTKEY_dom_it74;
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_it72 = bigsum_C1__ACCTBAL_dom.begin();
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_end71 = bigsum_C1__ACCTBAL_dom.end();
            for (
                ; bigsum_C1__ACCTBAL_dom_it72 != bigsum_C1__ACCTBAL_dom_end71; 
                ++bigsum_C1__ACCTBAL_dom_it72)
            {
                double bigsum_C1__ACCTBAL = *bigsum_C1__ACCTBAL_dom_it72;
                q[NATIONKEY] += ( ( ( bigsum_C1__ACCTBAL < qCUSTOMER1+-1*(0 < ACCTBAL?ACCTBAL:0) )
                    && ( 0 == qCUSTOMER2[bigsum_C1__CUSTKEY] )
                    && ( qCUSTOMER2[bigsum_C1__CUSTKEY] != 0 ) )?
                    qCUSTOMER3[make_tuple(bigsum_C1__ACCTBAL,bigsum_C1__CUSTKEY,NATIONKEY)] : 0 );
            }
        }
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it82 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end81 = q.end();
    for (; q_it82 != q_end81; ++q_it82)
    {
        int64_t NATIONKEY = q_it82->first;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_it80 = bigsum_C1__CUSTKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_end79 = bigsum_C1__CUSTKEY_dom.end();
        for (
            ; bigsum_C1__CUSTKEY_dom_it80 != bigsum_C1__CUSTKEY_dom_end79; 
            ++bigsum_C1__CUSTKEY_dom_it80)
        {
            int64_t bigsum_C1__CUSTKEY = *bigsum_C1__CUSTKEY_dom_it80;
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_it78 = bigsum_C1__ACCTBAL_dom.begin();
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_end77 = bigsum_C1__ACCTBAL_dom.end();
            for (
                ; bigsum_C1__ACCTBAL_dom_it78 != bigsum_C1__ACCTBAL_dom_end77; 
                ++bigsum_C1__ACCTBAL_dom_it78)
            {
                double bigsum_C1__ACCTBAL = *bigsum_C1__ACCTBAL_dom_it78;
                q[NATIONKEY] +=
                    -1*( ( ( qCUSTOMER1+-1*(0<ACCTBAL?ACCTBAL:0) <= bigsum_C1__ACCTBAL )
                       && ( bigsum_C1__ACCTBAL < qCUSTOMER1 )
                       && ( 0 == qCUSTOMER2[bigsum_C1__CUSTKEY] ) )?
                       qCUSTOMER3[make_tuple(bigsum_C1__ACCTBAL,bigsum_C1__CUSTKEY,NATIONKEY)] : 0 );
            }
        }
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it88 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end87 = q.end();
    for (; q_it88 != q_end87; ++q_it88)
    {
        int64_t NATIONKEY = q_it88->first;

        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_it86 = bigsum_C1__CUSTKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_end85 = bigsum_C1__CUSTKEY_dom.end();
        for (
            ; bigsum_C1__CUSTKEY_dom_it86 != bigsum_C1__CUSTKEY_dom_end85; 
            ++bigsum_C1__CUSTKEY_dom_it86)
        {
            int64_t bigsum_C1__CUSTKEY = *bigsum_C1__CUSTKEY_dom_it86;
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_it84 = bigsum_C1__ACCTBAL_dom.begin();
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_end83 = bigsum_C1__ACCTBAL_dom.end();
            for (
                ; bigsum_C1__ACCTBAL_dom_it84 != bigsum_C1__ACCTBAL_dom_end83; 
                ++bigsum_C1__ACCTBAL_dom_it84)
            {
                double bigsum_C1__ACCTBAL = *bigsum_C1__ACCTBAL_dom_it84;
                q[NATIONKEY] +=
                    -1*( ( ( qCUSTOMER2[bigsum_C1__CUSTKEY] != 0 )
                        && ( bigsum_C1__ACCTBAL < qCUSTOMER1 )
                        && ( 0 == qCUSTOMER2[bigsum_C1__CUSTKEY] ) )?
                        qCUSTOMER3[make_tuple(bigsum_C1__ACCTBAL,bigsum_C1__CUSTKEY,NATIONKEY)] : 0 );
            }
        }
    }

    qCUSTOMER1 += (0 < ACCTBAL? -ACCTBAL : 0);

    map<tuple<double,int64_t,int64_t>,double,std::less<tuple<double,int64_t,
        int64_t> >,boost::pool_allocator<pair<tuple<double,int64_t,int64_t>,double> > 
        >::iterator qCUSTOMER3_it92 = qCUSTOMER3.find(make_tuple(ACCTBAL,CUSTKEY,NATIONKEY));
    
    set<double, std::less<double>,
        boost::pool_allocator<double> >::iterator bigsum_C1__ACCTBAL_dom_it90 = 
        bigsum_C1__ACCTBAL_dom.find(ACCTBAL);

    if ( !( qCUSTOMER3_it92 == qCUSTOMER3.end() ||
            bigsum_C1__ACCTBAL_dom_it90 == bigsum_C1__ACCTBAL_dom.end() ) )
    {
        qCUSTOMER3[make_tuple(ACCTBAL,CUSTKEY,NATIONKEY)] += -ACCTBAL;
    }

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
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it98 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end97 = q.end();
    for (; q_it98 != q_end97; ++q_it98)
    {
        int64_t C1__NATIONKEY = q_it98->first;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_it96 = bigsum_C1__CUSTKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_end95 = bigsum_C1__CUSTKEY_dom.end();
        for (
            ; bigsum_C1__CUSTKEY_dom_it96 != bigsum_C1__CUSTKEY_dom_end95; 
            ++bigsum_C1__CUSTKEY_dom_it96)
        {
            int64_t bigsum_C1__CUSTKEY = *bigsum_C1__CUSTKEY_dom_it96;
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_it94 = bigsum_C1__ACCTBAL_dom.begin();
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_end93 = bigsum_C1__ACCTBAL_dom.end();
            for (
                ; bigsum_C1__ACCTBAL_dom_it94 != bigsum_C1__ACCTBAL_dom_end93; 
                ++bigsum_C1__ACCTBAL_dom_it94)
            {
                double bigsum_C1__ACCTBAL = *bigsum_C1__ACCTBAL_dom_it94;
                q[C1__NATIONKEY] += ( ( ( bigsum_C1__ACCTBAL < qCUSTOMER1 )
                            && ( 0 == qCUSTOMER2[bigsum_C1__CUSTKEY]+-1*(CUSTKEY == bigsum_C1__CUSTKEY? 1:0) )
                            && ( qCUSTOMER1 <= bigsum_C1__ACCTBAL ) )?
                     qCUSTOMER3[make_tuple(bigsum_C1__ACCTBAL,bigsum_C1__CUSTKEY,C1__NATIONKEY)] : 0 );
            }
        }
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it104 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end103 = q.end();
    for (; q_it104 != q_end103; ++q_it104)
    {
        int64_t C1__NATIONKEY = q_it104->first;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_it102 = bigsum_C1__CUSTKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_end101 = bigsum_C1__CUSTKEY_dom.end();
        for (
            ; bigsum_C1__CUSTKEY_dom_it102 != bigsum_C1__CUSTKEY_dom_end101; 
            ++bigsum_C1__CUSTKEY_dom_it102)
        {
            int64_t bigsum_C1__CUSTKEY = *bigsum_C1__CUSTKEY_dom_it102;
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_it100 = bigsum_C1__ACCTBAL_dom.begin();
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_end99 = bigsum_C1__ACCTBAL_dom.end();
            for (
                ; bigsum_C1__ACCTBAL_dom_it100 != bigsum_C1__ACCTBAL_dom_end99; 
                ++bigsum_C1__ACCTBAL_dom_it100)
            {
                double bigsum_C1__ACCTBAL = *bigsum_C1__ACCTBAL_dom_it100;
                q[C1__NATIONKEY] += ( ( ( bigsum_C1__ACCTBAL < qCUSTOMER1 )
                    && ( 0 == qCUSTOMER2[bigsum_C1__CUSTKEY]+-1*(CUSTKEY == bigsum_C1__CUSTKEY? 1:0 ) )
                    && ( qCUSTOMER2[bigsum_C1__CUSTKEY] != 0 ) )?
                    qCUSTOMER3[make_tuple(bigsum_C1__ACCTBAL,bigsum_C1__CUSTKEY,C1__NATIONKEY)] : 0);
            }
        }
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it110 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end109 = q.end();
    for (; q_it110 != q_end109; ++q_it110)
    {
        int64_t C1__NATIONKEY = q_it110->first;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_it108 = bigsum_C1__CUSTKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_end107 = bigsum_C1__CUSTKEY_dom.end();
        for (
            ; bigsum_C1__CUSTKEY_dom_it108 != bigsum_C1__CUSTKEY_dom_end107; 
            ++bigsum_C1__CUSTKEY_dom_it108)
        {
            int64_t bigsum_C1__CUSTKEY = *bigsum_C1__CUSTKEY_dom_it108;
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_it106 = bigsum_C1__ACCTBAL_dom.begin();
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_end105 = bigsum_C1__ACCTBAL_dom.end();
            for (
                ; bigsum_C1__ACCTBAL_dom_it106 != bigsum_C1__ACCTBAL_dom_end105; 
                ++bigsum_C1__ACCTBAL_dom_it106)
            {
                double bigsum_C1__ACCTBAL = *bigsum_C1__ACCTBAL_dom_it106;
                q[C1__NATIONKEY] +=
                    -1*( ( ( qCUSTOMER1 <= bigsum_C1__ACCTBAL )
                    && ( bigsum_C1__ACCTBAL < qCUSTOMER1 )
                    && ( 0 == qCUSTOMER2[bigsum_C1__CUSTKEY] ) )?
                    qCUSTOMER3[make_tuple(bigsum_C1__ACCTBAL,bigsum_C1__CUSTKEY,C1__NATIONKEY)] : 0 );
            }
        }
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it116 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end115 = q.end();
    for (; q_it116 != q_end115; ++q_it116)
    {
        int64_t C1__NATIONKEY = q_it116->first;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_it114 = bigsum_C1__CUSTKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_C1__CUSTKEY_dom_end113 = bigsum_C1__CUSTKEY_dom.end();
        for (
            ; bigsum_C1__CUSTKEY_dom_it114 != bigsum_C1__CUSTKEY_dom_end113; 
            ++bigsum_C1__CUSTKEY_dom_it114)
        {
            int64_t bigsum_C1__CUSTKEY = *bigsum_C1__CUSTKEY_dom_it114;
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_it112 = bigsum_C1__ACCTBAL_dom.begin();
            set<double, std::less<double>, boost::pool_allocator<double> 
                >::iterator bigsum_C1__ACCTBAL_dom_end111 = bigsum_C1__ACCTBAL_dom.end();
            for (
                ; bigsum_C1__ACCTBAL_dom_it112 != bigsum_C1__ACCTBAL_dom_end111; 
                ++bigsum_C1__ACCTBAL_dom_it112)
            {
                double bigsum_C1__ACCTBAL = *bigsum_C1__ACCTBAL_dom_it112;
                q[C1__NATIONKEY] +=
                    -1*( ( ( qCUSTOMER2[bigsum_C1__CUSTKEY]+-1*(CUSTKEY == bigsum_C1__CUSTKEY? 1:0) != 0 )
                    && ( bigsum_C1__ACCTBAL < qCUSTOMER1 )
                    && ( 0 == qCUSTOMER2[bigsum_C1__CUSTKEY] ) )?
                    qCUSTOMER3[make_tuple(bigsum_C1__ACCTBAL,bigsum_C1__CUSTKEY,C1__NATIONKEY)] : 0);
            }
        }
    }

    map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> 
        > >::iterator qCUSTOMER2_it118 = qCUSTOMER2.find(CUSTKEY);

    if ( qCUSTOMER2_it118 != qCUSTOMER2.end() )
    {
        qCUSTOMER2[CUSTKEY] += -1;
    }

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_ORDERS_sec_span, on_delete_ORDERS_usec_span);
}

DBToaster::DemoDatasets::CustomerStream SSBCustomer("/Users/yanif/datasets/tpch/sf1/singlefile/customer.tbl.a",&DBToaster::DemoDatasets::parseCustomerField,8,1600000, 512);

boost::shared_ptr<DBToaster::DemoDatasets::CustomerTupleAdaptor> SSBCustomer_adaptor(new DBToaster::DemoDatasets::CustomerTupleAdaptor());
static int streamSSBCustomerId = 0;

struct on_insert_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_insert_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_insert_CUSTOMER_fun_obj fo_on_insert_CUSTOMER_0;

DBToaster::DemoDatasets::OrderStream SSBOrder("/Users/yanif/datasets/tpch/sf1/singlefile/orders.tbl.a",&DBToaster::DemoDatasets::parseOrderField,9,17000000,512);

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

struct on_delete_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_delete_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_delete_CUSTOMER_fun_obj fo_on_delete_CUSTOMER_2;

struct on_delete_ORDERS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::OrderTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::OrderTupleAdaptor::Result>(data); 
        on_delete_ORDERS(results, log, stats, input.orderkey,input.custkey,input.orderstatus,input.totalprice,input.orderdate,input.orderpriority,input.clerk,input.shippriority,input.comment);
    }
};

on_delete_ORDERS_fun_obj fo_on_delete_ORDERS_3;


void init(DBToaster::StandaloneEngine::FileMultiplexer& sources,
    DBToaster::StandaloneEngine::FileStreamDispatcher& router, 
    ofstream* results, ofstream* log, ofstream* stats)
{
    router.setFiles(results, log, stats);
    sources.addStream<DBToaster::DemoDatasets::customer>(&SSBCustomer, boost::ref(*SSBCustomer_adaptor), streamSSBCustomerId);
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_CUSTOMER_0));
    sources.addStream<DBToaster::DemoDatasets::order>(&SSBOrder, boost::ref(*SSBOrder_adaptor), streamSSBOrderId);
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_ORDERS_1));
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_CUSTOMER_2));
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_ORDERS_3));
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
