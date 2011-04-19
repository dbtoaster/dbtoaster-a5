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

map<int64_t,double> q;

map<tuple<int64_t,int64_t>,double> qCUSTOMER1;

map<tuple<int64_t,int64_t>,double> qCUSTOMER1ORDERS1;

// Note this is a redundant map -- equivalent to qLINEITEM1CUSTOMER1 below,
// but the duplicate is not detected due to differences in implicit/explicit
// representation of constraints as either bound vars in relation schemas,
// or additional constraint formula in the calculus representation.
// See log for details.
map<tuple<int64_t,int64_t,int64_t>,int> qCUSTOMER1LINEITEM1;

map<tuple<int64_t,int64_t>,int> qCUSTOMER2; 

map<int64_t,double> qCUSTOMER3; 

map<tuple<int64_t,int64_t>,double> qCUSTOMER4;

map<tuple<int64_t,int64_t,int64_t>,int> qCUSTOMER4LINEITEM1;

map<tuple<int64_t,int64_t>,double> qCUSTOMER4ORDERS1;

map<tuple<int64_t,int64_t>,double> qORDERS1;

map<int64_t,int> qORDERS4;

multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> > LINEITEM;

map<tuple<int64_t,int64_t,int64_t>,int> qLINEITEM1;

// See note above regarding duplication of this map.
map<tuple<int64_t,int64_t>,int> qLINEITEM1CUSTOMER1;


// Bigsum var domains.
set<int64_t> bigsum_L1__ORDERKEY_dom;

set<int64_t> bigsum_L2__ORDERKEY_dom;

// Accumulation temporaries
int cstr615;
double q452;
double q376;
int cstr454;
double q507;
int cstr379;
int cstr64;
double q61;
int cstr169;
double q215;
int cstr509;
int cstr125;
double q301;
int cstr210;
int cstr259;
int cstr558;
double q563;
int cstr216;
int cstr303;
int cstr1;
int cstr564;





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
   cout << "qCUSTOMER1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER2 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER2" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER3 size: " << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER3" << "," << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   cout << "qCUSTOMER4 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER4.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER4" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER4.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "bigsum_L1__ORDERKEY_dom size: " << (((sizeof(set<int64_t>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_L1__ORDERKEY_dom.size())  + (sizeof(struct _Rb_tree<set<int64_t>::key_type, set<int64_t>::value_type, _Identity<set<int64_t>::value_type>, set<int64_t>::key_compare>))) << endl;

   (*stats) << "m," << "bigsum_L1__ORDERKEY_dom" << "," << (((sizeof(set<int64_t>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_L1__ORDERKEY_dom.size())  + (sizeof(struct _Rb_tree<set<int64_t>::key_type, set<int64_t>::value_type, _Identity<set<int64_t>::value_type>, set<int64_t>::key_compare>))) << endl;

   cout << "qCUSTOMER4LINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER4LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER4LINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER4LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "bigsum_L2__ORDERKEY_dom size: " << (((sizeof(set<int64_t>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_L2__ORDERKEY_dom.size())  + (sizeof(struct _Rb_tree<set<int64_t>::key_type, set<int64_t>::value_type, _Identity<set<int64_t>::value_type>, set<int64_t>::key_compare>))) << endl;

   (*stats) << "m," << "bigsum_L2__ORDERKEY_dom" << "," << (((sizeof(set<int64_t>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_L2__ORDERKEY_dom.size())  + (sizeof(struct _Rb_tree<set<int64_t>::key_type, set<int64_t>::value_type, _Identity<set<int64_t>::value_type>, set<int64_t>::key_compare>))) << endl;

   cout << "qCUSTOMER4ORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER4ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER4ORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER4ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "q size: " << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   (*stats) << "m," << "q" << "," << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   cout << "qORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS4 size: " << (((sizeof(map<int64_t,int>::key_type)
       + sizeof(map<int64_t,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS4.size())  + (sizeof(struct _Rb_tree<map<int64_t,int>::key_type, map<int64_t,int>::value_type, _Select1st<map<int64_t,int>::value_type>, map<int64_t,int>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS4" << "," << (((sizeof(map<int64_t,int>::key_type)
       + sizeof(map<int64_t,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS4.size())  + (sizeof(struct _Rb_tree<map<int64_t,int>::key_type, map<int64_t,int>::value_type, _Select1st<map<int64_t,int>::value_type>, map<int64_t,int>::key_compare>))) << endl;

   cout << "qCUSTOMER1ORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1ORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qLINEITEM1CUSTOMER1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1CUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1CUSTOMER1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1CUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qLINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "LINEITEM size: " << (((sizeof(multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * LINEITEM.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_type, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type>, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_compare>))) << endl;

   (*stats) << "m," << "LINEITEM" << "," << (((sizeof(multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * LINEITEM.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_type, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type>, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_compare>))) << endl;

   cout << "qCUSTOMER1LINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1LINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   stats->flush();
   cout.flush();
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

   stats->flush();
   cout.flush();
}


void on_insert_LINEITEM(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    PARTKEY,int64_t SUPPKEY,int LINENUMBER,double QUANTITY,double 
    EXTENDEDPRICE,double DISCOUNT,double TAX)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    if ( qCUSTOMER2.find(make_tuple(ORDERKEY,ORDERKEY)) == qCUSTOMER2.end() )
    {
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_it2 = LINEITEM.begin();
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_end1 = LINEITEM.end();
        for (; LINEITEM_it2 != LINEITEM_end1; ++LINEITEM_it2)
        {
            int64_t protect_L1__ORDERKEY = get<0>(*LINEITEM_it2);
            if ( ORDERKEY == protect_L1__ORDERKEY )
            {
                qCUSTOMER2[make_tuple(ORDERKEY,ORDERKEY)] += 1;
            }
        }
    }
    if ( qCUSTOMER3.find(ORDERKEY) == qCUSTOMER3.end() )
    {
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>
            >::iterator LINEITEM_it4 = LINEITEM.begin();
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>
                 >::iterator LINEITEM_end3 = LINEITEM.end();
        for (; LINEITEM_it4 != LINEITEM_end3; ++LINEITEM_it4)
        {
            int64_t protect_L1__ORDERKEY = get<0>(*LINEITEM_it4);
            double L3__QUANTITY = get<4>(*LINEITEM_it4);
            if ( ORDERKEY == protect_L1__ORDERKEY )
            {
                qCUSTOMER3[ORDERKEY] += L3__QUANTITY;
            }
        }
    }

    bigsum_L2__ORDERKEY_dom.insert(ORDERKEY);
    bigsum_L1__ORDERKEY_dom.insert(ORDERKEY);

    LINEITEM.insert(make_tuple(
        ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDEDPRICE,DISCOUNT,TAX));

    map<int64_t,double>::iterator q_it28 = q.begin();
    map<int64_t,double>::iterator q_end27 = q.end();
    for (; q_it28 != q_end27; ++q_it28)
    {
        int64_t C__CUSTKEY = q_it28->first;
        q215 = 0.0;
        set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_it26 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_end25 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it26 != bigsum_L1__ORDERKEY_dom_end25; 
            ++bigsum_L1__ORDERKEY_dom_it26)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it26;

            // new = old + (dt.1 + dt.2 + dt.3), where
            // dt.1 = if new then df else 0
            // dt.2 = if new and not old then f else 0
            // dt.3 = if not new and old then -f else 0
            cstr216 = 0;

            // new.1 (old): sum_{bigsum_l2__orderkey} t
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it6 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_end5 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it6 != bigsum_L2__ORDERKEY_dom_end5; 
                ++bigsum_L2__ORDERKEY_dom_it6)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it6;
                cstr216 += ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY]?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0);
            }

            // new.2 (dt.1): sum_{bigsum_l2__orderkey} if new then dt else 0
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it10 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L1__ORDERKEY == ORDERKEY &&
                 (bigsum_L2__ORDERKEY_dom_it10 != bigsum_L2__ORDERKEY_dom.end()) )
            {
                cstr216 += (100 < qCUSTOMER3[ORDERKEY]+QUANTITY? 1 : 0);
            }


            // new.3 (dt.2): sum_{bigsum_l2__orderkey} if new and not old then t else 0
            cstr216 = 0;
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it14 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it14 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr216 += (
                    (100 < qCUSTOMER3[ORDERKEY]+QUANTITY) && ( qCUSTOMER3[ORDERKEY] <= 100 )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0 );
            }

            // new.4 (dt.3): sum_{bigsum_l2__orderkey} if not new and old then -t else 0
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it18 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it18 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr216 += -1*(
                    ( (qCUSTOMER3[ORDERKEY]+QUANTITY <= 100 ) && ( 100 < qCUSTOMER3[ORDERKEY] ) )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0 );
            }

            // old: sum_{bigsum_l2__orderkey} t
            cstr259 = 0;
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it22 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_end21 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it22 != bigsum_L2__ORDERKEY_dom_end21; 
                ++bigsum_L2__ORDERKEY_dom_it22)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it22;
                cstr259 += ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY]?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0);
            }

            // Not new and old => -f
            if ( ( cstr216 < 1 ) && ( 1 <= cstr259 ) )
            {
                q215 += qCUSTOMER4[make_tuple(bigsum_L1__ORDERKEY,C__CUSTKEY)];
            }
        }

        q[C__CUSTKEY] += -1*q215;
    }

    map<int64_t,double>::iterator q_it48 = q.begin();
    map<int64_t,double>::iterator q_end47 = q.end();
    for (; q_it48 != q_end47; ++q_it48)
    {
        int64_t C__CUSTKEY = q_it48->first;
        set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_it46 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_end45 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it46 != bigsum_L1__ORDERKEY_dom_end45; 
            ++bigsum_L1__ORDERKEY_dom_it46)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it46;

            // new = old + (dt.1 + dt.2 + dt.3), where
            // dt.1 = if new then df else 0
            // dt.2 = if new and not old then f else 0
            // dt.3 = if not new and old then -f else 0
            cstr125 = 0;

            // new.1 (old): sum_{bigsum_l2__orderkey} t
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it30 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_end29 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it30 != bigsum_L2__ORDERKEY_dom_end29; 
                ++bigsum_L2__ORDERKEY_dom_it30)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it30;
                cstr125 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0 );
            }

            // new.2 (dt.1): sum_{bigsum_l2__orderkey} if new then dt else 0
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it34 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);
            if ( bigsum_L1__ORDERKEY == ORDERKEY &&
                 ( bigsum_L2__ORDERKEY_dom_it34 != bigsum_L2__ORDERKEY_dom.end() ) )
            {
                cstr125 += (100 < qCUSTOMER3[ORDERKEY]+QUANTITY? 1 : 0);
            }


            // new.3 (dt.2): sum_{bigsum_l2__orderkey} if new and not old then t else 0
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it38 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it38 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr125 += ( ( (100 < qCUSTOMER3[ORDERKEY]+QUANTITY)
                               && ( qCUSTOMER3[ORDERKEY] <= 100 ) )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0);
            }

            // new.4 (dt.3): sum_{bigsum_l2__orderkey} if not new and old then -t else 0
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it42 = bigsum_L2__ORDERKEY_dom.begin();

            if ( bigsum_L2__ORDERKEY_dom_it42 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr125 += -1*( (
                    (qCUSTOMER3[ORDERKEY]+QUANTITY <= 100) && ( 100 < qCUSTOMER3[ORDERKEY] ) )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0 );
            }

            // if new then delta f
            if ( 1 <= cstr125 )
            {
                q[C__CUSTKEY] += QUANTITY*qLINEITEM1[make_tuple(
                    ORDERKEY,C__CUSTKEY,bigsum_L1__ORDERKEY)];
            }
        }
    }

    map<int64_t,double>::iterator q_it72 = q.begin();
    map<int64_t,double>::iterator q_end71 = q.end();
    for (; q_it72 != q_end71; ++q_it72)
    {
        int64_t C__CUSTKEY = q_it72->first;
        set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_it70 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_end69 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it70 != bigsum_L1__ORDERKEY_dom_end69; 
            ++bigsum_L1__ORDERKEY_dom_it70)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it70;

            // new = old + (dt.1 + dt.2 + dt.3), where
            // dt.1 = if new then df else 0
            // dt.2 = if new and not old then f else 0
            // dt.3 = if not new and old then -f else 0
            cstr169 = 0;

            // new.1 (old): sum_{bigsum_l2__orderkey} t
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it50 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_end49 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it50 != bigsum_L2__ORDERKEY_dom_end49; 
                ++bigsum_L2__ORDERKEY_dom_it50)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it50;
                cstr169 += ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY]?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0 );
            }


            // new.2 (dt.1): sum_{bigsum_l2__orderkey} if new then dt else 0
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it54 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L1__ORDERKEY == ORDERKEY &&
                 (bigsum_L2__ORDERKEY_dom_it54 != bigsum_L2__ORDERKEY_dom.end()) )
            {
                cstr169 += ((100 < qCUSTOMER3[ORDERKEY]+QUANTITY)? 1 : 0 );
            }

            // new.3 (dt.2): sum_{bigsum_l2__orderkey} if new and not old then t else 0
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it58 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if (  bigsum_L2__ORDERKEY_dom_it58 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr169 += (
                    (100 < qCUSTOMER3[ORDERKEY]+QUANTITY) && (qCUSTOMER3[ORDERKEY] <= 100)?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0 );

            }

            // new.4 (dt.3): sum_{bigsum_l2__orderkey} if not new and old then -t else 0
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it62 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it62 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr169 += -1*( (
                    (qCUSTOMER3[ORDERKEY]+QUANTITY <= 100) && (100 < qCUSTOMER3[ORDERKEY]) )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0 );
            }

            cstr210 = 0;
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it66 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_end65 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it66 != bigsum_L2__ORDERKEY_dom_end65; 
                ++bigsum_L2__ORDERKEY_dom_it66)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it66;
                cstr210 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0 );
            }

            // if new and not old then f
            if ( ( 1 <= cstr169 ) && ( cstr210 < 1 ) )
            {
                q[C__CUSTKEY] += qCUSTOMER4[make_tuple(bigsum_L1__ORDERKEY,C__CUSTKEY)];
            }
        }
    }

    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1_it74 = qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1_end73 = qCUSTOMER1.end();
    for (; qCUSTOMER1_it74 != qCUSTOMER1_end73; ++qCUSTOMER1_it74)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER1_it74->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1_it74->first);
        qCUSTOMER1[make_tuple(
            bigsum_L1__ORDERKEY,x_qCUSTOMER_C__CUSTKEY)] += 
            QUANTITY*qCUSTOMER1LINEITEM1[make_tuple(
            bigsum_L1__ORDERKEY,x_qCUSTOMER_C__CUSTKEY,ORDERKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1ORDERS1_it76 = qCUSTOMER1ORDERS1.find(make_tuple(ORDERKEY,ORDERKEY));

    if ( qCUSTOMER1ORDERS1_it76 != qCUSTOMER1ORDERS1.end() )
    {
        qCUSTOMER1ORDERS1[make_tuple(ORDERKEY,ORDERKEY)] += QUANTITY;
    }

    map<tuple<int64_t,int64_t>,int>::iterator qCUSTOMER2_it78 = qCUSTOMER2.find(make_tuple(ORDERKEY,ORDERKEY));

    if ( qCUSTOMER2_it78 != qCUSTOMER2.end() ) {
        qCUSTOMER2[make_tuple(ORDERKEY,ORDERKEY)] += 1;        
    }

    map<int64_t,double>::iterator qCUSTOMER3_it80 = qCUSTOMER3.find(ORDERKEY);

    if ( qCUSTOMER3_it80 != qCUSTOMER3.end() )
    {
        qCUSTOMER3[ORDERKEY] += QUANTITY;
    }

    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER4_it82 = qCUSTOMER4.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER4_end81 = qCUSTOMER4.end();
    for (; qCUSTOMER4_it82 != qCUSTOMER4_end81; ++qCUSTOMER4_it82)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER4_it82->first);
        int64_t C__CUSTKEY = get<1>(qCUSTOMER4_it82->first);
        qCUSTOMER4[make_tuple(
            bigsum_L1__ORDERKEY,C__CUSTKEY)] += QUANTITY*qCUSTOMER4LINEITEM1[make_tuple(
            bigsum_L1__ORDERKEY,C__CUSTKEY,ORDERKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER4ORDERS1_it84 = qCUSTOMER4ORDERS1.find(make_tuple(ORDERKEY,ORDERKEY));

    if ( qCUSTOMER4ORDERS1_it84 != qCUSTOMER4ORDERS1.end() )
    {
        qCUSTOMER4ORDERS1[make_tuple(ORDERKEY,ORDERKEY)] += QUANTITY;
    }

    map<tuple<int64_t,int64_t>,double>::iterator qORDERS1_it86 = qORDERS1.find(make_tuple(ORDERKEY, ORDERKEY));

    if ( qORDERS1_it86 != qORDERS1.end() )
    {
        qORDERS1[make_tuple(ORDERKEY,ORDERKEY)] += QUANTITY;
    }

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

    // This is an example of the kind of optimization missing in the calculus.
    // Here, for a constraint-only aggregate AggSum(f,r), we have
    // delta_{customers} r = 0, but still produce our 3-conditional expansion,
    // whereas we can reduce this down to a single conditional expansion (the
    // case for <if new_r then delta f>) since new_r = old_r, thus the latter
    // 2 cases redundant.

    // Note this is a doubly-redundant loop since not only is the bigsum for
    // bigsum_l1__orderkey redundant, but also the bigsum for bigsum_l2__orderkey
    // We should really handle these cases...

    // This subloop should never be needed, but cannot be removed due to equivalence to zero.
    // It should not be needed since customers should be inserted before any orders placed
    // by that customer, hence the sum(l1.quantity) aggregate should be zero on the
    // insertion of a customer. However, in case an order and lineitem, placed by the
    // customer DO exist before the customer, the qCUSTOMER1, qCUSTOMER2 and qCUSTOMER3 maps
    // used below will have non-zero entries. However, we still only need to 
    // use the single condition for the reasons described above.

    set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_it100 = bigsum_L1__ORDERKEY_dom.begin();
    set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_end99 = bigsum_L1__ORDERKEY_dom.end();
    for (
        ; bigsum_L1__ORDERKEY_dom_it100 != bigsum_L1__ORDERKEY_dom_end99; 
        ++bigsum_L1__ORDERKEY_dom_it100)
    {
        int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it100;

        // new.1 (old): sum_{bigsum_l2__orderkey} t
        // Note: delta t = 0
        cstr1 = 0;

        set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it88 = bigsum_L2__ORDERKEY_dom.begin();
        set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_end87 = bigsum_L2__ORDERKEY_dom.end();
        for (
            ; bigsum_L2__ORDERKEY_dom_it88 != bigsum_L2__ORDERKEY_dom_end87; 
            ++bigsum_L2__ORDERKEY_dom_it88)
        {
            int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it88;
            cstr1 += ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY]?
                qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0);
        }

        // if new then delta f
        if ( 1 <= cstr1 )
        {
            q[CUSTKEY] += qCUSTOMER1[make_tuple(bigsum_L1__ORDERKEY,CUSTKEY)];
        }
    }

    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER4_it142 = qCUSTOMER4.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER4_end141 = qCUSTOMER4.end();
    for (; qCUSTOMER4_it142 != qCUSTOMER4_end141; ++qCUSTOMER4_it142)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER4_it142->first);
        qCUSTOMER4[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY)] += qCUSTOMER1[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY)];
    }
    
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qCUSTOMER4LINEITEM1_it144 = qCUSTOMER4LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qCUSTOMER4LINEITEM1_end143 = qCUSTOMER4LINEITEM1.end();
    for (
        ; qCUSTOMER4LINEITEM1_it144 != qCUSTOMER4LINEITEM1_end143; 
        ++qCUSTOMER4LINEITEM1_it144)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER4LINEITEM1_it144->first);
        qCUSTOMER4LINEITEM1[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY,bigsum_L1__ORDERKEY)] += 
            qLINEITEM1CUSTOMER1[make_tuple(bigsum_L1__ORDERKEY,CUSTKEY)];
    }
    
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_it146 = qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_end145 = qLINEITEM1.end();
    for (; qLINEITEM1_it146 != qLINEITEM1_end145; ++qLINEITEM1_it146)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qLINEITEM1_it146->first);
        qLINEITEM1[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY,bigsum_L1__ORDERKEY)] += 
            qLINEITEM1CUSTOMER1[make_tuple(bigsum_L1__ORDERKEY,CUSTKEY)];
    }

    qORDERS4[CUSTKEY] += 1;
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

    // This constraint-only aggregate condition cannot be commented out,
    // just as above with on_insert_CUSTOMER, if we accept arbitrary
    // insertion sequences of customers, orders and lineitems.
    q61 = 0.0;
    set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_it160 = bigsum_L1__ORDERKEY_dom.begin();
    set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_end159 = bigsum_L1__ORDERKEY_dom.end();
    for (
        ; bigsum_L1__ORDERKEY_dom_it160 != bigsum_L1__ORDERKEY_dom_end159; 
        ++bigsum_L1__ORDERKEY_dom_it160)
    {
        int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it160;
        cstr64 = 0;

        set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it148 = bigsum_L2__ORDERKEY_dom.begin();
        set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_end147 = bigsum_L2__ORDERKEY_dom.end();
        for (
            ; bigsum_L2__ORDERKEY_dom_it148 != bigsum_L2__ORDERKEY_dom_end147; 
            ++bigsum_L2__ORDERKEY_dom_it148)
        {
            int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it148;
            cstr64 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                 ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
        }


        if ( 1 <= cstr64 )
        {
            q61 += qORDERS1[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)];
        }
    }
    q[CUSTKEY] += qORDERS4[CUSTKEY]*q61;


    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1_it202 = qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1_end201 = qCUSTOMER1.end();
    for (; qCUSTOMER1_it202 != qCUSTOMER1_end201; ++qCUSTOMER1_it202)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER1_it202->first);
        qCUSTOMER1[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY)] += qCUSTOMER1ORDERS1[make_tuple(
            bigsum_L1__ORDERKEY,ORDERKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qCUSTOMER1LINEITEM1_it204 = qCUSTOMER1LINEITEM1.find(make_tuple(ORDERKEY,CUSTKEY,ORDERKEY));

    if ( qCUSTOMER1LINEITEM1_it204 != qCUSTOMER1LINEITEM1.end() )
    {
        qCUSTOMER1LINEITEM1[make_tuple(ORDERKEY,CUSTKEY,ORDERKEY)] += 1;
    }

    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER4_it206 = qCUSTOMER4.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER4_end205 = qCUSTOMER4.end();
    for (; qCUSTOMER4_it206 != qCUSTOMER4_end205; ++qCUSTOMER4_it206)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER4_it206->first);
        qCUSTOMER4[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY)] += qCUSTOMER4ORDERS1[make_tuple(
            bigsum_L1__ORDERKEY,ORDERKEY)]*qORDERS4[CUSTKEY];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qCUSTOMER4LINEITEM1_it208 = qCUSTOMER4LINEITEM1.find(make_tuple(ORDERKEY,CUSTKEY,ORDERKEY));

    if ( qCUSTOMER4LINEITEM1_it208 != qCUSTOMER4LINEITEM1.end() )
    {
        qCUSTOMER4LINEITEM1[make_tuple(ORDERKEY,CUSTKEY,ORDERKEY)] += qORDERS4[CUSTKEY];
    }
    
    qLINEITEM1[make_tuple(ORDERKEY,CUSTKEY,ORDERKEY)] += qORDERS4[CUSTKEY];

    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1CUSTOMER1_it210 = qLINEITEM1CUSTOMER1.find(make_tuple(ORDERKEY,CUSTKEY));

    if ( qLINEITEM1CUSTOMER1_it210 != qLINEITEM1CUSTOMER1.end() )
    {
        qLINEITEM1CUSTOMER1[make_tuple(ORDERKEY,CUSTKEY)] += 1;
    }

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

    tuple<int64_t, int64_t, int64_t, int, double, double, double, double> li_tuple =
        make_tuple(ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDEDPRICE,DISCOUNT,TAX);

    LINEITEM.erase(li_tuple);
    // TODO: keep a count of P-values, i.e. select p,count(*) from bids group by p
    // For now let's skip domain finalization.

    /*
    if ( ( bigsum_L1__ORDERKEY_dom.find(ORDERKEY) == 
        bigsum_L1__ORDERKEY_dom.end() ) && ( bigsum_L2__ORDERKEY_dom.find(
        ORDERKEY) == bigsum_L2__ORDERKEY_dom.end() ) )
    {
        qCUSTOMER2.erase(make_tuple(ORDERKEY,ORDERKEY));
    }
    if ( bigsum_L2__ORDERKEY_dom.find(ORDERKEY) == bigsum_L2__ORDERKEY_dom.end() )
    {
        qCUSTOMER3.erase(ORDERKEY);
    }
    */

    map<int64_t,double>::iterator q_it230 = q.begin();
    map<int64_t,double>::iterator q_end229 = q.end();
    for (; q_it230 != q_end229; ++q_it230)
    {
        int64_t C__CUSTKEY = q_it230->first;
        q452 = 0.0;
        set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_it228 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_end227 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it228 != bigsum_L1__ORDERKEY_dom_end227; 
            ++bigsum_L1__ORDERKEY_dom_it228)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it228;
            cstr454 = 0;

            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it212 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_end211 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it212 != bigsum_L2__ORDERKEY_dom_end211; 
                ++bigsum_L2__ORDERKEY_dom_it212)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it212;
                cstr454 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0 );
            }


            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it216 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( (bigsum_L1__ORDERKEY == ORDERKEY &&
                  bigsum_L2__ORDERKEY_dom_it216 != bigsum_L2__ORDERKEY_dom.end()) )
            {
                cstr454 += -1*( (100 < qCUSTOMER3[ORDERKEY]-QUANTITY)? 1 : 0 );
            }

            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it220 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it220 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr454 +=
                    ( ( (100 < qCUSTOMER3[ORDERKEY]-QUANTITY)
                        && ( qCUSTOMER3[ORDERKEY] <= 100 ) )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0);
            }

            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it224 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if (  bigsum_L2__ORDERKEY_dom_it224 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr454 += -1*
                    ( ( ( (qCUSTOMER3[ORDERKEY]-QUANTITY) <= 100 )
                        && ( 100 < qCUSTOMER3[ORDERKEY] ) )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)]: 0);
            }

            if ( 1 <= cstr454 )
            {
                q452 += QUANTITY*qLINEITEM1[make_tuple(
                        ORDERKEY,C__CUSTKEY,bigsum_L1__ORDERKEY)];
            }
        }

        q[C__CUSTKEY] += -1*q452;
    }

    map<int64_t,double>::iterator q_it254 = q.begin();
    map<int64_t,double>::iterator q_end253 = q.end();
    for (; q_it254 != q_end253; ++q_it254)
    {
        int64_t C__CUSTKEY = q_it254->first;
        q507 = 0.0;
        set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_it252 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_end251 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it252 != bigsum_L1__ORDERKEY_dom_end251; 
            ++bigsum_L1__ORDERKEY_dom_it252)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it252;

            cstr509 = 0;
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it232 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_end231 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it232 != bigsum_L2__ORDERKEY_dom_end231; 
                ++bigsum_L2__ORDERKEY_dom_it232)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it232;
                cstr509 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0);
            }

            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it236 =
                bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( (bigsum_L1__ORDERKEY == ORDERKEY) &&
                 (bigsum_L2__ORDERKEY_dom_it236 != bigsum_L2__ORDERKEY_dom.end()) )
            {
                cstr509 += (100 < qCUSTOMER3[ORDERKEY]-QUANTITY )? -1 : 0;
            }


            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it240 =
                bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it240 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr509 +=
                    ( (100 < qCUSTOMER3[ORDERKEY]-QUANTITY) && (qCUSTOMER3[ORDERKEY] <= 100)?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0 );
            }

            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it244 =
                bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it244 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr509 += -1*
                    ( ( (qCUSTOMER3[ORDERKEY]-QUANTITY <= 100 )
                        && ( 100 < qCUSTOMER3[ORDERKEY] ) )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0 );
            }

            cstr558 = 0;
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it248 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_end247 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it248 != bigsum_L2__ORDERKEY_dom_end247; 
                ++bigsum_L2__ORDERKEY_dom_it248)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it248;
                cstr558 +=
                    ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                      qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0);
            }

            if ( ( 1 <= cstr509 ) && ( cstr558 < 1 ) )
            {
                q507 += qCUSTOMER4[make_tuple(bigsum_L1__ORDERKEY,C__CUSTKEY)];
            }
        }

        q[C__CUSTKEY] += q507;
    }

    map<int64_t,double>::iterator q_it278 = q.begin();
    map<int64_t,double>::iterator q_end277 = q.end();
    for (; q_it278 != q_end277; ++q_it278)
    {
        int64_t C__CUSTKEY = q_it278->first;
        q563 = 0.0;
        set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_it276 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_end275 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it276 != bigsum_L1__ORDERKEY_dom_end275; 
            ++bigsum_L1__ORDERKEY_dom_it276)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it276;
            cstr564 = 0;
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it256 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_end255 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it256 != bigsum_L2__ORDERKEY_dom_end255; 
                ++bigsum_L2__ORDERKEY_dom_it256)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it256;
                cstr564 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0 );
            }

            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it260 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( (bigsum_L1__ORDERKEY == ORDERKEY) &&
                 (bigsum_L2__ORDERKEY_dom_it260 != bigsum_L2__ORDERKEY_dom.end()) )
            {
                cstr564 += (100 < qCUSTOMER3[ORDERKEY]-QUANTITY? -1 : 0);
            }


            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it264 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it264 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr564 +=
                    ( ( (100 < qCUSTOMER3[ORDERKEY]-QUANTITY) && ( qCUSTOMER3[ORDERKEY] <= 100 ) )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0 );
            }

            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it268 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it268 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr564 += -1*
                    ( ( (qCUSTOMER3[ORDERKEY]-QUANTITY <= 100 ) && ( 100 < qCUSTOMER3[ORDERKEY] ) )?
                      qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0);

            }

            cstr615 = 0;
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it272 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_end271 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it272 != bigsum_L2__ORDERKEY_dom_end271; 
                ++bigsum_L2__ORDERKEY_dom_it272)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it272;
                cstr615 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0);
            }

            if ( ( cstr564 < 1 ) && ( 1 <= cstr615 ) )
            {
                q563 += qCUSTOMER4[make_tuple(bigsum_L1__ORDERKEY,C__CUSTKEY)];
            }
        }

        q[C__CUSTKEY] += -1*q563;
    }

    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1_it280 = qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1_end279 = qCUSTOMER1.end();
    for (; qCUSTOMER1_it280 != qCUSTOMER1_end279; ++qCUSTOMER1_it280)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER1_it280->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1_it280->first);
        qCUSTOMER1[make_tuple(
            bigsum_L1__ORDERKEY,x_qCUSTOMER_C__CUSTKEY)] += 
            -1*QUANTITY*qCUSTOMER1LINEITEM1[make_tuple(
            bigsum_L1__ORDERKEY,x_qCUSTOMER_C__CUSTKEY,ORDERKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator 
        qCUSTOMER1ORDERS1_it282 = qCUSTOMER1ORDERS1.find(make_tuple(ORDERKEY,ORDERKEY));

    if ( qCUSTOMER1ORDERS1_it282 != qCUSTOMER1ORDERS1.end() )
    {
        qCUSTOMER1ORDERS1[make_tuple(ORDERKEY,ORDERKEY)] += -QUANTITY;
    }

    map<tuple<int64_t,int64_t>,int>::iterator 
        qCUSTOMER2_it284 = qCUSTOMER2.find(make_tuple(ORDERKEY,ORDERKEY));

    if ( qCUSTOMER2_it284 != qCUSTOMER2.end() ) {
        qCUSTOMER2[make_tuple(ORDERKEY,ORDERKEY)] += -1;
    }

    map<int64_t,double>::iterator qCUSTOMER3_it286 = qCUSTOMER3.find(ORDERKEY);

    if ( qCUSTOMER3_it286 != qCUSTOMER3.end() )
    {
        qCUSTOMER3[ORDERKEY] += -QUANTITY;
    }

    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER4_it288 = qCUSTOMER4.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER4_end287 = qCUSTOMER4.end();
    for (; qCUSTOMER4_it288 != qCUSTOMER4_end287; ++qCUSTOMER4_it288)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER4_it288->first);
        int64_t C__CUSTKEY = get<1>(qCUSTOMER4_it288->first);
        qCUSTOMER4[make_tuple(
            bigsum_L1__ORDERKEY,C__CUSTKEY)] += -1*QUANTITY*qCUSTOMER4LINEITEM1[make_tuple(
            bigsum_L1__ORDERKEY,C__CUSTKEY,ORDERKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator 
        qCUSTOMER4ORDERS1_it290 = qCUSTOMER4ORDERS1.find(make_tuple(ORDERKEY, ORDERKEY));

    if ( qCUSTOMER4ORDERS1_it290 != qCUSTOMER4ORDERS1.end() )
    {
        qCUSTOMER4ORDERS1[make_tuple(ORDERKEY,ORDERKEY)] += -QUANTITY;
    }

    map<tuple<int64_t,int64_t>,double>::iterator 
        qORDERS1_it292 = qORDERS1.find(make_tuple(ORDERKEY, ORDERKEY));

    if ( qORDERS1_it292 != qORDERS1.end() )
    {
        qORDERS1[make_tuple(ORDERKEY,ORDERKEY)] += -QUANTITY;
    }

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
    q301 = 0.0;
    set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_it306 = bigsum_L1__ORDERKEY_dom.begin();
    set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_end305 = bigsum_L1__ORDERKEY_dom.end();
    for (
        ; bigsum_L1__ORDERKEY_dom_it306 != bigsum_L1__ORDERKEY_dom_end305; 
        ++bigsum_L1__ORDERKEY_dom_it306)
    {
        int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it306;
        cstr303 = 0;
        set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it294 = bigsum_L2__ORDERKEY_dom.begin();
        set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_end293 = bigsum_L2__ORDERKEY_dom.end();
        for (
            ; bigsum_L2__ORDERKEY_dom_it294 != bigsum_L2__ORDERKEY_dom_end293; 
            ++bigsum_L2__ORDERKEY_dom_it294)
        {
            int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it294;
            cstr303 += ( (100 < qCUSTOMER3[bigsum_L2__ORDERKEY])?
                qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0);
        }

        // if new then delta f else 0
        if ( 1 <= cstr303 )
        {
            q301 += qCUSTOMER1[make_tuple(bigsum_L1__ORDERKEY,CUSTKEY)];
        }
    }
    q[CUSTKEY] += -1*q301;

    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER4_it348 = qCUSTOMER4.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER4_end347 = qCUSTOMER4.end();
    for (; qCUSTOMER4_it348 != qCUSTOMER4_end347; ++qCUSTOMER4_it348)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER4_it348->first);
        qCUSTOMER4[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY)] += -1*qCUSTOMER1[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY)];
    }
    
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qCUSTOMER4LINEITEM1_it350 = qCUSTOMER4LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qCUSTOMER4LINEITEM1_end349 = qCUSTOMER4LINEITEM1.end();
    for (
        ; qCUSTOMER4LINEITEM1_it350 != qCUSTOMER4LINEITEM1_end349; 
        ++qCUSTOMER4LINEITEM1_it350)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER4LINEITEM1_it350->first);
        qCUSTOMER4LINEITEM1[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY,bigsum_L1__ORDERKEY)] += 
            -1*qLINEITEM1CUSTOMER1[make_tuple(bigsum_L1__ORDERKEY,CUSTKEY)];
    }
    
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_it352 = qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_end351 = qLINEITEM1.end();
    for (; qLINEITEM1_it352 != qLINEITEM1_end351; ++qLINEITEM1_it352)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qLINEITEM1_it352->first);
        qLINEITEM1[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY,bigsum_L1__ORDERKEY)] += 
            -1*qLINEITEM1CUSTOMER1[make_tuple(bigsum_L1__ORDERKEY,CUSTKEY)];
    }

    qORDERS4[CUSTKEY] += -1;
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
    q376 = 0.0;
    set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_it366 = bigsum_L1__ORDERKEY_dom.begin();
    set<int64_t>::iterator bigsum_L1__ORDERKEY_dom_end365 = bigsum_L1__ORDERKEY_dom.end();
    for (
        ; bigsum_L1__ORDERKEY_dom_it366 != bigsum_L1__ORDERKEY_dom_end365; 
        ++bigsum_L1__ORDERKEY_dom_it366)
    {
        int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it366;
        cstr379 = 0;
        set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it354 = bigsum_L2__ORDERKEY_dom.begin();
        set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_end353 = bigsum_L2__ORDERKEY_dom.end();
        for (
            ; bigsum_L2__ORDERKEY_dom_it354 != bigsum_L2__ORDERKEY_dom_end353; 
            ++bigsum_L2__ORDERKEY_dom_it354)
        {
            int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it354;
            cstr379 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0);
        }

        // if new_r then delta- f else 0
        if ( 1 <= cstr379 )
        {
            q376 += qORDERS1[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)];
        }
    }
    q[CUSTKEY] += -1*qORDERS4[CUSTKEY]*q376;

    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1_it408 = qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1_end407 = qCUSTOMER1.end();
    for (; qCUSTOMER1_it408 != qCUSTOMER1_end407; ++qCUSTOMER1_it408)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER1_it408->first);
        qCUSTOMER1[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY)] += -1*qCUSTOMER1ORDERS1[make_tuple(
            bigsum_L1__ORDERKEY,ORDERKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qCUSTOMER1LINEITEM1_it410 = qCUSTOMER1LINEITEM1.find(make_tuple(ORDERKEY,CUSTKEY,ORDERKEY));

    if ( qCUSTOMER1LINEITEM1_it410 != qCUSTOMER1LINEITEM1.end() )
    {
        qCUSTOMER1LINEITEM1[make_tuple(ORDERKEY,CUSTKEY,ORDERKEY)] += -1;
    }

    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER4_it412 = qCUSTOMER4.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER4_end411 = qCUSTOMER4.end();
    for (; qCUSTOMER4_it412 != qCUSTOMER4_end411; ++qCUSTOMER4_it412)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER4_it412->first);
        qCUSTOMER4[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY)] += -1*qCUSTOMER4ORDERS1[make_tuple(
            bigsum_L1__ORDERKEY,ORDERKEY)]*qORDERS4[CUSTKEY];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qCUSTOMER4LINEITEM1_it414 = qCUSTOMER4LINEITEM1.find(make_tuple(ORDERKEY,CUSTKEY,ORDERKEY));

    if ( qCUSTOMER4LINEITEM1_it414 != qCUSTOMER4LINEITEM1.end() )
    {
        qCUSTOMER4LINEITEM1[make_tuple(ORDERKEY,CUSTKEY,ORDERKEY)] += -qORDERS4[CUSTKEY];
    }

    qLINEITEM1[make_tuple(ORDERKEY,CUSTKEY,ORDERKEY)] += -1*qORDERS4[CUSTKEY];

    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1CUSTOMER1_it416 = qLINEITEM1CUSTOMER1.find(make_tuple(ORDERKEY,CUSTKEY));

    if ( qLINEITEM1CUSTOMER1_it416 != qLINEITEM1CUSTOMER1.end() )
    {
        qLINEITEM1CUSTOMER1[make_tuple(ORDERKEY,CUSTKEY)] += -1;
    }

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
