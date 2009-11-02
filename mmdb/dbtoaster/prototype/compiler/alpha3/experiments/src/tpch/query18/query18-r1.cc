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
map<int64_t,double> q;

map<tuple<int64_t,int64_t>,double> qCUSTOMER1;
map<tuple<int64_t,int64_t>,int> qCUSTOMER2;
map<int64_t,double> qCUSTOMER3;
map<tuple<int64_t,int64_t>,double> qCUSTOMER4;

map<tuple<int64_t,int64_t>,double> qORDERS1;
map<int64_t,int> qORDERS4;

map<tuple<int64_t,int64_t,int64_t>,int> qLINEITEM1;

multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> > LINEITEM;
multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> > ORDERS;
multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> > CUSTOMER;

set<int64_t> bigsum_L1__ORDERKEY_dom;
set<int64_t> bigsum_L2__ORDERKEY_dom;

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
double on_insert_ORDERS_sec_span = 0.0;
double on_insert_ORDERS_usec_span = 0.0;
double on_insert_CUSTOMER_sec_span = 0.0;
double on_insert_CUSTOMER_usec_span = 0.0;
double on_delete_LINEITEM_sec_span = 0.0;
double on_delete_LINEITEM_usec_span = 0.0;
double on_delete_ORDERS_sec_span = 0.0;
double on_delete_ORDERS_usec_span = 0.0;
double on_delete_CUSTOMER_sec_span = 0.0;
double on_delete_CUSTOMER_usec_span = 0.0;



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

   cout << "ORDERS size: " << (((sizeof(multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * ORDERS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_type, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type>, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_compare>))) << endl;

   (*stats) << "m," << "ORDERS" << "," << (((sizeof(multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * ORDERS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_type, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type>, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_compare>))) << endl;

   cout << "CUSTOMER size: " << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * CUSTOMER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_compare>))) << endl;

   (*stats) << "m," << "CUSTOMER" << "," << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * CUSTOMER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_compare>))) << endl;

   cout << "bigsum_L1__ORDERKEY_dom size: " << (((sizeof(set<int64_t>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_L1__ORDERKEY_dom.size())  + (sizeof(struct _Rb_tree<set<int64_t>::key_type, set<int64_t>::value_type, _Identity<set<int64_t>::value_type>, set<int64_t>::key_compare>))) << endl;

   (*stats) << "m," << "bigsum_L1__ORDERKEY_dom" << "," << (((sizeof(set<int64_t>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_L1__ORDERKEY_dom.size())  + (sizeof(struct _Rb_tree<set<int64_t>::key_type, set<int64_t>::value_type, _Identity<set<int64_t>::value_type>, set<int64_t>::key_compare>))) << endl;

   cout << "bigsum_L2__ORDERKEY_dom size: " << (((sizeof(set<int64_t>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_L2__ORDERKEY_dom.size())  + (sizeof(struct _Rb_tree<set<int64_t>::key_type, set<int64_t>::value_type, _Identity<set<int64_t>::value_type>, set<int64_t>::key_compare>))) << endl;

   (*stats) << "m," << "bigsum_L2__ORDERKEY_dom" << "," << (((sizeof(set<int64_t>::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_L2__ORDERKEY_dom.size())  + (sizeof(struct _Rb_tree<set<int64_t>::key_type, set<int64_t>::value_type, _Identity<set<int64_t>::value_type>, set<int64_t>::key_compare>))) << endl;

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

}

void analyse_handler_usage(ofstream* stats)
{
   cout << "on_insert_LINEITEM cost: " << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_LINEITEM" << "," << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_insert_ORDERS cost: " << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_ORDERS" << "," << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   cout << "on_insert_CUSTOMER cost: " << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_CUSTOMER" << "," << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   cout << "on_delete_LINEITEM cost: " << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_LINEITEM" << "," << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_delete_ORDERS cost: " << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_ORDERS" << "," << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_CUSTOMER cost: " << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_CUSTOMER" << "," << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
}


////////////////////////////////////////
//
// qCUSTOMER* plans

void recompute_qCUSTOMER1()
{
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1_it78 = qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1_end77 = qCUSTOMER1.end();
    for (; qCUSTOMER1_it78 != qCUSTOMER1_end77; ++qCUSTOMER1_it78)
    {
        int64_t ORDERKEY = get<0>(qCUSTOMER1_it78->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1_it78->first);
        qCUSTOMER1[make_tuple(ORDERKEY,x_qCUSTOMER_C__CUSTKEY)] = 0;
        
        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
            >::iterator ORDERS_it76 = ORDERS.begin();
        
        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
            >::iterator ORDERS_end75 = ORDERS.end();
        for (; ORDERS_it76 != ORDERS_end75; ++ORDERS_it76)
        {
            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it76);
            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it76);
            string protect_O__ORDERSTATUS = get<2>(*ORDERS_it76);
            double protect_O__TOTALPRICE = get<3>(*ORDERS_it76);
            string protect_O__ORDERDATE = get<4>(*ORDERS_it76);
            string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it76);
            string protect_O__CLERK = get<6>(*ORDERS_it76);
            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it76);
            string protect_O__COMMENT = get<8>(*ORDERS_it76);
            
            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_it74 = LINEITEM.begin();
            
            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                >::iterator LINEITEM_end73 = LINEITEM.end();
            for (; LINEITEM_it74 != LINEITEM_end73; ++LINEITEM_it74)
            {
                int64_t protect_L1__ORDERKEY = get<0>(*LINEITEM_it74);
                int64_t protect_L1__PARTKEY = get<1>(*LINEITEM_it74);
                int64_t protect_L1__SUPPKEY = get<2>(*LINEITEM_it74);
                int protect_L1__LINENUMBER = get<3>(*LINEITEM_it74);
                double protect_L1__QUANTITY = get<4>(*LINEITEM_it74);
                double protect_L1__EXTENDEDPRICE = get<5>(*LINEITEM_it74);
                double protect_L1__DISCOUNT = get<6>(*LINEITEM_it74);
                double protect_L1__TAX = get<7>(*LINEITEM_it74);
                if ( protect_L1__ORDERKEY == protect_O__ORDERKEY
                     &&  x_qCUSTOMER_C__CUSTKEY == protect_O__CUSTKEY
                     &&  ORDERKEY == protect_L1__ORDERKEY )
                {
                    qCUSTOMER1[make_tuple(
                            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)] += protect_L1__QUANTITY;
                }
            }
        }
    }
}


void recompute_qCUSTOMER2()
{
    map<tuple<int64_t,int64_t>,int>::iterator qCUSTOMER2_it82 = qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qCUSTOMER2_end81 = qCUSTOMER2.end();
    for (; qCUSTOMER2_it82 != qCUSTOMER2_end81; ++qCUSTOMER2_it82)
    {
        int64_t ORDERKEY = get<0>(qCUSTOMER2_it82->first);

        qCUSTOMER2[make_tuple(ORDERKEY,ORDERKEY)] = 0;
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_it80 = LINEITEM.begin();
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_end79 = LINEITEM.end();
        for (; LINEITEM_it80 != LINEITEM_end79; ++LINEITEM_it80)
        {
            int64_t protect_L1__ORDERKEY = get<0>(*LINEITEM_it80);
            int64_t L2__PARTKEY = get<1>(*LINEITEM_it80);
            int64_t L2__SUPPKEY = get<2>(*LINEITEM_it80);
            int L2__LINENUMBER = get<3>(*LINEITEM_it80);
            double L2__QUANTITY = get<4>(*LINEITEM_it80);
            double L2__EXTENDEDPRICE = get<5>(*LINEITEM_it80);
            double L2__DISCOUNT = get<6>(*LINEITEM_it80);
            double L2__TAX = get<7>(*LINEITEM_it80);
            if ( ORDERKEY == protect_L1__ORDERKEY )
            {
                qCUSTOMER2[make_tuple(ORDERKEY,ORDERKEY)] += 1;
            }
        }
    }
}


void recompute_qCUSTOMER3()
{
    map<int64_t,double>::iterator qCUSTOMER3_it86 = qCUSTOMER3.begin();
    map<int64_t,double>::iterator qCUSTOMER3_end85 = qCUSTOMER3.end();
    for (; qCUSTOMER3_it86 != qCUSTOMER3_end85; ++qCUSTOMER3_it86)
    {
        int64_t ORDERKEY = qCUSTOMER3_it86->first;
        qCUSTOMER3[ORDERKEY] = 0;
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_it84 = LINEITEM.begin();
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_end83 = LINEITEM.end();
        for (; LINEITEM_it84 != LINEITEM_end83; ++LINEITEM_it84)
        {
            int64_t protect_L1__ORDERKEY = get<0>(*LINEITEM_it84);
            int64_t L3__PARTKEY = get<1>(*LINEITEM_it84);
            int64_t L3__SUPPKEY = get<2>(*LINEITEM_it84);
            int L3__LINENUMBER = get<3>(*LINEITEM_it84);
            double L3__QUANTITY = get<4>(*LINEITEM_it84);
            double L3__EXTENDEDPRICE = get<5>(*LINEITEM_it84);
            double L3__DISCOUNT = get<6>(*LINEITEM_it84);
            double L3__TAX = get<7>(*LINEITEM_it84);
            if ( ORDERKEY == protect_L1__ORDERKEY )
            {
                qCUSTOMER3[ORDERKEY] += L3__QUANTITY;
            }
        }
    }
}


void recompute_qCUSTOMER4()
{
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER4_it94 = qCUSTOMER4.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER4_end93 = qCUSTOMER4.end();
    for (; qCUSTOMER4_it94 != qCUSTOMER4_end93; ++qCUSTOMER4_it94)
    {
        int64_t ORDERKEY = get<0>(qCUSTOMER4_it94->first);
        int64_t C__CUSTKEY = get<1>(qCUSTOMER4_it94->first);
        qCUSTOMER4[make_tuple(ORDERKEY,C__CUSTKEY)] = 0;
        
        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it92 = CUSTOMER.begin();
        
        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end91 = CUSTOMER.end();
        for (; CUSTOMER_it92 != CUSTOMER_end91; ++CUSTOMER_it92)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it92);
            string protect_C__NAME = get<1>(*CUSTOMER_it92);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it92);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it92);
            string protect_C__PHONE = get<4>(*CUSTOMER_it92);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it92);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it92);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it92);
            
            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_it90 = ORDERS.begin();
            
            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_end89 = ORDERS.end();
            for (; ORDERS_it90 != ORDERS_end89; ++ORDERS_it90)
            {
                int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it90);
                int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it90);
                string protect_O__ORDERSTATUS = get<2>(*ORDERS_it90);
                double protect_O__TOTALPRICE = get<3>(*ORDERS_it90);
                string protect_O__ORDERDATE = get<4>(*ORDERS_it90);
                string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it90);
                string protect_O__CLERK = get<6>(*ORDERS_it90);
                int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it90);
                string protect_O__COMMENT = get<8>(*ORDERS_it90);
                
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                    >::iterator LINEITEM_it88 = LINEITEM.begin();
                
                multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                    >::iterator LINEITEM_end87 = LINEITEM.end();
                for (; LINEITEM_it88 != LINEITEM_end87; ++LINEITEM_it88)
                {
                    int64_t protect_L1__ORDERKEY = get<0>(*LINEITEM_it88);
                    int64_t protect_L1__PARTKEY = get<1>(*LINEITEM_it88);
                    int64_t protect_L1__SUPPKEY = get<2>(*LINEITEM_it88);
                    int protect_L1__LINENUMBER = get<3>(*LINEITEM_it88);
                    double protect_L1__QUANTITY = get<4>(*LINEITEM_it88);
                    double protect_L1__EXTENDEDPRICE = get<5>(*LINEITEM_it88);
                    double protect_L1__DISCOUNT = get<6>(*LINEITEM_it88);
                    double protect_L1__TAX = get<7>(*LINEITEM_it88);
                    if ( protect_L1__ORDERKEY == protect_O__ORDERKEY 
                         &&  C__CUSTKEY == protect_C__CUSTKEY
                         &&  C__CUSTKEY == protect_O__CUSTKEY 
                         &&  ORDERKEY == protect_L1__ORDERKEY )
                    {
                        qCUSTOMER4[make_tuple(ORDERKEY,C__CUSTKEY)] += protect_L1__QUANTITY;
                    }
                }
            }
        }
    }
}


////////////////////////////////////////
//
// qORDERS* plans

void recompute_qORDERS1()
{
    map<tuple<int64_t,int64_t>,double>::iterator qORDERS1_it98 = qORDERS1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qORDERS1_end97 = qORDERS1.end();
    for (; qORDERS1_it98 != qORDERS1_end97; ++qORDERS1_it98)
    {
        int64_t ORDERKEY = get<0>(qORDERS1_it98->first);
        int64_t x_qORDERS_O__ORDERKEY = get<1>(qORDERS1_it98->first);
        qORDERS1[make_tuple(ORDERKEY,x_qORDERS_O__ORDERKEY)] = 0;
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_it96 = LINEITEM.begin();
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
            >::iterator LINEITEM_end95 = LINEITEM.end();
        for (; LINEITEM_it96 != LINEITEM_end95; ++LINEITEM_it96)
        {
            int64_t protect_L1__ORDERKEY = get<0>(*LINEITEM_it96);
            int64_t protect_L1__PARTKEY = get<1>(*LINEITEM_it96);
            int64_t protect_L1__SUPPKEY = get<2>(*LINEITEM_it96);
            int protect_L1__LINENUMBER = get<3>(*LINEITEM_it96);
            double protect_L1__QUANTITY = get<4>(*LINEITEM_it96);
            double protect_L1__EXTENDEDPRICE = get<5>(*LINEITEM_it96);
            double protect_L1__DISCOUNT = get<6>(*LINEITEM_it96);
            double protect_L1__TAX = get<7>(*LINEITEM_it96);
            if ( ORDERKEY == x_qORDERS_O__ORDERKEY &&  ORDERKEY == protect_L1__ORDERKEY )
            {
                qORDERS1[make_tuple(
                        ORDERKEY,x_qORDERS_O__ORDERKEY)] += protect_L1__QUANTITY;
            }
        }
    }
}


void recompute_qORDERS4()
{
    map<int64_t,int>::iterator qORDERS4_it244 = qORDERS4.begin();
    map<int64_t,int>::iterator qORDERS4_end243 = qORDERS4.end();
    for (; qORDERS4_it244 != qORDERS4_end243; ++qORDERS4_it244)
    {
        int64_t x_qORDERS_O__CUSTKEY = qORDERS4_it244->first;
        qORDERS4[x_qORDERS_O__CUSTKEY] = 0;
        
        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it242 = CUSTOMER.begin();
        
        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end241 = CUSTOMER.end();
        for (; CUSTOMER_it242 != CUSTOMER_end241; ++CUSTOMER_it242)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it242);
            string protect_C__NAME = get<1>(*CUSTOMER_it242);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it242);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it242);
            string protect_C__PHONE = get<4>(*CUSTOMER_it242);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it242);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it242);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it242);
            if ( x_qORDERS_O__CUSTKEY == protect_C__CUSTKEY )
            {
                qORDERS4[x_qORDERS_O__CUSTKEY] += 1;
            }
        }
    }
}


////////////////////////////////////////
//
// qLINEITEM* plans

void recompute_qLINEITEM1()
{
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_it172 = qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_end171 = qLINEITEM1.end();
    for (; qLINEITEM1_it172 != qLINEITEM1_end171; ++qLINEITEM1_it172)
    {
        int64_t x_qLINEITEM_L1__ORDERKEY = get<0>(qLINEITEM1_it172->first);
        int64_t C__CUSTKEY = get<1>(qLINEITEM1_it172->first);
        int64_t L1__ORDERKEY = get<2>(qLINEITEM1_it172->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L1__ORDERKEY,C__CUSTKEY,L1__ORDERKEY)] = 0;
        
        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it170 = CUSTOMER.begin();
        
        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end169 = CUSTOMER.end();
        for (; CUSTOMER_it170 != CUSTOMER_end169; ++CUSTOMER_it170)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it170);
            string protect_C__NAME = get<1>(*CUSTOMER_it170);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it170);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it170);
            string protect_C__PHONE = get<4>(*CUSTOMER_it170);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it170);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it170);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it170);
            
            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_it168 = ORDERS.begin();
            
            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                >::iterator ORDERS_end167 = ORDERS.end();
            for (; ORDERS_it168 != ORDERS_end167; ++ORDERS_it168)
            {
                int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it168);
                int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it168);
                string protect_O__ORDERSTATUS = get<2>(*ORDERS_it168);
                double protect_O__TOTALPRICE = get<3>(*ORDERS_it168);
                string protect_O__ORDERDATE = get<4>(*ORDERS_it168);
                string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it168);
                string protect_O__CLERK = get<6>(*ORDERS_it168);
                int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it168);
                string protect_O__COMMENT = get<8>(*ORDERS_it168);
                if ( x_qLINEITEM_L1__ORDERKEY == L1__ORDERKEY
                     &&  C__CUSTKEY == protect_C__CUSTKEY
                     &&  C__CUSTKEY == protect_O__CUSTKEY
                     &&  x_qLINEITEM_L1__ORDERKEY == protect_O__ORDERKEY )
                {
                    qLINEITEM1[make_tuple(
                            x_qLINEITEM_L1__ORDERKEY,C__CUSTKEY,L1__ORDERKEY)] += 1;
                }
            }
        }
    }
}

////////////////////////////////////////
//
// Triggers

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

    recompute_qCUSTOMER1();
    recompute_qCUSTOMER2();
    recompute_qCUSTOMER3();
    recompute_qCUSTOMER4();

    recompute_qORDERS1();

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_LINEITEM_sec_span, on_insert_LINEITEM_usec_span);
}

void on_insert_ORDERS(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    CUSTKEY,string ORDERSTATUS,double TOTALPRICE,string ORDERDATE,string 
    ORDERPRIORITY,string CLERK,int SHIPPRIORITY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    ORDERS.insert(make_tuple(
        ORDERKEY,CUSTKEY,ORDERSTATUS,TOTALPRICE,ORDERDATE,ORDERPRIORITY,CLERK,
        SHIPPRIORITY,COMMENT));

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

    recompute_qCUSTOMER1();
    recompute_qCUSTOMER4();

    recompute_qLINEITEM1();

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_ORDERS_sec_span, on_insert_ORDERS_usec_span);
}

void on_insert_CUSTOMER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t CUSTKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    MKTSEGMENT,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);

    CUSTOMER.insert(make_tuple(
        CUSTKEY,NAME,ADDRESS,NATIONKEY,PHONE,ACCTBAL,MKTSEGMENT,COMMENT));

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

    recompute_qCUSTOMER4();

    recompute_qLINEITEM1();

    recompute_qORDERS4();

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_CUSTOMER_sec_span, on_insert_CUSTOMER_usec_span);
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

    /*
    if ( ( bigsum_L1__ORDERKEY_dom.find(ORDERKEY) == 
        bigsum_L1__ORDERKEY_dom.end() ) && ( bigsum_L2__ORDERKEY_dom.find(
        ORDERKEY) == bigsum_L2__ORDERKEY_dom.end() ) )
    {
        qCUSTOMER2.erase(make_tuple(ORDERKEY,ORDERKEY));
    }
    if ( bigsum_L2__ORDERKEY_dom.find(ORDERKEY) == bigsum_L2__ORDERKEY_dom.end(
        ) )
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

            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it260 =
                bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( (bigsum_L1__ORDERKEY == ORDERKEY) &&
                 (bigsum_L2__ORDERKEY_dom_it260 != bigsum_L2__ORDERKEY_dom.end()) )
            {
                cstr564 += (100 < qCUSTOMER3[ORDERKEY]-QUANTITY? -1 : 0);
            }


            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it264 =
                bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it264 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr564 +=
                    ( ( (100 < qCUSTOMER3[ORDERKEY]-QUANTITY) && ( qCUSTOMER3[ORDERKEY] <= 100 ) )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0 );
            }

            set<int64_t>::iterator bigsum_L2__ORDERKEY_dom_it268 =
                bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

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

    recompute_qCUSTOMER1();
    recompute_qCUSTOMER2();
    recompute_qCUSTOMER3();
    recompute_qCUSTOMER4();

    recompute_qORDERS1();

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_LINEITEM_sec_span, on_delete_LINEITEM_usec_span);
}

void on_delete_ORDERS(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    CUSTKEY,string ORDERSTATUS,double TOTALPRICE,string ORDERDATE,string 
    ORDERPRIORITY,string CLERK,int SHIPPRIORITY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);

    ORDERS.erase(make_tuple(
        ORDERKEY,CUSTKEY,ORDERSTATUS,TOTALPRICE,ORDERDATE,ORDERPRIORITY,CLERK,
        SHIPPRIORITY,COMMENT));

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

    recompute_qCUSTOMER1();
    recompute_qCUSTOMER4();
    recompute_qLINEITEM1();

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_ORDERS_sec_span, on_delete_ORDERS_usec_span);
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

    recompute_qCUSTOMER4();
    recompute_qLINEITEM1();
    recompute_qORDERS4();

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_CUSTOMER_sec_span, on_delete_CUSTOMER_usec_span);
}

DBToaster::DemoDatasets::LineitemStream SSBLineitem("/Users/yanif/datasets/tpch/sf1/singlefile/lineitem.tbl.a",&DBToaster::DemoDatasets::parseLineitemField,16,65000000,512);

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

DBToaster::DemoDatasets::CustomerStream SSBCustomer("/Users/yanif/datasets/tpch/sf1/singlefile/customer.tbl.a",&DBToaster::DemoDatasets::parseCustomerField,8,1600000, 512);

boost::shared_ptr<DBToaster::DemoDatasets::CustomerTupleAdaptor> SSBCustomer_adaptor(new DBToaster::DemoDatasets::CustomerTupleAdaptor());
static int streamSSBCustomerId = 2;

struct on_insert_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_insert_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_insert_CUSTOMER_fun_obj fo_on_insert_CUSTOMER_2;

struct on_delete_LINEITEM_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result>(data); 
        on_delete_LINEITEM(results, log, stats, input.orderkey,input.partkey,input.suppkey,input.linenumber,input.quantity,input.extendedprice,input.discount,input.tax);
    }
};

on_delete_LINEITEM_fun_obj fo_on_delete_LINEITEM_3;

struct on_delete_ORDERS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::OrderTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::OrderTupleAdaptor::Result>(data); 
        on_delete_ORDERS(results, log, stats, input.orderkey,input.custkey,input.orderstatus,input.totalprice,input.orderdate,input.orderpriority,input.clerk,input.shippriority,input.comment);
    }
};

on_delete_ORDERS_fun_obj fo_on_delete_ORDERS_4;

struct on_delete_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_delete_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_delete_CUSTOMER_fun_obj fo_on_delete_CUSTOMER_5;


void init(DBToaster::StandaloneEngine::FileMultiplexer& sources,
    DBToaster::StandaloneEngine::FileStreamDispatcher& router, 
    ofstream* results, ofstream* log, ofstream* stats)
{
    router.setFiles(results, log, stats);
    sources.addStream<DBToaster::DemoDatasets::lineitem>(&SSBLineitem, boost::ref(*SSBLineitem_adaptor), streamSSBLineitemId);
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_LINEITEM_0));
    sources.addStream<DBToaster::DemoDatasets::order>(&SSBOrder, boost::ref(*SSBOrder_adaptor), streamSSBOrderId);
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_ORDERS_1));
    sources.addStream<DBToaster::DemoDatasets::customer>(&SSBCustomer, boost::ref(*SSBCustomer_adaptor), streamSSBCustomerId);
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_CUSTOMER_2));
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_LINEITEM_3));
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_ORDERS_4));
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_CUSTOMER_5));
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
