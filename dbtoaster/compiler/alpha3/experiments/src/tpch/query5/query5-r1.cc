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
multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> > LINEITEM;
multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> > ORDERS;
multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >  CUSTOMER;
multiset<tuple<int64_t,string,string,int64_t,string,double,string> > SUPPLIER;
multiset<tuple<int64_t,string,int64_t,string> > NATION;
multiset<tuple<int64_t,string,string> > REGION;


map<string,double> q;

map<tuple<string,int64_t>,double> qREGION1;
map<tuple<string,int64_t>,double> qREGION2;

map<tuple<int64_t,int64_t,string>,double> qSUPPLIER1;
map<tuple<int64_t,int64_t,string>,double> qSUPPLIER2;

map<tuple<int64_t,int64_t,string>,double> qORDERS1;
map<tuple<int64_t,int64_t,string>,double> qORDERS2;


map<tuple<int64_t,int64_t,string>,int> qLINEITEM1;

map<int64_t,double> qNATION1;
map<int64_t,int> qNATION2;
map<int64_t,double> qNATION3;

map<tuple<int64_t,int64_t,string>,double> qCUSTOMER1;
map<tuple<int64_t,int64_t,string>,double> qCUSTOMER2;

double on_insert_REGION_sec_span = 0.0;
double on_insert_REGION_usec_span = 0.0;
double on_insert_LINEITEM_sec_span = 0.0;
double on_insert_LINEITEM_usec_span = 0.0;
double on_insert_ORDERS_sec_span = 0.0;
double on_insert_ORDERS_usec_span = 0.0;
double on_insert_SUPPLIER_sec_span = 0.0;
double on_insert_SUPPLIER_usec_span = 0.0;
double on_insert_CUSTOMER_sec_span = 0.0;
double on_insert_CUSTOMER_usec_span = 0.0;
double on_insert_NATION_sec_span = 0.0;
double on_insert_NATION_usec_span = 0.0;
double on_delete_REGION_sec_span = 0.0;
double on_delete_REGION_usec_span = 0.0;
double on_delete_LINEITEM_sec_span = 0.0;
double on_delete_LINEITEM_usec_span = 0.0;
double on_delete_ORDERS_sec_span = 0.0;
double on_delete_ORDERS_usec_span = 0.0;
double on_delete_SUPPLIER_sec_span = 0.0;
double on_delete_SUPPLIER_usec_span = 0.0;
double on_delete_CUSTOMER_sec_span = 0.0;
double on_delete_CUSTOMER_usec_span = 0.0;
double on_delete_NATION_sec_span = 0.0;
double on_delete_NATION_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
   cout << "SUPPLIER size: " << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * SUPPLIER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::key_compare>))) << endl;

   (*stats) << "m," << "SUPPLIER" << "," << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * SUPPLIER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::key_compare>))) << endl;

   cout << "qREGION1 size: " << (((sizeof(map<tuple<string,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qREGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t>,double>::key_type, map<tuple<string,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t>,double>::value_type>, map<tuple<string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qREGION1" << "," << (((sizeof(map<tuple<string,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qREGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t>,double>::key_type, map<tuple<string,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t>,double>::value_type>, map<tuple<string,int64_t>,double>::key_compare>))) << endl;

   cout << "qSUPPLIER1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "qREGION2 size: " << (((sizeof(map<tuple<string,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qREGION2.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t>,double>::key_type, map<tuple<string,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t>,double>::value_type>, map<tuple<string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qREGION2" << "," << (((sizeof(map<tuple<string,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qREGION2.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t>,double>::key_type, map<tuple<string,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t>,double>::value_type>, map<tuple<string,int64_t>,double>::key_compare>))) << endl;

   cout << "qSUPPLIER2 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER2" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "LINEITEM size: " << (((sizeof(multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * LINEITEM.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_type, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type>, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_compare>))) << endl;

   (*stats) << "m," << "LINEITEM" << "," << (((sizeof(multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * LINEITEM.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_type, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type>, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_compare>))) << endl;

   cout << "qORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "qORDERS2 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS2" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "ORDERS size: " << (((sizeof(multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * ORDERS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_type, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type>, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_compare>))) << endl;

   (*stats) << "m," << "ORDERS" << "," << (((sizeof(multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * ORDERS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_type, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type>, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_compare>))) << endl;

   cout << "REGION size: " << (((sizeof(multiset<tuple<int64_t,string,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * REGION.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string> >::key_type, multiset<tuple<int64_t,string,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string> >::value_type>, multiset<tuple<int64_t,string,string> >::key_compare>))) << endl;

   (*stats) << "m," << "REGION" << "," << (((sizeof(multiset<tuple<int64_t,string,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * REGION.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string> >::key_type, multiset<tuple<int64_t,string,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string> >::value_type>, multiset<tuple<int64_t,string,string> >::key_compare>))) << endl;

   cout << "qLINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,int>::key_type, map<tuple<int64_t,int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,int>::value_type>, map<tuple<int64_t,int64_t,string>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,int>::key_type, map<tuple<int64_t,int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,int>::value_type>, map<tuple<int64_t,int64_t,string>,int>::key_compare>))) << endl;

   cout << "qNATION1 size: " << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION1.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION1" << "," << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION1.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   cout << "q size: " << (((sizeof(map<string,double>::key_type)
       + sizeof(map<string,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<string,double>::key_type, map<string,double>::value_type, _Select1st<map<string,double>::value_type>, map<string,double>::key_compare>))) << endl;

   (*stats) << "m," << "q" << "," << (((sizeof(map<string,double>::key_type)
       + sizeof(map<string,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<string,double>::key_type, map<string,double>::value_type, _Select1st<map<string,double>::value_type>, map<string,double>::key_compare>))) << endl;

   cout << "CUSTOMER size: " << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * CUSTOMER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_compare>))) << endl;

   (*stats) << "m," << "CUSTOMER" << "," << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * CUSTOMER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_compare>))) << endl;

   cout << "qNATION2 size: " << (((sizeof(map<int64_t,int>::key_type)
       + sizeof(map<int64_t,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION2.size())  + (sizeof(struct _Rb_tree<map<int64_t,int>::key_type, map<int64_t,int>::value_type, _Select1st<map<int64_t,int>::value_type>, map<int64_t,int>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION2" << "," << (((sizeof(map<int64_t,int>::key_type)
       + sizeof(map<int64_t,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION2.size())  + (sizeof(struct _Rb_tree<map<int64_t,int>::key_type, map<int64_t,int>::value_type, _Select1st<map<int64_t,int>::value_type>, map<int64_t,int>::key_compare>))) << endl;

   cout << "qCUSTOMER1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "NATION size: " << (((sizeof(multiset<tuple<int64_t,string,int64_t,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * NATION.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,int64_t,string> >::key_type, multiset<tuple<int64_t,string,int64_t,string> >::value_type, _Identity<multiset<tuple<int64_t,string,int64_t,string> >::value_type>, multiset<tuple<int64_t,string,int64_t,string> >::key_compare>))) << endl;

   (*stats) << "m," << "NATION" << "," << (((sizeof(multiset<tuple<int64_t,string,int64_t,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * NATION.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,int64_t,string> >::key_type, multiset<tuple<int64_t,string,int64_t,string> >::value_type, _Identity<multiset<tuple<int64_t,string,int64_t,string> >::value_type>, multiset<tuple<int64_t,string,int64_t,string> >::key_compare>))) << endl;

   cout << "qNATION3 size: " << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION3.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION3" << "," << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION3.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   cout << "qCUSTOMER2 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER2" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

}

void analyse_handler_usage(ofstream* stats)
{
   cout << "on_insert_REGION cost: " << (on_insert_REGION_sec_span + (on_insert_REGION_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_REGION" << "," << (on_insert_REGION_sec_span + (on_insert_REGION_usec_span / 1000000.0)) << endl;
   cout << "on_insert_LINEITEM cost: " << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_LINEITEM" << "," << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_insert_ORDERS cost: " << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_ORDERS" << "," << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   cout << "on_insert_SUPPLIER cost: " << (on_insert_SUPPLIER_sec_span + (on_insert_SUPPLIER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_SUPPLIER" << "," << (on_insert_SUPPLIER_sec_span + (on_insert_SUPPLIER_usec_span / 1000000.0)) << endl;
   cout << "on_insert_CUSTOMER cost: " << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_CUSTOMER" << "," << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   cout << "on_insert_NATION cost: " << (on_insert_NATION_sec_span + (on_insert_NATION_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_NATION" << "," << (on_insert_NATION_sec_span + (on_insert_NATION_usec_span / 1000000.0)) << endl;
   cout << "on_delete_REGION cost: " << (on_delete_REGION_sec_span + (on_delete_REGION_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_REGION" << "," << (on_delete_REGION_sec_span + (on_delete_REGION_usec_span / 1000000.0)) << endl;
   cout << "on_delete_LINEITEM cost: " << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_LINEITEM" << "," << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_delete_ORDERS cost: " << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_ORDERS" << "," << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_SUPPLIER cost: " << (on_delete_SUPPLIER_sec_span + (on_delete_SUPPLIER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_SUPPLIER" << "," << (on_delete_SUPPLIER_sec_span + (on_delete_SUPPLIER_usec_span / 1000000.0)) << endl;
   cout << "on_delete_CUSTOMER cost: " << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_CUSTOMER" << "," << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
   cout << "on_delete_NATION cost: " << (on_delete_NATION_sec_span + (on_delete_NATION_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_NATION" << "," << (on_delete_NATION_sec_span + (on_delete_NATION_usec_span / 1000000.0)) << endl;
}


void on_insert_REGION(
    ofstream* results, ofstream* log, ofstream* stats, int64_t REGIONKEY,string 
    NAME,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    REGION.insert(make_tuple(REGIONKEY,NAME,COMMENT));
    map<string,double>::iterator q_it2 = q.begin();
    map<string,double>::iterator q_end1 = q.end();
    for (; q_it2 != q_end1; ++q_it2)
    {
        string N__NAME = q_it2->first;
        q[N__NAME] += qREGION1[make_tuple(N__NAME,REGIONKEY)];
    }
    map<string,double>::iterator q_it4 = q.begin();
    map<string,double>::iterator q_end3 = q.end();
    for (; q_it4 != q_end3; ++q_it4)
    {
        string N__NAME = q_it4->first;
        q[N__NAME] += qREGION2[make_tuple(N__NAME,REGIONKEY)]*-1;
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it16 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end15 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it16 != qCUSTOMER1_end15; ++qCUSTOMER1_it16)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it16->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER1_it16->first);
        string N__NAME = get<2>(qCUSTOMER1_it16->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it14 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end13 = 
            REGION.end();
        for (; REGION_it14 != REGION_end13; ++REGION_it14)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it14);
            string protect_R__NAME = get<1>(*REGION_it14);
            string protect_R__COMMENT = get<2>(*REGION_it14);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it12 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end11 = NATION.end();
            for (; NATION_it12 != NATION_end11; ++NATION_it12)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it12);
                string protect_N__NAME = get<1>(*NATION_it12);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it12);
                string protect_N__COMMENT = get<3>(*NATION_it12);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it10 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end9 = SUPPLIER.end();
                for (; SUPPLIER_it10 != SUPPLIER_end9; ++SUPPLIER_it10)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it10);
                    string protect_S__NAME = get<1>(*SUPPLIER_it10);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it10);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it10);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it10);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it10);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it10);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it8 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end7 = ORDERS.end();
                    for (; ORDERS_it8 != ORDERS_end7; ++ORDERS_it8)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it8);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it8);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it8);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it8);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it8);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it8);
                        string protect_O__CLERK = get<6>(*ORDERS_it8);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it8);
                        string protect_O__COMMENT = get<8>(*ORDERS_it8);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it6 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end5 = LINEITEM.end();
                        for (; LINEITEM_it6 != LINEITEM_end5; ++LINEITEM_it6)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it6);
                            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it6);
                            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it6);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it6);
                            double protect_L__QUANTITY = get<4>(*LINEITEM_it6);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it6);
                            double protect_L__DISCOUNT = get<6>(*LINEITEM_it6);
                            double protect_L__TAX = get<7>(*LINEITEM_it6);
                            int64_t var13 = 0;
                            var13 += protect_N__REGIONKEY;
                            int64_t var14 = 0;
                            var14 += protect_R__REGIONKEY;
                            if ( var13 == var14 )
                            {
                                int64_t var11 = 0;
                                var11 += protect_L__SUPPKEY;
                                int64_t var12 = 0;
                                var12 += protect_S__SUPPKEY;
                                if ( var11 == var12 )
                                {
                                    int64_t var9 = 0;
                                    var9 += protect_L__ORDERKEY;
                                    int64_t var10 = 0;
                                    var10 += protect_O__ORDERKEY;
                                    if ( var9 == var10 )
                                    {
                                        string var7 = 0;
                                        var7 += N__NAME;
                                        string var8 = 0;
                                        var8 += protect_N__NAME;
                                        if ( var7 == var8 )
                                        {
                                            int64_t var5 = 0;
                                            var5 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var6 = 0;
                                            var6 += protect_N__NATIONKEY;
                                            if ( var5 == var6 )
                                            {
                                                int64_t var3 = 0;
                                                var3 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var4 = 0;
                                                var4 += protect_S__NATIONKEY;
                                                if ( var3 == var4 )
                                                {
                                                    int64_t var1 = 0;
                                                    var1 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var2 = 0;
                                                    var2 += protect_O__CUSTKEY;
                                                    if ( var1 == var2 )
                                                    {
                                                        qCUSTOMER1[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it28 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end27 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it28 != qCUSTOMER2_end27; ++qCUSTOMER2_it28)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it28->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER2_it28->first);
        string N__NAME = get<2>(qCUSTOMER2_it28->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it26 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end25 = 
            REGION.end();
        for (; REGION_it26 != REGION_end25; ++REGION_it26)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it26);
            string protect_R__NAME = get<1>(*REGION_it26);
            string protect_R__COMMENT = get<2>(*REGION_it26);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it24 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end23 = NATION.end();
            for (; NATION_it24 != NATION_end23; ++NATION_it24)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it24);
                string protect_N__NAME = get<1>(*NATION_it24);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it24);
                string protect_N__COMMENT = get<3>(*NATION_it24);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it22 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end21 = SUPPLIER.end();
                for (; SUPPLIER_it22 != SUPPLIER_end21; ++SUPPLIER_it22)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it22);
                    string protect_S__NAME = get<1>(*SUPPLIER_it22);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it22);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it22);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it22);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it22);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it22);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it20 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end19 = ORDERS.end();
                    for (; ORDERS_it20 != ORDERS_end19; ++ORDERS_it20)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it20);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it20);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it20);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it20);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it20);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it20);
                        string protect_O__CLERK = get<6>(*ORDERS_it20);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it20);
                        string protect_O__COMMENT = get<8>(*ORDERS_it20);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it18 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end17 = LINEITEM.end();
                        for (; LINEITEM_it18 != LINEITEM_end17; ++LINEITEM_it18)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it18);
                            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it18);
                            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it18);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it18);
                            double protect_L__QUANTITY = get<4>(*LINEITEM_it18);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it18);
                            double protect_L__DISCOUNT = get<6>(*LINEITEM_it18);
                            double protect_L__TAX = get<7>(*LINEITEM_it18);
                            int64_t var28 = 0;
                            var28 += protect_N__REGIONKEY;
                            int64_t var29 = 0;
                            var29 += protect_R__REGIONKEY;
                            if ( var28 == var29 )
                            {
                                int64_t var26 = 0;
                                var26 += protect_L__SUPPKEY;
                                int64_t var27 = 0;
                                var27 += protect_S__SUPPKEY;
                                if ( var26 == var27 )
                                {
                                    int64_t var24 = 0;
                                    var24 += protect_L__ORDERKEY;
                                    int64_t var25 = 0;
                                    var25 += protect_O__ORDERKEY;
                                    if ( var24 == var25 )
                                    {
                                        string var22 = 0;
                                        var22 += N__NAME;
                                        string var23 = 0;
                                        var23 += protect_N__NAME;
                                        if ( var22 == var23 )
                                        {
                                            int64_t var20 = 0;
                                            var20 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var21 = 0;
                                            var21 += protect_N__NATIONKEY;
                                            if ( var20 == var21 )
                                            {
                                                int64_t var18 = 0;
                                                var18 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var19 = 0;
                                                var19 += protect_S__NATIONKEY;
                                                if ( var18 == var19 )
                                                {
                                                    int64_t var16 = 0;
                                                    var16 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var17 = 0;
                                                    var17 += protect_O__CUSTKEY;
                                                    if ( var16 == var17 )
                                                    {
                                                        double var15 = 1;
                                                        var15 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var15 *= 
                                                            protect_L__DISCOUNT;
                                                        qCUSTOMER2[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += var15;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it40 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end39 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it40 != qLINEITEM1_end39; ++qLINEITEM1_it40)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it40->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it40->first);
        string N__NAME = get<2>(qLINEITEM1_it40->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it38 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end37 = 
            REGION.end();
        for (; REGION_it38 != REGION_end37; ++REGION_it38)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it38);
            string protect_R__NAME = get<1>(*REGION_it38);
            string protect_R__COMMENT = get<2>(*REGION_it38);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it36 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end35 = NATION.end();
            for (; NATION_it36 != NATION_end35; ++NATION_it36)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it36);
                string protect_N__NAME = get<1>(*NATION_it36);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it36);
                string protect_N__COMMENT = get<3>(*NATION_it36);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it34 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end33 = SUPPLIER.end();
                for (; SUPPLIER_it34 != SUPPLIER_end33; ++SUPPLIER_it34)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it34);
                    string protect_S__NAME = get<1>(*SUPPLIER_it34);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it34);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it34);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it34);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it34);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it34);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_it32 = CUSTOMER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_end31 = CUSTOMER.end();
                    for (; CUSTOMER_it32 != CUSTOMER_end31; ++CUSTOMER_it32)
                    {
                        int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it32);
                        string protect_C__NAME = get<1>(*CUSTOMER_it32);
                        string protect_C__ADDRESS = get<2>(*CUSTOMER_it32);
                        int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it32);
                        string protect_C__PHONE = get<4>(*CUSTOMER_it32);
                        double protect_C__ACCTBAL = get<5>(*CUSTOMER_it32);
                        string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it32);
                        string protect_C__COMMENT = get<7>(*CUSTOMER_it32);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it30 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end29 = ORDERS.end();
                        for (; ORDERS_it30 != ORDERS_end29; ++ORDERS_it30)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it30);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it30);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it30);
                            double protect_O__TOTALPRICE = get<3>(*ORDERS_it30);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it30);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it30);
                            string protect_O__CLERK = get<6>(*ORDERS_it30);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it30);
                            string protect_O__COMMENT = get<8>(*ORDERS_it30);
                            int64_t var42 = 0;
                            var42 += protect_N__REGIONKEY;
                            int64_t var43 = 0;
                            var43 += protect_R__REGIONKEY;
                            if ( var42 == var43 )
                            {
                                int64_t var40 = 0;
                                var40 += protect_C__NATIONKEY;
                                int64_t var41 = 0;
                                var41 += protect_N__NATIONKEY;
                                if ( var40 == var41 )
                                {
                                    int64_t var38 = 0;
                                    var38 += protect_C__NATIONKEY;
                                    int64_t var39 = 0;
                                    var39 += protect_S__NATIONKEY;
                                    if ( var38 == var39 )
                                    {
                                        int64_t var36 = 0;
                                        var36 += protect_C__CUSTKEY;
                                        int64_t var37 = 0;
                                        var37 += protect_O__CUSTKEY;
                                        if ( var36 == var37 )
                                        {
                                            string var34 = 0;
                                            var34 += N__NAME;
                                            string var35 = 0;
                                            var35 += protect_N__NAME;
                                            if ( var34 == var35 )
                                            {
                                                int64_t var32 = 0;
                                                var32 += x_qLINEITEM_L__SUPPKEY;
                                                int64_t var33 = 0;
                                                var33 += protect_S__SUPPKEY;
                                                if ( var32 == var33 )
                                                {
                                                    int64_t var30 = 0;
                                                    var30 += 
                                                        x_qLINEITEM_L__ORDERKEY;
                                                    int64_t var31 = 0;
                                                    var31 += 
                                                        protect_O__ORDERKEY;
                                                    if ( var30 == var31 )
                                                    {
                                                        qLINEITEM1[make_tuple(
                                                            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += 1;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,int>::iterator qNATION2_it44 = qNATION2.begin();
    map<int64_t,int>::iterator qNATION2_end43 = qNATION2.end();
    for (; qNATION2_it44 != qNATION2_end43; ++qNATION2_it44)
    {
        int64_t x_qNATION_N__REGIONKEY = qNATION2_it44->first;
        qNATION2[x_qNATION_N__REGIONKEY] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it42 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end41 = 
            REGION.end();
        for (; REGION_it42 != REGION_end41; ++REGION_it42)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it42);
            string protect_R__NAME = get<1>(*REGION_it42);
            string protect_R__COMMENT = get<2>(*REGION_it42);
            int64_t var44 = 0;
            var44 += x_qNATION_N__REGIONKEY;
            int64_t var45 = 0;
            var45 += protect_R__REGIONKEY;
            if ( var44 == var45 )
            {
                qNATION2[x_qNATION_N__REGIONKEY] += 1;
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it56 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end55 = 
        qORDERS1.end();
    for (; qORDERS1_it56 != qORDERS1_end55; ++qORDERS1_it56)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it56->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it56->first);
        string N__NAME = get<2>(qORDERS1_it56->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it54 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end53 = 
            REGION.end();
        for (; REGION_it54 != REGION_end53; ++REGION_it54)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it54);
            string protect_R__NAME = get<1>(*REGION_it54);
            string protect_R__COMMENT = get<2>(*REGION_it54);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it52 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end51 = NATION.end();
            for (; NATION_it52 != NATION_end51; ++NATION_it52)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it52);
                string protect_N__NAME = get<1>(*NATION_it52);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it52);
                string protect_N__COMMENT = get<3>(*NATION_it52);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it50 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end49 = CUSTOMER.end();
                for (; CUSTOMER_it50 != CUSTOMER_end49; ++CUSTOMER_it50)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it50);
                    string protect_C__NAME = get<1>(*CUSTOMER_it50);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it50);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it50);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it50);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it50);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it50);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it50);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it48 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end47 = SUPPLIER.end();
                    for (; SUPPLIER_it48 != SUPPLIER_end47; ++SUPPLIER_it48)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it48);
                        string protect_S__NAME = get<1>(*SUPPLIER_it48);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it48);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it48);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it48);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it48);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it48);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it46 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end45 = LINEITEM.end();
                        for (; LINEITEM_it46 != LINEITEM_end45; ++LINEITEM_it46)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it46);
                            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it46);
                            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it46);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it46);
                            double protect_L__QUANTITY = get<4>(*LINEITEM_it46);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it46);
                            double protect_L__DISCOUNT = get<6>(*LINEITEM_it46);
                            double protect_L__TAX = get<7>(*LINEITEM_it46);
                            int64_t var58 = 0;
                            var58 += protect_N__REGIONKEY;
                            int64_t var59 = 0;
                            var59 += protect_R__REGIONKEY;
                            if ( var58 == var59 )
                            {
                                int64_t var56 = 0;
                                var56 += protect_C__NATIONKEY;
                                int64_t var57 = 0;
                                var57 += protect_N__NATIONKEY;
                                if ( var56 == var57 )
                                {
                                    int64_t var54 = 0;
                                    var54 += protect_C__NATIONKEY;
                                    int64_t var55 = 0;
                                    var55 += protect_S__NATIONKEY;
                                    if ( var54 == var55 )
                                    {
                                        int64_t var52 = 0;
                                        var52 += protect_L__SUPPKEY;
                                        int64_t var53 = 0;
                                        var53 += protect_S__SUPPKEY;
                                        if ( var52 == var53 )
                                        {
                                            string var50 = 0;
                                            var50 += N__NAME;
                                            string var51 = 0;
                                            var51 += protect_N__NAME;
                                            if ( var50 == var51 )
                                            {
                                                int64_t var48 = 0;
                                                var48 += x_qORDERS_O__CUSTKEY;
                                                int64_t var49 = 0;
                                                var49 += protect_C__CUSTKEY;
                                                if ( var48 == var49 )
                                                {
                                                    int64_t var46 = 0;
                                                    var46 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var47 = 0;
                                                    var47 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var46 == var47 )
                                                    {
                                                        qORDERS1[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it68 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end67 = 
        qORDERS2.end();
    for (; qORDERS2_it68 != qORDERS2_end67; ++qORDERS2_it68)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it68->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it68->first);
        string N__NAME = get<2>(qORDERS2_it68->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it66 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end65 = 
            REGION.end();
        for (; REGION_it66 != REGION_end65; ++REGION_it66)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it66);
            string protect_R__NAME = get<1>(*REGION_it66);
            string protect_R__COMMENT = get<2>(*REGION_it66);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it64 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end63 = NATION.end();
            for (; NATION_it64 != NATION_end63; ++NATION_it64)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it64);
                string protect_N__NAME = get<1>(*NATION_it64);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it64);
                string protect_N__COMMENT = get<3>(*NATION_it64);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it62 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end61 = CUSTOMER.end();
                for (; CUSTOMER_it62 != CUSTOMER_end61; ++CUSTOMER_it62)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it62);
                    string protect_C__NAME = get<1>(*CUSTOMER_it62);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it62);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it62);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it62);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it62);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it62);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it62);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it60 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end59 = SUPPLIER.end();
                    for (; SUPPLIER_it60 != SUPPLIER_end59; ++SUPPLIER_it60)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it60);
                        string protect_S__NAME = get<1>(*SUPPLIER_it60);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it60);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it60);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it60);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it60);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it60);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it58 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end57 = LINEITEM.end();
                        for (; LINEITEM_it58 != LINEITEM_end57; ++LINEITEM_it58)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it58);
                            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it58);
                            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it58);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it58);
                            double protect_L__QUANTITY = get<4>(*LINEITEM_it58);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it58);
                            double protect_L__DISCOUNT = get<6>(*LINEITEM_it58);
                            double protect_L__TAX = get<7>(*LINEITEM_it58);
                            int64_t var73 = 0;
                            var73 += protect_N__REGIONKEY;
                            int64_t var74 = 0;
                            var74 += protect_R__REGIONKEY;
                            if ( var73 == var74 )
                            {
                                int64_t var71 = 0;
                                var71 += protect_C__NATIONKEY;
                                int64_t var72 = 0;
                                var72 += protect_N__NATIONKEY;
                                if ( var71 == var72 )
                                {
                                    int64_t var69 = 0;
                                    var69 += protect_C__NATIONKEY;
                                    int64_t var70 = 0;
                                    var70 += protect_S__NATIONKEY;
                                    if ( var69 == var70 )
                                    {
                                        int64_t var67 = 0;
                                        var67 += protect_L__SUPPKEY;
                                        int64_t var68 = 0;
                                        var68 += protect_S__SUPPKEY;
                                        if ( var67 == var68 )
                                        {
                                            string var65 = 0;
                                            var65 += N__NAME;
                                            string var66 = 0;
                                            var66 += protect_N__NAME;
                                            if ( var65 == var66 )
                                            {
                                                int64_t var63 = 0;
                                                var63 += x_qORDERS_O__CUSTKEY;
                                                int64_t var64 = 0;
                                                var64 += protect_C__CUSTKEY;
                                                if ( var63 == var64 )
                                                {
                                                    int64_t var61 = 0;
                                                    var61 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var62 = 0;
                                                    var62 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var61 == var62 )
                                                    {
                                                        double var60 = 1;
                                                        var60 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var60 *= 
                                                            protect_L__DISCOUNT;
                                                        qORDERS2[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += var60;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it80 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end79 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it80 != qSUPPLIER1_end79; ++qSUPPLIER1_it80)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it80->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER1_it80->first);
        string N__NAME = get<2>(qSUPPLIER1_it80->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it78 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end77 = 
            REGION.end();
        for (; REGION_it78 != REGION_end77; ++REGION_it78)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it78);
            string protect_R__NAME = get<1>(*REGION_it78);
            string protect_R__COMMENT = get<2>(*REGION_it78);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it76 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end75 = NATION.end();
            for (; NATION_it76 != NATION_end75; ++NATION_it76)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it76);
                string protect_N__NAME = get<1>(*NATION_it76);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it76);
                string protect_N__COMMENT = get<3>(*NATION_it76);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it74 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end73 = CUSTOMER.end();
                for (; CUSTOMER_it74 != CUSTOMER_end73; ++CUSTOMER_it74)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it74);
                    string protect_C__NAME = get<1>(*CUSTOMER_it74);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it74);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it74);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it74);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it74);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it74);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it74);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it72 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end71 = ORDERS.end();
                    for (; ORDERS_it72 != ORDERS_end71; ++ORDERS_it72)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it72);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it72);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it72);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it72);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it72);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it72);
                        string protect_O__CLERK = get<6>(*ORDERS_it72);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it72);
                        string protect_O__COMMENT = get<8>(*ORDERS_it72);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it70 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end69 = LINEITEM.end();
                        for (; LINEITEM_it70 != LINEITEM_end69; ++LINEITEM_it70)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it70);
                            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it70);
                            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it70);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it70);
                            double protect_L__QUANTITY = get<4>(*LINEITEM_it70);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it70);
                            double protect_L__DISCOUNT = get<6>(*LINEITEM_it70);
                            double protect_L__TAX = get<7>(*LINEITEM_it70);
                            int64_t var87 = 0;
                            var87 += protect_N__REGIONKEY;
                            int64_t var88 = 0;
                            var88 += protect_R__REGIONKEY;
                            if ( var87 == var88 )
                            {
                                int64_t var85 = 0;
                                var85 += protect_C__CUSTKEY;
                                int64_t var86 = 0;
                                var86 += protect_O__CUSTKEY;
                                if ( var85 == var86 )
                                {
                                    int64_t var83 = 0;
                                    var83 += protect_L__ORDERKEY;
                                    int64_t var84 = 0;
                                    var84 += protect_O__ORDERKEY;
                                    if ( var83 == var84 )
                                    {
                                        string var81 = 0;
                                        var81 += N__NAME;
                                        string var82 = 0;
                                        var82 += protect_N__NAME;
                                        if ( var81 == var82 )
                                        {
                                            int64_t var79 = 0;
                                            var79 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var80 = 0;
                                            var80 += protect_N__NATIONKEY;
                                            if ( var79 == var80 )
                                            {
                                                int64_t var77 = 0;
                                                var77 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var78 = 0;
                                                var78 += protect_C__NATIONKEY;
                                                if ( var77 == var78 )
                                                {
                                                    int64_t var75 = 0;
                                                    var75 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var76 = 0;
                                                    var76 += protect_L__SUPPKEY;
                                                    if ( var75 == var76 )
                                                    {
                                                        qSUPPLIER1[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it92 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end91 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it92 != qSUPPLIER2_end91; ++qSUPPLIER2_it92)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it92->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER2_it92->first);
        string N__NAME = get<2>(qSUPPLIER2_it92->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it90 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end89 = 
            REGION.end();
        for (; REGION_it90 != REGION_end89; ++REGION_it90)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it90);
            string protect_R__NAME = get<1>(*REGION_it90);
            string protect_R__COMMENT = get<2>(*REGION_it90);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it88 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end87 = NATION.end();
            for (; NATION_it88 != NATION_end87; ++NATION_it88)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it88);
                string protect_N__NAME = get<1>(*NATION_it88);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it88);
                string protect_N__COMMENT = get<3>(*NATION_it88);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it86 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end85 = CUSTOMER.end();
                for (; CUSTOMER_it86 != CUSTOMER_end85; ++CUSTOMER_it86)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it86);
                    string protect_C__NAME = get<1>(*CUSTOMER_it86);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it86);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it86);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it86);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it86);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it86);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it86);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it84 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end83 = ORDERS.end();
                    for (; ORDERS_it84 != ORDERS_end83; ++ORDERS_it84)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it84);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it84);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it84);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it84);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it84);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it84);
                        string protect_O__CLERK = get<6>(*ORDERS_it84);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it84);
                        string protect_O__COMMENT = get<8>(*ORDERS_it84);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it82 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end81 = LINEITEM.end();
                        for (; LINEITEM_it82 != LINEITEM_end81; ++LINEITEM_it82)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it82);
                            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it82);
                            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it82);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it82);
                            double protect_L__QUANTITY = get<4>(*LINEITEM_it82);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it82);
                            double protect_L__DISCOUNT = get<6>(*LINEITEM_it82);
                            double protect_L__TAX = get<7>(*LINEITEM_it82);
                            int64_t var102 = 0;
                            var102 += protect_N__REGIONKEY;
                            int64_t var103 = 0;
                            var103 += protect_R__REGIONKEY;
                            if ( var102 == var103 )
                            {
                                int64_t var100 = 0;
                                var100 += protect_C__CUSTKEY;
                                int64_t var101 = 0;
                                var101 += protect_O__CUSTKEY;
                                if ( var100 == var101 )
                                {
                                    int64_t var98 = 0;
                                    var98 += protect_L__ORDERKEY;
                                    int64_t var99 = 0;
                                    var99 += protect_O__ORDERKEY;
                                    if ( var98 == var99 )
                                    {
                                        string var96 = 0;
                                        var96 += N__NAME;
                                        string var97 = 0;
                                        var97 += protect_N__NAME;
                                        if ( var96 == var97 )
                                        {
                                            int64_t var94 = 0;
                                            var94 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var95 = 0;
                                            var95 += protect_N__NATIONKEY;
                                            if ( var94 == var95 )
                                            {
                                                int64_t var92 = 0;
                                                var92 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var93 = 0;
                                                var93 += protect_C__NATIONKEY;
                                                if ( var92 == var93 )
                                                {
                                                    int64_t var90 = 0;
                                                    var90 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var91 = 0;
                                                    var91 += protect_L__SUPPKEY;
                                                    if ( var90 == var91 )
                                                    {
                                                        double var89 = 1;
                                                        var89 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var89 *= 
                                                            protect_L__DISCOUNT;
                                                        qSUPPLIER2[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += var89;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insertREGION_sec_span, on_insert_REGION_usec_span);
}

void on_insert_LINEITEM(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    PARTKEY,int64_t SUPPKEY,int LINENUMBER,double QUANTITY,double 
    EXTENDEDPRICE,double DISCOUNT,double TAX)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    LINEITEM.insert(make_tuple(
        ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDEDPRICE,DISCOUNT,TAX));
    map<string,double>::iterator q_it94 = q.begin();
    map<string,double>::iterator q_end93 = q.end();
    for (; q_it94 != q_end93; ++q_it94)
    {
        string N__NAME = q_it94->first;
        q[N__NAME] += EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(
            ORDERKEY,SUPPKEY,N__NAME)]*-1;
    }
    map<string,double>::iterator q_it96 = q.begin();
    map<string,double>::iterator q_end95 = q.end();
    for (; q_it96 != q_end95; ++q_it96)
    {
        string N__NAME = q_it96->first;
        q[N__NAME] += EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,SUPPKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it108 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end107 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it108 != qCUSTOMER1_end107; ++qCUSTOMER1_it108)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it108->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER1_it108->first);
        string N__NAME = get<2>(qCUSTOMER1_it108->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it106 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end105 = 
            REGION.end();
        for (; REGION_it106 != REGION_end105; ++REGION_it106)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it106);
            string protect_R__NAME = get<1>(*REGION_it106);
            string protect_R__COMMENT = get<2>(*REGION_it106);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it104 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end103 = NATION.end();
            for (; NATION_it104 != NATION_end103; ++NATION_it104)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it104);
                string protect_N__NAME = get<1>(*NATION_it104);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it104);
                string protect_N__COMMENT = get<3>(*NATION_it104);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it102 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end101 = SUPPLIER.end();
                for (; SUPPLIER_it102 != SUPPLIER_end101; ++SUPPLIER_it102)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it102);
                    string protect_S__NAME = get<1>(*SUPPLIER_it102);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it102);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it102);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it102);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it102);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it102);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it100 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end99 = ORDERS.end();
                    for (; ORDERS_it100 != ORDERS_end99; ++ORDERS_it100)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it100);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it100);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it100);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it100);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it100);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it100);
                        string protect_O__CLERK = get<6>(*ORDERS_it100);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it100);
                        string protect_O__COMMENT = get<8>(*ORDERS_it100);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it98 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end97 = LINEITEM.end();
                        for (; LINEITEM_it98 != LINEITEM_end97; ++LINEITEM_it98)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it98);
                            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it98);
                            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it98);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it98);
                            double protect_L__QUANTITY = get<4>(*LINEITEM_it98);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it98);
                            double protect_L__DISCOUNT = get<6>(*LINEITEM_it98);
                            double protect_L__TAX = get<7>(*LINEITEM_it98);
                            int64_t var116 = 0;
                            var116 += protect_N__REGIONKEY;
                            int64_t var117 = 0;
                            var117 += protect_R__REGIONKEY;
                            if ( var116 == var117 )
                            {
                                int64_t var114 = 0;
                                var114 += protect_L__SUPPKEY;
                                int64_t var115 = 0;
                                var115 += protect_S__SUPPKEY;
                                if ( var114 == var115 )
                                {
                                    int64_t var112 = 0;
                                    var112 += protect_L__ORDERKEY;
                                    int64_t var113 = 0;
                                    var113 += protect_O__ORDERKEY;
                                    if ( var112 == var113 )
                                    {
                                        string var110 = 0;
                                        var110 += N__NAME;
                                        string var111 = 0;
                                        var111 += protect_N__NAME;
                                        if ( var110 == var111 )
                                        {
                                            int64_t var108 = 0;
                                            var108 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var109 = 0;
                                            var109 += protect_N__NATIONKEY;
                                            if ( var108 == var109 )
                                            {
                                                int64_t var106 = 0;
                                                var106 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var107 = 0;
                                                var107 += protect_S__NATIONKEY;
                                                if ( var106 == var107 )
                                                {
                                                    int64_t var104 = 0;
                                                    var104 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var105 = 0;
                                                    var105 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var104 == var105 )
                                                    {
                                                        qCUSTOMER1[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it120 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end119 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it120 != qCUSTOMER2_end119; ++qCUSTOMER2_it120)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it120->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER2_it120->first);
        string N__NAME = get<2>(qCUSTOMER2_it120->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it118 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end117 = 
            REGION.end();
        for (; REGION_it118 != REGION_end117; ++REGION_it118)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it118);
            string protect_R__NAME = get<1>(*REGION_it118);
            string protect_R__COMMENT = get<2>(*REGION_it118);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it116 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end115 = NATION.end();
            for (; NATION_it116 != NATION_end115; ++NATION_it116)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it116);
                string protect_N__NAME = get<1>(*NATION_it116);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it116);
                string protect_N__COMMENT = get<3>(*NATION_it116);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it114 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end113 = SUPPLIER.end();
                for (; SUPPLIER_it114 != SUPPLIER_end113; ++SUPPLIER_it114)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it114);
                    string protect_S__NAME = get<1>(*SUPPLIER_it114);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it114);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it114);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it114);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it114);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it114);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it112 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end111 = ORDERS.end();
                    for (; ORDERS_it112 != ORDERS_end111; ++ORDERS_it112)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it112);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it112);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it112);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it112);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it112);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it112);
                        string protect_O__CLERK = get<6>(*ORDERS_it112);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it112);
                        string protect_O__COMMENT = get<8>(*ORDERS_it112);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it110 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end109 = LINEITEM.end();
                        for (
                            ; LINEITEM_it110 != LINEITEM_end109; ++LINEITEM_it110)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it110);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it110);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it110);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it110);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it110);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it110);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it110);
                            double protect_L__TAX = get<7>(*LINEITEM_it110);
                            int64_t var131 = 0;
                            var131 += protect_N__REGIONKEY;
                            int64_t var132 = 0;
                            var132 += protect_R__REGIONKEY;
                            if ( var131 == var132 )
                            {
                                int64_t var129 = 0;
                                var129 += protect_L__SUPPKEY;
                                int64_t var130 = 0;
                                var130 += protect_S__SUPPKEY;
                                if ( var129 == var130 )
                                {
                                    int64_t var127 = 0;
                                    var127 += protect_L__ORDERKEY;
                                    int64_t var128 = 0;
                                    var128 += protect_O__ORDERKEY;
                                    if ( var127 == var128 )
                                    {
                                        string var125 = 0;
                                        var125 += N__NAME;
                                        string var126 = 0;
                                        var126 += protect_N__NAME;
                                        if ( var125 == var126 )
                                        {
                                            int64_t var123 = 0;
                                            var123 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var124 = 0;
                                            var124 += protect_N__NATIONKEY;
                                            if ( var123 == var124 )
                                            {
                                                int64_t var121 = 0;
                                                var121 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var122 = 0;
                                                var122 += protect_S__NATIONKEY;
                                                if ( var121 == var122 )
                                                {
                                                    int64_t var119 = 0;
                                                    var119 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var120 = 0;
                                                    var120 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var119 == var120 )
                                                    {
                                                        double var118 = 1;
                                                        var118 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var118 *= 
                                                            protect_L__DISCOUNT;
                                                        qCUSTOMER2[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += var118;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qNATION1_it130 = qNATION1.begin();
    map<int64_t,double>::iterator qNATION1_end129 = qNATION1.end();
    for (; qNATION1_it130 != qNATION1_end129; ++qNATION1_it130)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION1_it130->first;
        qNATION1[x_qNATION_N__NATIONKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it128 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end127 = CUSTOMER.end();
        for (; CUSTOMER_it128 != CUSTOMER_end127; ++CUSTOMER_it128)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it128);
            string protect_C__NAME = get<1>(*CUSTOMER_it128);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it128);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it128);
            string protect_C__PHONE = get<4>(*CUSTOMER_it128);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it128);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it128);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it128);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it126 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end125 = SUPPLIER.end();
            for (; SUPPLIER_it126 != SUPPLIER_end125; ++SUPPLIER_it126)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it126);
                string protect_S__NAME = get<1>(*SUPPLIER_it126);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it126);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it126);
                string protect_S__PHONE = get<4>(*SUPPLIER_it126);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it126);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it126);
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it124 = ORDERS.begin();
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end123 = ORDERS.end();
                for (; ORDERS_it124 != ORDERS_end123; ++ORDERS_it124)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it124);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it124);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it124);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it124);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it124);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it124);
                    string protect_O__CLERK = get<6>(*ORDERS_it124);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it124);
                    string protect_O__COMMENT = get<8>(*ORDERS_it124);
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it122 = LINEITEM.begin();
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end121 = LINEITEM.end();
                    for (; LINEITEM_it122 != LINEITEM_end121; ++LINEITEM_it122)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it122);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it122);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it122);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it122);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it122);
                        double protect_L__EXTENDEDPRICE = get<5>(
                            *LINEITEM_it122);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it122);
                        double protect_L__TAX = get<7>(*LINEITEM_it122);
                        int64_t var141 = 0;
                        var141 += protect_C__CUSTKEY;
                        int64_t var142 = 0;
                        var142 += protect_O__CUSTKEY;
                        if ( var141 == var142 )
                        {
                            int64_t var139 = 0;
                            var139 += protect_L__SUPPKEY;
                            int64_t var140 = 0;
                            var140 += protect_S__SUPPKEY;
                            if ( var139 == var140 )
                            {
                                int64_t var137 = 0;
                                var137 += protect_L__ORDERKEY;
                                int64_t var138 = 0;
                                var138 += protect_O__ORDERKEY;
                                if ( var137 == var138 )
                                {
                                    int64_t var135 = 0;
                                    var135 += x_qNATION_N__NATIONKEY;
                                    int64_t var136 = 0;
                                    var136 += protect_C__NATIONKEY;
                                    if ( var135 == var136 )
                                    {
                                        int64_t var133 = 0;
                                        var133 += x_qNATION_N__NATIONKEY;
                                        int64_t var134 = 0;
                                        var134 += protect_S__NATIONKEY;
                                        if ( var133 == var134 )
                                        {
                                            qNATION1[x_qNATION_N__NATIONKEY] += 
                                                protect_L__EXTENDEDPRICE;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qNATION3_it140 = qNATION3.begin();
    map<int64_t,double>::iterator qNATION3_end139 = qNATION3.end();
    for (; qNATION3_it140 != qNATION3_end139; ++qNATION3_it140)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION3_it140->first;
        qNATION3[x_qNATION_N__NATIONKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it138 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end137 = CUSTOMER.end();
        for (; CUSTOMER_it138 != CUSTOMER_end137; ++CUSTOMER_it138)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it138);
            string protect_C__NAME = get<1>(*CUSTOMER_it138);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it138);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it138);
            string protect_C__PHONE = get<4>(*CUSTOMER_it138);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it138);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it138);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it138);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it136 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end135 = SUPPLIER.end();
            for (; SUPPLIER_it136 != SUPPLIER_end135; ++SUPPLIER_it136)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it136);
                string protect_S__NAME = get<1>(*SUPPLIER_it136);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it136);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it136);
                string protect_S__PHONE = get<4>(*SUPPLIER_it136);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it136);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it136);
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it134 = ORDERS.begin();
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end133 = ORDERS.end();
                for (; ORDERS_it134 != ORDERS_end133; ++ORDERS_it134)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it134);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it134);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it134);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it134);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it134);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it134);
                    string protect_O__CLERK = get<6>(*ORDERS_it134);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it134);
                    string protect_O__COMMENT = get<8>(*ORDERS_it134);
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it132 = LINEITEM.begin();
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end131 = LINEITEM.end();
                    for (; LINEITEM_it132 != LINEITEM_end131; ++LINEITEM_it132)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it132);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it132);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it132);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it132);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it132);
                        double protect_L__EXTENDEDPRICE = get<5>(
                            *LINEITEM_it132);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it132);
                        double protect_L__TAX = get<7>(*LINEITEM_it132);
                        int64_t var152 = 0;
                        var152 += protect_C__CUSTKEY;
                        int64_t var153 = 0;
                        var153 += protect_O__CUSTKEY;
                        if ( var152 == var153 )
                        {
                            int64_t var150 = 0;
                            var150 += protect_L__SUPPKEY;
                            int64_t var151 = 0;
                            var151 += protect_S__SUPPKEY;
                            if ( var150 == var151 )
                            {
                                int64_t var148 = 0;
                                var148 += protect_L__ORDERKEY;
                                int64_t var149 = 0;
                                var149 += protect_O__ORDERKEY;
                                if ( var148 == var149 )
                                {
                                    int64_t var146 = 0;
                                    var146 += x_qNATION_N__NATIONKEY;
                                    int64_t var147 = 0;
                                    var147 += protect_C__NATIONKEY;
                                    if ( var146 == var147 )
                                    {
                                        int64_t var144 = 0;
                                        var144 += x_qNATION_N__NATIONKEY;
                                        int64_t var145 = 0;
                                        var145 += protect_S__NATIONKEY;
                                        if ( var144 == var145 )
                                        {
                                            double var143 = 1;
                                            var143 *= protect_L__EXTENDEDPRICE;
                                            var143 *= protect_L__DISCOUNT;
                                            qNATION3[x_qNATION_N__NATIONKEY] += 
                                                var143;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it152 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end151 = 
        qORDERS1.end();
    for (; qORDERS1_it152 != qORDERS1_end151; ++qORDERS1_it152)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it152->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it152->first);
        string N__NAME = get<2>(qORDERS1_it152->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it150 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end149 = 
            REGION.end();
        for (; REGION_it150 != REGION_end149; ++REGION_it150)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it150);
            string protect_R__NAME = get<1>(*REGION_it150);
            string protect_R__COMMENT = get<2>(*REGION_it150);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it148 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end147 = NATION.end();
            for (; NATION_it148 != NATION_end147; ++NATION_it148)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it148);
                string protect_N__NAME = get<1>(*NATION_it148);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it148);
                string protect_N__COMMENT = get<3>(*NATION_it148);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it146 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end145 = CUSTOMER.end();
                for (; CUSTOMER_it146 != CUSTOMER_end145; ++CUSTOMER_it146)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it146);
                    string protect_C__NAME = get<1>(*CUSTOMER_it146);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it146);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it146);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it146);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it146);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it146);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it146);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it144 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end143 = SUPPLIER.end();
                    for (; SUPPLIER_it144 != SUPPLIER_end143; ++SUPPLIER_it144)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it144);
                        string protect_S__NAME = get<1>(*SUPPLIER_it144);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it144);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it144);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it144);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it144);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it144);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it142 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end141 = LINEITEM.end();
                        for (
                            ; LINEITEM_it142 != LINEITEM_end141; ++LINEITEM_it142)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it142);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it142);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it142);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it142);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it142);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it142);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it142);
                            double protect_L__TAX = get<7>(*LINEITEM_it142);
                            int64_t var166 = 0;
                            var166 += protect_N__REGIONKEY;
                            int64_t var167 = 0;
                            var167 += protect_R__REGIONKEY;
                            if ( var166 == var167 )
                            {
                                int64_t var164 = 0;
                                var164 += protect_C__NATIONKEY;
                                int64_t var165 = 0;
                                var165 += protect_N__NATIONKEY;
                                if ( var164 == var165 )
                                {
                                    int64_t var162 = 0;
                                    var162 += protect_C__NATIONKEY;
                                    int64_t var163 = 0;
                                    var163 += protect_S__NATIONKEY;
                                    if ( var162 == var163 )
                                    {
                                        int64_t var160 = 0;
                                        var160 += protect_L__SUPPKEY;
                                        int64_t var161 = 0;
                                        var161 += protect_S__SUPPKEY;
                                        if ( var160 == var161 )
                                        {
                                            string var158 = 0;
                                            var158 += N__NAME;
                                            string var159 = 0;
                                            var159 += protect_N__NAME;
                                            if ( var158 == var159 )
                                            {
                                                int64_t var156 = 0;
                                                var156 += x_qORDERS_O__CUSTKEY;
                                                int64_t var157 = 0;
                                                var157 += protect_C__CUSTKEY;
                                                if ( var156 == var157 )
                                                {
                                                    int64_t var154 = 0;
                                                    var154 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var155 = 0;
                                                    var155 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var154 == var155 )
                                                    {
                                                        qORDERS1[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it164 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end163 = 
        qORDERS2.end();
    for (; qORDERS2_it164 != qORDERS2_end163; ++qORDERS2_it164)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it164->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it164->first);
        string N__NAME = get<2>(qORDERS2_it164->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it162 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end161 = 
            REGION.end();
        for (; REGION_it162 != REGION_end161; ++REGION_it162)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it162);
            string protect_R__NAME = get<1>(*REGION_it162);
            string protect_R__COMMENT = get<2>(*REGION_it162);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it160 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end159 = NATION.end();
            for (; NATION_it160 != NATION_end159; ++NATION_it160)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it160);
                string protect_N__NAME = get<1>(*NATION_it160);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it160);
                string protect_N__COMMENT = get<3>(*NATION_it160);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it158 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end157 = CUSTOMER.end();
                for (; CUSTOMER_it158 != CUSTOMER_end157; ++CUSTOMER_it158)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it158);
                    string protect_C__NAME = get<1>(*CUSTOMER_it158);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it158);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it158);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it158);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it158);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it158);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it158);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it156 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end155 = SUPPLIER.end();
                    for (; SUPPLIER_it156 != SUPPLIER_end155; ++SUPPLIER_it156)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it156);
                        string protect_S__NAME = get<1>(*SUPPLIER_it156);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it156);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it156);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it156);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it156);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it156);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it154 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end153 = LINEITEM.end();
                        for (
                            ; LINEITEM_it154 != LINEITEM_end153; ++LINEITEM_it154)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it154);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it154);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it154);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it154);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it154);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it154);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it154);
                            double protect_L__TAX = get<7>(*LINEITEM_it154);
                            int64_t var181 = 0;
                            var181 += protect_N__REGIONKEY;
                            int64_t var182 = 0;
                            var182 += protect_R__REGIONKEY;
                            if ( var181 == var182 )
                            {
                                int64_t var179 = 0;
                                var179 += protect_C__NATIONKEY;
                                int64_t var180 = 0;
                                var180 += protect_N__NATIONKEY;
                                if ( var179 == var180 )
                                {
                                    int64_t var177 = 0;
                                    var177 += protect_C__NATIONKEY;
                                    int64_t var178 = 0;
                                    var178 += protect_S__NATIONKEY;
                                    if ( var177 == var178 )
                                    {
                                        int64_t var175 = 0;
                                        var175 += protect_L__SUPPKEY;
                                        int64_t var176 = 0;
                                        var176 += protect_S__SUPPKEY;
                                        if ( var175 == var176 )
                                        {
                                            string var173 = 0;
                                            var173 += N__NAME;
                                            string var174 = 0;
                                            var174 += protect_N__NAME;
                                            if ( var173 == var174 )
                                            {
                                                int64_t var171 = 0;
                                                var171 += x_qORDERS_O__CUSTKEY;
                                                int64_t var172 = 0;
                                                var172 += protect_C__CUSTKEY;
                                                if ( var171 == var172 )
                                                {
                                                    int64_t var169 = 0;
                                                    var169 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var170 = 0;
                                                    var170 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var169 == var170 )
                                                    {
                                                        double var168 = 1;
                                                        var168 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var168 *= 
                                                            protect_L__DISCOUNT;
                                                        qORDERS2[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += var168;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    map<tuple<string,int64_t>,double>::iterator qREGION1_it176 = qREGION1.begin();
    map<tuple<string,int64_t>,double>::iterator qREGION1_end175 = qREGION1.end();
    for (; qREGION1_it176 != qREGION1_end175; ++qREGION1_it176)
    {
        string N__NAME = get<0>(qREGION1_it176->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it176->first);
        qREGION1[make_tuple(N__NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_it174 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end173 
            = NATION.end();
        for (; NATION_it174 != NATION_end173; ++NATION_it174)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it174);
            string protect_N__NAME = get<1>(*NATION_it174);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it174);
            string protect_N__COMMENT = get<3>(*NATION_it174);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it172 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end171 = CUSTOMER.end();
            for (; CUSTOMER_it172 != CUSTOMER_end171; ++CUSTOMER_it172)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it172);
                string protect_C__NAME = get<1>(*CUSTOMER_it172);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it172);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it172);
                string protect_C__PHONE = get<4>(*CUSTOMER_it172);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it172);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it172);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it172);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it170 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end169 = SUPPLIER.end();
                for (; SUPPLIER_it170 != SUPPLIER_end169; ++SUPPLIER_it170)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it170);
                    string protect_S__NAME = get<1>(*SUPPLIER_it170);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it170);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it170);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it170);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it170);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it170);
                    
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
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it166 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end165 = LINEITEM.end();
                        for (
                            ; LINEITEM_it166 != LINEITEM_end165; ++LINEITEM_it166)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it166);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it166);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it166);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it166);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it166);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it166);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it166);
                            double protect_L__TAX = get<7>(*LINEITEM_it166);
                            int64_t var195 = 0;
                            var195 += protect_C__NATIONKEY;
                            int64_t var196 = 0;
                            var196 += protect_N__NATIONKEY;
                            if ( var195 == var196 )
                            {
                                int64_t var193 = 0;
                                var193 += protect_C__NATIONKEY;
                                int64_t var194 = 0;
                                var194 += protect_S__NATIONKEY;
                                if ( var193 == var194 )
                                {
                                    int64_t var191 = 0;
                                    var191 += protect_C__CUSTKEY;
                                    int64_t var192 = 0;
                                    var192 += protect_O__CUSTKEY;
                                    if ( var191 == var192 )
                                    {
                                        int64_t var189 = 0;
                                        var189 += protect_L__SUPPKEY;
                                        int64_t var190 = 0;
                                        var190 += protect_S__SUPPKEY;
                                        if ( var189 == var190 )
                                        {
                                            int64_t var187 = 0;
                                            var187 += protect_L__ORDERKEY;
                                            int64_t var188 = 0;
                                            var188 += protect_O__ORDERKEY;
                                            if ( var187 == var188 )
                                            {
                                                int64_t var185 = 0;
                                                var185 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var186 = 0;
                                                var186 += protect_N__REGIONKEY;
                                                if ( var185 == var186 )
                                                {
                                                    string var183 = 0;
                                                    var183 += N__NAME;
                                                    string var184 = 0;
                                                    var184 += protect_N__NAME;
                                                    if ( var183 == var184 )
                                                    {
                                                        qREGION1[make_tuple(
                                                            N__NAME,x_qREGION_R__REGIONKEY)] += protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it188 = qREGION2.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION2_end187 = qREGION2.end(
        );
    for (; qREGION2_it188 != qREGION2_end187; ++qREGION2_it188)
    {
        string N__NAME = get<0>(qREGION2_it188->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it188->first);
        qREGION2[make_tuple(N__NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_it186 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end185 
            = NATION.end();
        for (; NATION_it186 != NATION_end185; ++NATION_it186)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it186);
            string protect_N__NAME = get<1>(*NATION_it186);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it186);
            string protect_N__COMMENT = get<3>(*NATION_it186);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it184 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end183 = CUSTOMER.end();
            for (; CUSTOMER_it184 != CUSTOMER_end183; ++CUSTOMER_it184)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it184);
                string protect_C__NAME = get<1>(*CUSTOMER_it184);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it184);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it184);
                string protect_C__PHONE = get<4>(*CUSTOMER_it184);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it184);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it184);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it184);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it182 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end181 = SUPPLIER.end();
                for (; SUPPLIER_it182 != SUPPLIER_end181; ++SUPPLIER_it182)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it182);
                    string protect_S__NAME = get<1>(*SUPPLIER_it182);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it182);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it182);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it182);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it182);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it182);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it180 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end179 = ORDERS.end();
                    for (; ORDERS_it180 != ORDERS_end179; ++ORDERS_it180)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it180);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it180);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it180);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it180);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it180);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it180);
                        string protect_O__CLERK = get<6>(*ORDERS_it180);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it180);
                        string protect_O__COMMENT = get<8>(*ORDERS_it180);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it178 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end177 = LINEITEM.end();
                        for (
                            ; LINEITEM_it178 != LINEITEM_end177; ++LINEITEM_it178)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it178);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it178);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it178);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it178);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it178);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it178);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it178);
                            double protect_L__TAX = get<7>(*LINEITEM_it178);
                            int64_t var210 = 0;
                            var210 += protect_C__NATIONKEY;
                            int64_t var211 = 0;
                            var211 += protect_N__NATIONKEY;
                            if ( var210 == var211 )
                            {
                                int64_t var208 = 0;
                                var208 += protect_C__NATIONKEY;
                                int64_t var209 = 0;
                                var209 += protect_S__NATIONKEY;
                                if ( var208 == var209 )
                                {
                                    int64_t var206 = 0;
                                    var206 += protect_C__CUSTKEY;
                                    int64_t var207 = 0;
                                    var207 += protect_O__CUSTKEY;
                                    if ( var206 == var207 )
                                    {
                                        int64_t var204 = 0;
                                        var204 += protect_L__SUPPKEY;
                                        int64_t var205 = 0;
                                        var205 += protect_S__SUPPKEY;
                                        if ( var204 == var205 )
                                        {
                                            int64_t var202 = 0;
                                            var202 += protect_L__ORDERKEY;
                                            int64_t var203 = 0;
                                            var203 += protect_O__ORDERKEY;
                                            if ( var202 == var203 )
                                            {
                                                int64_t var200 = 0;
                                                var200 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var201 = 0;
                                                var201 += protect_N__REGIONKEY;
                                                if ( var200 == var201 )
                                                {
                                                    string var198 = 0;
                                                    var198 += N__NAME;
                                                    string var199 = 0;
                                                    var199 += protect_N__NAME;
                                                    if ( var198 == var199 )
                                                    {
                                                        double var197 = 1;
                                                        var197 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var197 *= 
                                                            protect_L__DISCOUNT;
                                                        qREGION2[make_tuple(
                                                            N__NAME,x_qREGION_R__REGIONKEY)] += var197;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it200 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end199 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it200 != qSUPPLIER1_end199; ++qSUPPLIER1_it200)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it200->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER1_it200->first);
        string N__NAME = get<2>(qSUPPLIER1_it200->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it198 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end197 = 
            REGION.end();
        for (; REGION_it198 != REGION_end197; ++REGION_it198)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it198);
            string protect_R__NAME = get<1>(*REGION_it198);
            string protect_R__COMMENT = get<2>(*REGION_it198);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it196 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end195 = NATION.end();
            for (; NATION_it196 != NATION_end195; ++NATION_it196)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it196);
                string protect_N__NAME = get<1>(*NATION_it196);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it196);
                string protect_N__COMMENT = get<3>(*NATION_it196);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it194 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end193 = CUSTOMER.end();
                for (; CUSTOMER_it194 != CUSTOMER_end193; ++CUSTOMER_it194)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it194);
                    string protect_C__NAME = get<1>(*CUSTOMER_it194);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it194);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it194);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it194);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it194);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it194);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it194);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it192 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end191 = ORDERS.end();
                    for (; ORDERS_it192 != ORDERS_end191; ++ORDERS_it192)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it192);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it192);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it192);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it192);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it192);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it192);
                        string protect_O__CLERK = get<6>(*ORDERS_it192);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it192);
                        string protect_O__COMMENT = get<8>(*ORDERS_it192);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it190 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end189 = LINEITEM.end();
                        for (
                            ; LINEITEM_it190 != LINEITEM_end189; ++LINEITEM_it190)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it190);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it190);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it190);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it190);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it190);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it190);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it190);
                            double protect_L__TAX = get<7>(*LINEITEM_it190);
                            int64_t var224 = 0;
                            var224 += protect_N__REGIONKEY;
                            int64_t var225 = 0;
                            var225 += protect_R__REGIONKEY;
                            if ( var224 == var225 )
                            {
                                int64_t var222 = 0;
                                var222 += protect_C__CUSTKEY;
                                int64_t var223 = 0;
                                var223 += protect_O__CUSTKEY;
                                if ( var222 == var223 )
                                {
                                    int64_t var220 = 0;
                                    var220 += protect_L__ORDERKEY;
                                    int64_t var221 = 0;
                                    var221 += protect_O__ORDERKEY;
                                    if ( var220 == var221 )
                                    {
                                        string var218 = 0;
                                        var218 += N__NAME;
                                        string var219 = 0;
                                        var219 += protect_N__NAME;
                                        if ( var218 == var219 )
                                        {
                                            int64_t var216 = 0;
                                            var216 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var217 = 0;
                                            var217 += protect_N__NATIONKEY;
                                            if ( var216 == var217 )
                                            {
                                                int64_t var214 = 0;
                                                var214 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var215 = 0;
                                                var215 += protect_C__NATIONKEY;
                                                if ( var214 == var215 )
                                                {
                                                    int64_t var212 = 0;
                                                    var212 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var213 = 0;
                                                    var213 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var212 == var213 )
                                                    {
                                                        qSUPPLIER1[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it212 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end211 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it212 != qSUPPLIER2_end211; ++qSUPPLIER2_it212)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it212->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER2_it212->first);
        string N__NAME = get<2>(qSUPPLIER2_it212->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it210 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end209 = 
            REGION.end();
        for (; REGION_it210 != REGION_end209; ++REGION_it210)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it210);
            string protect_R__NAME = get<1>(*REGION_it210);
            string protect_R__COMMENT = get<2>(*REGION_it210);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it208 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end207 = NATION.end();
            for (; NATION_it208 != NATION_end207; ++NATION_it208)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it208);
                string protect_N__NAME = get<1>(*NATION_it208);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it208);
                string protect_N__COMMENT = get<3>(*NATION_it208);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it206 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end205 = CUSTOMER.end();
                for (; CUSTOMER_it206 != CUSTOMER_end205; ++CUSTOMER_it206)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it206);
                    string protect_C__NAME = get<1>(*CUSTOMER_it206);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it206);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it206);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it206);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it206);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it206);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it206);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it204 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end203 = ORDERS.end();
                    for (; ORDERS_it204 != ORDERS_end203; ++ORDERS_it204)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it204);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it204);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it204);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it204);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it204);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it204);
                        string protect_O__CLERK = get<6>(*ORDERS_it204);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it204);
                        string protect_O__COMMENT = get<8>(*ORDERS_it204);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it202 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end201 = LINEITEM.end();
                        for (
                            ; LINEITEM_it202 != LINEITEM_end201; ++LINEITEM_it202)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it202);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it202);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it202);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it202);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it202);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it202);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it202);
                            double protect_L__TAX = get<7>(*LINEITEM_it202);
                            int64_t var239 = 0;
                            var239 += protect_N__REGIONKEY;
                            int64_t var240 = 0;
                            var240 += protect_R__REGIONKEY;
                            if ( var239 == var240 )
                            {
                                int64_t var237 = 0;
                                var237 += protect_C__CUSTKEY;
                                int64_t var238 = 0;
                                var238 += protect_O__CUSTKEY;
                                if ( var237 == var238 )
                                {
                                    int64_t var235 = 0;
                                    var235 += protect_L__ORDERKEY;
                                    int64_t var236 = 0;
                                    var236 += protect_O__ORDERKEY;
                                    if ( var235 == var236 )
                                    {
                                        string var233 = 0;
                                        var233 += N__NAME;
                                        string var234 = 0;
                                        var234 += protect_N__NAME;
                                        if ( var233 == var234 )
                                        {
                                            int64_t var231 = 0;
                                            var231 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var232 = 0;
                                            var232 += protect_N__NATIONKEY;
                                            if ( var231 == var232 )
                                            {
                                                int64_t var229 = 0;
                                                var229 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var230 = 0;
                                                var230 += protect_C__NATIONKEY;
                                                if ( var229 == var230 )
                                                {
                                                    int64_t var227 = 0;
                                                    var227 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var228 = 0;
                                                    var228 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var227 == var228 )
                                                    {
                                                        double var226 = 1;
                                                        var226 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var226 *= 
                                                            protect_L__DISCOUNT;
                                                        qSUPPLIER2[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += var226;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
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
    map<string,double>::iterator q_it214 = q.begin();
    map<string,double>::iterator q_end213 = q.end();
    for (; q_it214 != q_end213; ++q_it214)
    {
        string N__NAME = q_it214->first;
        q[N__NAME] += qORDERS1[make_tuple(ORDERKEY,CUSTKEY,N__NAME)];
    }
    map<string,double>::iterator q_it216 = q.begin();
    map<string,double>::iterator q_end215 = q.end();
    for (; q_it216 != q_end215; ++q_it216)
    {
        string N__NAME = q_it216->first;
        q[N__NAME] += qORDERS2[make_tuple(ORDERKEY,CUSTKEY,N__NAME)]*-1;
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it228 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end227 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it228 != qCUSTOMER1_end227; ++qCUSTOMER1_it228)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it228->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER1_it228->first);
        string N__NAME = get<2>(qCUSTOMER1_it228->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it226 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end225 = 
            REGION.end();
        for (; REGION_it226 != REGION_end225; ++REGION_it226)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it226);
            string protect_R__NAME = get<1>(*REGION_it226);
            string protect_R__COMMENT = get<2>(*REGION_it226);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it224 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end223 = NATION.end();
            for (; NATION_it224 != NATION_end223; ++NATION_it224)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it224);
                string protect_N__NAME = get<1>(*NATION_it224);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it224);
                string protect_N__COMMENT = get<3>(*NATION_it224);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it222 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end221 = SUPPLIER.end();
                for (; SUPPLIER_it222 != SUPPLIER_end221; ++SUPPLIER_it222)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it222);
                    string protect_S__NAME = get<1>(*SUPPLIER_it222);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it222);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it222);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it222);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it222);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it222);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it220 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end219 = ORDERS.end();
                    for (; ORDERS_it220 != ORDERS_end219; ++ORDERS_it220)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it220);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it220);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it220);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it220);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it220);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it220);
                        string protect_O__CLERK = get<6>(*ORDERS_it220);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it220);
                        string protect_O__COMMENT = get<8>(*ORDERS_it220);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it218 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end217 = LINEITEM.end();
                        for (
                            ; LINEITEM_it218 != LINEITEM_end217; ++LINEITEM_it218)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it218);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it218);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it218);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it218);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it218);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it218);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it218);
                            double protect_L__TAX = get<7>(*LINEITEM_it218);
                            int64_t var253 = 0;
                            var253 += protect_N__REGIONKEY;
                            int64_t var254 = 0;
                            var254 += protect_R__REGIONKEY;
                            if ( var253 == var254 )
                            {
                                int64_t var251 = 0;
                                var251 += protect_L__SUPPKEY;
                                int64_t var252 = 0;
                                var252 += protect_S__SUPPKEY;
                                if ( var251 == var252 )
                                {
                                    int64_t var249 = 0;
                                    var249 += protect_L__ORDERKEY;
                                    int64_t var250 = 0;
                                    var250 += protect_O__ORDERKEY;
                                    if ( var249 == var250 )
                                    {
                                        string var247 = 0;
                                        var247 += N__NAME;
                                        string var248 = 0;
                                        var248 += protect_N__NAME;
                                        if ( var247 == var248 )
                                        {
                                            int64_t var245 = 0;
                                            var245 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var246 = 0;
                                            var246 += protect_N__NATIONKEY;
                                            if ( var245 == var246 )
                                            {
                                                int64_t var243 = 0;
                                                var243 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var244 = 0;
                                                var244 += protect_S__NATIONKEY;
                                                if ( var243 == var244 )
                                                {
                                                    int64_t var241 = 0;
                                                    var241 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var242 = 0;
                                                    var242 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var241 == var242 )
                                                    {
                                                        qCUSTOMER1[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it240 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end239 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it240 != qCUSTOMER2_end239; ++qCUSTOMER2_it240)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it240->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER2_it240->first);
        string N__NAME = get<2>(qCUSTOMER2_it240->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it238 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end237 = 
            REGION.end();
        for (; REGION_it238 != REGION_end237; ++REGION_it238)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it238);
            string protect_R__NAME = get<1>(*REGION_it238);
            string protect_R__COMMENT = get<2>(*REGION_it238);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it236 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end235 = NATION.end();
            for (; NATION_it236 != NATION_end235; ++NATION_it236)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it236);
                string protect_N__NAME = get<1>(*NATION_it236);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it236);
                string protect_N__COMMENT = get<3>(*NATION_it236);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it234 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end233 = SUPPLIER.end();
                for (; SUPPLIER_it234 != SUPPLIER_end233; ++SUPPLIER_it234)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it234);
                    string protect_S__NAME = get<1>(*SUPPLIER_it234);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it234);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it234);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it234);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it234);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it234);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it232 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end231 = ORDERS.end();
                    for (; ORDERS_it232 != ORDERS_end231; ++ORDERS_it232)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it232);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it232);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it232);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it232);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it232);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it232);
                        string protect_O__CLERK = get<6>(*ORDERS_it232);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it232);
                        string protect_O__COMMENT = get<8>(*ORDERS_it232);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it230 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end229 = LINEITEM.end();
                        for (
                            ; LINEITEM_it230 != LINEITEM_end229; ++LINEITEM_it230)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it230);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it230);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it230);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it230);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it230);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it230);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it230);
                            double protect_L__TAX = get<7>(*LINEITEM_it230);
                            int64_t var268 = 0;
                            var268 += protect_N__REGIONKEY;
                            int64_t var269 = 0;
                            var269 += protect_R__REGIONKEY;
                            if ( var268 == var269 )
                            {
                                int64_t var266 = 0;
                                var266 += protect_L__SUPPKEY;
                                int64_t var267 = 0;
                                var267 += protect_S__SUPPKEY;
                                if ( var266 == var267 )
                                {
                                    int64_t var264 = 0;
                                    var264 += protect_L__ORDERKEY;
                                    int64_t var265 = 0;
                                    var265 += protect_O__ORDERKEY;
                                    if ( var264 == var265 )
                                    {
                                        string var262 = 0;
                                        var262 += N__NAME;
                                        string var263 = 0;
                                        var263 += protect_N__NAME;
                                        if ( var262 == var263 )
                                        {
                                            int64_t var260 = 0;
                                            var260 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var261 = 0;
                                            var261 += protect_N__NATIONKEY;
                                            if ( var260 == var261 )
                                            {
                                                int64_t var258 = 0;
                                                var258 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var259 = 0;
                                                var259 += protect_S__NATIONKEY;
                                                if ( var258 == var259 )
                                                {
                                                    int64_t var256 = 0;
                                                    var256 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var257 = 0;
                                                    var257 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var256 == var257 )
                                                    {
                                                        double var255 = 1;
                                                        var255 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var255 *= 
                                                            protect_L__DISCOUNT;
                                                        qCUSTOMER2[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += var255;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it252 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end251 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it252 != qLINEITEM1_end251; ++qLINEITEM1_it252)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it252->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it252->first);
        string N__NAME = get<2>(qLINEITEM1_it252->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it250 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end249 = 
            REGION.end();
        for (; REGION_it250 != REGION_end249; ++REGION_it250)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it250);
            string protect_R__NAME = get<1>(*REGION_it250);
            string protect_R__COMMENT = get<2>(*REGION_it250);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it248 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end247 = NATION.end();
            for (; NATION_it248 != NATION_end247; ++NATION_it248)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it248);
                string protect_N__NAME = get<1>(*NATION_it248);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it248);
                string protect_N__COMMENT = get<3>(*NATION_it248);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it246 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end245 = SUPPLIER.end();
                for (; SUPPLIER_it246 != SUPPLIER_end245; ++SUPPLIER_it246)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it246);
                    string protect_S__NAME = get<1>(*SUPPLIER_it246);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it246);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it246);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it246);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it246);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it246);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_it244 = CUSTOMER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_end243 = CUSTOMER.end();
                    for (; CUSTOMER_it244 != CUSTOMER_end243; ++CUSTOMER_it244)
                    {
                        int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it244);
                        string protect_C__NAME = get<1>(*CUSTOMER_it244);
                        string protect_C__ADDRESS = get<2>(*CUSTOMER_it244);
                        int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it244);
                        string protect_C__PHONE = get<4>(*CUSTOMER_it244);
                        double protect_C__ACCTBAL = get<5>(*CUSTOMER_it244);
                        string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it244);
                        string protect_C__COMMENT = get<7>(*CUSTOMER_it244);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it242 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end241 = ORDERS.end();
                        for (; ORDERS_it242 != ORDERS_end241; ++ORDERS_it242)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it242);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it242);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it242);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it242);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it242);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it242);
                            string protect_O__CLERK = get<6>(*ORDERS_it242);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it242);
                            string protect_O__COMMENT = get<8>(*ORDERS_it242);
                            int64_t var282 = 0;
                            var282 += protect_N__REGIONKEY;
                            int64_t var283 = 0;
                            var283 += protect_R__REGIONKEY;
                            if ( var282 == var283 )
                            {
                                int64_t var280 = 0;
                                var280 += protect_C__NATIONKEY;
                                int64_t var281 = 0;
                                var281 += protect_N__NATIONKEY;
                                if ( var280 == var281 )
                                {
                                    int64_t var278 = 0;
                                    var278 += protect_C__NATIONKEY;
                                    int64_t var279 = 0;
                                    var279 += protect_S__NATIONKEY;
                                    if ( var278 == var279 )
                                    {
                                        int64_t var276 = 0;
                                        var276 += protect_C__CUSTKEY;
                                        int64_t var277 = 0;
                                        var277 += protect_O__CUSTKEY;
                                        if ( var276 == var277 )
                                        {
                                            string var274 = 0;
                                            var274 += N__NAME;
                                            string var275 = 0;
                                            var275 += protect_N__NAME;
                                            if ( var274 == var275 )
                                            {
                                                int64_t var272 = 0;
                                                var272 += 
                                                    x_qLINEITEM_L__SUPPKEY;
                                                int64_t var273 = 0;
                                                var273 += protect_S__SUPPKEY;
                                                if ( var272 == var273 )
                                                {
                                                    int64_t var270 = 0;
                                                    var270 += 
                                                        x_qLINEITEM_L__ORDERKEY;
                                                    int64_t var271 = 0;
                                                    var271 += 
                                                        protect_O__ORDERKEY;
                                                    if ( var270 == var271 )
                                                    {
                                                        qLINEITEM1[make_tuple(
                                                            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += 1;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qNATION1_it262 = qNATION1.begin();
    map<int64_t,double>::iterator qNATION1_end261 = qNATION1.end();
    for (; qNATION1_it262 != qNATION1_end261; ++qNATION1_it262)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION1_it262->first;
        qNATION1[x_qNATION_N__NATIONKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it260 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end259 = CUSTOMER.end();
        for (; CUSTOMER_it260 != CUSTOMER_end259; ++CUSTOMER_it260)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it260);
            string protect_C__NAME = get<1>(*CUSTOMER_it260);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it260);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it260);
            string protect_C__PHONE = get<4>(*CUSTOMER_it260);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it260);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it260);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it260);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it258 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end257 = SUPPLIER.end();
            for (; SUPPLIER_it258 != SUPPLIER_end257; ++SUPPLIER_it258)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it258);
                string protect_S__NAME = get<1>(*SUPPLIER_it258);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it258);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it258);
                string protect_S__PHONE = get<4>(*SUPPLIER_it258);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it258);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it258);
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it256 = ORDERS.begin();
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end255 = ORDERS.end();
                for (; ORDERS_it256 != ORDERS_end255; ++ORDERS_it256)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it256);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it256);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it256);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it256);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it256);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it256);
                    string protect_O__CLERK = get<6>(*ORDERS_it256);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it256);
                    string protect_O__COMMENT = get<8>(*ORDERS_it256);
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it254 = LINEITEM.begin();
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end253 = LINEITEM.end();
                    for (; LINEITEM_it254 != LINEITEM_end253; ++LINEITEM_it254)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it254);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it254);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it254);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it254);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it254);
                        double protect_L__EXTENDEDPRICE = get<5>(
                            *LINEITEM_it254);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it254);
                        double protect_L__TAX = get<7>(*LINEITEM_it254);
                        int64_t var292 = 0;
                        var292 += protect_C__CUSTKEY;
                        int64_t var293 = 0;
                        var293 += protect_O__CUSTKEY;
                        if ( var292 == var293 )
                        {
                            int64_t var290 = 0;
                            var290 += protect_L__SUPPKEY;
                            int64_t var291 = 0;
                            var291 += protect_S__SUPPKEY;
                            if ( var290 == var291 )
                            {
                                int64_t var288 = 0;
                                var288 += protect_L__ORDERKEY;
                                int64_t var289 = 0;
                                var289 += protect_O__ORDERKEY;
                                if ( var288 == var289 )
                                {
                                    int64_t var286 = 0;
                                    var286 += x_qNATION_N__NATIONKEY;
                                    int64_t var287 = 0;
                                    var287 += protect_C__NATIONKEY;
                                    if ( var286 == var287 )
                                    {
                                        int64_t var284 = 0;
                                        var284 += x_qNATION_N__NATIONKEY;
                                        int64_t var285 = 0;
                                        var285 += protect_S__NATIONKEY;
                                        if ( var284 == var285 )
                                        {
                                            qNATION1[x_qNATION_N__NATIONKEY] += 
                                                protect_L__EXTENDEDPRICE;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qNATION3_it272 = qNATION3.begin();
    map<int64_t,double>::iterator qNATION3_end271 = qNATION3.end();
    for (; qNATION3_it272 != qNATION3_end271; ++qNATION3_it272)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION3_it272->first;
        qNATION3[x_qNATION_N__NATIONKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it270 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end269 = CUSTOMER.end();
        for (; CUSTOMER_it270 != CUSTOMER_end269; ++CUSTOMER_it270)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it270);
            string protect_C__NAME = get<1>(*CUSTOMER_it270);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it270);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it270);
            string protect_C__PHONE = get<4>(*CUSTOMER_it270);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it270);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it270);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it270);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it268 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end267 = SUPPLIER.end();
            for (; SUPPLIER_it268 != SUPPLIER_end267; ++SUPPLIER_it268)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it268);
                string protect_S__NAME = get<1>(*SUPPLIER_it268);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it268);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it268);
                string protect_S__PHONE = get<4>(*SUPPLIER_it268);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it268);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it268);
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it266 = ORDERS.begin();
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end265 = ORDERS.end();
                for (; ORDERS_it266 != ORDERS_end265; ++ORDERS_it266)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it266);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it266);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it266);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it266);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it266);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it266);
                    string protect_O__CLERK = get<6>(*ORDERS_it266);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it266);
                    string protect_O__COMMENT = get<8>(*ORDERS_it266);
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it264 = LINEITEM.begin();
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end263 = LINEITEM.end();
                    for (; LINEITEM_it264 != LINEITEM_end263; ++LINEITEM_it264)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it264);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it264);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it264);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it264);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it264);
                        double protect_L__EXTENDEDPRICE = get<5>(
                            *LINEITEM_it264);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it264);
                        double protect_L__TAX = get<7>(*LINEITEM_it264);
                        int64_t var303 = 0;
                        var303 += protect_C__CUSTKEY;
                        int64_t var304 = 0;
                        var304 += protect_O__CUSTKEY;
                        if ( var303 == var304 )
                        {
                            int64_t var301 = 0;
                            var301 += protect_L__SUPPKEY;
                            int64_t var302 = 0;
                            var302 += protect_S__SUPPKEY;
                            if ( var301 == var302 )
                            {
                                int64_t var299 = 0;
                                var299 += protect_L__ORDERKEY;
                                int64_t var300 = 0;
                                var300 += protect_O__ORDERKEY;
                                if ( var299 == var300 )
                                {
                                    int64_t var297 = 0;
                                    var297 += x_qNATION_N__NATIONKEY;
                                    int64_t var298 = 0;
                                    var298 += protect_C__NATIONKEY;
                                    if ( var297 == var298 )
                                    {
                                        int64_t var295 = 0;
                                        var295 += x_qNATION_N__NATIONKEY;
                                        int64_t var296 = 0;
                                        var296 += protect_S__NATIONKEY;
                                        if ( var295 == var296 )
                                        {
                                            double var294 = 1;
                                            var294 *= protect_L__EXTENDEDPRICE;
                                            var294 *= protect_L__DISCOUNT;
                                            qNATION3[x_qNATION_N__NATIONKEY] += 
                                                var294;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    map<tuple<string,int64_t>,double>::iterator qREGION1_it284 = qREGION1.begin();
    map<tuple<string,int64_t>,double>::iterator qREGION1_end283 = qREGION1.end();
    for (; qREGION1_it284 != qREGION1_end283; ++qREGION1_it284)
    {
        string N__NAME = get<0>(qREGION1_it284->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it284->first);
        qREGION1[make_tuple(N__NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_it282 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end281 
            = NATION.end();
        for (; NATION_it282 != NATION_end281; ++NATION_it282)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it282);
            string protect_N__NAME = get<1>(*NATION_it282);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it282);
            string protect_N__COMMENT = get<3>(*NATION_it282);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it280 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end279 = CUSTOMER.end();
            for (; CUSTOMER_it280 != CUSTOMER_end279; ++CUSTOMER_it280)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it280);
                string protect_C__NAME = get<1>(*CUSTOMER_it280);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it280);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it280);
                string protect_C__PHONE = get<4>(*CUSTOMER_it280);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it280);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it280);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it280);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it278 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end277 = SUPPLIER.end();
                for (; SUPPLIER_it278 != SUPPLIER_end277; ++SUPPLIER_it278)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it278);
                    string protect_S__NAME = get<1>(*SUPPLIER_it278);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it278);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it278);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it278);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it278);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it278);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it276 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end275 = ORDERS.end();
                    for (; ORDERS_it276 != ORDERS_end275; ++ORDERS_it276)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it276);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it276);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it276);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it276);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it276);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it276);
                        string protect_O__CLERK = get<6>(*ORDERS_it276);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it276);
                        string protect_O__COMMENT = get<8>(*ORDERS_it276);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it274 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end273 = LINEITEM.end();
                        for (
                            ; LINEITEM_it274 != LINEITEM_end273; ++LINEITEM_it274)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it274);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it274);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it274);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it274);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it274);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it274);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it274);
                            double protect_L__TAX = get<7>(*LINEITEM_it274);
                            int64_t var317 = 0;
                            var317 += protect_C__NATIONKEY;
                            int64_t var318 = 0;
                            var318 += protect_N__NATIONKEY;
                            if ( var317 == var318 )
                            {
                                int64_t var315 = 0;
                                var315 += protect_C__NATIONKEY;
                                int64_t var316 = 0;
                                var316 += protect_S__NATIONKEY;
                                if ( var315 == var316 )
                                {
                                    int64_t var313 = 0;
                                    var313 += protect_C__CUSTKEY;
                                    int64_t var314 = 0;
                                    var314 += protect_O__CUSTKEY;
                                    if ( var313 == var314 )
                                    {
                                        int64_t var311 = 0;
                                        var311 += protect_L__SUPPKEY;
                                        int64_t var312 = 0;
                                        var312 += protect_S__SUPPKEY;
                                        if ( var311 == var312 )
                                        {
                                            int64_t var309 = 0;
                                            var309 += protect_L__ORDERKEY;
                                            int64_t var310 = 0;
                                            var310 += protect_O__ORDERKEY;
                                            if ( var309 == var310 )
                                            {
                                                int64_t var307 = 0;
                                                var307 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var308 = 0;
                                                var308 += protect_N__REGIONKEY;
                                                if ( var307 == var308 )
                                                {
                                                    string var305 = 0;
                                                    var305 += N__NAME;
                                                    string var306 = 0;
                                                    var306 += protect_N__NAME;
                                                    if ( var305 == var306 )
                                                    {
                                                        qREGION1[make_tuple(
                                                            N__NAME,x_qREGION_R__REGIONKEY)] += protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it296 = qREGION2.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION2_end295 = qREGION2.end(
        );
    for (; qREGION2_it296 != qREGION2_end295; ++qREGION2_it296)
    {
        string N__NAME = get<0>(qREGION2_it296->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it296->first);
        qREGION2[make_tuple(N__NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_it294 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end293 
            = NATION.end();
        for (; NATION_it294 != NATION_end293; ++NATION_it294)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it294);
            string protect_N__NAME = get<1>(*NATION_it294);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it294);
            string protect_N__COMMENT = get<3>(*NATION_it294);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it292 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end291 = CUSTOMER.end();
            for (; CUSTOMER_it292 != CUSTOMER_end291; ++CUSTOMER_it292)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it292);
                string protect_C__NAME = get<1>(*CUSTOMER_it292);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it292);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it292);
                string protect_C__PHONE = get<4>(*CUSTOMER_it292);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it292);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it292);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it292);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it290 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end289 = SUPPLIER.end();
                for (; SUPPLIER_it290 != SUPPLIER_end289; ++SUPPLIER_it290)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it290);
                    string protect_S__NAME = get<1>(*SUPPLIER_it290);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it290);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it290);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it290);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it290);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it290);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it288 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end287 = ORDERS.end();
                    for (; ORDERS_it288 != ORDERS_end287; ++ORDERS_it288)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it288);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it288);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it288);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it288);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it288);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it288);
                        string protect_O__CLERK = get<6>(*ORDERS_it288);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it288);
                        string protect_O__COMMENT = get<8>(*ORDERS_it288);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it286 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end285 = LINEITEM.end();
                        for (
                            ; LINEITEM_it286 != LINEITEM_end285; ++LINEITEM_it286)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it286);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it286);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it286);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it286);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it286);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it286);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it286);
                            double protect_L__TAX = get<7>(*LINEITEM_it286);
                            int64_t var332 = 0;
                            var332 += protect_C__NATIONKEY;
                            int64_t var333 = 0;
                            var333 += protect_N__NATIONKEY;
                            if ( var332 == var333 )
                            {
                                int64_t var330 = 0;
                                var330 += protect_C__NATIONKEY;
                                int64_t var331 = 0;
                                var331 += protect_S__NATIONKEY;
                                if ( var330 == var331 )
                                {
                                    int64_t var328 = 0;
                                    var328 += protect_C__CUSTKEY;
                                    int64_t var329 = 0;
                                    var329 += protect_O__CUSTKEY;
                                    if ( var328 == var329 )
                                    {
                                        int64_t var326 = 0;
                                        var326 += protect_L__SUPPKEY;
                                        int64_t var327 = 0;
                                        var327 += protect_S__SUPPKEY;
                                        if ( var326 == var327 )
                                        {
                                            int64_t var324 = 0;
                                            var324 += protect_L__ORDERKEY;
                                            int64_t var325 = 0;
                                            var325 += protect_O__ORDERKEY;
                                            if ( var324 == var325 )
                                            {
                                                int64_t var322 = 0;
                                                var322 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var323 = 0;
                                                var323 += protect_N__REGIONKEY;
                                                if ( var322 == var323 )
                                                {
                                                    string var320 = 0;
                                                    var320 += N__NAME;
                                                    string var321 = 0;
                                                    var321 += protect_N__NAME;
                                                    if ( var320 == var321 )
                                                    {
                                                        double var319 = 1;
                                                        var319 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var319 *= 
                                                            protect_L__DISCOUNT;
                                                        qREGION2[make_tuple(
                                                            N__NAME,x_qREGION_R__REGIONKEY)] += var319;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it308 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end307 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it308 != qSUPPLIER1_end307; ++qSUPPLIER1_it308)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it308->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER1_it308->first);
        string N__NAME = get<2>(qSUPPLIER1_it308->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it306 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end305 = 
            REGION.end();
        for (; REGION_it306 != REGION_end305; ++REGION_it306)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it306);
            string protect_R__NAME = get<1>(*REGION_it306);
            string protect_R__COMMENT = get<2>(*REGION_it306);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it304 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end303 = NATION.end();
            for (; NATION_it304 != NATION_end303; ++NATION_it304)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it304);
                string protect_N__NAME = get<1>(*NATION_it304);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it304);
                string protect_N__COMMENT = get<3>(*NATION_it304);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it302 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end301 = CUSTOMER.end();
                for (; CUSTOMER_it302 != CUSTOMER_end301; ++CUSTOMER_it302)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it302);
                    string protect_C__NAME = get<1>(*CUSTOMER_it302);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it302);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it302);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it302);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it302);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it302);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it302);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it300 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end299 = ORDERS.end();
                    for (; ORDERS_it300 != ORDERS_end299; ++ORDERS_it300)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it300);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it300);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it300);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it300);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it300);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it300);
                        string protect_O__CLERK = get<6>(*ORDERS_it300);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it300);
                        string protect_O__COMMENT = get<8>(*ORDERS_it300);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it298 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end297 = LINEITEM.end();
                        for (
                            ; LINEITEM_it298 != LINEITEM_end297; ++LINEITEM_it298)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it298);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it298);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it298);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it298);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it298);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it298);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it298);
                            double protect_L__TAX = get<7>(*LINEITEM_it298);
                            int64_t var346 = 0;
                            var346 += protect_N__REGIONKEY;
                            int64_t var347 = 0;
                            var347 += protect_R__REGIONKEY;
                            if ( var346 == var347 )
                            {
                                int64_t var344 = 0;
                                var344 += protect_C__CUSTKEY;
                                int64_t var345 = 0;
                                var345 += protect_O__CUSTKEY;
                                if ( var344 == var345 )
                                {
                                    int64_t var342 = 0;
                                    var342 += protect_L__ORDERKEY;
                                    int64_t var343 = 0;
                                    var343 += protect_O__ORDERKEY;
                                    if ( var342 == var343 )
                                    {
                                        string var340 = 0;
                                        var340 += N__NAME;
                                        string var341 = 0;
                                        var341 += protect_N__NAME;
                                        if ( var340 == var341 )
                                        {
                                            int64_t var338 = 0;
                                            var338 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var339 = 0;
                                            var339 += protect_N__NATIONKEY;
                                            if ( var338 == var339 )
                                            {
                                                int64_t var336 = 0;
                                                var336 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var337 = 0;
                                                var337 += protect_C__NATIONKEY;
                                                if ( var336 == var337 )
                                                {
                                                    int64_t var334 = 0;
                                                    var334 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var335 = 0;
                                                    var335 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var334 == var335 )
                                                    {
                                                        qSUPPLIER1[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it320 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end319 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it320 != qSUPPLIER2_end319; ++qSUPPLIER2_it320)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it320->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER2_it320->first);
        string N__NAME = get<2>(qSUPPLIER2_it320->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it318 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end317 = 
            REGION.end();
        for (; REGION_it318 != REGION_end317; ++REGION_it318)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it318);
            string protect_R__NAME = get<1>(*REGION_it318);
            string protect_R__COMMENT = get<2>(*REGION_it318);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it316 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end315 = NATION.end();
            for (; NATION_it316 != NATION_end315; ++NATION_it316)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it316);
                string protect_N__NAME = get<1>(*NATION_it316);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it316);
                string protect_N__COMMENT = get<3>(*NATION_it316);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it314 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end313 = CUSTOMER.end();
                for (; CUSTOMER_it314 != CUSTOMER_end313; ++CUSTOMER_it314)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it314);
                    string protect_C__NAME = get<1>(*CUSTOMER_it314);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it314);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it314);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it314);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it314);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it314);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it314);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it312 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end311 = ORDERS.end();
                    for (; ORDERS_it312 != ORDERS_end311; ++ORDERS_it312)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it312);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it312);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it312);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it312);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it312);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it312);
                        string protect_O__CLERK = get<6>(*ORDERS_it312);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it312);
                        string protect_O__COMMENT = get<8>(*ORDERS_it312);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it310 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end309 = LINEITEM.end();
                        for (
                            ; LINEITEM_it310 != LINEITEM_end309; ++LINEITEM_it310)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it310);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it310);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it310);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it310);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it310);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it310);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it310);
                            double protect_L__TAX = get<7>(*LINEITEM_it310);
                            int64_t var361 = 0;
                            var361 += protect_N__REGIONKEY;
                            int64_t var362 = 0;
                            var362 += protect_R__REGIONKEY;
                            if ( var361 == var362 )
                            {
                                int64_t var359 = 0;
                                var359 += protect_C__CUSTKEY;
                                int64_t var360 = 0;
                                var360 += protect_O__CUSTKEY;
                                if ( var359 == var360 )
                                {
                                    int64_t var357 = 0;
                                    var357 += protect_L__ORDERKEY;
                                    int64_t var358 = 0;
                                    var358 += protect_O__ORDERKEY;
                                    if ( var357 == var358 )
                                    {
                                        string var355 = 0;
                                        var355 += N__NAME;
                                        string var356 = 0;
                                        var356 += protect_N__NAME;
                                        if ( var355 == var356 )
                                        {
                                            int64_t var353 = 0;
                                            var353 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var354 = 0;
                                            var354 += protect_N__NATIONKEY;
                                            if ( var353 == var354 )
                                            {
                                                int64_t var351 = 0;
                                                var351 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var352 = 0;
                                                var352 += protect_C__NATIONKEY;
                                                if ( var351 == var352 )
                                                {
                                                    int64_t var349 = 0;
                                                    var349 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var350 = 0;
                                                    var350 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var349 == var350 )
                                                    {
                                                        double var348 = 1;
                                                        var348 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var348 *= 
                                                            protect_L__DISCOUNT;
                                                        qSUPPLIER2[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += var348;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_ORDERS_sec_span, on_insert_ORDERS_usec_span);
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
    map<string,double>::iterator q_it322 = q.begin();
    map<string,double>::iterator q_end321 = q.end();
    for (; q_it322 != q_end321; ++q_it322)
    {
        string N__NAME = q_it322->first;
        q[N__NAME] += qSUPPLIER1[make_tuple(SUPPKEY,NATIONKEY,N__NAME)];
    }
    map<string,double>::iterator q_it324 = q.begin();
    map<string,double>::iterator q_end323 = q.end();
    for (; q_it324 != q_end323; ++q_it324)
    {
        string N__NAME = q_it324->first;
        q[N__NAME] += qSUPPLIER2[make_tuple(SUPPKEY,NATIONKEY,N__NAME)]*-1;
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it336 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end335 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it336 != qCUSTOMER1_end335; ++qCUSTOMER1_it336)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it336->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER1_it336->first);
        string N__NAME = get<2>(qCUSTOMER1_it336->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it334 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end333 = 
            REGION.end();
        for (; REGION_it334 != REGION_end333; ++REGION_it334)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it334);
            string protect_R__NAME = get<1>(*REGION_it334);
            string protect_R__COMMENT = get<2>(*REGION_it334);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it332 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end331 = NATION.end();
            for (; NATION_it332 != NATION_end331; ++NATION_it332)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it332);
                string protect_N__NAME = get<1>(*NATION_it332);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it332);
                string protect_N__COMMENT = get<3>(*NATION_it332);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it330 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end329 = SUPPLIER.end();
                for (; SUPPLIER_it330 != SUPPLIER_end329; ++SUPPLIER_it330)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it330);
                    string protect_S__NAME = get<1>(*SUPPLIER_it330);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it330);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it330);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it330);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it330);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it330);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it328 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end327 = ORDERS.end();
                    for (; ORDERS_it328 != ORDERS_end327; ++ORDERS_it328)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it328);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it328);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it328);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it328);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it328);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it328);
                        string protect_O__CLERK = get<6>(*ORDERS_it328);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it328);
                        string protect_O__COMMENT = get<8>(*ORDERS_it328);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it326 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end325 = LINEITEM.end();
                        for (
                            ; LINEITEM_it326 != LINEITEM_end325; ++LINEITEM_it326)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it326);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it326);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it326);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it326);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it326);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it326);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it326);
                            double protect_L__TAX = get<7>(*LINEITEM_it326);
                            int64_t var375 = 0;
                            var375 += protect_N__REGIONKEY;
                            int64_t var376 = 0;
                            var376 += protect_R__REGIONKEY;
                            if ( var375 == var376 )
                            {
                                int64_t var373 = 0;
                                var373 += protect_L__SUPPKEY;
                                int64_t var374 = 0;
                                var374 += protect_S__SUPPKEY;
                                if ( var373 == var374 )
                                {
                                    int64_t var371 = 0;
                                    var371 += protect_L__ORDERKEY;
                                    int64_t var372 = 0;
                                    var372 += protect_O__ORDERKEY;
                                    if ( var371 == var372 )
                                    {
                                        string var369 = 0;
                                        var369 += N__NAME;
                                        string var370 = 0;
                                        var370 += protect_N__NAME;
                                        if ( var369 == var370 )
                                        {
                                            int64_t var367 = 0;
                                            var367 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var368 = 0;
                                            var368 += protect_N__NATIONKEY;
                                            if ( var367 == var368 )
                                            {
                                                int64_t var365 = 0;
                                                var365 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var366 = 0;
                                                var366 += protect_S__NATIONKEY;
                                                if ( var365 == var366 )
                                                {
                                                    int64_t var363 = 0;
                                                    var363 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var364 = 0;
                                                    var364 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var363 == var364 )
                                                    {
                                                        qCUSTOMER1[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it348 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end347 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it348 != qCUSTOMER2_end347; ++qCUSTOMER2_it348)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it348->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER2_it348->first);
        string N__NAME = get<2>(qCUSTOMER2_it348->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it346 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end345 = 
            REGION.end();
        for (; REGION_it346 != REGION_end345; ++REGION_it346)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it346);
            string protect_R__NAME = get<1>(*REGION_it346);
            string protect_R__COMMENT = get<2>(*REGION_it346);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it344 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end343 = NATION.end();
            for (; NATION_it344 != NATION_end343; ++NATION_it344)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it344);
                string protect_N__NAME = get<1>(*NATION_it344);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it344);
                string protect_N__COMMENT = get<3>(*NATION_it344);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it342 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end341 = SUPPLIER.end();
                for (; SUPPLIER_it342 != SUPPLIER_end341; ++SUPPLIER_it342)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it342);
                    string protect_S__NAME = get<1>(*SUPPLIER_it342);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it342);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it342);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it342);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it342);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it342);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it340 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end339 = ORDERS.end();
                    for (; ORDERS_it340 != ORDERS_end339; ++ORDERS_it340)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it340);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it340);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it340);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it340);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it340);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it340);
                        string protect_O__CLERK = get<6>(*ORDERS_it340);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it340);
                        string protect_O__COMMENT = get<8>(*ORDERS_it340);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it338 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end337 = LINEITEM.end();
                        for (
                            ; LINEITEM_it338 != LINEITEM_end337; ++LINEITEM_it338)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it338);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it338);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it338);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it338);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it338);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it338);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it338);
                            double protect_L__TAX = get<7>(*LINEITEM_it338);
                            int64_t var390 = 0;
                            var390 += protect_N__REGIONKEY;
                            int64_t var391 = 0;
                            var391 += protect_R__REGIONKEY;
                            if ( var390 == var391 )
                            {
                                int64_t var388 = 0;
                                var388 += protect_L__SUPPKEY;
                                int64_t var389 = 0;
                                var389 += protect_S__SUPPKEY;
                                if ( var388 == var389 )
                                {
                                    int64_t var386 = 0;
                                    var386 += protect_L__ORDERKEY;
                                    int64_t var387 = 0;
                                    var387 += protect_O__ORDERKEY;
                                    if ( var386 == var387 )
                                    {
                                        string var384 = 0;
                                        var384 += N__NAME;
                                        string var385 = 0;
                                        var385 += protect_N__NAME;
                                        if ( var384 == var385 )
                                        {
                                            int64_t var382 = 0;
                                            var382 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var383 = 0;
                                            var383 += protect_N__NATIONKEY;
                                            if ( var382 == var383 )
                                            {
                                                int64_t var380 = 0;
                                                var380 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var381 = 0;
                                                var381 += protect_S__NATIONKEY;
                                                if ( var380 == var381 )
                                                {
                                                    int64_t var378 = 0;
                                                    var378 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var379 = 0;
                                                    var379 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var378 == var379 )
                                                    {
                                                        double var377 = 1;
                                                        var377 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var377 *= 
                                                            protect_L__DISCOUNT;
                                                        qCUSTOMER2[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += var377;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it360 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end359 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it360 != qLINEITEM1_end359; ++qLINEITEM1_it360)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it360->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it360->first);
        string N__NAME = get<2>(qLINEITEM1_it360->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it358 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end357 = 
            REGION.end();
        for (; REGION_it358 != REGION_end357; ++REGION_it358)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it358);
            string protect_R__NAME = get<1>(*REGION_it358);
            string protect_R__COMMENT = get<2>(*REGION_it358);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it356 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end355 = NATION.end();
            for (; NATION_it356 != NATION_end355; ++NATION_it356)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it356);
                string protect_N__NAME = get<1>(*NATION_it356);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it356);
                string protect_N__COMMENT = get<3>(*NATION_it356);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it354 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end353 = SUPPLIER.end();
                for (; SUPPLIER_it354 != SUPPLIER_end353; ++SUPPLIER_it354)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it354);
                    string protect_S__NAME = get<1>(*SUPPLIER_it354);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it354);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it354);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it354);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it354);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it354);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_it352 = CUSTOMER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_end351 = CUSTOMER.end();
                    for (; CUSTOMER_it352 != CUSTOMER_end351; ++CUSTOMER_it352)
                    {
                        int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it352);
                        string protect_C__NAME = get<1>(*CUSTOMER_it352);
                        string protect_C__ADDRESS = get<2>(*CUSTOMER_it352);
                        int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it352);
                        string protect_C__PHONE = get<4>(*CUSTOMER_it352);
                        double protect_C__ACCTBAL = get<5>(*CUSTOMER_it352);
                        string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it352);
                        string protect_C__COMMENT = get<7>(*CUSTOMER_it352);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it350 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end349 = ORDERS.end();
                        for (; ORDERS_it350 != ORDERS_end349; ++ORDERS_it350)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it350);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it350);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it350);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it350);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it350);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it350);
                            string protect_O__CLERK = get<6>(*ORDERS_it350);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it350);
                            string protect_O__COMMENT = get<8>(*ORDERS_it350);
                            int64_t var404 = 0;
                            var404 += protect_N__REGIONKEY;
                            int64_t var405 = 0;
                            var405 += protect_R__REGIONKEY;
                            if ( var404 == var405 )
                            {
                                int64_t var402 = 0;
                                var402 += protect_C__NATIONKEY;
                                int64_t var403 = 0;
                                var403 += protect_N__NATIONKEY;
                                if ( var402 == var403 )
                                {
                                    int64_t var400 = 0;
                                    var400 += protect_C__NATIONKEY;
                                    int64_t var401 = 0;
                                    var401 += protect_S__NATIONKEY;
                                    if ( var400 == var401 )
                                    {
                                        int64_t var398 = 0;
                                        var398 += protect_C__CUSTKEY;
                                        int64_t var399 = 0;
                                        var399 += protect_O__CUSTKEY;
                                        if ( var398 == var399 )
                                        {
                                            string var396 = 0;
                                            var396 += N__NAME;
                                            string var397 = 0;
                                            var397 += protect_N__NAME;
                                            if ( var396 == var397 )
                                            {
                                                int64_t var394 = 0;
                                                var394 += 
                                                    x_qLINEITEM_L__SUPPKEY;
                                                int64_t var395 = 0;
                                                var395 += protect_S__SUPPKEY;
                                                if ( var394 == var395 )
                                                {
                                                    int64_t var392 = 0;
                                                    var392 += 
                                                        x_qLINEITEM_L__ORDERKEY;
                                                    int64_t var393 = 0;
                                                    var393 += 
                                                        protect_O__ORDERKEY;
                                                    if ( var392 == var393 )
                                                    {
                                                        qLINEITEM1[make_tuple(
                                                            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += 1;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qNATION1_it370 = qNATION1.begin();
    map<int64_t,double>::iterator qNATION1_end369 = qNATION1.end();
    for (; qNATION1_it370 != qNATION1_end369; ++qNATION1_it370)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION1_it370->first;
        qNATION1[x_qNATION_N__NATIONKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it368 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end367 = CUSTOMER.end();
        for (; CUSTOMER_it368 != CUSTOMER_end367; ++CUSTOMER_it368)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it368);
            string protect_C__NAME = get<1>(*CUSTOMER_it368);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it368);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it368);
            string protect_C__PHONE = get<4>(*CUSTOMER_it368);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it368);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it368);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it368);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it366 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end365 = SUPPLIER.end();
            for (; SUPPLIER_it366 != SUPPLIER_end365; ++SUPPLIER_it366)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it366);
                string protect_S__NAME = get<1>(*SUPPLIER_it366);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it366);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it366);
                string protect_S__PHONE = get<4>(*SUPPLIER_it366);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it366);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it366);
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it364 = ORDERS.begin();
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end363 = ORDERS.end();
                for (; ORDERS_it364 != ORDERS_end363; ++ORDERS_it364)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it364);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it364);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it364);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it364);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it364);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it364);
                    string protect_O__CLERK = get<6>(*ORDERS_it364);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it364);
                    string protect_O__COMMENT = get<8>(*ORDERS_it364);
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it362 = LINEITEM.begin();
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end361 = LINEITEM.end();
                    for (; LINEITEM_it362 != LINEITEM_end361; ++LINEITEM_it362)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it362);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it362);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it362);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it362);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it362);
                        double protect_L__EXTENDEDPRICE = get<5>(
                            *LINEITEM_it362);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it362);
                        double protect_L__TAX = get<7>(*LINEITEM_it362);
                        int64_t var414 = 0;
                        var414 += protect_C__CUSTKEY;
                        int64_t var415 = 0;
                        var415 += protect_O__CUSTKEY;
                        if ( var414 == var415 )
                        {
                            int64_t var412 = 0;
                            var412 += protect_L__SUPPKEY;
                            int64_t var413 = 0;
                            var413 += protect_S__SUPPKEY;
                            if ( var412 == var413 )
                            {
                                int64_t var410 = 0;
                                var410 += protect_L__ORDERKEY;
                                int64_t var411 = 0;
                                var411 += protect_O__ORDERKEY;
                                if ( var410 == var411 )
                                {
                                    int64_t var408 = 0;
                                    var408 += x_qNATION_N__NATIONKEY;
                                    int64_t var409 = 0;
                                    var409 += protect_C__NATIONKEY;
                                    if ( var408 == var409 )
                                    {
                                        int64_t var406 = 0;
                                        var406 += x_qNATION_N__NATIONKEY;
                                        int64_t var407 = 0;
                                        var407 += protect_S__NATIONKEY;
                                        if ( var406 == var407 )
                                        {
                                            qNATION1[x_qNATION_N__NATIONKEY] += 
                                                protect_L__EXTENDEDPRICE;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qNATION3_it380 = qNATION3.begin();
    map<int64_t,double>::iterator qNATION3_end379 = qNATION3.end();
    for (; qNATION3_it380 != qNATION3_end379; ++qNATION3_it380)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION3_it380->first;
        qNATION3[x_qNATION_N__NATIONKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it378 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end377 = CUSTOMER.end();
        for (; CUSTOMER_it378 != CUSTOMER_end377; ++CUSTOMER_it378)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it378);
            string protect_C__NAME = get<1>(*CUSTOMER_it378);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it378);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it378);
            string protect_C__PHONE = get<4>(*CUSTOMER_it378);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it378);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it378);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it378);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it376 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end375 = SUPPLIER.end();
            for (; SUPPLIER_it376 != SUPPLIER_end375; ++SUPPLIER_it376)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it376);
                string protect_S__NAME = get<1>(*SUPPLIER_it376);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it376);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it376);
                string protect_S__PHONE = get<4>(*SUPPLIER_it376);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it376);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it376);
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it374 = ORDERS.begin();
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end373 = ORDERS.end();
                for (; ORDERS_it374 != ORDERS_end373; ++ORDERS_it374)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it374);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it374);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it374);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it374);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it374);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it374);
                    string protect_O__CLERK = get<6>(*ORDERS_it374);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it374);
                    string protect_O__COMMENT = get<8>(*ORDERS_it374);
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it372 = LINEITEM.begin();
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end371 = LINEITEM.end();
                    for (; LINEITEM_it372 != LINEITEM_end371; ++LINEITEM_it372)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it372);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it372);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it372);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it372);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it372);
                        double protect_L__EXTENDEDPRICE = get<5>(
                            *LINEITEM_it372);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it372);
                        double protect_L__TAX = get<7>(*LINEITEM_it372);
                        int64_t var425 = 0;
                        var425 += protect_C__CUSTKEY;
                        int64_t var426 = 0;
                        var426 += protect_O__CUSTKEY;
                        if ( var425 == var426 )
                        {
                            int64_t var423 = 0;
                            var423 += protect_L__SUPPKEY;
                            int64_t var424 = 0;
                            var424 += protect_S__SUPPKEY;
                            if ( var423 == var424 )
                            {
                                int64_t var421 = 0;
                                var421 += protect_L__ORDERKEY;
                                int64_t var422 = 0;
                                var422 += protect_O__ORDERKEY;
                                if ( var421 == var422 )
                                {
                                    int64_t var419 = 0;
                                    var419 += x_qNATION_N__NATIONKEY;
                                    int64_t var420 = 0;
                                    var420 += protect_C__NATIONKEY;
                                    if ( var419 == var420 )
                                    {
                                        int64_t var417 = 0;
                                        var417 += x_qNATION_N__NATIONKEY;
                                        int64_t var418 = 0;
                                        var418 += protect_S__NATIONKEY;
                                        if ( var417 == var418 )
                                        {
                                            double var416 = 1;
                                            var416 *= protect_L__EXTENDEDPRICE;
                                            var416 *= protect_L__DISCOUNT;
                                            qNATION3[x_qNATION_N__NATIONKEY] += 
                                                var416;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it392 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end391 = 
        qORDERS1.end();
    for (; qORDERS1_it392 != qORDERS1_end391; ++qORDERS1_it392)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it392->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it392->first);
        string N__NAME = get<2>(qORDERS1_it392->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it390 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end389 = 
            REGION.end();
        for (; REGION_it390 != REGION_end389; ++REGION_it390)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it390);
            string protect_R__NAME = get<1>(*REGION_it390);
            string protect_R__COMMENT = get<2>(*REGION_it390);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it388 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end387 = NATION.end();
            for (; NATION_it388 != NATION_end387; ++NATION_it388)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it388);
                string protect_N__NAME = get<1>(*NATION_it388);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it388);
                string protect_N__COMMENT = get<3>(*NATION_it388);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it386 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end385 = CUSTOMER.end();
                for (; CUSTOMER_it386 != CUSTOMER_end385; ++CUSTOMER_it386)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it386);
                    string protect_C__NAME = get<1>(*CUSTOMER_it386);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it386);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it386);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it386);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it386);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it386);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it386);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it384 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end383 = SUPPLIER.end();
                    for (; SUPPLIER_it384 != SUPPLIER_end383; ++SUPPLIER_it384)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it384);
                        string protect_S__NAME = get<1>(*SUPPLIER_it384);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it384);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it384);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it384);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it384);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it384);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it382 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end381 = LINEITEM.end();
                        for (
                            ; LINEITEM_it382 != LINEITEM_end381; ++LINEITEM_it382)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it382);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it382);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it382);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it382);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it382);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it382);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it382);
                            double protect_L__TAX = get<7>(*LINEITEM_it382);
                            int64_t var439 = 0;
                            var439 += protect_N__REGIONKEY;
                            int64_t var440 = 0;
                            var440 += protect_R__REGIONKEY;
                            if ( var439 == var440 )
                            {
                                int64_t var437 = 0;
                                var437 += protect_C__NATIONKEY;
                                int64_t var438 = 0;
                                var438 += protect_N__NATIONKEY;
                                if ( var437 == var438 )
                                {
                                    int64_t var435 = 0;
                                    var435 += protect_C__NATIONKEY;
                                    int64_t var436 = 0;
                                    var436 += protect_S__NATIONKEY;
                                    if ( var435 == var436 )
                                    {
                                        int64_t var433 = 0;
                                        var433 += protect_L__SUPPKEY;
                                        int64_t var434 = 0;
                                        var434 += protect_S__SUPPKEY;
                                        if ( var433 == var434 )
                                        {
                                            string var431 = 0;
                                            var431 += N__NAME;
                                            string var432 = 0;
                                            var432 += protect_N__NAME;
                                            if ( var431 == var432 )
                                            {
                                                int64_t var429 = 0;
                                                var429 += x_qORDERS_O__CUSTKEY;
                                                int64_t var430 = 0;
                                                var430 += protect_C__CUSTKEY;
                                                if ( var429 == var430 )
                                                {
                                                    int64_t var427 = 0;
                                                    var427 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var428 = 0;
                                                    var428 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var427 == var428 )
                                                    {
                                                        qORDERS1[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it404 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end403 = 
        qORDERS2.end();
    for (; qORDERS2_it404 != qORDERS2_end403; ++qORDERS2_it404)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it404->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it404->first);
        string N__NAME = get<2>(qORDERS2_it404->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it402 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end401 = 
            REGION.end();
        for (; REGION_it402 != REGION_end401; ++REGION_it402)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it402);
            string protect_R__NAME = get<1>(*REGION_it402);
            string protect_R__COMMENT = get<2>(*REGION_it402);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it400 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end399 = NATION.end();
            for (; NATION_it400 != NATION_end399; ++NATION_it400)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it400);
                string protect_N__NAME = get<1>(*NATION_it400);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it400);
                string protect_N__COMMENT = get<3>(*NATION_it400);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it398 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end397 = CUSTOMER.end();
                for (; CUSTOMER_it398 != CUSTOMER_end397; ++CUSTOMER_it398)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it398);
                    string protect_C__NAME = get<1>(*CUSTOMER_it398);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it398);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it398);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it398);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it398);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it398);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it398);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it396 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end395 = SUPPLIER.end();
                    for (; SUPPLIER_it396 != SUPPLIER_end395; ++SUPPLIER_it396)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it396);
                        string protect_S__NAME = get<1>(*SUPPLIER_it396);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it396);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it396);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it396);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it396);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it396);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it394 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end393 = LINEITEM.end();
                        for (
                            ; LINEITEM_it394 != LINEITEM_end393; ++LINEITEM_it394)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it394);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it394);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it394);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it394);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it394);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it394);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it394);
                            double protect_L__TAX = get<7>(*LINEITEM_it394);
                            int64_t var454 = 0;
                            var454 += protect_N__REGIONKEY;
                            int64_t var455 = 0;
                            var455 += protect_R__REGIONKEY;
                            if ( var454 == var455 )
                            {
                                int64_t var452 = 0;
                                var452 += protect_C__NATIONKEY;
                                int64_t var453 = 0;
                                var453 += protect_N__NATIONKEY;
                                if ( var452 == var453 )
                                {
                                    int64_t var450 = 0;
                                    var450 += protect_C__NATIONKEY;
                                    int64_t var451 = 0;
                                    var451 += protect_S__NATIONKEY;
                                    if ( var450 == var451 )
                                    {
                                        int64_t var448 = 0;
                                        var448 += protect_L__SUPPKEY;
                                        int64_t var449 = 0;
                                        var449 += protect_S__SUPPKEY;
                                        if ( var448 == var449 )
                                        {
                                            string var446 = 0;
                                            var446 += N__NAME;
                                            string var447 = 0;
                                            var447 += protect_N__NAME;
                                            if ( var446 == var447 )
                                            {
                                                int64_t var444 = 0;
                                                var444 += x_qORDERS_O__CUSTKEY;
                                                int64_t var445 = 0;
                                                var445 += protect_C__CUSTKEY;
                                                if ( var444 == var445 )
                                                {
                                                    int64_t var442 = 0;
                                                    var442 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var443 = 0;
                                                    var443 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var442 == var443 )
                                                    {
                                                        double var441 = 1;
                                                        var441 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var441 *= 
                                                            protect_L__DISCOUNT;
                                                        qORDERS2[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += var441;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    map<tuple<string,int64_t>,double>::iterator qREGION1_it416 = qREGION1.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION1_end415 = qREGION1.end(
        );
    for (; qREGION1_it416 != qREGION1_end415; ++qREGION1_it416)
    {
        string N__NAME = get<0>(qREGION1_it416->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it416->first);
        qREGION1[make_tuple(N__NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_it414 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end413 
            = NATION.end();
        for (; NATION_it414 != NATION_end413; ++NATION_it414)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it414);
            string protect_N__NAME = get<1>(*NATION_it414);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it414);
            string protect_N__COMMENT = get<3>(*NATION_it414);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it412 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end411 = CUSTOMER.end();
            for (; CUSTOMER_it412 != CUSTOMER_end411; ++CUSTOMER_it412)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it412);
                string protect_C__NAME = get<1>(*CUSTOMER_it412);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it412);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it412);
                string protect_C__PHONE = get<4>(*CUSTOMER_it412);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it412);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it412);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it412);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it410 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end409 = SUPPLIER.end();
                for (; SUPPLIER_it410 != SUPPLIER_end409; ++SUPPLIER_it410)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it410);
                    string protect_S__NAME = get<1>(*SUPPLIER_it410);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it410);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it410);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it410);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it410);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it410);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it408 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end407 = ORDERS.end();
                    for (; ORDERS_it408 != ORDERS_end407; ++ORDERS_it408)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it408);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it408);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it408);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it408);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it408);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it408);
                        string protect_O__CLERK = get<6>(*ORDERS_it408);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it408);
                        string protect_O__COMMENT = get<8>(*ORDERS_it408);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it406 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end405 = LINEITEM.end();
                        for (
                            ; LINEITEM_it406 != LINEITEM_end405; ++LINEITEM_it406)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it406);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it406);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it406);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it406);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it406);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it406);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it406);
                            double protect_L__TAX = get<7>(*LINEITEM_it406);
                            int64_t var468 = 0;
                            var468 += protect_C__NATIONKEY;
                            int64_t var469 = 0;
                            var469 += protect_N__NATIONKEY;
                            if ( var468 == var469 )
                            {
                                int64_t var466 = 0;
                                var466 += protect_C__NATIONKEY;
                                int64_t var467 = 0;
                                var467 += protect_S__NATIONKEY;
                                if ( var466 == var467 )
                                {
                                    int64_t var464 = 0;
                                    var464 += protect_C__CUSTKEY;
                                    int64_t var465 = 0;
                                    var465 += protect_O__CUSTKEY;
                                    if ( var464 == var465 )
                                    {
                                        int64_t var462 = 0;
                                        var462 += protect_L__SUPPKEY;
                                        int64_t var463 = 0;
                                        var463 += protect_S__SUPPKEY;
                                        if ( var462 == var463 )
                                        {
                                            int64_t var460 = 0;
                                            var460 += protect_L__ORDERKEY;
                                            int64_t var461 = 0;
                                            var461 += protect_O__ORDERKEY;
                                            if ( var460 == var461 )
                                            {
                                                int64_t var458 = 0;
                                                var458 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var459 = 0;
                                                var459 += protect_N__REGIONKEY;
                                                if ( var458 == var459 )
                                                {
                                                    string var456 = 0;
                                                    var456 += N__NAME;
                                                    string var457 = 0;
                                                    var457 += protect_N__NAME;
                                                    if ( var456 == var457 )
                                                    {
                                                        qREGION1[make_tuple(
                                                            N__NAME,x_qREGION_R__REGIONKEY)] += protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it428 = qREGION2.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION2_end427 = qREGION2.end(
        );
    for (; qREGION2_it428 != qREGION2_end427; ++qREGION2_it428)
    {
        string N__NAME = get<0>(qREGION2_it428->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it428->first);
        qREGION2[make_tuple(N__NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_it426 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end425 
            = NATION.end();
        for (; NATION_it426 != NATION_end425; ++NATION_it426)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it426);
            string protect_N__NAME = get<1>(*NATION_it426);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it426);
            string protect_N__COMMENT = get<3>(*NATION_it426);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it424 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end423 = CUSTOMER.end();
            for (; CUSTOMER_it424 != CUSTOMER_end423; ++CUSTOMER_it424)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it424);
                string protect_C__NAME = get<1>(*CUSTOMER_it424);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it424);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it424);
                string protect_C__PHONE = get<4>(*CUSTOMER_it424);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it424);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it424);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it424);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it422 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end421 = SUPPLIER.end();
                for (; SUPPLIER_it422 != SUPPLIER_end421; ++SUPPLIER_it422)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it422);
                    string protect_S__NAME = get<1>(*SUPPLIER_it422);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it422);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it422);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it422);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it422);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it422);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it420 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end419 = ORDERS.end();
                    for (; ORDERS_it420 != ORDERS_end419; ++ORDERS_it420)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it420);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it420);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it420);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it420);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it420);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it420);
                        string protect_O__CLERK = get<6>(*ORDERS_it420);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it420);
                        string protect_O__COMMENT = get<8>(*ORDERS_it420);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it418 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end417 = LINEITEM.end();
                        for (
                            ; LINEITEM_it418 != LINEITEM_end417; ++LINEITEM_it418)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it418);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it418);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it418);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it418);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it418);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it418);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it418);
                            double protect_L__TAX = get<7>(*LINEITEM_it418);
                            int64_t var483 = 0;
                            var483 += protect_C__NATIONKEY;
                            int64_t var484 = 0;
                            var484 += protect_N__NATIONKEY;
                            if ( var483 == var484 )
                            {
                                int64_t var481 = 0;
                                var481 += protect_C__NATIONKEY;
                                int64_t var482 = 0;
                                var482 += protect_S__NATIONKEY;
                                if ( var481 == var482 )
                                {
                                    int64_t var479 = 0;
                                    var479 += protect_C__CUSTKEY;
                                    int64_t var480 = 0;
                                    var480 += protect_O__CUSTKEY;
                                    if ( var479 == var480 )
                                    {
                                        int64_t var477 = 0;
                                        var477 += protect_L__SUPPKEY;
                                        int64_t var478 = 0;
                                        var478 += protect_S__SUPPKEY;
                                        if ( var477 == var478 )
                                        {
                                            int64_t var475 = 0;
                                            var475 += protect_L__ORDERKEY;
                                            int64_t var476 = 0;
                                            var476 += protect_O__ORDERKEY;
                                            if ( var475 == var476 )
                                            {
                                                int64_t var473 = 0;
                                                var473 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var474 = 0;
                                                var474 += protect_N__REGIONKEY;
                                                if ( var473 == var474 )
                                                {
                                                    string var471 = 0;
                                                    var471 += N__NAME;
                                                    string var472 = 0;
                                                    var472 += protect_N__NAME;
                                                    if ( var471 == var472 )
                                                    {
                                                        double var470 = 1;
                                                        var470 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var470 *= 
                                                            protect_L__DISCOUNT;
                                                        qREGION2[make_tuple(
                                                            N__NAME,x_qREGION_R__REGIONKEY)] += var470;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_SUPPLIER_sec_span, on_insert_SUPPLIER_usec_span);
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
    map<string,double>::iterator q_it430 = q.begin();
    map<string,double>::iterator q_end429 = q.end();
    for (; q_it430 != q_end429; ++q_it430)
    {
        string N__NAME = q_it430->first;
        q[N__NAME] += qCUSTOMER1[make_tuple(CUSTKEY,NATIONKEY,N__NAME)];
    }
    map<string,double>::iterator q_it432 = q.begin();
    map<string,double>::iterator q_end431 = q.end();
    for (; q_it432 != q_end431; ++q_it432)
    {
        string N__NAME = q_it432->first;
        q[N__NAME] += qCUSTOMER2[make_tuple(CUSTKEY,NATIONKEY,N__NAME)]*-1;
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it444 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end443 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it444 != qLINEITEM1_end443; ++qLINEITEM1_it444)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it444->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it444->first);
        string N__NAME = get<2>(qLINEITEM1_it444->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it442 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end441 = 
            REGION.end();
        for (; REGION_it442 != REGION_end441; ++REGION_it442)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it442);
            string protect_R__NAME = get<1>(*REGION_it442);
            string protect_R__COMMENT = get<2>(*REGION_it442);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it440 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end439 = NATION.end();
            for (; NATION_it440 != NATION_end439; ++NATION_it440)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it440);
                string protect_N__NAME = get<1>(*NATION_it440);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it440);
                string protect_N__COMMENT = get<3>(*NATION_it440);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it438 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end437 = SUPPLIER.end();
                for (; SUPPLIER_it438 != SUPPLIER_end437; ++SUPPLIER_it438)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it438);
                    string protect_S__NAME = get<1>(*SUPPLIER_it438);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it438);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it438);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it438);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it438);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it438);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_it436 = CUSTOMER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_end435 = CUSTOMER.end();
                    for (; CUSTOMER_it436 != CUSTOMER_end435; ++CUSTOMER_it436)
                    {
                        int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it436);
                        string protect_C__NAME = get<1>(*CUSTOMER_it436);
                        string protect_C__ADDRESS = get<2>(*CUSTOMER_it436);
                        int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it436);
                        string protect_C__PHONE = get<4>(*CUSTOMER_it436);
                        double protect_C__ACCTBAL = get<5>(*CUSTOMER_it436);
                        string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it436);
                        string protect_C__COMMENT = get<7>(*CUSTOMER_it436);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it434 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end433 = ORDERS.end();
                        for (; ORDERS_it434 != ORDERS_end433; ++ORDERS_it434)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it434);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it434);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it434);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it434);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it434);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it434);
                            string protect_O__CLERK = get<6>(*ORDERS_it434);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it434);
                            string protect_O__COMMENT = get<8>(*ORDERS_it434);
                            int64_t var497 = 0;
                            var497 += protect_N__REGIONKEY;
                            int64_t var498 = 0;
                            var498 += protect_R__REGIONKEY;
                            if ( var497 == var498 )
                            {
                                int64_t var495 = 0;
                                var495 += protect_C__NATIONKEY;
                                int64_t var496 = 0;
                                var496 += protect_N__NATIONKEY;
                                if ( var495 == var496 )
                                {
                                    int64_t var493 = 0;
                                    var493 += protect_C__NATIONKEY;
                                    int64_t var494 = 0;
                                    var494 += protect_S__NATIONKEY;
                                    if ( var493 == var494 )
                                    {
                                        int64_t var491 = 0;
                                        var491 += protect_C__CUSTKEY;
                                        int64_t var492 = 0;
                                        var492 += protect_O__CUSTKEY;
                                        if ( var491 == var492 )
                                        {
                                            string var489 = 0;
                                            var489 += N__NAME;
                                            string var490 = 0;
                                            var490 += protect_N__NAME;
                                            if ( var489 == var490 )
                                            {
                                                int64_t var487 = 0;
                                                var487 += 
                                                    x_qLINEITEM_L__SUPPKEY;
                                                int64_t var488 = 0;
                                                var488 += protect_S__SUPPKEY;
                                                if ( var487 == var488 )
                                                {
                                                    int64_t var485 = 0;
                                                    var485 += 
                                                        x_qLINEITEM_L__ORDERKEY;
                                                    int64_t var486 = 0;
                                                    var486 += 
                                                        protect_O__ORDERKEY;
                                                    if ( var485 == var486 )
                                                    {
                                                        qLINEITEM1[make_tuple(
                                                            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += 1;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qNATION1_it454 = qNATION1.begin();
    map<int64_t,double>::iterator qNATION1_end453 = qNATION1.end();
    for (; qNATION1_it454 != qNATION1_end453; ++qNATION1_it454)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION1_it454->first;
        qNATION1[x_qNATION_N__NATIONKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it452 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end451 = CUSTOMER.end();
        for (; CUSTOMER_it452 != CUSTOMER_end451; ++CUSTOMER_it452)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it452);
            string protect_C__NAME = get<1>(*CUSTOMER_it452);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it452);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it452);
            string protect_C__PHONE = get<4>(*CUSTOMER_it452);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it452);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it452);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it452);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it450 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end449 = SUPPLIER.end();
            for (; SUPPLIER_it450 != SUPPLIER_end449; ++SUPPLIER_it450)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it450);
                string protect_S__NAME = get<1>(*SUPPLIER_it450);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it450);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it450);
                string protect_S__PHONE = get<4>(*SUPPLIER_it450);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it450);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it450);
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it448 = ORDERS.begin();
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end447 = ORDERS.end();
                for (; ORDERS_it448 != ORDERS_end447; ++ORDERS_it448)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it448);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it448);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it448);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it448);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it448);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it448);
                    string protect_O__CLERK = get<6>(*ORDERS_it448);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it448);
                    string protect_O__COMMENT = get<8>(*ORDERS_it448);
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it446 = LINEITEM.begin();
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end445 = LINEITEM.end();
                    for (; LINEITEM_it446 != LINEITEM_end445; ++LINEITEM_it446)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it446);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it446);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it446);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it446);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it446);
                        double protect_L__EXTENDEDPRICE = get<5>(
                            *LINEITEM_it446);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it446);
                        double protect_L__TAX = get<7>(*LINEITEM_it446);
                        int64_t var507 = 0;
                        var507 += protect_C__CUSTKEY;
                        int64_t var508 = 0;
                        var508 += protect_O__CUSTKEY;
                        if ( var507 == var508 )
                        {
                            int64_t var505 = 0;
                            var505 += protect_L__SUPPKEY;
                            int64_t var506 = 0;
                            var506 += protect_S__SUPPKEY;
                            if ( var505 == var506 )
                            {
                                int64_t var503 = 0;
                                var503 += protect_L__ORDERKEY;
                                int64_t var504 = 0;
                                var504 += protect_O__ORDERKEY;
                                if ( var503 == var504 )
                                {
                                    int64_t var501 = 0;
                                    var501 += x_qNATION_N__NATIONKEY;
                                    int64_t var502 = 0;
                                    var502 += protect_C__NATIONKEY;
                                    if ( var501 == var502 )
                                    {
                                        int64_t var499 = 0;
                                        var499 += x_qNATION_N__NATIONKEY;
                                        int64_t var500 = 0;
                                        var500 += protect_S__NATIONKEY;
                                        if ( var499 == var500 )
                                        {
                                            qNATION1[x_qNATION_N__NATIONKEY] += 
                                                protect_L__EXTENDEDPRICE;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qNATION3_it464 = qNATION3.begin();
    map<int64_t,double>::iterator qNATION3_end463 = qNATION3.end();
    for (; qNATION3_it464 != qNATION3_end463; ++qNATION3_it464)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION3_it464->first;
        qNATION3[x_qNATION_N__NATIONKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it462 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end461 = CUSTOMER.end();
        for (; CUSTOMER_it462 != CUSTOMER_end461; ++CUSTOMER_it462)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it462);
            string protect_C__NAME = get<1>(*CUSTOMER_it462);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it462);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it462);
            string protect_C__PHONE = get<4>(*CUSTOMER_it462);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it462);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it462);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it462);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it460 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end459 = SUPPLIER.end();
            for (; SUPPLIER_it460 != SUPPLIER_end459; ++SUPPLIER_it460)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it460);
                string protect_S__NAME = get<1>(*SUPPLIER_it460);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it460);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it460);
                string protect_S__PHONE = get<4>(*SUPPLIER_it460);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it460);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it460);
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it458 = ORDERS.begin();
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end457 = ORDERS.end();
                for (; ORDERS_it458 != ORDERS_end457; ++ORDERS_it458)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it458);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it458);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it458);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it458);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it458);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it458);
                    string protect_O__CLERK = get<6>(*ORDERS_it458);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it458);
                    string protect_O__COMMENT = get<8>(*ORDERS_it458);
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it456 = LINEITEM.begin();
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end455 = LINEITEM.end();
                    for (; LINEITEM_it456 != LINEITEM_end455; ++LINEITEM_it456)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it456);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it456);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it456);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it456);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it456);
                        double protect_L__EXTENDEDPRICE = get<5>(
                            *LINEITEM_it456);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it456);
                        double protect_L__TAX = get<7>(*LINEITEM_it456);
                        int64_t var518 = 0;
                        var518 += protect_C__CUSTKEY;
                        int64_t var519 = 0;
                        var519 += protect_O__CUSTKEY;
                        if ( var518 == var519 )
                        {
                            int64_t var516 = 0;
                            var516 += protect_L__SUPPKEY;
                            int64_t var517 = 0;
                            var517 += protect_S__SUPPKEY;
                            if ( var516 == var517 )
                            {
                                int64_t var514 = 0;
                                var514 += protect_L__ORDERKEY;
                                int64_t var515 = 0;
                                var515 += protect_O__ORDERKEY;
                                if ( var514 == var515 )
                                {
                                    int64_t var512 = 0;
                                    var512 += x_qNATION_N__NATIONKEY;
                                    int64_t var513 = 0;
                                    var513 += protect_C__NATIONKEY;
                                    if ( var512 == var513 )
                                    {
                                        int64_t var510 = 0;
                                        var510 += x_qNATION_N__NATIONKEY;
                                        int64_t var511 = 0;
                                        var511 += protect_S__NATIONKEY;
                                        if ( var510 == var511 )
                                        {
                                            double var509 = 1;
                                            var509 *= protect_L__EXTENDEDPRICE;
                                            var509 *= protect_L__DISCOUNT;
                                            qNATION3[x_qNATION_N__NATIONKEY] += 
                                                var509;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it476 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end475 = 
        qORDERS1.end();
    for (; qORDERS1_it476 != qORDERS1_end475; ++qORDERS1_it476)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it476->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it476->first);
        string N__NAME = get<2>(qORDERS1_it476->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it474 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end473 = 
            REGION.end();
        for (; REGION_it474 != REGION_end473; ++REGION_it474)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it474);
            string protect_R__NAME = get<1>(*REGION_it474);
            string protect_R__COMMENT = get<2>(*REGION_it474);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it472 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end471 = NATION.end();
            for (; NATION_it472 != NATION_end471; ++NATION_it472)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it472);
                string protect_N__NAME = get<1>(*NATION_it472);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it472);
                string protect_N__COMMENT = get<3>(*NATION_it472);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it470 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end469 = CUSTOMER.end();
                for (; CUSTOMER_it470 != CUSTOMER_end469; ++CUSTOMER_it470)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it470);
                    string protect_C__NAME = get<1>(*CUSTOMER_it470);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it470);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it470);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it470);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it470);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it470);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it470);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it468 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end467 = SUPPLIER.end();
                    for (; SUPPLIER_it468 != SUPPLIER_end467; ++SUPPLIER_it468)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it468);
                        string protect_S__NAME = get<1>(*SUPPLIER_it468);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it468);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it468);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it468);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it468);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it468);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it466 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end465 = LINEITEM.end();
                        for (
                            ; LINEITEM_it466 != LINEITEM_end465; ++LINEITEM_it466)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it466);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it466);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it466);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it466);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it466);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it466);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it466);
                            double protect_L__TAX = get<7>(*LINEITEM_it466);
                            int64_t var532 = 0;
                            var532 += protect_N__REGIONKEY;
                            int64_t var533 = 0;
                            var533 += protect_R__REGIONKEY;
                            if ( var532 == var533 )
                            {
                                int64_t var530 = 0;
                                var530 += protect_C__NATIONKEY;
                                int64_t var531 = 0;
                                var531 += protect_N__NATIONKEY;
                                if ( var530 == var531 )
                                {
                                    int64_t var528 = 0;
                                    var528 += protect_C__NATIONKEY;
                                    int64_t var529 = 0;
                                    var529 += protect_S__NATIONKEY;
                                    if ( var528 == var529 )
                                    {
                                        int64_t var526 = 0;
                                        var526 += protect_L__SUPPKEY;
                                        int64_t var527 = 0;
                                        var527 += protect_S__SUPPKEY;
                                        if ( var526 == var527 )
                                        {
                                            string var524 = 0;
                                            var524 += N__NAME;
                                            string var525 = 0;
                                            var525 += protect_N__NAME;
                                            if ( var524 == var525 )
                                            {
                                                int64_t var522 = 0;
                                                var522 += x_qORDERS_O__CUSTKEY;
                                                int64_t var523 = 0;
                                                var523 += protect_C__CUSTKEY;
                                                if ( var522 == var523 )
                                                {
                                                    int64_t var520 = 0;
                                                    var520 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var521 = 0;
                                                    var521 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var520 == var521 )
                                                    {
                                                        qORDERS1[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it488 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end487 = 
        qORDERS2.end();
    for (; qORDERS2_it488 != qORDERS2_end487; ++qORDERS2_it488)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it488->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it488->first);
        string N__NAME = get<2>(qORDERS2_it488->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it486 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end485 = 
            REGION.end();
        for (; REGION_it486 != REGION_end485; ++REGION_it486)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it486);
            string protect_R__NAME = get<1>(*REGION_it486);
            string protect_R__COMMENT = get<2>(*REGION_it486);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it484 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end483 = NATION.end();
            for (; NATION_it484 != NATION_end483; ++NATION_it484)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it484);
                string protect_N__NAME = get<1>(*NATION_it484);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it484);
                string protect_N__COMMENT = get<3>(*NATION_it484);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it482 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end481 = CUSTOMER.end();
                for (; CUSTOMER_it482 != CUSTOMER_end481; ++CUSTOMER_it482)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it482);
                    string protect_C__NAME = get<1>(*CUSTOMER_it482);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it482);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it482);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it482);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it482);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it482);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it482);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it480 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end479 = SUPPLIER.end();
                    for (; SUPPLIER_it480 != SUPPLIER_end479; ++SUPPLIER_it480)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it480);
                        string protect_S__NAME = get<1>(*SUPPLIER_it480);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it480);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it480);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it480);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it480);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it480);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it478 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end477 = LINEITEM.end();
                        for (
                            ; LINEITEM_it478 != LINEITEM_end477; ++LINEITEM_it478)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it478);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it478);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it478);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it478);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it478);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it478);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it478);
                            double protect_L__TAX = get<7>(*LINEITEM_it478);
                            int64_t var547 = 0;
                            var547 += protect_N__REGIONKEY;
                            int64_t var548 = 0;
                            var548 += protect_R__REGIONKEY;
                            if ( var547 == var548 )
                            {
                                int64_t var545 = 0;
                                var545 += protect_C__NATIONKEY;
                                int64_t var546 = 0;
                                var546 += protect_N__NATIONKEY;
                                if ( var545 == var546 )
                                {
                                    int64_t var543 = 0;
                                    var543 += protect_C__NATIONKEY;
                                    int64_t var544 = 0;
                                    var544 += protect_S__NATIONKEY;
                                    if ( var543 == var544 )
                                    {
                                        int64_t var541 = 0;
                                        var541 += protect_L__SUPPKEY;
                                        int64_t var542 = 0;
                                        var542 += protect_S__SUPPKEY;
                                        if ( var541 == var542 )
                                        {
                                            string var539 = 0;
                                            var539 += N__NAME;
                                            string var540 = 0;
                                            var540 += protect_N__NAME;
                                            if ( var539 == var540 )
                                            {
                                                int64_t var537 = 0;
                                                var537 += x_qORDERS_O__CUSTKEY;
                                                int64_t var538 = 0;
                                                var538 += protect_C__CUSTKEY;
                                                if ( var537 == var538 )
                                                {
                                                    int64_t var535 = 0;
                                                    var535 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var536 = 0;
                                                    var536 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var535 == var536 )
                                                    {
                                                        double var534 = 1;
                                                        var534 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var534 *= 
                                                            protect_L__DISCOUNT;
                                                        qORDERS2[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += var534;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<string,int64_t>,double>::iterator qREGION1_it500 = qREGION1.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION1_end499 = qREGION1.end(
        );
    for (; qREGION1_it500 != qREGION1_end499; ++qREGION1_it500)
    {
        string N__NAME = get<0>(qREGION1_it500->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it500->first);
        qREGION1[make_tuple(N__NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_it498 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end497 
            = NATION.end();
        for (; NATION_it498 != NATION_end497; ++NATION_it498)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it498);
            string protect_N__NAME = get<1>(*NATION_it498);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it498);
            string protect_N__COMMENT = get<3>(*NATION_it498);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it496 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end495 = CUSTOMER.end();
            for (; CUSTOMER_it496 != CUSTOMER_end495; ++CUSTOMER_it496)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it496);
                string protect_C__NAME = get<1>(*CUSTOMER_it496);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it496);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it496);
                string protect_C__PHONE = get<4>(*CUSTOMER_it496);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it496);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it496);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it496);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it494 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end493 = SUPPLIER.end();
                for (; SUPPLIER_it494 != SUPPLIER_end493; ++SUPPLIER_it494)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it494);
                    string protect_S__NAME = get<1>(*SUPPLIER_it494);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it494);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it494);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it494);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it494);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it494);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it492 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end491 = ORDERS.end();
                    for (; ORDERS_it492 != ORDERS_end491; ++ORDERS_it492)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it492);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it492);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it492);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it492);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it492);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it492);
                        string protect_O__CLERK = get<6>(*ORDERS_it492);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it492);
                        string protect_O__COMMENT = get<8>(*ORDERS_it492);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it490 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end489 = LINEITEM.end();
                        for (
                            ; LINEITEM_it490 != LINEITEM_end489; ++LINEITEM_it490)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it490);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it490);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it490);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it490);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it490);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it490);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it490);
                            double protect_L__TAX = get<7>(*LINEITEM_it490);
                            int64_t var561 = 0;
                            var561 += protect_C__NATIONKEY;
                            int64_t var562 = 0;
                            var562 += protect_N__NATIONKEY;
                            if ( var561 == var562 )
                            {
                                int64_t var559 = 0;
                                var559 += protect_C__NATIONKEY;
                                int64_t var560 = 0;
                                var560 += protect_S__NATIONKEY;
                                if ( var559 == var560 )
                                {
                                    int64_t var557 = 0;
                                    var557 += protect_C__CUSTKEY;
                                    int64_t var558 = 0;
                                    var558 += protect_O__CUSTKEY;
                                    if ( var557 == var558 )
                                    {
                                        int64_t var555 = 0;
                                        var555 += protect_L__SUPPKEY;
                                        int64_t var556 = 0;
                                        var556 += protect_S__SUPPKEY;
                                        if ( var555 == var556 )
                                        {
                                            int64_t var553 = 0;
                                            var553 += protect_L__ORDERKEY;
                                            int64_t var554 = 0;
                                            var554 += protect_O__ORDERKEY;
                                            if ( var553 == var554 )
                                            {
                                                int64_t var551 = 0;
                                                var551 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var552 = 0;
                                                var552 += protect_N__REGIONKEY;
                                                if ( var551 == var552 )
                                                {
                                                    string var549 = 0;
                                                    var549 += N__NAME;
                                                    string var550 = 0;
                                                    var550 += protect_N__NAME;
                                                    if ( var549 == var550 )
                                                    {
                                                        qREGION1[make_tuple(
                                                            N__NAME,x_qREGION_R__REGIONKEY)] += protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it512 = qREGION2.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION2_end511 = qREGION2.end(
        );
    for (; qREGION2_it512 != qREGION2_end511; ++qREGION2_it512)
    {
        string N__NAME = get<0>(qREGION2_it512->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it512->first);
        qREGION2[make_tuple(N__NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_it510 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end509 
            = NATION.end();
        for (; NATION_it510 != NATION_end509; ++NATION_it510)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it510);
            string protect_N__NAME = get<1>(*NATION_it510);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it510);
            string protect_N__COMMENT = get<3>(*NATION_it510);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it508 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end507 = CUSTOMER.end();
            for (; CUSTOMER_it508 != CUSTOMER_end507; ++CUSTOMER_it508)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it508);
                string protect_C__NAME = get<1>(*CUSTOMER_it508);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it508);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it508);
                string protect_C__PHONE = get<4>(*CUSTOMER_it508);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it508);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it508);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it508);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it506 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end505 = SUPPLIER.end();
                for (; SUPPLIER_it506 != SUPPLIER_end505; ++SUPPLIER_it506)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it506);
                    string protect_S__NAME = get<1>(*SUPPLIER_it506);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it506);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it506);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it506);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it506);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it506);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it504 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end503 = ORDERS.end();
                    for (; ORDERS_it504 != ORDERS_end503; ++ORDERS_it504)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it504);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it504);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it504);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it504);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it504);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it504);
                        string protect_O__CLERK = get<6>(*ORDERS_it504);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it504);
                        string protect_O__COMMENT = get<8>(*ORDERS_it504);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it502 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end501 = LINEITEM.end();
                        for (
                            ; LINEITEM_it502 != LINEITEM_end501; ++LINEITEM_it502)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it502);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it502);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it502);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it502);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it502);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it502);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it502);
                            double protect_L__TAX = get<7>(*LINEITEM_it502);
                            int64_t var576 = 0;
                            var576 += protect_C__NATIONKEY;
                            int64_t var577 = 0;
                            var577 += protect_N__NATIONKEY;
                            if ( var576 == var577 )
                            {
                                int64_t var574 = 0;
                                var574 += protect_C__NATIONKEY;
                                int64_t var575 = 0;
                                var575 += protect_S__NATIONKEY;
                                if ( var574 == var575 )
                                {
                                    int64_t var572 = 0;
                                    var572 += protect_C__CUSTKEY;
                                    int64_t var573 = 0;
                                    var573 += protect_O__CUSTKEY;
                                    if ( var572 == var573 )
                                    {
                                        int64_t var570 = 0;
                                        var570 += protect_L__SUPPKEY;
                                        int64_t var571 = 0;
                                        var571 += protect_S__SUPPKEY;
                                        if ( var570 == var571 )
                                        {
                                            int64_t var568 = 0;
                                            var568 += protect_L__ORDERKEY;
                                            int64_t var569 = 0;
                                            var569 += protect_O__ORDERKEY;
                                            if ( var568 == var569 )
                                            {
                                                int64_t var566 = 0;
                                                var566 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var567 = 0;
                                                var567 += protect_N__REGIONKEY;
                                                if ( var566 == var567 )
                                                {
                                                    string var564 = 0;
                                                    var564 += N__NAME;
                                                    string var565 = 0;
                                                    var565 += protect_N__NAME;
                                                    if ( var564 == var565 )
                                                    {
                                                        double var563 = 1;
                                                        var563 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var563 *= 
                                                            protect_L__DISCOUNT;
                                                        qREGION2[make_tuple(
                                                            N__NAME,x_qREGION_R__REGIONKEY)] += var563;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it524 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end523 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it524 != qSUPPLIER1_end523; ++qSUPPLIER1_it524)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it524->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER1_it524->first);
        string N__NAME = get<2>(qSUPPLIER1_it524->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it522 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end521 = 
            REGION.end();
        for (; REGION_it522 != REGION_end521; ++REGION_it522)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it522);
            string protect_R__NAME = get<1>(*REGION_it522);
            string protect_R__COMMENT = get<2>(*REGION_it522);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it520 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end519 = NATION.end();
            for (; NATION_it520 != NATION_end519; ++NATION_it520)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it520);
                string protect_N__NAME = get<1>(*NATION_it520);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it520);
                string protect_N__COMMENT = get<3>(*NATION_it520);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it518 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end517 = CUSTOMER.end();
                for (; CUSTOMER_it518 != CUSTOMER_end517; ++CUSTOMER_it518)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it518);
                    string protect_C__NAME = get<1>(*CUSTOMER_it518);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it518);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it518);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it518);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it518);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it518);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it518);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it516 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end515 = ORDERS.end();
                    for (; ORDERS_it516 != ORDERS_end515; ++ORDERS_it516)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it516);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it516);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it516);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it516);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it516);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it516);
                        string protect_O__CLERK = get<6>(*ORDERS_it516);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it516);
                        string protect_O__COMMENT = get<8>(*ORDERS_it516);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it514 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end513 = LINEITEM.end();
                        for (
                            ; LINEITEM_it514 != LINEITEM_end513; ++LINEITEM_it514)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it514);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it514);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it514);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it514);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it514);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it514);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it514);
                            double protect_L__TAX = get<7>(*LINEITEM_it514);
                            int64_t var590 = 0;
                            var590 += protect_N__REGIONKEY;
                            int64_t var591 = 0;
                            var591 += protect_R__REGIONKEY;
                            if ( var590 == var591 )
                            {
                                int64_t var588 = 0;
                                var588 += protect_C__CUSTKEY;
                                int64_t var589 = 0;
                                var589 += protect_O__CUSTKEY;
                                if ( var588 == var589 )
                                {
                                    int64_t var586 = 0;
                                    var586 += protect_L__ORDERKEY;
                                    int64_t var587 = 0;
                                    var587 += protect_O__ORDERKEY;
                                    if ( var586 == var587 )
                                    {
                                        string var584 = 0;
                                        var584 += N__NAME;
                                        string var585 = 0;
                                        var585 += protect_N__NAME;
                                        if ( var584 == var585 )
                                        {
                                            int64_t var582 = 0;
                                            var582 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var583 = 0;
                                            var583 += protect_N__NATIONKEY;
                                            if ( var582 == var583 )
                                            {
                                                int64_t var580 = 0;
                                                var580 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var581 = 0;
                                                var581 += protect_C__NATIONKEY;
                                                if ( var580 == var581 )
                                                {
                                                    int64_t var578 = 0;
                                                    var578 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var579 = 0;
                                                    var579 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var578 == var579 )
                                                    {
                                                        qSUPPLIER1[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it536 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end535 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it536 != qSUPPLIER2_end535; ++qSUPPLIER2_it536)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it536->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER2_it536->first);
        string N__NAME = get<2>(qSUPPLIER2_it536->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it534 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end533 = 
            REGION.end();
        for (; REGION_it534 != REGION_end533; ++REGION_it534)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it534);
            string protect_R__NAME = get<1>(*REGION_it534);
            string protect_R__COMMENT = get<2>(*REGION_it534);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it532 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end531 = NATION.end();
            for (; NATION_it532 != NATION_end531; ++NATION_it532)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it532);
                string protect_N__NAME = get<1>(*NATION_it532);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it532);
                string protect_N__COMMENT = get<3>(*NATION_it532);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it530 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end529 = CUSTOMER.end();
                for (; CUSTOMER_it530 != CUSTOMER_end529; ++CUSTOMER_it530)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it530);
                    string protect_C__NAME = get<1>(*CUSTOMER_it530);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it530);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it530);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it530);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it530);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it530);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it530);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it528 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end527 = ORDERS.end();
                    for (; ORDERS_it528 != ORDERS_end527; ++ORDERS_it528)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it528);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it528);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it528);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it528);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it528);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it528);
                        string protect_O__CLERK = get<6>(*ORDERS_it528);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it528);
                        string protect_O__COMMENT = get<8>(*ORDERS_it528);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it526 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end525 = LINEITEM.end();
                        for (
                            ; LINEITEM_it526 != LINEITEM_end525; ++LINEITEM_it526)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it526);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it526);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it526);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it526);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it526);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it526);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it526);
                            double protect_L__TAX = get<7>(*LINEITEM_it526);
                            int64_t var605 = 0;
                            var605 += protect_N__REGIONKEY;
                            int64_t var606 = 0;
                            var606 += protect_R__REGIONKEY;
                            if ( var605 == var606 )
                            {
                                int64_t var603 = 0;
                                var603 += protect_C__CUSTKEY;
                                int64_t var604 = 0;
                                var604 += protect_O__CUSTKEY;
                                if ( var603 == var604 )
                                {
                                    int64_t var601 = 0;
                                    var601 += protect_L__ORDERKEY;
                                    int64_t var602 = 0;
                                    var602 += protect_O__ORDERKEY;
                                    if ( var601 == var602 )
                                    {
                                        string var599 = 0;
                                        var599 += N__NAME;
                                        string var600 = 0;
                                        var600 += protect_N__NAME;
                                        if ( var599 == var600 )
                                        {
                                            int64_t var597 = 0;
                                            var597 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var598 = 0;
                                            var598 += protect_N__NATIONKEY;
                                            if ( var597 == var598 )
                                            {
                                                int64_t var595 = 0;
                                                var595 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var596 = 0;
                                                var596 += protect_C__NATIONKEY;
                                                if ( var595 == var596 )
                                                {
                                                    int64_t var593 = 0;
                                                    var593 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var594 = 0;
                                                    var594 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var593 == var594 )
                                                    {
                                                        double var592 = 1;
                                                        var592 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var592 *= 
                                                            protect_L__DISCOUNT;
                                                        qSUPPLIER2[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += var592;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_CUSTOMER_sec_span, on_insert_CUSTOMER_usec_span);
}

void on_insert_NATION(
    ofstream* results, ofstream* log, ofstream* stats, int64_t NATIONKEY,string 
    NAME,int64_t REGIONKEY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    NATION.insert(make_tuple(NATIONKEY,NAME,REGIONKEY,COMMENT));
    q[NAME] += qNATION1[NATIONKEY]*qNATION2[REGIONKEY];
    q[NAME] += qNATION3[NATIONKEY]*qNATION2[REGIONKEY]*-1;
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it548 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end547 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it548 != qCUSTOMER1_end547; ++qCUSTOMER1_it548)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it548->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER1_it548->first);
        string NAME = get<2>(qCUSTOMER1_it548->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it546 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end545 = 
            REGION.end();
        for (; REGION_it546 != REGION_end545; ++REGION_it546)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it546);
            string protect_R__NAME = get<1>(*REGION_it546);
            string protect_R__COMMENT = get<2>(*REGION_it546);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it544 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end543 = NATION.end();
            for (; NATION_it544 != NATION_end543; ++NATION_it544)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it544);
                string protect_N__NAME = get<1>(*NATION_it544);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it544);
                string protect_N__COMMENT = get<3>(*NATION_it544);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it542 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end541 = SUPPLIER.end();
                for (; SUPPLIER_it542 != SUPPLIER_end541; ++SUPPLIER_it542)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it542);
                    string protect_S__NAME = get<1>(*SUPPLIER_it542);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it542);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it542);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it542);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it542);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it542);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it540 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end539 = ORDERS.end();
                    for (; ORDERS_it540 != ORDERS_end539; ++ORDERS_it540)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it540);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it540);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it540);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it540);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it540);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it540);
                        string protect_O__CLERK = get<6>(*ORDERS_it540);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it540);
                        string protect_O__COMMENT = get<8>(*ORDERS_it540);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it538 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end537 = LINEITEM.end();
                        for (
                            ; LINEITEM_it538 != LINEITEM_end537; ++LINEITEM_it538)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it538);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it538);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it538);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it538);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it538);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it538);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it538);
                            double protect_L__TAX = get<7>(*LINEITEM_it538);
                            int64_t var619 = 0;
                            var619 += protect_N__REGIONKEY;
                            int64_t var620 = 0;
                            var620 += protect_R__REGIONKEY;
                            if ( var619 == var620 )
                            {
                                int64_t var617 = 0;
                                var617 += protect_L__SUPPKEY;
                                int64_t var618 = 0;
                                var618 += protect_S__SUPPKEY;
                                if ( var617 == var618 )
                                {
                                    int64_t var615 = 0;
                                    var615 += protect_L__ORDERKEY;
                                    int64_t var616 = 0;
                                    var616 += protect_O__ORDERKEY;
                                    if ( var615 == var616 )
                                    {
                                        string var613 = 0;
                                        var613 += NAME;
                                        string var614 = 0;
                                        var614 += protect_N__NAME;
                                        if ( var613 == var614 )
                                        {
                                            int64_t var611 = 0;
                                            var611 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var612 = 0;
                                            var612 += protect_N__NATIONKEY;
                                            if ( var611 == var612 )
                                            {
                                                int64_t var609 = 0;
                                                var609 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var610 = 0;
                                                var610 += protect_S__NATIONKEY;
                                                if ( var609 == var610 )
                                                {
                                                    int64_t var607 = 0;
                                                    var607 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var608 = 0;
                                                    var608 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var607 == var608 )
                                                    {
                                                        qCUSTOMER1[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it560 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end559 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it560 != qCUSTOMER2_end559; ++qCUSTOMER2_it560)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it560->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER2_it560->first);
        string NAME = get<2>(qCUSTOMER2_it560->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it558 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end557 = 
            REGION.end();
        for (; REGION_it558 != REGION_end557; ++REGION_it558)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it558);
            string protect_R__NAME = get<1>(*REGION_it558);
            string protect_R__COMMENT = get<2>(*REGION_it558);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it556 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end555 = NATION.end();
            for (; NATION_it556 != NATION_end555; ++NATION_it556)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it556);
                string protect_N__NAME = get<1>(*NATION_it556);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it556);
                string protect_N__COMMENT = get<3>(*NATION_it556);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it554 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end553 = SUPPLIER.end();
                for (; SUPPLIER_it554 != SUPPLIER_end553; ++SUPPLIER_it554)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it554);
                    string protect_S__NAME = get<1>(*SUPPLIER_it554);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it554);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it554);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it554);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it554);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it554);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it552 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end551 = ORDERS.end();
                    for (; ORDERS_it552 != ORDERS_end551; ++ORDERS_it552)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it552);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it552);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it552);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it552);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it552);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it552);
                        string protect_O__CLERK = get<6>(*ORDERS_it552);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it552);
                        string protect_O__COMMENT = get<8>(*ORDERS_it552);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it550 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end549 = LINEITEM.end();
                        for (
                            ; LINEITEM_it550 != LINEITEM_end549; ++LINEITEM_it550)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it550);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it550);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it550);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it550);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it550);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it550);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it550);
                            double protect_L__TAX = get<7>(*LINEITEM_it550);
                            int64_t var634 = 0;
                            var634 += protect_N__REGIONKEY;
                            int64_t var635 = 0;
                            var635 += protect_R__REGIONKEY;
                            if ( var634 == var635 )
                            {
                                int64_t var632 = 0;
                                var632 += protect_L__SUPPKEY;
                                int64_t var633 = 0;
                                var633 += protect_S__SUPPKEY;
                                if ( var632 == var633 )
                                {
                                    int64_t var630 = 0;
                                    var630 += protect_L__ORDERKEY;
                                    int64_t var631 = 0;
                                    var631 += protect_O__ORDERKEY;
                                    if ( var630 == var631 )
                                    {
                                        string var628 = 0;
                                        var628 += NAME;
                                        string var629 = 0;
                                        var629 += protect_N__NAME;
                                        if ( var628 == var629 )
                                        {
                                            int64_t var626 = 0;
                                            var626 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var627 = 0;
                                            var627 += protect_N__NATIONKEY;
                                            if ( var626 == var627 )
                                            {
                                                int64_t var624 = 0;
                                                var624 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var625 = 0;
                                                var625 += protect_S__NATIONKEY;
                                                if ( var624 == var625 )
                                                {
                                                    int64_t var622 = 0;
                                                    var622 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var623 = 0;
                                                    var623 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var622 == var623 )
                                                    {
                                                        double var621 = 1;
                                                        var621 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var621 *= 
                                                            protect_L__DISCOUNT;
                                                        qCUSTOMER2[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,NAME)] += var621;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it572 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end571 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it572 != qLINEITEM1_end571; ++qLINEITEM1_it572)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it572->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it572->first);
        string NAME = get<2>(qLINEITEM1_it572->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it570 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end569 = 
            REGION.end();
        for (; REGION_it570 != REGION_end569; ++REGION_it570)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it570);
            string protect_R__NAME = get<1>(*REGION_it570);
            string protect_R__COMMENT = get<2>(*REGION_it570);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it568 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end567 = NATION.end();
            for (; NATION_it568 != NATION_end567; ++NATION_it568)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it568);
                string protect_N__NAME = get<1>(*NATION_it568);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it568);
                string protect_N__COMMENT = get<3>(*NATION_it568);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it566 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end565 = SUPPLIER.end();
                for (; SUPPLIER_it566 != SUPPLIER_end565; ++SUPPLIER_it566)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it566);
                    string protect_S__NAME = get<1>(*SUPPLIER_it566);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it566);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it566);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it566);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it566);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it566);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_it564 = CUSTOMER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_end563 = CUSTOMER.end();
                    for (; CUSTOMER_it564 != CUSTOMER_end563; ++CUSTOMER_it564)
                    {
                        int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it564);
                        string protect_C__NAME = get<1>(*CUSTOMER_it564);
                        string protect_C__ADDRESS = get<2>(*CUSTOMER_it564);
                        int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it564);
                        string protect_C__PHONE = get<4>(*CUSTOMER_it564);
                        double protect_C__ACCTBAL = get<5>(*CUSTOMER_it564);
                        string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it564);
                        string protect_C__COMMENT = get<7>(*CUSTOMER_it564);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it562 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end561 = ORDERS.end();
                        for (; ORDERS_it562 != ORDERS_end561; ++ORDERS_it562)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it562);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it562);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it562);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it562);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it562);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it562);
                            string protect_O__CLERK = get<6>(*ORDERS_it562);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it562);
                            string protect_O__COMMENT = get<8>(*ORDERS_it562);
                            int64_t var648 = 0;
                            var648 += protect_N__REGIONKEY;
                            int64_t var649 = 0;
                            var649 += protect_R__REGIONKEY;
                            if ( var648 == var649 )
                            {
                                int64_t var646 = 0;
                                var646 += protect_C__NATIONKEY;
                                int64_t var647 = 0;
                                var647 += protect_N__NATIONKEY;
                                if ( var646 == var647 )
                                {
                                    int64_t var644 = 0;
                                    var644 += protect_C__NATIONKEY;
                                    int64_t var645 = 0;
                                    var645 += protect_S__NATIONKEY;
                                    if ( var644 == var645 )
                                    {
                                        int64_t var642 = 0;
                                        var642 += protect_C__CUSTKEY;
                                        int64_t var643 = 0;
                                        var643 += protect_O__CUSTKEY;
                                        if ( var642 == var643 )
                                        {
                                            string var640 = 0;
                                            var640 += NAME;
                                            string var641 = 0;
                                            var641 += protect_N__NAME;
                                            if ( var640 == var641 )
                                            {
                                                int64_t var638 = 0;
                                                var638 += 
                                                    x_qLINEITEM_L__SUPPKEY;
                                                int64_t var639 = 0;
                                                var639 += protect_S__SUPPKEY;
                                                if ( var638 == var639 )
                                                {
                                                    int64_t var636 = 0;
                                                    var636 += 
                                                        x_qLINEITEM_L__ORDERKEY;
                                                    int64_t var637 = 0;
                                                    var637 += 
                                                        protect_O__ORDERKEY;
                                                    if ( var636 == var637 )
                                                    {
                                                        qLINEITEM1[make_tuple(
                                                            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,NAME)] += 1;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it584 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end583 = 
        qORDERS1.end();
    for (; qORDERS1_it584 != qORDERS1_end583; ++qORDERS1_it584)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it584->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it584->first);
        string NAME = get<2>(qORDERS1_it584->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it582 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end581 = 
            REGION.end();
        for (; REGION_it582 != REGION_end581; ++REGION_it582)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it582);
            string protect_R__NAME = get<1>(*REGION_it582);
            string protect_R__COMMENT = get<2>(*REGION_it582);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it580 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end579 = NATION.end();
            for (; NATION_it580 != NATION_end579; ++NATION_it580)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it580);
                string protect_N__NAME = get<1>(*NATION_it580);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it580);
                string protect_N__COMMENT = get<3>(*NATION_it580);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it578 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end577 = CUSTOMER.end();
                for (; CUSTOMER_it578 != CUSTOMER_end577; ++CUSTOMER_it578)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it578);
                    string protect_C__NAME = get<1>(*CUSTOMER_it578);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it578);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it578);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it578);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it578);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it578);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it578);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it576 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end575 = SUPPLIER.end();
                    for (; SUPPLIER_it576 != SUPPLIER_end575; ++SUPPLIER_it576)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it576);
                        string protect_S__NAME = get<1>(*SUPPLIER_it576);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it576);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it576);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it576);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it576);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it576);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it574 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end573 = LINEITEM.end();
                        for (
                            ; LINEITEM_it574 != LINEITEM_end573; ++LINEITEM_it574)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it574);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it574);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it574);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it574);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it574);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it574);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it574);
                            double protect_L__TAX = get<7>(*LINEITEM_it574);
                            int64_t var662 = 0;
                            var662 += protect_N__REGIONKEY;
                            int64_t var663 = 0;
                            var663 += protect_R__REGIONKEY;
                            if ( var662 == var663 )
                            {
                                int64_t var660 = 0;
                                var660 += protect_C__NATIONKEY;
                                int64_t var661 = 0;
                                var661 += protect_N__NATIONKEY;
                                if ( var660 == var661 )
                                {
                                    int64_t var658 = 0;
                                    var658 += protect_C__NATIONKEY;
                                    int64_t var659 = 0;
                                    var659 += protect_S__NATIONKEY;
                                    if ( var658 == var659 )
                                    {
                                        int64_t var656 = 0;
                                        var656 += protect_L__SUPPKEY;
                                        int64_t var657 = 0;
                                        var657 += protect_S__SUPPKEY;
                                        if ( var656 == var657 )
                                        {
                                            string var654 = 0;
                                            var654 += NAME;
                                            string var655 = 0;
                                            var655 += protect_N__NAME;
                                            if ( var654 == var655 )
                                            {
                                                int64_t var652 = 0;
                                                var652 += x_qORDERS_O__CUSTKEY;
                                                int64_t var653 = 0;
                                                var653 += protect_C__CUSTKEY;
                                                if ( var652 == var653 )
                                                {
                                                    int64_t var650 = 0;
                                                    var650 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var651 = 0;
                                                    var651 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var650 == var651 )
                                                    {
                                                        qORDERS1[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,NAME)] += protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it596 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end595 = 
        qORDERS2.end();
    for (; qORDERS2_it596 != qORDERS2_end595; ++qORDERS2_it596)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it596->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it596->first);
        string NAME = get<2>(qORDERS2_it596->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it594 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end593 = 
            REGION.end();
        for (; REGION_it594 != REGION_end593; ++REGION_it594)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it594);
            string protect_R__NAME = get<1>(*REGION_it594);
            string protect_R__COMMENT = get<2>(*REGION_it594);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it592 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end591 = NATION.end();
            for (; NATION_it592 != NATION_end591; ++NATION_it592)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it592);
                string protect_N__NAME = get<1>(*NATION_it592);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it592);
                string protect_N__COMMENT = get<3>(*NATION_it592);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it590 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end589 = CUSTOMER.end();
                for (; CUSTOMER_it590 != CUSTOMER_end589; ++CUSTOMER_it590)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it590);
                    string protect_C__NAME = get<1>(*CUSTOMER_it590);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it590);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it590);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it590);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it590);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it590);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it590);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it588 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end587 = SUPPLIER.end();
                    for (; SUPPLIER_it588 != SUPPLIER_end587; ++SUPPLIER_it588)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it588);
                        string protect_S__NAME = get<1>(*SUPPLIER_it588);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it588);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it588);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it588);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it588);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it588);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it586 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end585 = LINEITEM.end();
                        for (
                            ; LINEITEM_it586 != LINEITEM_end585; ++LINEITEM_it586)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it586);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it586);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it586);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it586);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it586);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it586);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it586);
                            double protect_L__TAX = get<7>(*LINEITEM_it586);
                            int64_t var677 = 0;
                            var677 += protect_N__REGIONKEY;
                            int64_t var678 = 0;
                            var678 += protect_R__REGIONKEY;
                            if ( var677 == var678 )
                            {
                                int64_t var675 = 0;
                                var675 += protect_C__NATIONKEY;
                                int64_t var676 = 0;
                                var676 += protect_N__NATIONKEY;
                                if ( var675 == var676 )
                                {
                                    int64_t var673 = 0;
                                    var673 += protect_C__NATIONKEY;
                                    int64_t var674 = 0;
                                    var674 += protect_S__NATIONKEY;
                                    if ( var673 == var674 )
                                    {
                                        int64_t var671 = 0;
                                        var671 += protect_L__SUPPKEY;
                                        int64_t var672 = 0;
                                        var672 += protect_S__SUPPKEY;
                                        if ( var671 == var672 )
                                        {
                                            string var669 = 0;
                                            var669 += NAME;
                                            string var670 = 0;
                                            var670 += protect_N__NAME;
                                            if ( var669 == var670 )
                                            {
                                                int64_t var667 = 0;
                                                var667 += x_qORDERS_O__CUSTKEY;
                                                int64_t var668 = 0;
                                                var668 += protect_C__CUSTKEY;
                                                if ( var667 == var668 )
                                                {
                                                    int64_t var665 = 0;
                                                    var665 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var666 = 0;
                                                    var666 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var665 == var666 )
                                                    {
                                                        double var664 = 1;
                                                        var664 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var664 *= 
                                                            protect_L__DISCOUNT;
                                                        qORDERS2[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,NAME)] += var664;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    map<tuple<string,int64_t>,double>::iterator qREGION1_it608 = qREGION1.begin();
    map<tuple<string,int64_t>,double>::iterator qREGION1_end607 = qREGION1.end();
    for (; qREGION1_it608 != qREGION1_end607; ++qREGION1_it608)
    {
        string NAME = get<0>(qREGION1_it608->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it608->first);
        qREGION1[make_tuple(NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_it606 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end605 
            = NATION.end();
        for (; NATION_it606 != NATION_end605; ++NATION_it606)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it606);
            string protect_N__NAME = get<1>(*NATION_it606);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it606);
            string protect_N__COMMENT = get<3>(*NATION_it606);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it604 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end603 = CUSTOMER.end();
            for (; CUSTOMER_it604 != CUSTOMER_end603; ++CUSTOMER_it604)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it604);
                string protect_C__NAME = get<1>(*CUSTOMER_it604);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it604);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it604);
                string protect_C__PHONE = get<4>(*CUSTOMER_it604);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it604);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it604);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it604);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it602 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end601 = SUPPLIER.end();
                for (; SUPPLIER_it602 != SUPPLIER_end601; ++SUPPLIER_it602)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it602);
                    string protect_S__NAME = get<1>(*SUPPLIER_it602);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it602);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it602);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it602);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it602);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it602);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it600 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end599 = ORDERS.end();
                    for (; ORDERS_it600 != ORDERS_end599; ++ORDERS_it600)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it600);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it600);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it600);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it600);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it600);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it600);
                        string protect_O__CLERK = get<6>(*ORDERS_it600);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it600);
                        string protect_O__COMMENT = get<8>(*ORDERS_it600);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it598 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end597 = LINEITEM.end();
                        for (
                            ; LINEITEM_it598 != LINEITEM_end597; ++LINEITEM_it598)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it598);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it598);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it598);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it598);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it598);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it598);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it598);
                            double protect_L__TAX = get<7>(*LINEITEM_it598);
                            int64_t var691 = 0;
                            var691 += protect_C__NATIONKEY;
                            int64_t var692 = 0;
                            var692 += protect_N__NATIONKEY;
                            if ( var691 == var692 )
                            {
                                int64_t var689 = 0;
                                var689 += protect_C__NATIONKEY;
                                int64_t var690 = 0;
                                var690 += protect_S__NATIONKEY;
                                if ( var689 == var690 )
                                {
                                    int64_t var687 = 0;
                                    var687 += protect_C__CUSTKEY;
                                    int64_t var688 = 0;
                                    var688 += protect_O__CUSTKEY;
                                    if ( var687 == var688 )
                                    {
                                        int64_t var685 = 0;
                                        var685 += protect_L__SUPPKEY;
                                        int64_t var686 = 0;
                                        var686 += protect_S__SUPPKEY;
                                        if ( var685 == var686 )
                                        {
                                            int64_t var683 = 0;
                                            var683 += protect_L__ORDERKEY;
                                            int64_t var684 = 0;
                                            var684 += protect_O__ORDERKEY;
                                            if ( var683 == var684 )
                                            {
                                                int64_t var681 = 0;
                                                var681 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var682 = 0;
                                                var682 += protect_N__REGIONKEY;
                                                if ( var681 == var682 )
                                                {
                                                    string var679 = 0;
                                                    var679 += NAME;
                                                    string var680 = 0;
                                                    var680 += protect_N__NAME;
                                                    if ( var679 == var680 )
                                                    {
                                                        qREGION1[make_tuple(
                                                            NAME,x_qREGION_R__REGIONKEY)] += protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    map<tuple<string,int64_t>,double>::iterator qREGION2_it620 = qREGION2.begin();
    map<tuple<string,int64_t>,double>::iterator qREGION2_end619 = qREGION2.end();
    for (; qREGION2_it620 != qREGION2_end619; ++qREGION2_it620)
    {
        string NAME = get<0>(qREGION2_it620->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it620->first);
        qREGION2[make_tuple(NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_it618 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end617 
            = NATION.end();
        for (; NATION_it618 != NATION_end617; ++NATION_it618)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it618);
            string protect_N__NAME = get<1>(*NATION_it618);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it618);
            string protect_N__COMMENT = get<3>(*NATION_it618);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it616 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end615 = CUSTOMER.end();
            for (; CUSTOMER_it616 != CUSTOMER_end615; ++CUSTOMER_it616)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it616);
                string protect_C__NAME = get<1>(*CUSTOMER_it616);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it616);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it616);
                string protect_C__PHONE = get<4>(*CUSTOMER_it616);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it616);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it616);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it616);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it614 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end613 = SUPPLIER.end();
                for (; SUPPLIER_it614 != SUPPLIER_end613; ++SUPPLIER_it614)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it614);
                    string protect_S__NAME = get<1>(*SUPPLIER_it614);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it614);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it614);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it614);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it614);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it614);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it612 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end611 = ORDERS.end();
                    for (; ORDERS_it612 != ORDERS_end611; ++ORDERS_it612)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it612);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it612);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it612);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it612);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it612);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it612);
                        string protect_O__CLERK = get<6>(*ORDERS_it612);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it612);
                        string protect_O__COMMENT = get<8>(*ORDERS_it612);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it610 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end609 = LINEITEM.end();
                        for (
                            ; LINEITEM_it610 != LINEITEM_end609; ++LINEITEM_it610)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it610);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it610);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it610);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it610);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it610);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it610);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it610);
                            double protect_L__TAX = get<7>(*LINEITEM_it610);
                            int64_t var706 = 0;
                            var706 += protect_C__NATIONKEY;
                            int64_t var707 = 0;
                            var707 += protect_N__NATIONKEY;
                            if ( var706 == var707 )
                            {
                                int64_t var704 = 0;
                                var704 += protect_C__NATIONKEY;
                                int64_t var705 = 0;
                                var705 += protect_S__NATIONKEY;
                                if ( var704 == var705 )
                                {
                                    int64_t var702 = 0;
                                    var702 += protect_C__CUSTKEY;
                                    int64_t var703 = 0;
                                    var703 += protect_O__CUSTKEY;
                                    if ( var702 == var703 )
                                    {
                                        int64_t var700 = 0;
                                        var700 += protect_L__SUPPKEY;
                                        int64_t var701 = 0;
                                        var701 += protect_S__SUPPKEY;
                                        if ( var700 == var701 )
                                        {
                                            int64_t var698 = 0;
                                            var698 += protect_L__ORDERKEY;
                                            int64_t var699 = 0;
                                            var699 += protect_O__ORDERKEY;
                                            if ( var698 == var699 )
                                            {
                                                int64_t var696 = 0;
                                                var696 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var697 = 0;
                                                var697 += protect_N__REGIONKEY;
                                                if ( var696 == var697 )
                                                {
                                                    string var694 = 0;
                                                    var694 += NAME;
                                                    string var695 = 0;
                                                    var695 += protect_N__NAME;
                                                    if ( var694 == var695 )
                                                    {
                                                        double var693 = 1;
                                                        var693 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var693 *= 
                                                            protect_L__DISCOUNT;
                                                        qREGION2[make_tuple(
                                                            NAME,x_qREGION_R__REGIONKEY)] += var693;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it632 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end631 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it632 != qSUPPLIER1_end631; ++qSUPPLIER1_it632)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it632->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER1_it632->first);
        string NAME = get<2>(qSUPPLIER1_it632->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it630 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end629 = 
            REGION.end();
        for (; REGION_it630 != REGION_end629; ++REGION_it630)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it630);
            string protect_R__NAME = get<1>(*REGION_it630);
            string protect_R__COMMENT = get<2>(*REGION_it630);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it628 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end627 = NATION.end();
            for (; NATION_it628 != NATION_end627; ++NATION_it628)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it628);
                string protect_N__NAME = get<1>(*NATION_it628);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it628);
                string protect_N__COMMENT = get<3>(*NATION_it628);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it626 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end625 = CUSTOMER.end();
                for (; CUSTOMER_it626 != CUSTOMER_end625; ++CUSTOMER_it626)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it626);
                    string protect_C__NAME = get<1>(*CUSTOMER_it626);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it626);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it626);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it626);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it626);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it626);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it626);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it624 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end623 = ORDERS.end();
                    for (; ORDERS_it624 != ORDERS_end623; ++ORDERS_it624)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it624);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it624);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it624);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it624);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it624);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it624);
                        string protect_O__CLERK = get<6>(*ORDERS_it624);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it624);
                        string protect_O__COMMENT = get<8>(*ORDERS_it624);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it622 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end621 = LINEITEM.end();
                        for (
                            ; LINEITEM_it622 != LINEITEM_end621; ++LINEITEM_it622)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it622);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it622);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it622);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it622);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it622);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it622);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it622);
                            double protect_L__TAX = get<7>(*LINEITEM_it622);
                            int64_t var720 = 0;
                            var720 += protect_N__REGIONKEY;
                            int64_t var721 = 0;
                            var721 += protect_R__REGIONKEY;
                            if ( var720 == var721 )
                            {
                                int64_t var718 = 0;
                                var718 += protect_C__CUSTKEY;
                                int64_t var719 = 0;
                                var719 += protect_O__CUSTKEY;
                                if ( var718 == var719 )
                                {
                                    int64_t var716 = 0;
                                    var716 += protect_L__ORDERKEY;
                                    int64_t var717 = 0;
                                    var717 += protect_O__ORDERKEY;
                                    if ( var716 == var717 )
                                    {
                                        string var714 = 0;
                                        var714 += NAME;
                                        string var715 = 0;
                                        var715 += protect_N__NAME;
                                        if ( var714 == var715 )
                                        {
                                            int64_t var712 = 0;
                                            var712 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var713 = 0;
                                            var713 += protect_N__NATIONKEY;
                                            if ( var712 == var713 )
                                            {
                                                int64_t var710 = 0;
                                                var710 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var711 = 0;
                                                var711 += protect_C__NATIONKEY;
                                                if ( var710 == var711 )
                                                {
                                                    int64_t var708 = 0;
                                                    var708 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var709 = 0;
                                                    var709 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var708 == var709 )
                                                    {
                                                        qSUPPLIER1[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it644 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end643 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it644 != qSUPPLIER2_end643; ++qSUPPLIER2_it644)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it644->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER2_it644->first);
        string NAME = get<2>(qSUPPLIER2_it644->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it642 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end641 = 
            REGION.end();
        for (; REGION_it642 != REGION_end641; ++REGION_it642)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it642);
            string protect_R__NAME = get<1>(*REGION_it642);
            string protect_R__COMMENT = get<2>(*REGION_it642);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it640 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end639 = NATION.end();
            for (; NATION_it640 != NATION_end639; ++NATION_it640)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it640);
                string protect_N__NAME = get<1>(*NATION_it640);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it640);
                string protect_N__COMMENT = get<3>(*NATION_it640);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it638 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end637 = CUSTOMER.end();
                for (; CUSTOMER_it638 != CUSTOMER_end637; ++CUSTOMER_it638)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it638);
                    string protect_C__NAME = get<1>(*CUSTOMER_it638);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it638);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it638);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it638);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it638);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it638);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it638);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it636 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end635 = ORDERS.end();
                    for (; ORDERS_it636 != ORDERS_end635; ++ORDERS_it636)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it636);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it636);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it636);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it636);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it636);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it636);
                        string protect_O__CLERK = get<6>(*ORDERS_it636);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it636);
                        string protect_O__COMMENT = get<8>(*ORDERS_it636);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it634 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end633 = LINEITEM.end();
                        for (
                            ; LINEITEM_it634 != LINEITEM_end633; ++LINEITEM_it634)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it634);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it634);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it634);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it634);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it634);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it634);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it634);
                            double protect_L__TAX = get<7>(*LINEITEM_it634);
                            int64_t var735 = 0;
                            var735 += protect_N__REGIONKEY;
                            int64_t var736 = 0;
                            var736 += protect_R__REGIONKEY;
                            if ( var735 == var736 )
                            {
                                int64_t var733 = 0;
                                var733 += protect_C__CUSTKEY;
                                int64_t var734 = 0;
                                var734 += protect_O__CUSTKEY;
                                if ( var733 == var734 )
                                {
                                    int64_t var731 = 0;
                                    var731 += protect_L__ORDERKEY;
                                    int64_t var732 = 0;
                                    var732 += protect_O__ORDERKEY;
                                    if ( var731 == var732 )
                                    {
                                        string var729 = 0;
                                        var729 += NAME;
                                        string var730 = 0;
                                        var730 += protect_N__NAME;
                                        if ( var729 == var730 )
                                        {
                                            int64_t var727 = 0;
                                            var727 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var728 = 0;
                                            var728 += protect_N__NATIONKEY;
                                            if ( var727 == var728 )
                                            {
                                                int64_t var725 = 0;
                                                var725 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var726 = 0;
                                                var726 += protect_C__NATIONKEY;
                                                if ( var725 == var726 )
                                                {
                                                    int64_t var723 = 0;
                                                    var723 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var724 = 0;
                                                    var724 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var723 == var724 )
                                                    {
                                                        double var722 = 1;
                                                        var722 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var722 *= 
                                                            protect_L__DISCOUNT;
                                                        qSUPPLIER2[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,NAME)] += var722;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_NATION_sec_span, on_insert_NATION_usec_span);
}

void on_delete_REGION(
    ofstream* results, ofstream* log, ofstream* stats, int64_t REGIONKEY,string 
    NAME,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    REGION.erase(make_tuple(REGIONKEY,NAME,COMMENT));
    map<string,double>::iterator q_it646 = q.begin();
    map<string,double>::iterator q_end645 = q.end();
    for (; q_it646 != q_end645; ++q_it646)
    {
        string N__NAME = q_it646->first;
        q[N__NAME] += -1*qREGION1[make_tuple(N__NAME,REGIONKEY)];
    }
    map<string,double>::iterator q_it648 = q.begin();
    map<string,double>::iterator q_end647 = q.end();
    for (; q_it648 != q_end647; ++q_it648)
    {
        string N__NAME = q_it648->first;
        q[N__NAME] += -1*qREGION2[make_tuple(N__NAME,REGIONKEY)]*-1;
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it660 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end659 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it660 != qCUSTOMER1_end659; ++qCUSTOMER1_it660)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it660->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER1_it660->first);
        string N__NAME = get<2>(qCUSTOMER1_it660->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it658 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end657 = 
            REGION.end();
        for (; REGION_it658 != REGION_end657; ++REGION_it658)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it658);
            string protect_R__NAME = get<1>(*REGION_it658);
            string protect_R__COMMENT = get<2>(*REGION_it658);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it656 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end655 = NATION.end();
            for (; NATION_it656 != NATION_end655; ++NATION_it656)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it656);
                string protect_N__NAME = get<1>(*NATION_it656);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it656);
                string protect_N__COMMENT = get<3>(*NATION_it656);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it654 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end653 = SUPPLIER.end();
                for (; SUPPLIER_it654 != SUPPLIER_end653; ++SUPPLIER_it654)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it654);
                    string protect_S__NAME = get<1>(*SUPPLIER_it654);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it654);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it654);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it654);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it654);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it654);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it652 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end651 = ORDERS.end();
                    for (; ORDERS_it652 != ORDERS_end651; ++ORDERS_it652)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it652);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it652);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it652);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it652);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it652);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it652);
                        string protect_O__CLERK = get<6>(*ORDERS_it652);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it652);
                        string protect_O__COMMENT = get<8>(*ORDERS_it652);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it650 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end649 = LINEITEM.end();
                        for (
                            ; LINEITEM_it650 != LINEITEM_end649; ++LINEITEM_it650)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it650);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it650);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it650);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it650);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it650);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it650);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it650);
                            double protect_L__TAX = get<7>(*LINEITEM_it650);
                            int64_t var749 = 0;
                            var749 += protect_N__REGIONKEY;
                            int64_t var750 = 0;
                            var750 += protect_R__REGIONKEY;
                            if ( var749 == var750 )
                            {
                                int64_t var747 = 0;
                                var747 += protect_L__SUPPKEY;
                                int64_t var748 = 0;
                                var748 += protect_S__SUPPKEY;
                                if ( var747 == var748 )
                                {
                                    int64_t var745 = 0;
                                    var745 += protect_L__ORDERKEY;
                                    int64_t var746 = 0;
                                    var746 += protect_O__ORDERKEY;
                                    if ( var745 == var746 )
                                    {
                                        string var743 = 0;
                                        var743 += N__NAME;
                                        string var744 = 0;
                                        var744 += protect_N__NAME;
                                        if ( var743 == var744 )
                                        {
                                            int64_t var741 = 0;
                                            var741 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var742 = 0;
                                            var742 += protect_N__NATIONKEY;
                                            if ( var741 == var742 )
                                            {
                                                int64_t var739 = 0;
                                                var739 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var740 = 0;
                                                var740 += protect_S__NATIONKEY;
                                                if ( var739 == var740 )
                                                {
                                                    int64_t var737 = 0;
                                                    var737 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var738 = 0;
                                                    var738 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var737 == var738 )
                                                    {
                                                        qCUSTOMER1[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it672 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end671 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it672 != qCUSTOMER2_end671; ++qCUSTOMER2_it672)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it672->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER2_it672->first);
        string N__NAME = get<2>(qCUSTOMER2_it672->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it670 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end669 = 
            REGION.end();
        for (; REGION_it670 != REGION_end669; ++REGION_it670)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it670);
            string protect_R__NAME = get<1>(*REGION_it670);
            string protect_R__COMMENT = get<2>(*REGION_it670);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it668 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end667 = NATION.end();
            for (; NATION_it668 != NATION_end667; ++NATION_it668)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it668);
                string protect_N__NAME = get<1>(*NATION_it668);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it668);
                string protect_N__COMMENT = get<3>(*NATION_it668);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it666 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end665 = SUPPLIER.end();
                for (; SUPPLIER_it666 != SUPPLIER_end665; ++SUPPLIER_it666)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it666);
                    string protect_S__NAME = get<1>(*SUPPLIER_it666);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it666);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it666);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it666);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it666);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it666);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it664 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end663 = ORDERS.end();
                    for (; ORDERS_it664 != ORDERS_end663; ++ORDERS_it664)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it664);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it664);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it664);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it664);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it664);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it664);
                        string protect_O__CLERK = get<6>(*ORDERS_it664);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it664);
                        string protect_O__COMMENT = get<8>(*ORDERS_it664);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it662 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end661 = LINEITEM.end();
                        for (
                            ; LINEITEM_it662 != LINEITEM_end661; ++LINEITEM_it662)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it662);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it662);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it662);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it662);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it662);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it662);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it662);
                            double protect_L__TAX = get<7>(*LINEITEM_it662);
                            int64_t var764 = 0;
                            var764 += protect_N__REGIONKEY;
                            int64_t var765 = 0;
                            var765 += protect_R__REGIONKEY;
                            if ( var764 == var765 )
                            {
                                int64_t var762 = 0;
                                var762 += protect_L__SUPPKEY;
                                int64_t var763 = 0;
                                var763 += protect_S__SUPPKEY;
                                if ( var762 == var763 )
                                {
                                    int64_t var760 = 0;
                                    var760 += protect_L__ORDERKEY;
                                    int64_t var761 = 0;
                                    var761 += protect_O__ORDERKEY;
                                    if ( var760 == var761 )
                                    {
                                        string var758 = 0;
                                        var758 += N__NAME;
                                        string var759 = 0;
                                        var759 += protect_N__NAME;
                                        if ( var758 == var759 )
                                        {
                                            int64_t var756 = 0;
                                            var756 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var757 = 0;
                                            var757 += protect_N__NATIONKEY;
                                            if ( var756 == var757 )
                                            {
                                                int64_t var754 = 0;
                                                var754 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var755 = 0;
                                                var755 += protect_S__NATIONKEY;
                                                if ( var754 == var755 )
                                                {
                                                    int64_t var752 = 0;
                                                    var752 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var753 = 0;
                                                    var753 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var752 == var753 )
                                                    {
                                                        double var751 = 1;
                                                        var751 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var751 *= 
                                                            protect_L__DISCOUNT;
                                                        qCUSTOMER2[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += var751;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it684 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end683 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it684 != qLINEITEM1_end683; ++qLINEITEM1_it684)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it684->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it684->first);
        string N__NAME = get<2>(qLINEITEM1_it684->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it682 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end681 = 
            REGION.end();
        for (; REGION_it682 != REGION_end681; ++REGION_it682)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it682);
            string protect_R__NAME = get<1>(*REGION_it682);
            string protect_R__COMMENT = get<2>(*REGION_it682);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it680 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end679 = NATION.end();
            for (; NATION_it680 != NATION_end679; ++NATION_it680)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it680);
                string protect_N__NAME = get<1>(*NATION_it680);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it680);
                string protect_N__COMMENT = get<3>(*NATION_it680);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it678 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end677 = SUPPLIER.end();
                for (; SUPPLIER_it678 != SUPPLIER_end677; ++SUPPLIER_it678)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it678);
                    string protect_S__NAME = get<1>(*SUPPLIER_it678);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it678);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it678);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it678);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it678);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it678);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_it676 = CUSTOMER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_end675 = CUSTOMER.end();
                    for (; CUSTOMER_it676 != CUSTOMER_end675; ++CUSTOMER_it676)
                    {
                        int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it676);
                        string protect_C__NAME = get<1>(*CUSTOMER_it676);
                        string protect_C__ADDRESS = get<2>(*CUSTOMER_it676);
                        int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it676);
                        string protect_C__PHONE = get<4>(*CUSTOMER_it676);
                        double protect_C__ACCTBAL = get<5>(*CUSTOMER_it676);
                        string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it676);
                        string protect_C__COMMENT = get<7>(*CUSTOMER_it676);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it674 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end673 = ORDERS.end();
                        for (; ORDERS_it674 != ORDERS_end673; ++ORDERS_it674)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it674);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it674);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it674);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it674);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it674);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it674);
                            string protect_O__CLERK = get<6>(*ORDERS_it674);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it674);
                            string protect_O__COMMENT = get<8>(*ORDERS_it674);
                            int64_t var778 = 0;
                            var778 += protect_N__REGIONKEY;
                            int64_t var779 = 0;
                            var779 += protect_R__REGIONKEY;
                            if ( var778 == var779 )
                            {
                                int64_t var776 = 0;
                                var776 += protect_C__NATIONKEY;
                                int64_t var777 = 0;
                                var777 += protect_N__NATIONKEY;
                                if ( var776 == var777 )
                                {
                                    int64_t var774 = 0;
                                    var774 += protect_C__NATIONKEY;
                                    int64_t var775 = 0;
                                    var775 += protect_S__NATIONKEY;
                                    if ( var774 == var775 )
                                    {
                                        int64_t var772 = 0;
                                        var772 += protect_C__CUSTKEY;
                                        int64_t var773 = 0;
                                        var773 += protect_O__CUSTKEY;
                                        if ( var772 == var773 )
                                        {
                                            string var770 = 0;
                                            var770 += N__NAME;
                                            string var771 = 0;
                                            var771 += protect_N__NAME;
                                            if ( var770 == var771 )
                                            {
                                                int64_t var768 = 0;
                                                var768 += 
                                                    x_qLINEITEM_L__SUPPKEY;
                                                int64_t var769 = 0;
                                                var769 += protect_S__SUPPKEY;
                                                if ( var768 == var769 )
                                                {
                                                    int64_t var766 = 0;
                                                    var766 += 
                                                        x_qLINEITEM_L__ORDERKEY;
                                                    int64_t var767 = 0;
                                                    var767 += 
                                                        protect_O__ORDERKEY;
                                                    if ( var766 == var767 )
                                                    {
                                                        qLINEITEM1[make_tuple(
                                                            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += 1;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,int>::iterator qNATION2_it688 = qNATION2.begin();
    map<int64_t,int>::iterator qNATION2_end687 = qNATION2.end();
    for (; qNATION2_it688 != qNATION2_end687; ++qNATION2_it688)
    {
        int64_t x_qNATION_N__REGIONKEY = qNATION2_it688->first;
        qNATION2[x_qNATION_N__REGIONKEY] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it686 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end685 = 
            REGION.end();
        for (; REGION_it686 != REGION_end685; ++REGION_it686)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it686);
            string protect_R__NAME = get<1>(*REGION_it686);
            string protect_R__COMMENT = get<2>(*REGION_it686);
            int64_t var780 = 0;
            var780 += x_qNATION_N__REGIONKEY;
            int64_t var781 = 0;
            var781 += protect_R__REGIONKEY;
            if ( var780 == var781 )
            {
                qNATION2[x_qNATION_N__REGIONKEY] += 1;
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it700 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end699 = 
        qORDERS1.end();
    for (; qORDERS1_it700 != qORDERS1_end699; ++qORDERS1_it700)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it700->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it700->first);
        string N__NAME = get<2>(qORDERS1_it700->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it698 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end697 = 
            REGION.end();
        for (; REGION_it698 != REGION_end697; ++REGION_it698)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it698);
            string protect_R__NAME = get<1>(*REGION_it698);
            string protect_R__COMMENT = get<2>(*REGION_it698);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it696 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end695 = NATION.end();
            for (; NATION_it696 != NATION_end695; ++NATION_it696)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it696);
                string protect_N__NAME = get<1>(*NATION_it696);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it696);
                string protect_N__COMMENT = get<3>(*NATION_it696);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it694 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end693 = CUSTOMER.end();
                for (; CUSTOMER_it694 != CUSTOMER_end693; ++CUSTOMER_it694)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it694);
                    string protect_C__NAME = get<1>(*CUSTOMER_it694);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it694);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it694);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it694);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it694);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it694);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it694);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it692 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end691 = SUPPLIER.end();
                    for (; SUPPLIER_it692 != SUPPLIER_end691; ++SUPPLIER_it692)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it692);
                        string protect_S__NAME = get<1>(*SUPPLIER_it692);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it692);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it692);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it692);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it692);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it692);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it690 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end689 = LINEITEM.end();
                        for (
                            ; LINEITEM_it690 != LINEITEM_end689; ++LINEITEM_it690)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it690);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it690);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it690);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it690);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it690);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it690);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it690);
                            double protect_L__TAX = get<7>(*LINEITEM_it690);
                            int64_t var794 = 0;
                            var794 += protect_N__REGIONKEY;
                            int64_t var795 = 0;
                            var795 += protect_R__REGIONKEY;
                            if ( var794 == var795 )
                            {
                                int64_t var792 = 0;
                                var792 += protect_C__NATIONKEY;
                                int64_t var793 = 0;
                                var793 += protect_N__NATIONKEY;
                                if ( var792 == var793 )
                                {
                                    int64_t var790 = 0;
                                    var790 += protect_C__NATIONKEY;
                                    int64_t var791 = 0;
                                    var791 += protect_S__NATIONKEY;
                                    if ( var790 == var791 )
                                    {
                                        int64_t var788 = 0;
                                        var788 += protect_L__SUPPKEY;
                                        int64_t var789 = 0;
                                        var789 += protect_S__SUPPKEY;
                                        if ( var788 == var789 )
                                        {
                                            string var786 = 0;
                                            var786 += N__NAME;
                                            string var787 = 0;
                                            var787 += protect_N__NAME;
                                            if ( var786 == var787 )
                                            {
                                                int64_t var784 = 0;
                                                var784 += x_qORDERS_O__CUSTKEY;
                                                int64_t var785 = 0;
                                                var785 += protect_C__CUSTKEY;
                                                if ( var784 == var785 )
                                                {
                                                    int64_t var782 = 0;
                                                    var782 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var783 = 0;
                                                    var783 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var782 == var783 )
                                                    {
                                                        qORDERS1[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it712 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end711 = 
        qORDERS2.end();
    for (; qORDERS2_it712 != qORDERS2_end711; ++qORDERS2_it712)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it712->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it712->first);
        string N__NAME = get<2>(qORDERS2_it712->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it710 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end709 = 
            REGION.end();
        for (; REGION_it710 != REGION_end709; ++REGION_it710)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it710);
            string protect_R__NAME = get<1>(*REGION_it710);
            string protect_R__COMMENT = get<2>(*REGION_it710);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it708 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end707 = NATION.end();
            for (; NATION_it708 != NATION_end707; ++NATION_it708)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it708);
                string protect_N__NAME = get<1>(*NATION_it708);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it708);
                string protect_N__COMMENT = get<3>(*NATION_it708);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it706 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end705 = CUSTOMER.end();
                for (; CUSTOMER_it706 != CUSTOMER_end705; ++CUSTOMER_it706)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it706);
                    string protect_C__NAME = get<1>(*CUSTOMER_it706);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it706);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it706);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it706);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it706);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it706);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it706);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it704 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end703 = SUPPLIER.end();
                    for (; SUPPLIER_it704 != SUPPLIER_end703; ++SUPPLIER_it704)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it704);
                        string protect_S__NAME = get<1>(*SUPPLIER_it704);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it704);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it704);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it704);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it704);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it704);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it702 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end701 = LINEITEM.end();
                        for (
                            ; LINEITEM_it702 != LINEITEM_end701; ++LINEITEM_it702)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it702);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it702);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it702);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it702);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it702);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it702);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it702);
                            double protect_L__TAX = get<7>(*LINEITEM_it702);
                            int64_t var809 = 0;
                            var809 += protect_N__REGIONKEY;
                            int64_t var810 = 0;
                            var810 += protect_R__REGIONKEY;
                            if ( var809 == var810 )
                            {
                                int64_t var807 = 0;
                                var807 += protect_C__NATIONKEY;
                                int64_t var808 = 0;
                                var808 += protect_N__NATIONKEY;
                                if ( var807 == var808 )
                                {
                                    int64_t var805 = 0;
                                    var805 += protect_C__NATIONKEY;
                                    int64_t var806 = 0;
                                    var806 += protect_S__NATIONKEY;
                                    if ( var805 == var806 )
                                    {
                                        int64_t var803 = 0;
                                        var803 += protect_L__SUPPKEY;
                                        int64_t var804 = 0;
                                        var804 += protect_S__SUPPKEY;
                                        if ( var803 == var804 )
                                        {
                                            string var801 = 0;
                                            var801 += N__NAME;
                                            string var802 = 0;
                                            var802 += protect_N__NAME;
                                            if ( var801 == var802 )
                                            {
                                                int64_t var799 = 0;
                                                var799 += x_qORDERS_O__CUSTKEY;
                                                int64_t var800 = 0;
                                                var800 += protect_C__CUSTKEY;
                                                if ( var799 == var800 )
                                                {
                                                    int64_t var797 = 0;
                                                    var797 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var798 = 0;
                                                    var798 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var797 == var798 )
                                                    {
                                                        double var796 = 1;
                                                        var796 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var796 *= 
                                                            protect_L__DISCOUNT;
                                                        qORDERS2[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += var796;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it724 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end723 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it724 != qSUPPLIER1_end723; ++qSUPPLIER1_it724)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it724->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER1_it724->first);
        string N__NAME = get<2>(qSUPPLIER1_it724->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it722 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end721 = 
            REGION.end();
        for (; REGION_it722 != REGION_end721; ++REGION_it722)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it722);
            string protect_R__NAME = get<1>(*REGION_it722);
            string protect_R__COMMENT = get<2>(*REGION_it722);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it720 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end719 = NATION.end();
            for (; NATION_it720 != NATION_end719; ++NATION_it720)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it720);
                string protect_N__NAME = get<1>(*NATION_it720);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it720);
                string protect_N__COMMENT = get<3>(*NATION_it720);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it718 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end717 = CUSTOMER.end();
                for (; CUSTOMER_it718 != CUSTOMER_end717; ++CUSTOMER_it718)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it718);
                    string protect_C__NAME = get<1>(*CUSTOMER_it718);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it718);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it718);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it718);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it718);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it718);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it718);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it716 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end715 = ORDERS.end();
                    for (; ORDERS_it716 != ORDERS_end715; ++ORDERS_it716)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it716);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it716);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it716);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it716);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it716);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it716);
                        string protect_O__CLERK = get<6>(*ORDERS_it716);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it716);
                        string protect_O__COMMENT = get<8>(*ORDERS_it716);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it714 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end713 = LINEITEM.end();
                        for (
                            ; LINEITEM_it714 != LINEITEM_end713; ++LINEITEM_it714)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it714);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it714);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it714);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it714);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it714);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it714);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it714);
                            double protect_L__TAX = get<7>(*LINEITEM_it714);
                            int64_t var823 = 0;
                            var823 += protect_N__REGIONKEY;
                            int64_t var824 = 0;
                            var824 += protect_R__REGIONKEY;
                            if ( var823 == var824 )
                            {
                                int64_t var821 = 0;
                                var821 += protect_C__CUSTKEY;
                                int64_t var822 = 0;
                                var822 += protect_O__CUSTKEY;
                                if ( var821 == var822 )
                                {
                                    int64_t var819 = 0;
                                    var819 += protect_L__ORDERKEY;
                                    int64_t var820 = 0;
                                    var820 += protect_O__ORDERKEY;
                                    if ( var819 == var820 )
                                    {
                                        string var817 = 0;
                                        var817 += N__NAME;
                                        string var818 = 0;
                                        var818 += protect_N__NAME;
                                        if ( var817 == var818 )
                                        {
                                            int64_t var815 = 0;
                                            var815 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var816 = 0;
                                            var816 += protect_N__NATIONKEY;
                                            if ( var815 == var816 )
                                            {
                                                int64_t var813 = 0;
                                                var813 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var814 = 0;
                                                var814 += protect_C__NATIONKEY;
                                                if ( var813 == var814 )
                                                {
                                                    int64_t var811 = 0;
                                                    var811 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var812 = 0;
                                                    var812 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var811 == var812 )
                                                    {
                                                        qSUPPLIER1[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it736 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end735 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it736 != qSUPPLIER2_end735; ++qSUPPLIER2_it736)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it736->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER2_it736->first);
        string N__NAME = get<2>(qSUPPLIER2_it736->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it734 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end733 = 
            REGION.end();
        for (; REGION_it734 != REGION_end733; ++REGION_it734)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it734);
            string protect_R__NAME = get<1>(*REGION_it734);
            string protect_R__COMMENT = get<2>(*REGION_it734);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it732 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end731 = NATION.end();
            for (; NATION_it732 != NATION_end731; ++NATION_it732)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it732);
                string protect_N__NAME = get<1>(*NATION_it732);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it732);
                string protect_N__COMMENT = get<3>(*NATION_it732);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it730 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end729 = CUSTOMER.end();
                for (; CUSTOMER_it730 != CUSTOMER_end729; ++CUSTOMER_it730)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it730);
                    string protect_C__NAME = get<1>(*CUSTOMER_it730);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it730);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it730);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it730);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it730);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it730);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it730);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it728 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end727 = ORDERS.end();
                    for (; ORDERS_it728 != ORDERS_end727; ++ORDERS_it728)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it728);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it728);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it728);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it728);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it728);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it728);
                        string protect_O__CLERK = get<6>(*ORDERS_it728);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it728);
                        string protect_O__COMMENT = get<8>(*ORDERS_it728);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it726 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end725 = LINEITEM.end();
                        for (
                            ; LINEITEM_it726 != LINEITEM_end725; ++LINEITEM_it726)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it726);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it726);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it726);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it726);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it726);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it726);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it726);
                            double protect_L__TAX = get<7>(*LINEITEM_it726);
                            int64_t var838 = 0;
                            var838 += protect_N__REGIONKEY;
                            int64_t var839 = 0;
                            var839 += protect_R__REGIONKEY;
                            if ( var838 == var839 )
                            {
                                int64_t var836 = 0;
                                var836 += protect_C__CUSTKEY;
                                int64_t var837 = 0;
                                var837 += protect_O__CUSTKEY;
                                if ( var836 == var837 )
                                {
                                    int64_t var834 = 0;
                                    var834 += protect_L__ORDERKEY;
                                    int64_t var835 = 0;
                                    var835 += protect_O__ORDERKEY;
                                    if ( var834 == var835 )
                                    {
                                        string var832 = 0;
                                        var832 += N__NAME;
                                        string var833 = 0;
                                        var833 += protect_N__NAME;
                                        if ( var832 == var833 )
                                        {
                                            int64_t var830 = 0;
                                            var830 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var831 = 0;
                                            var831 += protect_N__NATIONKEY;
                                            if ( var830 == var831 )
                                            {
                                                int64_t var828 = 0;
                                                var828 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var829 = 0;
                                                var829 += protect_C__NATIONKEY;
                                                if ( var828 == var829 )
                                                {
                                                    int64_t var826 = 0;
                                                    var826 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var827 = 0;
                                                    var827 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var826 == var827 )
                                                    {
                                                        double var825 = 1;
                                                        var825 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var825 *= 
                                                            protect_L__DISCOUNT;
                                                        qSUPPLIER2[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += var825;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_REGION_sec_span, on_delete_REGION_usec_span);
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
    map<string,double>::iterator q_it738 = q.begin();
    map<string,double>::iterator q_end737 = q.end();
    for (; q_it738 != q_end737; ++q_it738)
    {
        string N__NAME = q_it738->first;
        q[N__NAME] += -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(
            ORDERKEY,SUPPKEY,N__NAME)]*-1;
    }
    map<string,double>::iterator q_it740 = q.begin();
    map<string,double>::iterator q_end739 = q.end();
    for (; q_it740 != q_end739; ++q_it740)
    {
        string N__NAME = q_it740->first;
        q[N__NAME] += -1*EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,SUPPKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it752 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end751 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it752 != qCUSTOMER1_end751; ++qCUSTOMER1_it752)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it752->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER1_it752->first);
        string N__NAME = get<2>(qCUSTOMER1_it752->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it750 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end749 = 
            REGION.end();
        for (; REGION_it750 != REGION_end749; ++REGION_it750)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it750);
            string protect_R__NAME = get<1>(*REGION_it750);
            string protect_R__COMMENT = get<2>(*REGION_it750);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it748 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end747 = NATION.end();
            for (; NATION_it748 != NATION_end747; ++NATION_it748)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it748);
                string protect_N__NAME = get<1>(*NATION_it748);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it748);
                string protect_N__COMMENT = get<3>(*NATION_it748);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it746 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end745 = SUPPLIER.end();
                for (; SUPPLIER_it746 != SUPPLIER_end745; ++SUPPLIER_it746)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it746);
                    string protect_S__NAME = get<1>(*SUPPLIER_it746);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it746);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it746);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it746);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it746);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it746);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it744 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end743 = ORDERS.end();
                    for (; ORDERS_it744 != ORDERS_end743; ++ORDERS_it744)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it744);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it744);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it744);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it744);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it744);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it744);
                        string protect_O__CLERK = get<6>(*ORDERS_it744);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it744);
                        string protect_O__COMMENT = get<8>(*ORDERS_it744);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it742 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end741 = LINEITEM.end();
                        for (
                            ; LINEITEM_it742 != LINEITEM_end741; ++LINEITEM_it742)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it742);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it742);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it742);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it742);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it742);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it742);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it742);
                            double protect_L__TAX = get<7>(*LINEITEM_it742);
                            int64_t var852 = 0;
                            var852 += protect_N__REGIONKEY;
                            int64_t var853 = 0;
                            var853 += protect_R__REGIONKEY;
                            if ( var852 == var853 )
                            {
                                int64_t var850 = 0;
                                var850 += protect_L__SUPPKEY;
                                int64_t var851 = 0;
                                var851 += protect_S__SUPPKEY;
                                if ( var850 == var851 )
                                {
                                    int64_t var848 = 0;
                                    var848 += protect_L__ORDERKEY;
                                    int64_t var849 = 0;
                                    var849 += protect_O__ORDERKEY;
                                    if ( var848 == var849 )
                                    {
                                        string var846 = 0;
                                        var846 += N__NAME;
                                        string var847 = 0;
                                        var847 += protect_N__NAME;
                                        if ( var846 == var847 )
                                        {
                                            int64_t var844 = 0;
                                            var844 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var845 = 0;
                                            var845 += protect_N__NATIONKEY;
                                            if ( var844 == var845 )
                                            {
                                                int64_t var842 = 0;
                                                var842 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var843 = 0;
                                                var843 += protect_S__NATIONKEY;
                                                if ( var842 == var843 )
                                                {
                                                    int64_t var840 = 0;
                                                    var840 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var841 = 0;
                                                    var841 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var840 == var841 )
                                                    {
                                                        qCUSTOMER1[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it764 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end763 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it764 != qCUSTOMER2_end763; ++qCUSTOMER2_it764)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it764->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER2_it764->first);
        string N__NAME = get<2>(qCUSTOMER2_it764->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it762 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end761 = 
            REGION.end();
        for (; REGION_it762 != REGION_end761; ++REGION_it762)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it762);
            string protect_R__NAME = get<1>(*REGION_it762);
            string protect_R__COMMENT = get<2>(*REGION_it762);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it760 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end759 = NATION.end();
            for (; NATION_it760 != NATION_end759; ++NATION_it760)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it760);
                string protect_N__NAME = get<1>(*NATION_it760);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it760);
                string protect_N__COMMENT = get<3>(*NATION_it760);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it758 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end757 = SUPPLIER.end();
                for (; SUPPLIER_it758 != SUPPLIER_end757; ++SUPPLIER_it758)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it758);
                    string protect_S__NAME = get<1>(*SUPPLIER_it758);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it758);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it758);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it758);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it758);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it758);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it756 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end755 = ORDERS.end();
                    for (; ORDERS_it756 != ORDERS_end755; ++ORDERS_it756)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it756);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it756);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it756);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it756);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it756);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it756);
                        string protect_O__CLERK = get<6>(*ORDERS_it756);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it756);
                        string protect_O__COMMENT = get<8>(*ORDERS_it756);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it754 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end753 = LINEITEM.end();
                        for (
                            ; LINEITEM_it754 != LINEITEM_end753; ++LINEITEM_it754)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it754);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it754);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it754);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it754);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it754);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it754);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it754);
                            double protect_L__TAX = get<7>(*LINEITEM_it754);
                            int64_t var867 = 0;
                            var867 += protect_N__REGIONKEY;
                            int64_t var868 = 0;
                            var868 += protect_R__REGIONKEY;
                            if ( var867 == var868 )
                            {
                                int64_t var865 = 0;
                                var865 += protect_L__SUPPKEY;
                                int64_t var866 = 0;
                                var866 += protect_S__SUPPKEY;
                                if ( var865 == var866 )
                                {
                                    int64_t var863 = 0;
                                    var863 += protect_L__ORDERKEY;
                                    int64_t var864 = 0;
                                    var864 += protect_O__ORDERKEY;
                                    if ( var863 == var864 )
                                    {
                                        string var861 = 0;
                                        var861 += N__NAME;
                                        string var862 = 0;
                                        var862 += protect_N__NAME;
                                        if ( var861 == var862 )
                                        {
                                            int64_t var859 = 0;
                                            var859 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var860 = 0;
                                            var860 += protect_N__NATIONKEY;
                                            if ( var859 == var860 )
                                            {
                                                int64_t var857 = 0;
                                                var857 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var858 = 0;
                                                var858 += protect_S__NATIONKEY;
                                                if ( var857 == var858 )
                                                {
                                                    int64_t var855 = 0;
                                                    var855 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var856 = 0;
                                                    var856 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var855 == var856 )
                                                    {
                                                        double var854 = 1;
                                                        var854 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var854 *= 
                                                            protect_L__DISCOUNT;
                                                        qCUSTOMER2[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += var854;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qNATION1_it774 = qNATION1.begin();
    map<int64_t,double>::iterator qNATION1_end773 = qNATION1.end();
    for (; qNATION1_it774 != qNATION1_end773; ++qNATION1_it774)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION1_it774->first;
        qNATION1[x_qNATION_N__NATIONKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it772 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end771 = CUSTOMER.end();
        for (; CUSTOMER_it772 != CUSTOMER_end771; ++CUSTOMER_it772)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it772);
            string protect_C__NAME = get<1>(*CUSTOMER_it772);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it772);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it772);
            string protect_C__PHONE = get<4>(*CUSTOMER_it772);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it772);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it772);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it772);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it770 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end769 = SUPPLIER.end();
            for (; SUPPLIER_it770 != SUPPLIER_end769; ++SUPPLIER_it770)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it770);
                string protect_S__NAME = get<1>(*SUPPLIER_it770);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it770);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it770);
                string protect_S__PHONE = get<4>(*SUPPLIER_it770);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it770);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it770);
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it768 = ORDERS.begin();
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end767 = ORDERS.end();
                for (; ORDERS_it768 != ORDERS_end767; ++ORDERS_it768)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it768);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it768);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it768);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it768);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it768);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it768);
                    string protect_O__CLERK = get<6>(*ORDERS_it768);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it768);
                    string protect_O__COMMENT = get<8>(*ORDERS_it768);
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it766 = LINEITEM.begin();
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end765 = LINEITEM.end();
                    for (; LINEITEM_it766 != LINEITEM_end765; ++LINEITEM_it766)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it766);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it766);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it766);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it766);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it766);
                        double protect_L__EXTENDEDPRICE = get<5>(
                            *LINEITEM_it766);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it766);
                        double protect_L__TAX = get<7>(*LINEITEM_it766);
                        int64_t var877 = 0;
                        var877 += protect_C__CUSTKEY;
                        int64_t var878 = 0;
                        var878 += protect_O__CUSTKEY;
                        if ( var877 == var878 )
                        {
                            int64_t var875 = 0;
                            var875 += protect_L__SUPPKEY;
                            int64_t var876 = 0;
                            var876 += protect_S__SUPPKEY;
                            if ( var875 == var876 )
                            {
                                int64_t var873 = 0;
                                var873 += protect_L__ORDERKEY;
                                int64_t var874 = 0;
                                var874 += protect_O__ORDERKEY;
                                if ( var873 == var874 )
                                {
                                    int64_t var871 = 0;
                                    var871 += x_qNATION_N__NATIONKEY;
                                    int64_t var872 = 0;
                                    var872 += protect_C__NATIONKEY;
                                    if ( var871 == var872 )
                                    {
                                        int64_t var869 = 0;
                                        var869 += x_qNATION_N__NATIONKEY;
                                        int64_t var870 = 0;
                                        var870 += protect_S__NATIONKEY;
                                        if ( var869 == var870 )
                                        {
                                            qNATION1[x_qNATION_N__NATIONKEY] += 
                                                protect_L__EXTENDEDPRICE;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qNATION3_it784 = qNATION3.begin();
    map<int64_t,double>::iterator qNATION3_end783 = qNATION3.end();
    for (; qNATION3_it784 != qNATION3_end783; ++qNATION3_it784)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION3_it784->first;
        qNATION3[x_qNATION_N__NATIONKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it782 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end781 = CUSTOMER.end();
        for (; CUSTOMER_it782 != CUSTOMER_end781; ++CUSTOMER_it782)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it782);
            string protect_C__NAME = get<1>(*CUSTOMER_it782);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it782);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it782);
            string protect_C__PHONE = get<4>(*CUSTOMER_it782);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it782);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it782);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it782);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it780 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end779 = SUPPLIER.end();
            for (; SUPPLIER_it780 != SUPPLIER_end779; ++SUPPLIER_it780)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it780);
                string protect_S__NAME = get<1>(*SUPPLIER_it780);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it780);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it780);
                string protect_S__PHONE = get<4>(*SUPPLIER_it780);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it780);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it780);
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it778 = ORDERS.begin();
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end777 = ORDERS.end();
                for (; ORDERS_it778 != ORDERS_end777; ++ORDERS_it778)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it778);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it778);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it778);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it778);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it778);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it778);
                    string protect_O__CLERK = get<6>(*ORDERS_it778);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it778);
                    string protect_O__COMMENT = get<8>(*ORDERS_it778);
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it776 = LINEITEM.begin();
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end775 = LINEITEM.end();
                    for (; LINEITEM_it776 != LINEITEM_end775; ++LINEITEM_it776)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it776);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it776);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it776);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it776);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it776);
                        double protect_L__EXTENDEDPRICE = get<5>(
                            *LINEITEM_it776);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it776);
                        double protect_L__TAX = get<7>(*LINEITEM_it776);
                        int64_t var888 = 0;
                        var888 += protect_C__CUSTKEY;
                        int64_t var889 = 0;
                        var889 += protect_O__CUSTKEY;
                        if ( var888 == var889 )
                        {
                            int64_t var886 = 0;
                            var886 += protect_L__SUPPKEY;
                            int64_t var887 = 0;
                            var887 += protect_S__SUPPKEY;
                            if ( var886 == var887 )
                            {
                                int64_t var884 = 0;
                                var884 += protect_L__ORDERKEY;
                                int64_t var885 = 0;
                                var885 += protect_O__ORDERKEY;
                                if ( var884 == var885 )
                                {
                                    int64_t var882 = 0;
                                    var882 += x_qNATION_N__NATIONKEY;
                                    int64_t var883 = 0;
                                    var883 += protect_C__NATIONKEY;
                                    if ( var882 == var883 )
                                    {
                                        int64_t var880 = 0;
                                        var880 += x_qNATION_N__NATIONKEY;
                                        int64_t var881 = 0;
                                        var881 += protect_S__NATIONKEY;
                                        if ( var880 == var881 )
                                        {
                                            double var879 = 1;
                                            var879 *= protect_L__EXTENDEDPRICE;
                                            var879 *= protect_L__DISCOUNT;
                                            qNATION3[x_qNATION_N__NATIONKEY] += 
                                                var879;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it796 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end795 = 
        qORDERS1.end();
    for (; qORDERS1_it796 != qORDERS1_end795; ++qORDERS1_it796)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it796->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it796->first);
        string N__NAME = get<2>(qORDERS1_it796->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it794 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end793 = 
            REGION.end();
        for (; REGION_it794 != REGION_end793; ++REGION_it794)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it794);
            string protect_R__NAME = get<1>(*REGION_it794);
            string protect_R__COMMENT = get<2>(*REGION_it794);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it792 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end791 = NATION.end();
            for (; NATION_it792 != NATION_end791; ++NATION_it792)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it792);
                string protect_N__NAME = get<1>(*NATION_it792);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it792);
                string protect_N__COMMENT = get<3>(*NATION_it792);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it790 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end789 = CUSTOMER.end();
                for (; CUSTOMER_it790 != CUSTOMER_end789; ++CUSTOMER_it790)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it790);
                    string protect_C__NAME = get<1>(*CUSTOMER_it790);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it790);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it790);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it790);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it790);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it790);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it790);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it788 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end787 = SUPPLIER.end();
                    for (; SUPPLIER_it788 != SUPPLIER_end787; ++SUPPLIER_it788)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it788);
                        string protect_S__NAME = get<1>(*SUPPLIER_it788);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it788);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it788);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it788);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it788);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it788);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it786 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end785 = LINEITEM.end();
                        for (
                            ; LINEITEM_it786 != LINEITEM_end785; ++LINEITEM_it786)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it786);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it786);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it786);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it786);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it786);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it786);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it786);
                            double protect_L__TAX = get<7>(*LINEITEM_it786);
                            int64_t var902 = 0;
                            var902 += protect_N__REGIONKEY;
                            int64_t var903 = 0;
                            var903 += protect_R__REGIONKEY;
                            if ( var902 == var903 )
                            {
                                int64_t var900 = 0;
                                var900 += protect_C__NATIONKEY;
                                int64_t var901 = 0;
                                var901 += protect_N__NATIONKEY;
                                if ( var900 == var901 )
                                {
                                    int64_t var898 = 0;
                                    var898 += protect_C__NATIONKEY;
                                    int64_t var899 = 0;
                                    var899 += protect_S__NATIONKEY;
                                    if ( var898 == var899 )
                                    {
                                        int64_t var896 = 0;
                                        var896 += protect_L__SUPPKEY;
                                        int64_t var897 = 0;
                                        var897 += protect_S__SUPPKEY;
                                        if ( var896 == var897 )
                                        {
                                            string var894 = 0;
                                            var894 += N__NAME;
                                            string var895 = 0;
                                            var895 += protect_N__NAME;
                                            if ( var894 == var895 )
                                            {
                                                int64_t var892 = 0;
                                                var892 += x_qORDERS_O__CUSTKEY;
                                                int64_t var893 = 0;
                                                var893 += protect_C__CUSTKEY;
                                                if ( var892 == var893 )
                                                {
                                                    int64_t var890 = 0;
                                                    var890 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var891 = 0;
                                                    var891 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var890 == var891 )
                                                    {
                                                        qORDERS1[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it808 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end807 = 
        qORDERS2.end();
    for (; qORDERS2_it808 != qORDERS2_end807; ++qORDERS2_it808)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it808->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it808->first);
        string N__NAME = get<2>(qORDERS2_it808->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it806 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end805 = 
            REGION.end();
        for (; REGION_it806 != REGION_end805; ++REGION_it806)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it806);
            string protect_R__NAME = get<1>(*REGION_it806);
            string protect_R__COMMENT = get<2>(*REGION_it806);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it804 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end803 = NATION.end();
            for (; NATION_it804 != NATION_end803; ++NATION_it804)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it804);
                string protect_N__NAME = get<1>(*NATION_it804);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it804);
                string protect_N__COMMENT = get<3>(*NATION_it804);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it802 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end801 = CUSTOMER.end();
                for (; CUSTOMER_it802 != CUSTOMER_end801; ++CUSTOMER_it802)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it802);
                    string protect_C__NAME = get<1>(*CUSTOMER_it802);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it802);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it802);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it802);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it802);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it802);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it802);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it800 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end799 = SUPPLIER.end();
                    for (; SUPPLIER_it800 != SUPPLIER_end799; ++SUPPLIER_it800)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it800);
                        string protect_S__NAME = get<1>(*SUPPLIER_it800);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it800);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it800);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it800);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it800);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it800);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it798 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end797 = LINEITEM.end();
                        for (
                            ; LINEITEM_it798 != LINEITEM_end797; ++LINEITEM_it798)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it798);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it798);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it798);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it798);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it798);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it798);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it798);
                            double protect_L__TAX = get<7>(*LINEITEM_it798);
                            int64_t var917 = 0;
                            var917 += protect_N__REGIONKEY;
                            int64_t var918 = 0;
                            var918 += protect_R__REGIONKEY;
                            if ( var917 == var918 )
                            {
                                int64_t var915 = 0;
                                var915 += protect_C__NATIONKEY;
                                int64_t var916 = 0;
                                var916 += protect_N__NATIONKEY;
                                if ( var915 == var916 )
                                {
                                    int64_t var913 = 0;
                                    var913 += protect_C__NATIONKEY;
                                    int64_t var914 = 0;
                                    var914 += protect_S__NATIONKEY;
                                    if ( var913 == var914 )
                                    {
                                        int64_t var911 = 0;
                                        var911 += protect_L__SUPPKEY;
                                        int64_t var912 = 0;
                                        var912 += protect_S__SUPPKEY;
                                        if ( var911 == var912 )
                                        {
                                            string var909 = 0;
                                            var909 += N__NAME;
                                            string var910 = 0;
                                            var910 += protect_N__NAME;
                                            if ( var909 == var910 )
                                            {
                                                int64_t var907 = 0;
                                                var907 += x_qORDERS_O__CUSTKEY;
                                                int64_t var908 = 0;
                                                var908 += protect_C__CUSTKEY;
                                                if ( var907 == var908 )
                                                {
                                                    int64_t var905 = 0;
                                                    var905 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var906 = 0;
                                                    var906 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var905 == var906 )
                                                    {
                                                        double var904 = 1;
                                                        var904 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var904 *= 
                                                            protect_L__DISCOUNT;
                                                        qORDERS2[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += var904;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<string,int64_t>,double>::iterator qREGION1_it820 = qREGION1.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION1_end819 = qREGION1.end(
        );
    for (; qREGION1_it820 != qREGION1_end819; ++qREGION1_it820)
    {
        string N__NAME = get<0>(qREGION1_it820->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it820->first);
        qREGION1[make_tuple(N__NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_it818 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end817 
            = NATION.end();
        for (; NATION_it818 != NATION_end817; ++NATION_it818)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it818);
            string protect_N__NAME = get<1>(*NATION_it818);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it818);
            string protect_N__COMMENT = get<3>(*NATION_it818);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it816 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end815 = CUSTOMER.end();
            for (; CUSTOMER_it816 != CUSTOMER_end815; ++CUSTOMER_it816)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it816);
                string protect_C__NAME = get<1>(*CUSTOMER_it816);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it816);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it816);
                string protect_C__PHONE = get<4>(*CUSTOMER_it816);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it816);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it816);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it816);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it814 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end813 = SUPPLIER.end();
                for (; SUPPLIER_it814 != SUPPLIER_end813; ++SUPPLIER_it814)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it814);
                    string protect_S__NAME = get<1>(*SUPPLIER_it814);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it814);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it814);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it814);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it814);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it814);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it812 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end811 = ORDERS.end();
                    for (; ORDERS_it812 != ORDERS_end811; ++ORDERS_it812)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it812);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it812);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it812);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it812);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it812);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it812);
                        string protect_O__CLERK = get<6>(*ORDERS_it812);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it812);
                        string protect_O__COMMENT = get<8>(*ORDERS_it812);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it810 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end809 = LINEITEM.end();
                        for (
                            ; LINEITEM_it810 != LINEITEM_end809; ++LINEITEM_it810)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it810);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it810);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it810);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it810);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it810);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it810);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it810);
                            double protect_L__TAX = get<7>(*LINEITEM_it810);
                            int64_t var931 = 0;
                            var931 += protect_C__NATIONKEY;
                            int64_t var932 = 0;
                            var932 += protect_N__NATIONKEY;
                            if ( var931 == var932 )
                            {
                                int64_t var929 = 0;
                                var929 += protect_C__NATIONKEY;
                                int64_t var930 = 0;
                                var930 += protect_S__NATIONKEY;
                                if ( var929 == var930 )
                                {
                                    int64_t var927 = 0;
                                    var927 += protect_C__CUSTKEY;
                                    int64_t var928 = 0;
                                    var928 += protect_O__CUSTKEY;
                                    if ( var927 == var928 )
                                    {
                                        int64_t var925 = 0;
                                        var925 += protect_L__SUPPKEY;
                                        int64_t var926 = 0;
                                        var926 += protect_S__SUPPKEY;
                                        if ( var925 == var926 )
                                        {
                                            int64_t var923 = 0;
                                            var923 += protect_L__ORDERKEY;
                                            int64_t var924 = 0;
                                            var924 += protect_O__ORDERKEY;
                                            if ( var923 == var924 )
                                            {
                                                int64_t var921 = 0;
                                                var921 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var922 = 0;
                                                var922 += protect_N__REGIONKEY;
                                                if ( var921 == var922 )
                                                {
                                                    string var919 = 0;
                                                    var919 += N__NAME;
                                                    string var920 = 0;
                                                    var920 += protect_N__NAME;
                                                    if ( var919 == var920 )
                                                    {
                                                        qREGION1[make_tuple(
                                                            N__NAME,x_qREGION_R__REGIONKEY)] += protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it832 = qREGION2.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION2_end831 = qREGION2.end(
        );
    for (; qREGION2_it832 != qREGION2_end831; ++qREGION2_it832)
    {
        string N__NAME = get<0>(qREGION2_it832->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it832->first);
        qREGION2[make_tuple(N__NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_it830 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end829 
            = NATION.end();
        for (; NATION_it830 != NATION_end829; ++NATION_it830)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it830);
            string protect_N__NAME = get<1>(*NATION_it830);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it830);
            string protect_N__COMMENT = get<3>(*NATION_it830);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it828 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end827 = CUSTOMER.end();
            for (; CUSTOMER_it828 != CUSTOMER_end827; ++CUSTOMER_it828)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it828);
                string protect_C__NAME = get<1>(*CUSTOMER_it828);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it828);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it828);
                string protect_C__PHONE = get<4>(*CUSTOMER_it828);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it828);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it828);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it828);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it826 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end825 = SUPPLIER.end();
                for (; SUPPLIER_it826 != SUPPLIER_end825; ++SUPPLIER_it826)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it826);
                    string protect_S__NAME = get<1>(*SUPPLIER_it826);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it826);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it826);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it826);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it826);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it826);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it824 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end823 = ORDERS.end();
                    for (; ORDERS_it824 != ORDERS_end823; ++ORDERS_it824)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it824);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it824);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it824);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it824);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it824);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it824);
                        string protect_O__CLERK = get<6>(*ORDERS_it824);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it824);
                        string protect_O__COMMENT = get<8>(*ORDERS_it824);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it822 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end821 = LINEITEM.end();
                        for (
                            ; LINEITEM_it822 != LINEITEM_end821; ++LINEITEM_it822)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it822);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it822);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it822);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it822);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it822);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it822);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it822);
                            double protect_L__TAX = get<7>(*LINEITEM_it822);
                            int64_t var946 = 0;
                            var946 += protect_C__NATIONKEY;
                            int64_t var947 = 0;
                            var947 += protect_N__NATIONKEY;
                            if ( var946 == var947 )
                            {
                                int64_t var944 = 0;
                                var944 += protect_C__NATIONKEY;
                                int64_t var945 = 0;
                                var945 += protect_S__NATIONKEY;
                                if ( var944 == var945 )
                                {
                                    int64_t var942 = 0;
                                    var942 += protect_C__CUSTKEY;
                                    int64_t var943 = 0;
                                    var943 += protect_O__CUSTKEY;
                                    if ( var942 == var943 )
                                    {
                                        int64_t var940 = 0;
                                        var940 += protect_L__SUPPKEY;
                                        int64_t var941 = 0;
                                        var941 += protect_S__SUPPKEY;
                                        if ( var940 == var941 )
                                        {
                                            int64_t var938 = 0;
                                            var938 += protect_L__ORDERKEY;
                                            int64_t var939 = 0;
                                            var939 += protect_O__ORDERKEY;
                                            if ( var938 == var939 )
                                            {
                                                int64_t var936 = 0;
                                                var936 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var937 = 0;
                                                var937 += protect_N__REGIONKEY;
                                                if ( var936 == var937 )
                                                {
                                                    string var934 = 0;
                                                    var934 += N__NAME;
                                                    string var935 = 0;
                                                    var935 += protect_N__NAME;
                                                    if ( var934 == var935 )
                                                    {
                                                        double var933 = 1;
                                                        var933 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var933 *= 
                                                            protect_L__DISCOUNT;
                                                        qREGION2[make_tuple(
                                                            N__NAME,x_qREGION_R__REGIONKEY)] += var933;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it844 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end843 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it844 != qSUPPLIER1_end843; ++qSUPPLIER1_it844)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it844->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER1_it844->first);
        string N__NAME = get<2>(qSUPPLIER1_it844->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it842 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end841 = 
            REGION.end();
        for (; REGION_it842 != REGION_end841; ++REGION_it842)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it842);
            string protect_R__NAME = get<1>(*REGION_it842);
            string protect_R__COMMENT = get<2>(*REGION_it842);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it840 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end839 = NATION.end();
            for (; NATION_it840 != NATION_end839; ++NATION_it840)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it840);
                string protect_N__NAME = get<1>(*NATION_it840);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it840);
                string protect_N__COMMENT = get<3>(*NATION_it840);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it838 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end837 = CUSTOMER.end();
                for (; CUSTOMER_it838 != CUSTOMER_end837; ++CUSTOMER_it838)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it838);
                    string protect_C__NAME = get<1>(*CUSTOMER_it838);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it838);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it838);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it838);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it838);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it838);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it838);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it836 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end835 = ORDERS.end();
                    for (; ORDERS_it836 != ORDERS_end835; ++ORDERS_it836)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it836);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it836);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it836);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it836);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it836);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it836);
                        string protect_O__CLERK = get<6>(*ORDERS_it836);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it836);
                        string protect_O__COMMENT = get<8>(*ORDERS_it836);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it834 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end833 = LINEITEM.end();
                        for (
                            ; LINEITEM_it834 != LINEITEM_end833; ++LINEITEM_it834)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it834);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it834);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it834);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it834);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it834);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it834);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it834);
                            double protect_L__TAX = get<7>(*LINEITEM_it834);
                            int64_t var960 = 0;
                            var960 += protect_N__REGIONKEY;
                            int64_t var961 = 0;
                            var961 += protect_R__REGIONKEY;
                            if ( var960 == var961 )
                            {
                                int64_t var958 = 0;
                                var958 += protect_C__CUSTKEY;
                                int64_t var959 = 0;
                                var959 += protect_O__CUSTKEY;
                                if ( var958 == var959 )
                                {
                                    int64_t var956 = 0;
                                    var956 += protect_L__ORDERKEY;
                                    int64_t var957 = 0;
                                    var957 += protect_O__ORDERKEY;
                                    if ( var956 == var957 )
                                    {
                                        string var954 = 0;
                                        var954 += N__NAME;
                                        string var955 = 0;
                                        var955 += protect_N__NAME;
                                        if ( var954 == var955 )
                                        {
                                            int64_t var952 = 0;
                                            var952 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var953 = 0;
                                            var953 += protect_N__NATIONKEY;
                                            if ( var952 == var953 )
                                            {
                                                int64_t var950 = 0;
                                                var950 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var951 = 0;
                                                var951 += protect_C__NATIONKEY;
                                                if ( var950 == var951 )
                                                {
                                                    int64_t var948 = 0;
                                                    var948 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var949 = 0;
                                                    var949 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var948 == var949 )
                                                    {
                                                        qSUPPLIER1[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it856 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end855 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it856 != qSUPPLIER2_end855; ++qSUPPLIER2_it856)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it856->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER2_it856->first);
        string N__NAME = get<2>(qSUPPLIER2_it856->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it854 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end853 = 
            REGION.end();
        for (; REGION_it854 != REGION_end853; ++REGION_it854)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it854);
            string protect_R__NAME = get<1>(*REGION_it854);
            string protect_R__COMMENT = get<2>(*REGION_it854);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it852 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end851 = NATION.end();
            for (; NATION_it852 != NATION_end851; ++NATION_it852)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it852);
                string protect_N__NAME = get<1>(*NATION_it852);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it852);
                string protect_N__COMMENT = get<3>(*NATION_it852);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it850 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end849 = CUSTOMER.end();
                for (; CUSTOMER_it850 != CUSTOMER_end849; ++CUSTOMER_it850)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it850);
                    string protect_C__NAME = get<1>(*CUSTOMER_it850);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it850);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it850);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it850);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it850);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it850);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it850);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it848 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end847 = ORDERS.end();
                    for (; ORDERS_it848 != ORDERS_end847; ++ORDERS_it848)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it848);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it848);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it848);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it848);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it848);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it848);
                        string protect_O__CLERK = get<6>(*ORDERS_it848);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it848);
                        string protect_O__COMMENT = get<8>(*ORDERS_it848);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it846 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end845 = LINEITEM.end();
                        for (
                            ; LINEITEM_it846 != LINEITEM_end845; ++LINEITEM_it846)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it846);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it846);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it846);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it846);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it846);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it846);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it846);
                            double protect_L__TAX = get<7>(*LINEITEM_it846);
                            int64_t var975 = 0;
                            var975 += protect_N__REGIONKEY;
                            int64_t var976 = 0;
                            var976 += protect_R__REGIONKEY;
                            if ( var975 == var976 )
                            {
                                int64_t var973 = 0;
                                var973 += protect_C__CUSTKEY;
                                int64_t var974 = 0;
                                var974 += protect_O__CUSTKEY;
                                if ( var973 == var974 )
                                {
                                    int64_t var971 = 0;
                                    var971 += protect_L__ORDERKEY;
                                    int64_t var972 = 0;
                                    var972 += protect_O__ORDERKEY;
                                    if ( var971 == var972 )
                                    {
                                        string var969 = 0;
                                        var969 += N__NAME;
                                        string var970 = 0;
                                        var970 += protect_N__NAME;
                                        if ( var969 == var970 )
                                        {
                                            int64_t var967 = 0;
                                            var967 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var968 = 0;
                                            var968 += protect_N__NATIONKEY;
                                            if ( var967 == var968 )
                                            {
                                                int64_t var965 = 0;
                                                var965 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var966 = 0;
                                                var966 += protect_C__NATIONKEY;
                                                if ( var965 == var966 )
                                                {
                                                    int64_t var963 = 0;
                                                    var963 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var964 = 0;
                                                    var964 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var963 == var964 )
                                                    {
                                                        double var962 = 1;
                                                        var962 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var962 *= 
                                                            protect_L__DISCOUNT;
                                                        qSUPPLIER2[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += var962;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
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
    map<string,double>::iterator q_it858 = q.begin();
    map<string,double>::iterator q_end857 = q.end();
    for (; q_it858 != q_end857; ++q_it858)
    {
        string N__NAME = q_it858->first;
        q[N__NAME] += -1*qORDERS1[make_tuple(ORDERKEY,CUSTKEY,N__NAME)];
    }
    map<string,double>::iterator q_it860 = q.begin();
    map<string,double>::iterator q_end859 = q.end();
    for (; q_it860 != q_end859; ++q_it860)
    {
        string N__NAME = q_it860->first;
        q[N__NAME] += -1*qORDERS2[make_tuple(ORDERKEY,CUSTKEY,N__NAME)]*-1;
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it872 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end871 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it872 != qCUSTOMER1_end871; ++qCUSTOMER1_it872)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it872->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER1_it872->first);
        string N__NAME = get<2>(qCUSTOMER1_it872->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it870 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end869 = 
            REGION.end();
        for (; REGION_it870 != REGION_end869; ++REGION_it870)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it870);
            string protect_R__NAME = get<1>(*REGION_it870);
            string protect_R__COMMENT = get<2>(*REGION_it870);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it868 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end867 = NATION.end();
            for (; NATION_it868 != NATION_end867; ++NATION_it868)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it868);
                string protect_N__NAME = get<1>(*NATION_it868);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it868);
                string protect_N__COMMENT = get<3>(*NATION_it868);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it866 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end865 = SUPPLIER.end();
                for (; SUPPLIER_it866 != SUPPLIER_end865; ++SUPPLIER_it866)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it866);
                    string protect_S__NAME = get<1>(*SUPPLIER_it866);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it866);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it866);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it866);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it866);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it866);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it864 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end863 = ORDERS.end();
                    for (; ORDERS_it864 != ORDERS_end863; ++ORDERS_it864)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it864);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it864);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it864);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it864);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it864);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it864);
                        string protect_O__CLERK = get<6>(*ORDERS_it864);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it864);
                        string protect_O__COMMENT = get<8>(*ORDERS_it864);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it862 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end861 = LINEITEM.end();
                        for (
                            ; LINEITEM_it862 != LINEITEM_end861; ++LINEITEM_it862)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it862);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it862);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it862);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it862);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it862);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it862);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it862);
                            double protect_L__TAX = get<7>(*LINEITEM_it862);
                            int64_t var989 = 0;
                            var989 += protect_N__REGIONKEY;
                            int64_t var990 = 0;
                            var990 += protect_R__REGIONKEY;
                            if ( var989 == var990 )
                            {
                                int64_t var987 = 0;
                                var987 += protect_L__SUPPKEY;
                                int64_t var988 = 0;
                                var988 += protect_S__SUPPKEY;
                                if ( var987 == var988 )
                                {
                                    int64_t var985 = 0;
                                    var985 += protect_L__ORDERKEY;
                                    int64_t var986 = 0;
                                    var986 += protect_O__ORDERKEY;
                                    if ( var985 == var986 )
                                    {
                                        string var983 = 0;
                                        var983 += N__NAME;
                                        string var984 = 0;
                                        var984 += protect_N__NAME;
                                        if ( var983 == var984 )
                                        {
                                            int64_t var981 = 0;
                                            var981 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var982 = 0;
                                            var982 += protect_N__NATIONKEY;
                                            if ( var981 == var982 )
                                            {
                                                int64_t var979 = 0;
                                                var979 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var980 = 0;
                                                var980 += protect_S__NATIONKEY;
                                                if ( var979 == var980 )
                                                {
                                                    int64_t var977 = 0;
                                                    var977 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var978 = 0;
                                                    var978 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var977 == var978 )
                                                    {
                                                        qCUSTOMER1[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it884 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end883 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it884 != qCUSTOMER2_end883; ++qCUSTOMER2_it884)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it884->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER2_it884->first);
        string N__NAME = get<2>(qCUSTOMER2_it884->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it882 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end881 = 
            REGION.end();
        for (; REGION_it882 != REGION_end881; ++REGION_it882)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it882);
            string protect_R__NAME = get<1>(*REGION_it882);
            string protect_R__COMMENT = get<2>(*REGION_it882);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it880 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end879 = NATION.end();
            for (; NATION_it880 != NATION_end879; ++NATION_it880)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it880);
                string protect_N__NAME = get<1>(*NATION_it880);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it880);
                string protect_N__COMMENT = get<3>(*NATION_it880);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it878 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end877 = SUPPLIER.end();
                for (; SUPPLIER_it878 != SUPPLIER_end877; ++SUPPLIER_it878)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it878);
                    string protect_S__NAME = get<1>(*SUPPLIER_it878);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it878);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it878);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it878);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it878);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it878);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it876 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end875 = ORDERS.end();
                    for (; ORDERS_it876 != ORDERS_end875; ++ORDERS_it876)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it876);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it876);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it876);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it876);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it876);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it876);
                        string protect_O__CLERK = get<6>(*ORDERS_it876);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it876);
                        string protect_O__COMMENT = get<8>(*ORDERS_it876);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it874 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end873 = LINEITEM.end();
                        for (
                            ; LINEITEM_it874 != LINEITEM_end873; ++LINEITEM_it874)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it874);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it874);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it874);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it874);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it874);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it874);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it874);
                            double protect_L__TAX = get<7>(*LINEITEM_it874);
                            int64_t var1004 = 0;
                            var1004 += protect_N__REGIONKEY;
                            int64_t var1005 = 0;
                            var1005 += protect_R__REGIONKEY;
                            if ( var1004 == var1005 )
                            {
                                int64_t var1002 = 0;
                                var1002 += protect_L__SUPPKEY;
                                int64_t var1003 = 0;
                                var1003 += protect_S__SUPPKEY;
                                if ( var1002 == var1003 )
                                {
                                    int64_t var1000 = 0;
                                    var1000 += protect_L__ORDERKEY;
                                    int64_t var1001 = 0;
                                    var1001 += protect_O__ORDERKEY;
                                    if ( var1000 == var1001 )
                                    {
                                        string var998 = 0;
                                        var998 += N__NAME;
                                        string var999 = 0;
                                        var999 += protect_N__NAME;
                                        if ( var998 == var999 )
                                        {
                                            int64_t var996 = 0;
                                            var996 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var997 = 0;
                                            var997 += protect_N__NATIONKEY;
                                            if ( var996 == var997 )
                                            {
                                                int64_t var994 = 0;
                                                var994 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var995 = 0;
                                                var995 += protect_S__NATIONKEY;
                                                if ( var994 == var995 )
                                                {
                                                    int64_t var992 = 0;
                                                    var992 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var993 = 0;
                                                    var993 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var992 == var993 )
                                                    {
                                                        double var991 = 1;
                                                        var991 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var991 *= 
                                                            protect_L__DISCOUNT;
                                                        qCUSTOMER2[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += var991;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it896 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end895 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it896 != qLINEITEM1_end895; ++qLINEITEM1_it896)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it896->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it896->first);
        string N__NAME = get<2>(qLINEITEM1_it896->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it894 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end893 = 
            REGION.end();
        for (; REGION_it894 != REGION_end893; ++REGION_it894)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it894);
            string protect_R__NAME = get<1>(*REGION_it894);
            string protect_R__COMMENT = get<2>(*REGION_it894);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it892 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end891 = NATION.end();
            for (; NATION_it892 != NATION_end891; ++NATION_it892)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it892);
                string protect_N__NAME = get<1>(*NATION_it892);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it892);
                string protect_N__COMMENT = get<3>(*NATION_it892);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it890 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end889 = SUPPLIER.end();
                for (; SUPPLIER_it890 != SUPPLIER_end889; ++SUPPLIER_it890)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it890);
                    string protect_S__NAME = get<1>(*SUPPLIER_it890);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it890);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it890);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it890);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it890);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it890);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_it888 = CUSTOMER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_end887 = CUSTOMER.end();
                    for (; CUSTOMER_it888 != CUSTOMER_end887; ++CUSTOMER_it888)
                    {
                        int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it888);
                        string protect_C__NAME = get<1>(*CUSTOMER_it888);
                        string protect_C__ADDRESS = get<2>(*CUSTOMER_it888);
                        int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it888);
                        string protect_C__PHONE = get<4>(*CUSTOMER_it888);
                        double protect_C__ACCTBAL = get<5>(*CUSTOMER_it888);
                        string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it888);
                        string protect_C__COMMENT = get<7>(*CUSTOMER_it888);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it886 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end885 = ORDERS.end();
                        for (; ORDERS_it886 != ORDERS_end885; ++ORDERS_it886)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it886);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it886);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it886);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it886);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it886);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it886);
                            string protect_O__CLERK = get<6>(*ORDERS_it886);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it886);
                            string protect_O__COMMENT = get<8>(*ORDERS_it886);
                            int64_t var1018 = 0;
                            var1018 += protect_N__REGIONKEY;
                            int64_t var1019 = 0;
                            var1019 += protect_R__REGIONKEY;
                            if ( var1018 == var1019 )
                            {
                                int64_t var1016 = 0;
                                var1016 += protect_C__NATIONKEY;
                                int64_t var1017 = 0;
                                var1017 += protect_N__NATIONKEY;
                                if ( var1016 == var1017 )
                                {
                                    int64_t var1014 = 0;
                                    var1014 += protect_C__NATIONKEY;
                                    int64_t var1015 = 0;
                                    var1015 += protect_S__NATIONKEY;
                                    if ( var1014 == var1015 )
                                    {
                                        int64_t var1012 = 0;
                                        var1012 += protect_C__CUSTKEY;
                                        int64_t var1013 = 0;
                                        var1013 += protect_O__CUSTKEY;
                                        if ( var1012 == var1013 )
                                        {
                                            string var1010 = 0;
                                            var1010 += N__NAME;
                                            string var1011 = 0;
                                            var1011 += protect_N__NAME;
                                            if ( var1010 == var1011 )
                                            {
                                                int64_t var1008 = 0;
                                                var1008 += 
                                                    x_qLINEITEM_L__SUPPKEY;
                                                int64_t var1009 = 0;
                                                var1009 += protect_S__SUPPKEY;
                                                if ( var1008 == var1009 )
                                                {
                                                    int64_t var1006 = 0;
                                                    var1006 += 
                                                        x_qLINEITEM_L__ORDERKEY;
                                                    int64_t var1007 = 0;
                                                    var1007 += 
                                                        protect_O__ORDERKEY;
                                                    if ( var1006 == var1007 )
                                                    {
                                                        qLINEITEM1[make_tuple(
                                                            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += 1;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qNATION1_it906 = qNATION1.begin();
    map<int64_t,double>::iterator qNATION1_end905 = qNATION1.end();
    for (; qNATION1_it906 != qNATION1_end905; ++qNATION1_it906)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION1_it906->first;
        qNATION1[x_qNATION_N__NATIONKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it904 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end903 = CUSTOMER.end();
        for (; CUSTOMER_it904 != CUSTOMER_end903; ++CUSTOMER_it904)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it904);
            string protect_C__NAME = get<1>(*CUSTOMER_it904);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it904);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it904);
            string protect_C__PHONE = get<4>(*CUSTOMER_it904);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it904);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it904);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it904);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it902 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end901 = SUPPLIER.end();
            for (; SUPPLIER_it902 != SUPPLIER_end901; ++SUPPLIER_it902)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it902);
                string protect_S__NAME = get<1>(*SUPPLIER_it902);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it902);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it902);
                string protect_S__PHONE = get<4>(*SUPPLIER_it902);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it902);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it902);
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it900 = ORDERS.begin();
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end899 = ORDERS.end();
                for (; ORDERS_it900 != ORDERS_end899; ++ORDERS_it900)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it900);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it900);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it900);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it900);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it900);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it900);
                    string protect_O__CLERK = get<6>(*ORDERS_it900);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it900);
                    string protect_O__COMMENT = get<8>(*ORDERS_it900);
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it898 = LINEITEM.begin();
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end897 = LINEITEM.end();
                    for (; LINEITEM_it898 != LINEITEM_end897; ++LINEITEM_it898)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it898);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it898);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it898);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it898);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it898);
                        double protect_L__EXTENDEDPRICE = get<5>(
                            *LINEITEM_it898);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it898);
                        double protect_L__TAX = get<7>(*LINEITEM_it898);
                        int64_t var1028 = 0;
                        var1028 += protect_C__CUSTKEY;
                        int64_t var1029 = 0;
                        var1029 += protect_O__CUSTKEY;
                        if ( var1028 == var1029 )
                        {
                            int64_t var1026 = 0;
                            var1026 += protect_L__SUPPKEY;
                            int64_t var1027 = 0;
                            var1027 += protect_S__SUPPKEY;
                            if ( var1026 == var1027 )
                            {
                                int64_t var1024 = 0;
                                var1024 += protect_L__ORDERKEY;
                                int64_t var1025 = 0;
                                var1025 += protect_O__ORDERKEY;
                                if ( var1024 == var1025 )
                                {
                                    int64_t var1022 = 0;
                                    var1022 += x_qNATION_N__NATIONKEY;
                                    int64_t var1023 = 0;
                                    var1023 += protect_C__NATIONKEY;
                                    if ( var1022 == var1023 )
                                    {
                                        int64_t var1020 = 0;
                                        var1020 += x_qNATION_N__NATIONKEY;
                                        int64_t var1021 = 0;
                                        var1021 += protect_S__NATIONKEY;
                                        if ( var1020 == var1021 )
                                        {
                                            qNATION1[x_qNATION_N__NATIONKEY] += 
                                                protect_L__EXTENDEDPRICE;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qNATION3_it916 = qNATION3.begin();
    map<int64_t,double>::iterator qNATION3_end915 = qNATION3.end();
    for (; qNATION3_it916 != qNATION3_end915; ++qNATION3_it916)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION3_it916->first;
        qNATION3[x_qNATION_N__NATIONKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it914 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end913 = CUSTOMER.end();
        for (; CUSTOMER_it914 != CUSTOMER_end913; ++CUSTOMER_it914)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it914);
            string protect_C__NAME = get<1>(*CUSTOMER_it914);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it914);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it914);
            string protect_C__PHONE = get<4>(*CUSTOMER_it914);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it914);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it914);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it914);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it912 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end911 = SUPPLIER.end();
            for (; SUPPLIER_it912 != SUPPLIER_end911; ++SUPPLIER_it912)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it912);
                string protect_S__NAME = get<1>(*SUPPLIER_it912);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it912);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it912);
                string protect_S__PHONE = get<4>(*SUPPLIER_it912);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it912);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it912);
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it910 = ORDERS.begin();
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end909 = ORDERS.end();
                for (; ORDERS_it910 != ORDERS_end909; ++ORDERS_it910)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it910);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it910);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it910);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it910);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it910);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it910);
                    string protect_O__CLERK = get<6>(*ORDERS_it910);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it910);
                    string protect_O__COMMENT = get<8>(*ORDERS_it910);
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it908 = LINEITEM.begin();
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end907 = LINEITEM.end();
                    for (; LINEITEM_it908 != LINEITEM_end907; ++LINEITEM_it908)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it908);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it908);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it908);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it908);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it908);
                        double protect_L__EXTENDEDPRICE = get<5>(
                            *LINEITEM_it908);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it908);
                        double protect_L__TAX = get<7>(*LINEITEM_it908);
                        int64_t var1039 = 0;
                        var1039 += protect_C__CUSTKEY;
                        int64_t var1040 = 0;
                        var1040 += protect_O__CUSTKEY;
                        if ( var1039 == var1040 )
                        {
                            int64_t var1037 = 0;
                            var1037 += protect_L__SUPPKEY;
                            int64_t var1038 = 0;
                            var1038 += protect_S__SUPPKEY;
                            if ( var1037 == var1038 )
                            {
                                int64_t var1035 = 0;
                                var1035 += protect_L__ORDERKEY;
                                int64_t var1036 = 0;
                                var1036 += protect_O__ORDERKEY;
                                if ( var1035 == var1036 )
                                {
                                    int64_t var1033 = 0;
                                    var1033 += x_qNATION_N__NATIONKEY;
                                    int64_t var1034 = 0;
                                    var1034 += protect_C__NATIONKEY;
                                    if ( var1033 == var1034 )
                                    {
                                        int64_t var1031 = 0;
                                        var1031 += x_qNATION_N__NATIONKEY;
                                        int64_t var1032 = 0;
                                        var1032 += protect_S__NATIONKEY;
                                        if ( var1031 == var1032 )
                                        {
                                            double var1030 = 1;
                                            var1030 *= protect_L__EXTENDEDPRICE;
                                            var1030 *= protect_L__DISCOUNT;
                                            qNATION3[x_qNATION_N__NATIONKEY] += 
                                                var1030;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<string,int64_t>,double>::iterator qREGION1_it928 = qREGION1.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION1_end927 = qREGION1.end(
        );
    for (; qREGION1_it928 != qREGION1_end927; ++qREGION1_it928)
    {
        string N__NAME = get<0>(qREGION1_it928->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it928->first);
        qREGION1[make_tuple(N__NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_it926 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end925 
            = NATION.end();
        for (; NATION_it926 != NATION_end925; ++NATION_it926)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it926);
            string protect_N__NAME = get<1>(*NATION_it926);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it926);
            string protect_N__COMMENT = get<3>(*NATION_it926);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it924 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end923 = CUSTOMER.end();
            for (; CUSTOMER_it924 != CUSTOMER_end923; ++CUSTOMER_it924)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it924);
                string protect_C__NAME = get<1>(*CUSTOMER_it924);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it924);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it924);
                string protect_C__PHONE = get<4>(*CUSTOMER_it924);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it924);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it924);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it924);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it922 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end921 = SUPPLIER.end();
                for (; SUPPLIER_it922 != SUPPLIER_end921; ++SUPPLIER_it922)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it922);
                    string protect_S__NAME = get<1>(*SUPPLIER_it922);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it922);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it922);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it922);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it922);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it922);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it920 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end919 = ORDERS.end();
                    for (; ORDERS_it920 != ORDERS_end919; ++ORDERS_it920)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it920);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it920);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it920);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it920);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it920);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it920);
                        string protect_O__CLERK = get<6>(*ORDERS_it920);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it920);
                        string protect_O__COMMENT = get<8>(*ORDERS_it920);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it918 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end917 = LINEITEM.end();
                        for (
                            ; LINEITEM_it918 != LINEITEM_end917; ++LINEITEM_it918)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it918);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it918);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it918);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it918);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it918);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it918);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it918);
                            double protect_L__TAX = get<7>(*LINEITEM_it918);
                            int64_t var1053 = 0;
                            var1053 += protect_C__NATIONKEY;
                            int64_t var1054 = 0;
                            var1054 += protect_N__NATIONKEY;
                            if ( var1053 == var1054 )
                            {
                                int64_t var1051 = 0;
                                var1051 += protect_C__NATIONKEY;
                                int64_t var1052 = 0;
                                var1052 += protect_S__NATIONKEY;
                                if ( var1051 == var1052 )
                                {
                                    int64_t var1049 = 0;
                                    var1049 += protect_C__CUSTKEY;
                                    int64_t var1050 = 0;
                                    var1050 += protect_O__CUSTKEY;
                                    if ( var1049 == var1050 )
                                    {
                                        int64_t var1047 = 0;
                                        var1047 += protect_L__SUPPKEY;
                                        int64_t var1048 = 0;
                                        var1048 += protect_S__SUPPKEY;
                                        if ( var1047 == var1048 )
                                        {
                                            int64_t var1045 = 0;
                                            var1045 += protect_L__ORDERKEY;
                                            int64_t var1046 = 0;
                                            var1046 += protect_O__ORDERKEY;
                                            if ( var1045 == var1046 )
                                            {
                                                int64_t var1043 = 0;
                                                var1043 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var1044 = 0;
                                                var1044 += protect_N__REGIONKEY;
                                                if ( var1043 == var1044 )
                                                {
                                                    string var1041 = 0;
                                                    var1041 += N__NAME;
                                                    string var1042 = 0;
                                                    var1042 += protect_N__NAME;
                                                    if ( var1041 == var1042 )
                                                    {
                                                        qREGION1[make_tuple(
                                                            N__NAME,x_qREGION_R__REGIONKEY)] += protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it940 = qREGION2.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION2_end939 = qREGION2.end(
        );
    for (; qREGION2_it940 != qREGION2_end939; ++qREGION2_it940)
    {
        string N__NAME = get<0>(qREGION2_it940->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it940->first);
        qREGION2[make_tuple(N__NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_it938 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end937 
            = NATION.end();
        for (; NATION_it938 != NATION_end937; ++NATION_it938)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it938);
            string protect_N__NAME = get<1>(*NATION_it938);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it938);
            string protect_N__COMMENT = get<3>(*NATION_it938);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it936 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end935 = CUSTOMER.end();
            for (; CUSTOMER_it936 != CUSTOMER_end935; ++CUSTOMER_it936)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it936);
                string protect_C__NAME = get<1>(*CUSTOMER_it936);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it936);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it936);
                string protect_C__PHONE = get<4>(*CUSTOMER_it936);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it936);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it936);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it936);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it934 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end933 = SUPPLIER.end();
                for (; SUPPLIER_it934 != SUPPLIER_end933; ++SUPPLIER_it934)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it934);
                    string protect_S__NAME = get<1>(*SUPPLIER_it934);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it934);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it934);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it934);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it934);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it934);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it932 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end931 = ORDERS.end();
                    for (; ORDERS_it932 != ORDERS_end931; ++ORDERS_it932)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it932);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it932);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it932);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it932);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it932);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it932);
                        string protect_O__CLERK = get<6>(*ORDERS_it932);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it932);
                        string protect_O__COMMENT = get<8>(*ORDERS_it932);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it930 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end929 = LINEITEM.end();
                        for (
                            ; LINEITEM_it930 != LINEITEM_end929; ++LINEITEM_it930)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it930);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it930);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it930);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it930);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it930);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it930);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it930);
                            double protect_L__TAX = get<7>(*LINEITEM_it930);
                            int64_t var1068 = 0;
                            var1068 += protect_C__NATIONKEY;
                            int64_t var1069 = 0;
                            var1069 += protect_N__NATIONKEY;
                            if ( var1068 == var1069 )
                            {
                                int64_t var1066 = 0;
                                var1066 += protect_C__NATIONKEY;
                                int64_t var1067 = 0;
                                var1067 += protect_S__NATIONKEY;
                                if ( var1066 == var1067 )
                                {
                                    int64_t var1064 = 0;
                                    var1064 += protect_C__CUSTKEY;
                                    int64_t var1065 = 0;
                                    var1065 += protect_O__CUSTKEY;
                                    if ( var1064 == var1065 )
                                    {
                                        int64_t var1062 = 0;
                                        var1062 += protect_L__SUPPKEY;
                                        int64_t var1063 = 0;
                                        var1063 += protect_S__SUPPKEY;
                                        if ( var1062 == var1063 )
                                        {
                                            int64_t var1060 = 0;
                                            var1060 += protect_L__ORDERKEY;
                                            int64_t var1061 = 0;
                                            var1061 += protect_O__ORDERKEY;
                                            if ( var1060 == var1061 )
                                            {
                                                int64_t var1058 = 0;
                                                var1058 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var1059 = 0;
                                                var1059 += protect_N__REGIONKEY;
                                                if ( var1058 == var1059 )
                                                {
                                                    string var1056 = 0;
                                                    var1056 += N__NAME;
                                                    string var1057 = 0;
                                                    var1057 += protect_N__NAME;
                                                    if ( var1056 == var1057 )
                                                    {
                                                        double var1055 = 1;
                                                        var1055 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var1055 *= 
                                                            protect_L__DISCOUNT;
                                                        qREGION2[make_tuple(
                                                            N__NAME,x_qREGION_R__REGIONKEY)] += var1055;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it952 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end951 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it952 != qSUPPLIER1_end951; ++qSUPPLIER1_it952)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it952->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER1_it952->first);
        string N__NAME = get<2>(qSUPPLIER1_it952->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it950 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end949 = 
            REGION.end();
        for (; REGION_it950 != REGION_end949; ++REGION_it950)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it950);
            string protect_R__NAME = get<1>(*REGION_it950);
            string protect_R__COMMENT = get<2>(*REGION_it950);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it948 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end947 = NATION.end();
            for (; NATION_it948 != NATION_end947; ++NATION_it948)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it948);
                string protect_N__NAME = get<1>(*NATION_it948);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it948);
                string protect_N__COMMENT = get<3>(*NATION_it948);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it946 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end945 = CUSTOMER.end();
                for (; CUSTOMER_it946 != CUSTOMER_end945; ++CUSTOMER_it946)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it946);
                    string protect_C__NAME = get<1>(*CUSTOMER_it946);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it946);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it946);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it946);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it946);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it946);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it946);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it944 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end943 = ORDERS.end();
                    for (; ORDERS_it944 != ORDERS_end943; ++ORDERS_it944)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it944);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it944);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it944);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it944);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it944);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it944);
                        string protect_O__CLERK = get<6>(*ORDERS_it944);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it944);
                        string protect_O__COMMENT = get<8>(*ORDERS_it944);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it942 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end941 = LINEITEM.end();
                        for (
                            ; LINEITEM_it942 != LINEITEM_end941; ++LINEITEM_it942)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it942);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it942);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it942);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it942);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it942);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it942);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it942);
                            double protect_L__TAX = get<7>(*LINEITEM_it942);
                            int64_t var1082 = 0;
                            var1082 += protect_N__REGIONKEY;
                            int64_t var1083 = 0;
                            var1083 += protect_R__REGIONKEY;
                            if ( var1082 == var1083 )
                            {
                                int64_t var1080 = 0;
                                var1080 += protect_C__CUSTKEY;
                                int64_t var1081 = 0;
                                var1081 += protect_O__CUSTKEY;
                                if ( var1080 == var1081 )
                                {
                                    int64_t var1078 = 0;
                                    var1078 += protect_L__ORDERKEY;
                                    int64_t var1079 = 0;
                                    var1079 += protect_O__ORDERKEY;
                                    if ( var1078 == var1079 )
                                    {
                                        string var1076 = 0;
                                        var1076 += N__NAME;
                                        string var1077 = 0;
                                        var1077 += protect_N__NAME;
                                        if ( var1076 == var1077 )
                                        {
                                            int64_t var1074 = 0;
                                            var1074 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var1075 = 0;
                                            var1075 += protect_N__NATIONKEY;
                                            if ( var1074 == var1075 )
                                            {
                                                int64_t var1072 = 0;
                                                var1072 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var1073 = 0;
                                                var1073 += protect_C__NATIONKEY;
                                                if ( var1072 == var1073 )
                                                {
                                                    int64_t var1070 = 0;
                                                    var1070 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var1071 = 0;
                                                    var1071 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var1070 == var1071 )
                                                    {
                                                        qSUPPLIER1[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it964 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end963 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it964 != qSUPPLIER2_end963; ++qSUPPLIER2_it964)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it964->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER2_it964->first);
        string N__NAME = get<2>(qSUPPLIER2_it964->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it962 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end961 = 
            REGION.end();
        for (; REGION_it962 != REGION_end961; ++REGION_it962)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it962);
            string protect_R__NAME = get<1>(*REGION_it962);
            string protect_R__COMMENT = get<2>(*REGION_it962);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it960 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end959 = NATION.end();
            for (; NATION_it960 != NATION_end959; ++NATION_it960)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it960);
                string protect_N__NAME = get<1>(*NATION_it960);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it960);
                string protect_N__COMMENT = get<3>(*NATION_it960);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it958 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end957 = CUSTOMER.end();
                for (; CUSTOMER_it958 != CUSTOMER_end957; ++CUSTOMER_it958)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it958);
                    string protect_C__NAME = get<1>(*CUSTOMER_it958);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it958);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it958);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it958);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it958);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it958);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it958);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it956 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end955 = ORDERS.end();
                    for (; ORDERS_it956 != ORDERS_end955; ++ORDERS_it956)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it956);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it956);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it956);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it956);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it956);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it956);
                        string protect_O__CLERK = get<6>(*ORDERS_it956);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it956);
                        string protect_O__COMMENT = get<8>(*ORDERS_it956);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it954 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end953 = LINEITEM.end();
                        for (
                            ; LINEITEM_it954 != LINEITEM_end953; ++LINEITEM_it954)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it954);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it954);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it954);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it954);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it954);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it954);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it954);
                            double protect_L__TAX = get<7>(*LINEITEM_it954);
                            int64_t var1097 = 0;
                            var1097 += protect_N__REGIONKEY;
                            int64_t var1098 = 0;
                            var1098 += protect_R__REGIONKEY;
                            if ( var1097 == var1098 )
                            {
                                int64_t var1095 = 0;
                                var1095 += protect_C__CUSTKEY;
                                int64_t var1096 = 0;
                                var1096 += protect_O__CUSTKEY;
                                if ( var1095 == var1096 )
                                {
                                    int64_t var1093 = 0;
                                    var1093 += protect_L__ORDERKEY;
                                    int64_t var1094 = 0;
                                    var1094 += protect_O__ORDERKEY;
                                    if ( var1093 == var1094 )
                                    {
                                        string var1091 = 0;
                                        var1091 += N__NAME;
                                        string var1092 = 0;
                                        var1092 += protect_N__NAME;
                                        if ( var1091 == var1092 )
                                        {
                                            int64_t var1089 = 0;
                                            var1089 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var1090 = 0;
                                            var1090 += protect_N__NATIONKEY;
                                            if ( var1089 == var1090 )
                                            {
                                                int64_t var1087 = 0;
                                                var1087 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var1088 = 0;
                                                var1088 += protect_C__NATIONKEY;
                                                if ( var1087 == var1088 )
                                                {
                                                    int64_t var1085 = 0;
                                                    var1085 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var1086 = 0;
                                                    var1086 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var1085 == var1086 )
                                                    {
                                                        double var1084 = 1;
                                                        var1084 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var1084 *= 
                                                            protect_L__DISCOUNT;
                                                        qSUPPLIER2[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += var1084;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_ORDERS_sec_span, on_delete_ORDERS_usec_span);
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
    map<string,double>::iterator q_it966 = q.begin();
    map<string,double>::iterator q_end965 = q.end();
    for (; q_it966 != q_end965; ++q_it966)
    {
        string N__NAME = q_it966->first;
        q[N__NAME] += -1*qSUPPLIER1[make_tuple(SUPPKEY,NATIONKEY,N__NAME)];
    }
    map<string,double>::iterator q_it968 = q.begin();
    map<string,double>::iterator q_end967 = q.end();
    for (; q_it968 != q_end967; ++q_it968)
    {
        string N__NAME = q_it968->first;
        q[N__NAME] += -1*qSUPPLIER2[make_tuple(SUPPKEY,NATIONKEY,N__NAME)]*-1;
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it980 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end979 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it980 != qCUSTOMER1_end979; ++qCUSTOMER1_it980)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it980->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER1_it980->first);
        string N__NAME = get<2>(qCUSTOMER1_it980->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it978 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end977 = 
            REGION.end();
        for (; REGION_it978 != REGION_end977; ++REGION_it978)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it978);
            string protect_R__NAME = get<1>(*REGION_it978);
            string protect_R__COMMENT = get<2>(*REGION_it978);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it976 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end975 = NATION.end();
            for (; NATION_it976 != NATION_end975; ++NATION_it976)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it976);
                string protect_N__NAME = get<1>(*NATION_it976);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it976);
                string protect_N__COMMENT = get<3>(*NATION_it976);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it974 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end973 = SUPPLIER.end();
                for (; SUPPLIER_it974 != SUPPLIER_end973; ++SUPPLIER_it974)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it974);
                    string protect_S__NAME = get<1>(*SUPPLIER_it974);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it974);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it974);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it974);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it974);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it974);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it972 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end971 = ORDERS.end();
                    for (; ORDERS_it972 != ORDERS_end971; ++ORDERS_it972)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it972);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it972);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it972);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it972);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it972);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it972);
                        string protect_O__CLERK = get<6>(*ORDERS_it972);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it972);
                        string protect_O__COMMENT = get<8>(*ORDERS_it972);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it970 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end969 = LINEITEM.end();
                        for (
                            ; LINEITEM_it970 != LINEITEM_end969; ++LINEITEM_it970)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it970);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it970);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it970);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it970);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it970);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it970);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it970);
                            double protect_L__TAX = get<7>(*LINEITEM_it970);
                            int64_t var1111 = 0;
                            var1111 += protect_N__REGIONKEY;
                            int64_t var1112 = 0;
                            var1112 += protect_R__REGIONKEY;
                            if ( var1111 == var1112 )
                            {
                                int64_t var1109 = 0;
                                var1109 += protect_L__SUPPKEY;
                                int64_t var1110 = 0;
                                var1110 += protect_S__SUPPKEY;
                                if ( var1109 == var1110 )
                                {
                                    int64_t var1107 = 0;
                                    var1107 += protect_L__ORDERKEY;
                                    int64_t var1108 = 0;
                                    var1108 += protect_O__ORDERKEY;
                                    if ( var1107 == var1108 )
                                    {
                                        string var1105 = 0;
                                        var1105 += N__NAME;
                                        string var1106 = 0;
                                        var1106 += protect_N__NAME;
                                        if ( var1105 == var1106 )
                                        {
                                            int64_t var1103 = 0;
                                            var1103 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var1104 = 0;
                                            var1104 += protect_N__NATIONKEY;
                                            if ( var1103 == var1104 )
                                            {
                                                int64_t var1101 = 0;
                                                var1101 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var1102 = 0;
                                                var1102 += protect_S__NATIONKEY;
                                                if ( var1101 == var1102 )
                                                {
                                                    int64_t var1099 = 0;
                                                    var1099 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var1100 = 0;
                                                    var1100 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var1099 == var1100 )
                                                    {
                                                        qCUSTOMER1[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it992 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end991 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it992 != qCUSTOMER2_end991; ++qCUSTOMER2_it992)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it992->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER2_it992->first);
        string N__NAME = get<2>(qCUSTOMER2_it992->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it990 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end989 = 
            REGION.end();
        for (; REGION_it990 != REGION_end989; ++REGION_it990)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it990);
            string protect_R__NAME = get<1>(*REGION_it990);
            string protect_R__COMMENT = get<2>(*REGION_it990);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it988 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end987 = NATION.end();
            for (; NATION_it988 != NATION_end987; ++NATION_it988)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it988);
                string protect_N__NAME = get<1>(*NATION_it988);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it988);
                string protect_N__COMMENT = get<3>(*NATION_it988);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it986 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end985 = SUPPLIER.end();
                for (; SUPPLIER_it986 != SUPPLIER_end985; ++SUPPLIER_it986)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it986);
                    string protect_S__NAME = get<1>(*SUPPLIER_it986);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it986);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it986);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it986);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it986);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it986);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it984 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end983 = ORDERS.end();
                    for (; ORDERS_it984 != ORDERS_end983; ++ORDERS_it984)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it984);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it984);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it984);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it984);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it984);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it984);
                        string protect_O__CLERK = get<6>(*ORDERS_it984);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it984);
                        string protect_O__COMMENT = get<8>(*ORDERS_it984);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it982 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end981 = LINEITEM.end();
                        for (
                            ; LINEITEM_it982 != LINEITEM_end981; ++LINEITEM_it982)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it982);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it982);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it982);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it982);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it982);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it982);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it982);
                            double protect_L__TAX = get<7>(*LINEITEM_it982);
                            int64_t var1126 = 0;
                            var1126 += protect_N__REGIONKEY;
                            int64_t var1127 = 0;
                            var1127 += protect_R__REGIONKEY;
                            if ( var1126 == var1127 )
                            {
                                int64_t var1124 = 0;
                                var1124 += protect_L__SUPPKEY;
                                int64_t var1125 = 0;
                                var1125 += protect_S__SUPPKEY;
                                if ( var1124 == var1125 )
                                {
                                    int64_t var1122 = 0;
                                    var1122 += protect_L__ORDERKEY;
                                    int64_t var1123 = 0;
                                    var1123 += protect_O__ORDERKEY;
                                    if ( var1122 == var1123 )
                                    {
                                        string var1120 = 0;
                                        var1120 += N__NAME;
                                        string var1121 = 0;
                                        var1121 += protect_N__NAME;
                                        if ( var1120 == var1121 )
                                        {
                                            int64_t var1118 = 0;
                                            var1118 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var1119 = 0;
                                            var1119 += protect_N__NATIONKEY;
                                            if ( var1118 == var1119 )
                                            {
                                                int64_t var1116 = 0;
                                                var1116 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var1117 = 0;
                                                var1117 += protect_S__NATIONKEY;
                                                if ( var1116 == var1117 )
                                                {
                                                    int64_t var1114 = 0;
                                                    var1114 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var1115 = 0;
                                                    var1115 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var1114 == var1115 )
                                                    {
                                                        double var1113 = 1;
                                                        var1113 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var1113 *= 
                                                            protect_L__DISCOUNT;
                                                        qCUSTOMER2[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += var1113;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it1004 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end1003 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it1004 != qLINEITEM1_end1003; ++qLINEITEM1_it1004)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it1004->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it1004->first);
        string N__NAME = get<2>(qLINEITEM1_it1004->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it1002 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end1001 = 
            REGION.end();
        for (; REGION_it1002 != REGION_end1001; ++REGION_it1002)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it1002);
            string protect_R__NAME = get<1>(*REGION_it1002);
            string protect_R__COMMENT = get<2>(*REGION_it1002);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it1000 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end999 = NATION.end();
            for (; NATION_it1000 != NATION_end999; ++NATION_it1000)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it1000);
                string protect_N__NAME = get<1>(*NATION_it1000);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it1000);
                string protect_N__COMMENT = get<3>(*NATION_it1000);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it998 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end997 = SUPPLIER.end();
                for (; SUPPLIER_it998 != SUPPLIER_end997; ++SUPPLIER_it998)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it998);
                    string protect_S__NAME = get<1>(*SUPPLIER_it998);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it998);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it998);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it998);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it998);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it998);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_it996 = CUSTOMER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_end995 = CUSTOMER.end();
                    for (; CUSTOMER_it996 != CUSTOMER_end995; ++CUSTOMER_it996)
                    {
                        int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it996);
                        string protect_C__NAME = get<1>(*CUSTOMER_it996);
                        string protect_C__ADDRESS = get<2>(*CUSTOMER_it996);
                        int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it996);
                        string protect_C__PHONE = get<4>(*CUSTOMER_it996);
                        double protect_C__ACCTBAL = get<5>(*CUSTOMER_it996);
                        string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it996);
                        string protect_C__COMMENT = get<7>(*CUSTOMER_it996);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it994 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end993 = ORDERS.end();
                        for (; ORDERS_it994 != ORDERS_end993; ++ORDERS_it994)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it994);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it994);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it994);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it994);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it994);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it994);
                            string protect_O__CLERK = get<6>(*ORDERS_it994);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it994);
                            string protect_O__COMMENT = get<8>(*ORDERS_it994);
                            int64_t var1140 = 0;
                            var1140 += protect_N__REGIONKEY;
                            int64_t var1141 = 0;
                            var1141 += protect_R__REGIONKEY;
                            if ( var1140 == var1141 )
                            {
                                int64_t var1138 = 0;
                                var1138 += protect_C__NATIONKEY;
                                int64_t var1139 = 0;
                                var1139 += protect_N__NATIONKEY;
                                if ( var1138 == var1139 )
                                {
                                    int64_t var1136 = 0;
                                    var1136 += protect_C__NATIONKEY;
                                    int64_t var1137 = 0;
                                    var1137 += protect_S__NATIONKEY;
                                    if ( var1136 == var1137 )
                                    {
                                        int64_t var1134 = 0;
                                        var1134 += protect_C__CUSTKEY;
                                        int64_t var1135 = 0;
                                        var1135 += protect_O__CUSTKEY;
                                        if ( var1134 == var1135 )
                                        {
                                            string var1132 = 0;
                                            var1132 += N__NAME;
                                            string var1133 = 0;
                                            var1133 += protect_N__NAME;
                                            if ( var1132 == var1133 )
                                            {
                                                int64_t var1130 = 0;
                                                var1130 += 
                                                    x_qLINEITEM_L__SUPPKEY;
                                                int64_t var1131 = 0;
                                                var1131 += protect_S__SUPPKEY;
                                                if ( var1130 == var1131 )
                                                {
                                                    int64_t var1128 = 0;
                                                    var1128 += 
                                                        x_qLINEITEM_L__ORDERKEY;
                                                    int64_t var1129 = 0;
                                                    var1129 += 
                                                        protect_O__ORDERKEY;
                                                    if ( var1128 == var1129 )
                                                    {
                                                        qLINEITEM1[make_tuple(
                                                            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += 1;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qNATION1_it1014 = qNATION1.begin();
    map<int64_t,double>::iterator qNATION1_end1013 = qNATION1.end();
    for (; qNATION1_it1014 != qNATION1_end1013; ++qNATION1_it1014)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION1_it1014->first;
        qNATION1[x_qNATION_N__NATIONKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it1012 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end1011 = CUSTOMER.end();
        for (; CUSTOMER_it1012 != CUSTOMER_end1011; ++CUSTOMER_it1012)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1012);
            string protect_C__NAME = get<1>(*CUSTOMER_it1012);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it1012);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1012);
            string protect_C__PHONE = get<4>(*CUSTOMER_it1012);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1012);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1012);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it1012);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it1010 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end1009 = SUPPLIER.end();
            for (; SUPPLIER_it1010 != SUPPLIER_end1009; ++SUPPLIER_it1010)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1010);
                string protect_S__NAME = get<1>(*SUPPLIER_it1010);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it1010);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1010);
                string protect_S__PHONE = get<4>(*SUPPLIER_it1010);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1010);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it1010);
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it1008 = ORDERS.begin();
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end1007 = ORDERS.end();
                for (; ORDERS_it1008 != ORDERS_end1007; ++ORDERS_it1008)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it1008);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1008);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it1008);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it1008);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it1008);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it1008);
                    string protect_O__CLERK = get<6>(*ORDERS_it1008);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it1008);
                    string protect_O__COMMENT = get<8>(*ORDERS_it1008);
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it1006 = LINEITEM.begin();
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end1005 = LINEITEM.end();
                    for (
                        ; LINEITEM_it1006 != LINEITEM_end1005; ++LINEITEM_it1006)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it1006);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it1006);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it1006);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it1006);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it1006);
                        double protect_L__EXTENDEDPRICE = get<5>(
                            *LINEITEM_it1006);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it1006);
                        double protect_L__TAX = get<7>(*LINEITEM_it1006);
                        int64_t var1150 = 0;
                        var1150 += protect_C__CUSTKEY;
                        int64_t var1151 = 0;
                        var1151 += protect_O__CUSTKEY;
                        if ( var1150 == var1151 )
                        {
                            int64_t var1148 = 0;
                            var1148 += protect_L__SUPPKEY;
                            int64_t var1149 = 0;
                            var1149 += protect_S__SUPPKEY;
                            if ( var1148 == var1149 )
                            {
                                int64_t var1146 = 0;
                                var1146 += protect_L__ORDERKEY;
                                int64_t var1147 = 0;
                                var1147 += protect_O__ORDERKEY;
                                if ( var1146 == var1147 )
                                {
                                    int64_t var1144 = 0;
                                    var1144 += x_qNATION_N__NATIONKEY;
                                    int64_t var1145 = 0;
                                    var1145 += protect_C__NATIONKEY;
                                    if ( var1144 == var1145 )
                                    {
                                        int64_t var1142 = 0;
                                        var1142 += x_qNATION_N__NATIONKEY;
                                        int64_t var1143 = 0;
                                        var1143 += protect_S__NATIONKEY;
                                        if ( var1142 == var1143 )
                                        {
                                            qNATION1[x_qNATION_N__NATIONKEY] += 
                                                protect_L__EXTENDEDPRICE;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qNATION3_it1024 = qNATION3.begin();
    map<int64_t,double>::iterator qNATION3_end1023 = qNATION3.end();
    for (; qNATION3_it1024 != qNATION3_end1023; ++qNATION3_it1024)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION3_it1024->first;
        qNATION3[x_qNATION_N__NATIONKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it1022 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end1021 = CUSTOMER.end();
        for (; CUSTOMER_it1022 != CUSTOMER_end1021; ++CUSTOMER_it1022)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1022);
            string protect_C__NAME = get<1>(*CUSTOMER_it1022);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it1022);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1022);
            string protect_C__PHONE = get<4>(*CUSTOMER_it1022);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1022);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1022);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it1022);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it1020 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end1019 = SUPPLIER.end();
            for (; SUPPLIER_it1020 != SUPPLIER_end1019; ++SUPPLIER_it1020)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1020);
                string protect_S__NAME = get<1>(*SUPPLIER_it1020);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it1020);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1020);
                string protect_S__PHONE = get<4>(*SUPPLIER_it1020);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1020);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it1020);
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it1018 = ORDERS.begin();
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end1017 = ORDERS.end();
                for (; ORDERS_it1018 != ORDERS_end1017; ++ORDERS_it1018)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it1018);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1018);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it1018);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it1018);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it1018);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it1018);
                    string protect_O__CLERK = get<6>(*ORDERS_it1018);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it1018);
                    string protect_O__COMMENT = get<8>(*ORDERS_it1018);
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it1016 = LINEITEM.begin();
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end1015 = LINEITEM.end();
                    for (
                        ; LINEITEM_it1016 != LINEITEM_end1015; ++LINEITEM_it1016)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it1016);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it1016);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it1016);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it1016);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it1016);
                        double protect_L__EXTENDEDPRICE = get<5>(
                            *LINEITEM_it1016);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it1016);
                        double protect_L__TAX = get<7>(*LINEITEM_it1016);
                        int64_t var1161 = 0;
                        var1161 += protect_C__CUSTKEY;
                        int64_t var1162 = 0;
                        var1162 += protect_O__CUSTKEY;
                        if ( var1161 == var1162 )
                        {
                            int64_t var1159 = 0;
                            var1159 += protect_L__SUPPKEY;
                            int64_t var1160 = 0;
                            var1160 += protect_S__SUPPKEY;
                            if ( var1159 == var1160 )
                            {
                                int64_t var1157 = 0;
                                var1157 += protect_L__ORDERKEY;
                                int64_t var1158 = 0;
                                var1158 += protect_O__ORDERKEY;
                                if ( var1157 == var1158 )
                                {
                                    int64_t var1155 = 0;
                                    var1155 += x_qNATION_N__NATIONKEY;
                                    int64_t var1156 = 0;
                                    var1156 += protect_C__NATIONKEY;
                                    if ( var1155 == var1156 )
                                    {
                                        int64_t var1153 = 0;
                                        var1153 += x_qNATION_N__NATIONKEY;
                                        int64_t var1154 = 0;
                                        var1154 += protect_S__NATIONKEY;
                                        if ( var1153 == var1154 )
                                        {
                                            double var1152 = 1;
                                            var1152 *= protect_L__EXTENDEDPRICE;
                                            var1152 *= protect_L__DISCOUNT;
                                            qNATION3[x_qNATION_N__NATIONKEY] += 
                                                var1152;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it1036 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end1035 = 
        qORDERS1.end();
    for (; qORDERS1_it1036 != qORDERS1_end1035; ++qORDERS1_it1036)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it1036->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it1036->first);
        string N__NAME = get<2>(qORDERS1_it1036->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it1034 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end1033 = 
            REGION.end();
        for (; REGION_it1034 != REGION_end1033; ++REGION_it1034)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it1034);
            string protect_R__NAME = get<1>(*REGION_it1034);
            string protect_R__COMMENT = get<2>(*REGION_it1034);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it1032 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end1031 = NATION.end();
            for (; NATION_it1032 != NATION_end1031; ++NATION_it1032)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it1032);
                string protect_N__NAME = get<1>(*NATION_it1032);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it1032);
                string protect_N__COMMENT = get<3>(*NATION_it1032);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it1030 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end1029 = CUSTOMER.end();
                for (; CUSTOMER_it1030 != CUSTOMER_end1029; ++CUSTOMER_it1030)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1030);
                    string protect_C__NAME = get<1>(*CUSTOMER_it1030);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it1030);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1030);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it1030);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1030);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1030);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it1030);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it1028 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end1027 = SUPPLIER.end();
                    for (
                        ; SUPPLIER_it1028 != SUPPLIER_end1027; ++SUPPLIER_it1028)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1028);
                        string protect_S__NAME = get<1>(*SUPPLIER_it1028);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it1028);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1028);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it1028);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1028);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it1028);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1026 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1025 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1026 != LINEITEM_end1025; ++LINEITEM_it1026)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1026);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1026);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1026);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1026);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1026);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1026);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1026);
                            double protect_L__TAX = get<7>(*LINEITEM_it1026);
                            int64_t var1175 = 0;
                            var1175 += protect_N__REGIONKEY;
                            int64_t var1176 = 0;
                            var1176 += protect_R__REGIONKEY;
                            if ( var1175 == var1176 )
                            {
                                int64_t var1173 = 0;
                                var1173 += protect_C__NATIONKEY;
                                int64_t var1174 = 0;
                                var1174 += protect_N__NATIONKEY;
                                if ( var1173 == var1174 )
                                {
                                    int64_t var1171 = 0;
                                    var1171 += protect_C__NATIONKEY;
                                    int64_t var1172 = 0;
                                    var1172 += protect_S__NATIONKEY;
                                    if ( var1171 == var1172 )
                                    {
                                        int64_t var1169 = 0;
                                        var1169 += protect_L__SUPPKEY;
                                        int64_t var1170 = 0;
                                        var1170 += protect_S__SUPPKEY;
                                        if ( var1169 == var1170 )
                                        {
                                            string var1167 = 0;
                                            var1167 += N__NAME;
                                            string var1168 = 0;
                                            var1168 += protect_N__NAME;
                                            if ( var1167 == var1168 )
                                            {
                                                int64_t var1165 = 0;
                                                var1165 += x_qORDERS_O__CUSTKEY;
                                                int64_t var1166 = 0;
                                                var1166 += protect_C__CUSTKEY;
                                                if ( var1165 == var1166 )
                                                {
                                                    int64_t var1163 = 0;
                                                    var1163 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var1164 = 0;
                                                    var1164 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var1163 == var1164 )
                                                    {
                                                        qORDERS1[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it1048 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end1047 = 
        qORDERS2.end();
    for (; qORDERS2_it1048 != qORDERS2_end1047; ++qORDERS2_it1048)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it1048->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it1048->first);
        string N__NAME = get<2>(qORDERS2_it1048->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it1046 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end1045 = 
            REGION.end();
        for (; REGION_it1046 != REGION_end1045; ++REGION_it1046)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it1046);
            string protect_R__NAME = get<1>(*REGION_it1046);
            string protect_R__COMMENT = get<2>(*REGION_it1046);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it1044 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end1043 = NATION.end();
            for (; NATION_it1044 != NATION_end1043; ++NATION_it1044)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it1044);
                string protect_N__NAME = get<1>(*NATION_it1044);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it1044);
                string protect_N__COMMENT = get<3>(*NATION_it1044);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it1042 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end1041 = CUSTOMER.end();
                for (; CUSTOMER_it1042 != CUSTOMER_end1041; ++CUSTOMER_it1042)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1042);
                    string protect_C__NAME = get<1>(*CUSTOMER_it1042);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it1042);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1042);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it1042);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1042);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1042);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it1042);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it1040 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end1039 = SUPPLIER.end();
                    for (
                        ; SUPPLIER_it1040 != SUPPLIER_end1039; ++SUPPLIER_it1040)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1040);
                        string protect_S__NAME = get<1>(*SUPPLIER_it1040);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it1040);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1040);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it1040);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1040);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it1040);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1038 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1037 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1038 != LINEITEM_end1037; ++LINEITEM_it1038)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1038);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1038);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1038);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1038);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1038);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1038);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1038);
                            double protect_L__TAX = get<7>(*LINEITEM_it1038);
                            int64_t var1190 = 0;
                            var1190 += protect_N__REGIONKEY;
                            int64_t var1191 = 0;
                            var1191 += protect_R__REGIONKEY;
                            if ( var1190 == var1191 )
                            {
                                int64_t var1188 = 0;
                                var1188 += protect_C__NATIONKEY;
                                int64_t var1189 = 0;
                                var1189 += protect_N__NATIONKEY;
                                if ( var1188 == var1189 )
                                {
                                    int64_t var1186 = 0;
                                    var1186 += protect_C__NATIONKEY;
                                    int64_t var1187 = 0;
                                    var1187 += protect_S__NATIONKEY;
                                    if ( var1186 == var1187 )
                                    {
                                        int64_t var1184 = 0;
                                        var1184 += protect_L__SUPPKEY;
                                        int64_t var1185 = 0;
                                        var1185 += protect_S__SUPPKEY;
                                        if ( var1184 == var1185 )
                                        {
                                            string var1182 = 0;
                                            var1182 += N__NAME;
                                            string var1183 = 0;
                                            var1183 += protect_N__NAME;
                                            if ( var1182 == var1183 )
                                            {
                                                int64_t var1180 = 0;
                                                var1180 += x_qORDERS_O__CUSTKEY;
                                                int64_t var1181 = 0;
                                                var1181 += protect_C__CUSTKEY;
                                                if ( var1180 == var1181 )
                                                {
                                                    int64_t var1178 = 0;
                                                    var1178 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var1179 = 0;
                                                    var1179 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var1178 == var1179 )
                                                    {
                                                        double var1177 = 1;
                                                        var1177 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var1177 *= 
                                                            protect_L__DISCOUNT;
                                                        qORDERS2[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += var1177;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<string,int64_t>,double>::iterator qREGION1_it1060 = 
        qREGION1.begin();
    map<tuple<string,int64_t>,double>::iterator qREGION1_end1059 = qREGION1.end(
        );
    for (; qREGION1_it1060 != qREGION1_end1059; ++qREGION1_it1060)
    {
        string N__NAME = get<0>(qREGION1_it1060->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it1060->first);
        qREGION1[make_tuple(N__NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it1058 
            = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_end1057 = NATION.end();
        for (; NATION_it1058 != NATION_end1057; ++NATION_it1058)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it1058);
            string protect_N__NAME = get<1>(*NATION_it1058);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it1058);
            string protect_N__COMMENT = get<3>(*NATION_it1058);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it1056 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end1055 = CUSTOMER.end();
            for (; CUSTOMER_it1056 != CUSTOMER_end1055; ++CUSTOMER_it1056)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1056);
                string protect_C__NAME = get<1>(*CUSTOMER_it1056);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it1056);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1056);
                string protect_C__PHONE = get<4>(*CUSTOMER_it1056);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1056);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1056);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it1056);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it1054 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end1053 = SUPPLIER.end();
                for (; SUPPLIER_it1054 != SUPPLIER_end1053; ++SUPPLIER_it1054)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1054);
                    string protect_S__NAME = get<1>(*SUPPLIER_it1054);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it1054);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1054);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it1054);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1054);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it1054);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it1052 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end1051 = ORDERS.end();
                    for (; ORDERS_it1052 != ORDERS_end1051; ++ORDERS_it1052)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it1052);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1052);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it1052);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it1052);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it1052);
                        string protect_O__ORDERPRIORITY = get<5>(
                            *ORDERS_it1052);
                        string protect_O__CLERK = get<6>(*ORDERS_it1052);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it1052);
                        string protect_O__COMMENT = get<8>(*ORDERS_it1052);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1050 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1049 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1050 != LINEITEM_end1049; ++LINEITEM_it1050)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1050);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1050);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1050);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1050);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1050);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1050);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1050);
                            double protect_L__TAX = get<7>(*LINEITEM_it1050);
                            int64_t var1204 = 0;
                            var1204 += protect_C__NATIONKEY;
                            int64_t var1205 = 0;
                            var1205 += protect_N__NATIONKEY;
                            if ( var1204 == var1205 )
                            {
                                int64_t var1202 = 0;
                                var1202 += protect_C__NATIONKEY;
                                int64_t var1203 = 0;
                                var1203 += protect_S__NATIONKEY;
                                if ( var1202 == var1203 )
                                {
                                    int64_t var1200 = 0;
                                    var1200 += protect_C__CUSTKEY;
                                    int64_t var1201 = 0;
                                    var1201 += protect_O__CUSTKEY;
                                    if ( var1200 == var1201 )
                                    {
                                        int64_t var1198 = 0;
                                        var1198 += protect_L__SUPPKEY;
                                        int64_t var1199 = 0;
                                        var1199 += protect_S__SUPPKEY;
                                        if ( var1198 == var1199 )
                                        {
                                            int64_t var1196 = 0;
                                            var1196 += protect_L__ORDERKEY;
                                            int64_t var1197 = 0;
                                            var1197 += protect_O__ORDERKEY;
                                            if ( var1196 == var1197 )
                                            {
                                                int64_t var1194 = 0;
                                                var1194 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var1195 = 0;
                                                var1195 += protect_N__REGIONKEY;
                                                if ( var1194 == var1195 )
                                                {
                                                    string var1192 = 0;
                                                    var1192 += N__NAME;
                                                    string var1193 = 0;
                                                    var1193 += protect_N__NAME;
                                                    if ( var1192 == var1193 )
                                                    {
                                                        qREGION1[make_tuple(
                                                            N__NAME,x_qREGION_R__REGIONKEY)] += protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it1072 = 
        qREGION2.begin();
    map<tuple<string,int64_t>,double>::iterator qREGION2_end1071 = qREGION2.end(
        );
    for (; qREGION2_it1072 != qREGION2_end1071; ++qREGION2_it1072)
    {
        string N__NAME = get<0>(qREGION2_it1072->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it1072->first);
        qREGION2[make_tuple(N__NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it1070 
            = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_end1069 = NATION.end();
        for (; NATION_it1070 != NATION_end1069; ++NATION_it1070)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it1070);
            string protect_N__NAME = get<1>(*NATION_it1070);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it1070);
            string protect_N__COMMENT = get<3>(*NATION_it1070);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it1068 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end1067 = CUSTOMER.end();
            for (; CUSTOMER_it1068 != CUSTOMER_end1067; ++CUSTOMER_it1068)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1068);
                string protect_C__NAME = get<1>(*CUSTOMER_it1068);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it1068);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1068);
                string protect_C__PHONE = get<4>(*CUSTOMER_it1068);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1068);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1068);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it1068);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it1066 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end1065 = SUPPLIER.end();
                for (; SUPPLIER_it1066 != SUPPLIER_end1065; ++SUPPLIER_it1066)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1066);
                    string protect_S__NAME = get<1>(*SUPPLIER_it1066);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it1066);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1066);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it1066);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1066);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it1066);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it1064 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end1063 = ORDERS.end();
                    for (; ORDERS_it1064 != ORDERS_end1063; ++ORDERS_it1064)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it1064);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1064);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it1064);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it1064);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it1064);
                        string protect_O__ORDERPRIORITY = get<5>(
                            *ORDERS_it1064);
                        string protect_O__CLERK = get<6>(*ORDERS_it1064);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it1064);
                        string protect_O__COMMENT = get<8>(*ORDERS_it1064);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1062 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1061 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1062 != LINEITEM_end1061; ++LINEITEM_it1062)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1062);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1062);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1062);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1062);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1062);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1062);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1062);
                            double protect_L__TAX = get<7>(*LINEITEM_it1062);
                            int64_t var1219 = 0;
                            var1219 += protect_C__NATIONKEY;
                            int64_t var1220 = 0;
                            var1220 += protect_N__NATIONKEY;
                            if ( var1219 == var1220 )
                            {
                                int64_t var1217 = 0;
                                var1217 += protect_C__NATIONKEY;
                                int64_t var1218 = 0;
                                var1218 += protect_S__NATIONKEY;
                                if ( var1217 == var1218 )
                                {
                                    int64_t var1215 = 0;
                                    var1215 += protect_C__CUSTKEY;
                                    int64_t var1216 = 0;
                                    var1216 += protect_O__CUSTKEY;
                                    if ( var1215 == var1216 )
                                    {
                                        int64_t var1213 = 0;
                                        var1213 += protect_L__SUPPKEY;
                                        int64_t var1214 = 0;
                                        var1214 += protect_S__SUPPKEY;
                                        if ( var1213 == var1214 )
                                        {
                                            int64_t var1211 = 0;
                                            var1211 += protect_L__ORDERKEY;
                                            int64_t var1212 = 0;
                                            var1212 += protect_O__ORDERKEY;
                                            if ( var1211 == var1212 )
                                            {
                                                int64_t var1209 = 0;
                                                var1209 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var1210 = 0;
                                                var1210 += protect_N__REGIONKEY;
                                                if ( var1209 == var1210 )
                                                {
                                                    string var1207 = 0;
                                                    var1207 += N__NAME;
                                                    string var1208 = 0;
                                                    var1208 += protect_N__NAME;
                                                    if ( var1207 == var1208 )
                                                    {
                                                        double var1206 = 1;
                                                        var1206 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var1206 *= 
                                                            protect_L__DISCOUNT;
                                                        qREGION2[make_tuple(
                                                            N__NAME,x_qREGION_R__REGIONKEY)] += var1206;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_SUPPLIER_sec_span, on_delete_SUPPLIER_usec_span);
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
    map<string,double>::iterator q_it1074 = q.begin();
    map<string,double>::iterator q_end1073 = q.end();
    for (; q_it1074 != q_end1073; ++q_it1074)
    {
        string N__NAME = q_it1074->first;
        q[N__NAME] += -1*qCUSTOMER1[make_tuple(CUSTKEY,NATIONKEY,N__NAME)];
    }
    map<string,double>::iterator q_it1076 = q.begin();
    map<string,double>::iterator q_end1075 = q.end();
    for (; q_it1076 != q_end1075; ++q_it1076)
    {
        string N__NAME = q_it1076->first;
        q[N__NAME] += -1*qCUSTOMER2[make_tuple(CUSTKEY,NATIONKEY,N__NAME)]*-1;
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it1088 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end1087 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it1088 != qLINEITEM1_end1087; ++qLINEITEM1_it1088)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it1088->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it1088->first);
        string N__NAME = get<2>(qLINEITEM1_it1088->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it1086 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end1085 = 
            REGION.end();
        for (; REGION_it1086 != REGION_end1085; ++REGION_it1086)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it1086);
            string protect_R__NAME = get<1>(*REGION_it1086);
            string protect_R__COMMENT = get<2>(*REGION_it1086);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it1084 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end1083 = NATION.end();
            for (; NATION_it1084 != NATION_end1083; ++NATION_it1084)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it1084);
                string protect_N__NAME = get<1>(*NATION_it1084);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it1084);
                string protect_N__COMMENT = get<3>(*NATION_it1084);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it1082 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end1081 = SUPPLIER.end();
                for (; SUPPLIER_it1082 != SUPPLIER_end1081; ++SUPPLIER_it1082)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1082);
                    string protect_S__NAME = get<1>(*SUPPLIER_it1082);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it1082);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1082);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it1082);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1082);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it1082);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_it1080 = CUSTOMER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_end1079 = CUSTOMER.end();
                    for (
                        ; CUSTOMER_it1080 != CUSTOMER_end1079; ++CUSTOMER_it1080)
                    {
                        int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1080);
                        string protect_C__NAME = get<1>(*CUSTOMER_it1080);
                        string protect_C__ADDRESS = get<2>(*CUSTOMER_it1080);
                        int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1080);
                        string protect_C__PHONE = get<4>(*CUSTOMER_it1080);
                        double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1080);
                        string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1080);
                        string protect_C__COMMENT = get<7>(*CUSTOMER_it1080);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it1078 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end1077 = ORDERS.end();
                        for (; ORDERS_it1078 != ORDERS_end1077; ++ORDERS_it1078)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(
                                *ORDERS_it1078);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1078);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it1078);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it1078);
                            string protect_O__ORDERDATE = get<4>(
                                *ORDERS_it1078);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it1078);
                            string protect_O__CLERK = get<6>(*ORDERS_it1078);
                            int protect_O__SHIPPRIORITY = get<7>(
                                *ORDERS_it1078);
                            string protect_O__COMMENT = get<8>(*ORDERS_it1078);
                            int64_t var1233 = 0;
                            var1233 += protect_N__REGIONKEY;
                            int64_t var1234 = 0;
                            var1234 += protect_R__REGIONKEY;
                            if ( var1233 == var1234 )
                            {
                                int64_t var1231 = 0;
                                var1231 += protect_C__NATIONKEY;
                                int64_t var1232 = 0;
                                var1232 += protect_N__NATIONKEY;
                                if ( var1231 == var1232 )
                                {
                                    int64_t var1229 = 0;
                                    var1229 += protect_C__NATIONKEY;
                                    int64_t var1230 = 0;
                                    var1230 += protect_S__NATIONKEY;
                                    if ( var1229 == var1230 )
                                    {
                                        int64_t var1227 = 0;
                                        var1227 += protect_C__CUSTKEY;
                                        int64_t var1228 = 0;
                                        var1228 += protect_O__CUSTKEY;
                                        if ( var1227 == var1228 )
                                        {
                                            string var1225 = 0;
                                            var1225 += N__NAME;
                                            string var1226 = 0;
                                            var1226 += protect_N__NAME;
                                            if ( var1225 == var1226 )
                                            {
                                                int64_t var1223 = 0;
                                                var1223 += 
                                                    x_qLINEITEM_L__SUPPKEY;
                                                int64_t var1224 = 0;
                                                var1224 += protect_S__SUPPKEY;
                                                if ( var1223 == var1224 )
                                                {
                                                    int64_t var1221 = 0;
                                                    var1221 += 
                                                        x_qLINEITEM_L__ORDERKEY;
                                                    int64_t var1222 = 0;
                                                    var1222 += 
                                                        protect_O__ORDERKEY;
                                                    if ( var1221 == var1222 )
                                                    {
                                                        qLINEITEM1[make_tuple(
                                                            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += 1;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qNATION1_it1098 = qNATION1.begin();
    map<int64_t,double>::iterator qNATION1_end1097 = qNATION1.end();
    for (; qNATION1_it1098 != qNATION1_end1097; ++qNATION1_it1098)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION1_it1098->first;
        qNATION1[x_qNATION_N__NATIONKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it1096 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end1095 = CUSTOMER.end();
        for (; CUSTOMER_it1096 != CUSTOMER_end1095; ++CUSTOMER_it1096)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1096);
            string protect_C__NAME = get<1>(*CUSTOMER_it1096);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it1096);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1096);
            string protect_C__PHONE = get<4>(*CUSTOMER_it1096);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1096);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1096);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it1096);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it1094 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end1093 = SUPPLIER.end();
            for (; SUPPLIER_it1094 != SUPPLIER_end1093; ++SUPPLIER_it1094)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1094);
                string protect_S__NAME = get<1>(*SUPPLIER_it1094);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it1094);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1094);
                string protect_S__PHONE = get<4>(*SUPPLIER_it1094);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1094);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it1094);
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it1092 = ORDERS.begin();
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end1091 = ORDERS.end();
                for (; ORDERS_it1092 != ORDERS_end1091; ++ORDERS_it1092)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it1092);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1092);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it1092);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it1092);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it1092);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it1092);
                    string protect_O__CLERK = get<6>(*ORDERS_it1092);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it1092);
                    string protect_O__COMMENT = get<8>(*ORDERS_it1092);
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it1090 = LINEITEM.begin();
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end1089 = LINEITEM.end();
                    for (
                        ; LINEITEM_it1090 != LINEITEM_end1089; ++LINEITEM_it1090)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it1090);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it1090);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it1090);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it1090);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it1090);
                        double protect_L__EXTENDEDPRICE = get<5>(
                            *LINEITEM_it1090);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it1090);
                        double protect_L__TAX = get<7>(*LINEITEM_it1090);
                        int64_t var1243 = 0;
                        var1243 += protect_C__CUSTKEY;
                        int64_t var1244 = 0;
                        var1244 += protect_O__CUSTKEY;
                        if ( var1243 == var1244 )
                        {
                            int64_t var1241 = 0;
                            var1241 += protect_L__SUPPKEY;
                            int64_t var1242 = 0;
                            var1242 += protect_S__SUPPKEY;
                            if ( var1241 == var1242 )
                            {
                                int64_t var1239 = 0;
                                var1239 += protect_L__ORDERKEY;
                                int64_t var1240 = 0;
                                var1240 += protect_O__ORDERKEY;
                                if ( var1239 == var1240 )
                                {
                                    int64_t var1237 = 0;
                                    var1237 += x_qNATION_N__NATIONKEY;
                                    int64_t var1238 = 0;
                                    var1238 += protect_C__NATIONKEY;
                                    if ( var1237 == var1238 )
                                    {
                                        int64_t var1235 = 0;
                                        var1235 += x_qNATION_N__NATIONKEY;
                                        int64_t var1236 = 0;
                                        var1236 += protect_S__NATIONKEY;
                                        if ( var1235 == var1236 )
                                        {
                                            qNATION1[x_qNATION_N__NATIONKEY] += 
                                                protect_L__EXTENDEDPRICE;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<int64_t,double>::iterator qNATION3_it1108 = qNATION3.begin();
    map<int64_t,double>::iterator qNATION3_end1107 = qNATION3.end();
    for (; qNATION3_it1108 != qNATION3_end1107; ++qNATION3_it1108)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION3_it1108->first;
        qNATION3[x_qNATION_N__NATIONKEY] = 0;
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it1106 = CUSTOMER.begin();
        
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end1105 = CUSTOMER.end();
        for (; CUSTOMER_it1106 != CUSTOMER_end1105; ++CUSTOMER_it1106)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1106);
            string protect_C__NAME = get<1>(*CUSTOMER_it1106);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it1106);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1106);
            string protect_C__PHONE = get<4>(*CUSTOMER_it1106);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1106);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1106);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it1106);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it1104 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end1103 = SUPPLIER.end();
            for (; SUPPLIER_it1104 != SUPPLIER_end1103; ++SUPPLIER_it1104)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1104);
                string protect_S__NAME = get<1>(*SUPPLIER_it1104);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it1104);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1104);
                string protect_S__PHONE = get<4>(*SUPPLIER_it1104);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1104);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it1104);
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it1102 = ORDERS.begin();
                
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end1101 = ORDERS.end();
                for (; ORDERS_it1102 != ORDERS_end1101; ++ORDERS_it1102)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it1102);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1102);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it1102);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it1102);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it1102);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it1102);
                    string protect_O__CLERK = get<6>(*ORDERS_it1102);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it1102);
                    string protect_O__COMMENT = get<8>(*ORDERS_it1102);
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it1100 = LINEITEM.begin();
                    
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end1099 = LINEITEM.end();
                    for (
                        ; LINEITEM_it1100 != LINEITEM_end1099; ++LINEITEM_it1100)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it1100);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it1100);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it1100);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it1100);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it1100);
                        double protect_L__EXTENDEDPRICE = get<5>(
                            *LINEITEM_it1100);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it1100);
                        double protect_L__TAX = get<7>(*LINEITEM_it1100);
                        int64_t var1254 = 0;
                        var1254 += protect_C__CUSTKEY;
                        int64_t var1255 = 0;
                        var1255 += protect_O__CUSTKEY;
                        if ( var1254 == var1255 )
                        {
                            int64_t var1252 = 0;
                            var1252 += protect_L__SUPPKEY;
                            int64_t var1253 = 0;
                            var1253 += protect_S__SUPPKEY;
                            if ( var1252 == var1253 )
                            {
                                int64_t var1250 = 0;
                                var1250 += protect_L__ORDERKEY;
                                int64_t var1251 = 0;
                                var1251 += protect_O__ORDERKEY;
                                if ( var1250 == var1251 )
                                {
                                    int64_t var1248 = 0;
                                    var1248 += x_qNATION_N__NATIONKEY;
                                    int64_t var1249 = 0;
                                    var1249 += protect_C__NATIONKEY;
                                    if ( var1248 == var1249 )
                                    {
                                        int64_t var1246 = 0;
                                        var1246 += x_qNATION_N__NATIONKEY;
                                        int64_t var1247 = 0;
                                        var1247 += protect_S__NATIONKEY;
                                        if ( var1246 == var1247 )
                                        {
                                            double var1245 = 1;
                                            var1245 *= protect_L__EXTENDEDPRICE;
                                            var1245 *= protect_L__DISCOUNT;
                                            qNATION3[x_qNATION_N__NATIONKEY] += 
                                                var1245;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it1120 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end1119 = 
        qORDERS1.end();
    for (; qORDERS1_it1120 != qORDERS1_end1119; ++qORDERS1_it1120)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it1120->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it1120->first);
        string N__NAME = get<2>(qORDERS1_it1120->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it1118 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end1117 = 
            REGION.end();
        for (; REGION_it1118 != REGION_end1117; ++REGION_it1118)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it1118);
            string protect_R__NAME = get<1>(*REGION_it1118);
            string protect_R__COMMENT = get<2>(*REGION_it1118);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it1116 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end1115 = NATION.end();
            for (; NATION_it1116 != NATION_end1115; ++NATION_it1116)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it1116);
                string protect_N__NAME = get<1>(*NATION_it1116);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it1116);
                string protect_N__COMMENT = get<3>(*NATION_it1116);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it1114 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end1113 = CUSTOMER.end();
                for (; CUSTOMER_it1114 != CUSTOMER_end1113; ++CUSTOMER_it1114)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1114);
                    string protect_C__NAME = get<1>(*CUSTOMER_it1114);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it1114);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1114);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it1114);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1114);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1114);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it1114);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it1112 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end1111 = SUPPLIER.end();
                    for (
                        ; SUPPLIER_it1112 != SUPPLIER_end1111; ++SUPPLIER_it1112)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1112);
                        string protect_S__NAME = get<1>(*SUPPLIER_it1112);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it1112);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1112);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it1112);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1112);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it1112);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1110 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1109 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1110 != LINEITEM_end1109; ++LINEITEM_it1110)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1110);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1110);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1110);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1110);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1110);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1110);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1110);
                            double protect_L__TAX = get<7>(*LINEITEM_it1110);
                            int64_t var1268 = 0;
                            var1268 += protect_N__REGIONKEY;
                            int64_t var1269 = 0;
                            var1269 += protect_R__REGIONKEY;
                            if ( var1268 == var1269 )
                            {
                                int64_t var1266 = 0;
                                var1266 += protect_C__NATIONKEY;
                                int64_t var1267 = 0;
                                var1267 += protect_N__NATIONKEY;
                                if ( var1266 == var1267 )
                                {
                                    int64_t var1264 = 0;
                                    var1264 += protect_C__NATIONKEY;
                                    int64_t var1265 = 0;
                                    var1265 += protect_S__NATIONKEY;
                                    if ( var1264 == var1265 )
                                    {
                                        int64_t var1262 = 0;
                                        var1262 += protect_L__SUPPKEY;
                                        int64_t var1263 = 0;
                                        var1263 += protect_S__SUPPKEY;
                                        if ( var1262 == var1263 )
                                        {
                                            string var1260 = 0;
                                            var1260 += N__NAME;
                                            string var1261 = 0;
                                            var1261 += protect_N__NAME;
                                            if ( var1260 == var1261 )
                                            {
                                                int64_t var1258 = 0;
                                                var1258 += x_qORDERS_O__CUSTKEY;
                                                int64_t var1259 = 0;
                                                var1259 += protect_C__CUSTKEY;
                                                if ( var1258 == var1259 )
                                                {
                                                    int64_t var1256 = 0;
                                                    var1256 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var1257 = 0;
                                                    var1257 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var1256 == var1257 )
                                                    {
                                                        qORDERS1[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it1132 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end1131 = 
        qORDERS2.end();
    for (; qORDERS2_it1132 != qORDERS2_end1131; ++qORDERS2_it1132)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it1132->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it1132->first);
        string N__NAME = get<2>(qORDERS2_it1132->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it1130 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end1129 = 
            REGION.end();
        for (; REGION_it1130 != REGION_end1129; ++REGION_it1130)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it1130);
            string protect_R__NAME = get<1>(*REGION_it1130);
            string protect_R__COMMENT = get<2>(*REGION_it1130);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it1128 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end1127 = NATION.end();
            for (; NATION_it1128 != NATION_end1127; ++NATION_it1128)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it1128);
                string protect_N__NAME = get<1>(*NATION_it1128);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it1128);
                string protect_N__COMMENT = get<3>(*NATION_it1128);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it1126 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end1125 = CUSTOMER.end();
                for (; CUSTOMER_it1126 != CUSTOMER_end1125; ++CUSTOMER_it1126)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1126);
                    string protect_C__NAME = get<1>(*CUSTOMER_it1126);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it1126);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1126);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it1126);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1126);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1126);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it1126);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it1124 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end1123 = SUPPLIER.end();
                    for (
                        ; SUPPLIER_it1124 != SUPPLIER_end1123; ++SUPPLIER_it1124)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1124);
                        string protect_S__NAME = get<1>(*SUPPLIER_it1124);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it1124);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1124);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it1124);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1124);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it1124);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1122 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1121 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1122 != LINEITEM_end1121; ++LINEITEM_it1122)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1122);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1122);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1122);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1122);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1122);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1122);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1122);
                            double protect_L__TAX = get<7>(*LINEITEM_it1122);
                            int64_t var1283 = 0;
                            var1283 += protect_N__REGIONKEY;
                            int64_t var1284 = 0;
                            var1284 += protect_R__REGIONKEY;
                            if ( var1283 == var1284 )
                            {
                                int64_t var1281 = 0;
                                var1281 += protect_C__NATIONKEY;
                                int64_t var1282 = 0;
                                var1282 += protect_N__NATIONKEY;
                                if ( var1281 == var1282 )
                                {
                                    int64_t var1279 = 0;
                                    var1279 += protect_C__NATIONKEY;
                                    int64_t var1280 = 0;
                                    var1280 += protect_S__NATIONKEY;
                                    if ( var1279 == var1280 )
                                    {
                                        int64_t var1277 = 0;
                                        var1277 += protect_L__SUPPKEY;
                                        int64_t var1278 = 0;
                                        var1278 += protect_S__SUPPKEY;
                                        if ( var1277 == var1278 )
                                        {
                                            string var1275 = 0;
                                            var1275 += N__NAME;
                                            string var1276 = 0;
                                            var1276 += protect_N__NAME;
                                            if ( var1275 == var1276 )
                                            {
                                                int64_t var1273 = 0;
                                                var1273 += x_qORDERS_O__CUSTKEY;
                                                int64_t var1274 = 0;
                                                var1274 += protect_C__CUSTKEY;
                                                if ( var1273 == var1274 )
                                                {
                                                    int64_t var1271 = 0;
                                                    var1271 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var1272 = 0;
                                                    var1272 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var1271 == var1272 )
                                                    {
                                                        double var1270 = 1;
                                                        var1270 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var1270 *= 
                                                            protect_L__DISCOUNT;
                                                        qORDERS2[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += var1270;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<string,int64_t>,double>::iterator qREGION1_it1144 = 
        qREGION1.begin();
    map<tuple<string,int64_t>,double>::iterator qREGION1_end1143 = qREGION1.end(
        );
    for (; qREGION1_it1144 != qREGION1_end1143; ++qREGION1_it1144)
    {
        string N__NAME = get<0>(qREGION1_it1144->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it1144->first);
        qREGION1[make_tuple(N__NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it1142 
            = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_end1141 = NATION.end();
        for (; NATION_it1142 != NATION_end1141; ++NATION_it1142)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it1142);
            string protect_N__NAME = get<1>(*NATION_it1142);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it1142);
            string protect_N__COMMENT = get<3>(*NATION_it1142);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it1140 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end1139 = CUSTOMER.end();
            for (; CUSTOMER_it1140 != CUSTOMER_end1139; ++CUSTOMER_it1140)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1140);
                string protect_C__NAME = get<1>(*CUSTOMER_it1140);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it1140);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1140);
                string protect_C__PHONE = get<4>(*CUSTOMER_it1140);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1140);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1140);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it1140);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it1138 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end1137 = SUPPLIER.end();
                for (; SUPPLIER_it1138 != SUPPLIER_end1137; ++SUPPLIER_it1138)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1138);
                    string protect_S__NAME = get<1>(*SUPPLIER_it1138);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it1138);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1138);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it1138);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1138);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it1138);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it1136 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end1135 = ORDERS.end();
                    for (; ORDERS_it1136 != ORDERS_end1135; ++ORDERS_it1136)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it1136);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1136);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it1136);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it1136);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it1136);
                        string protect_O__ORDERPRIORITY = get<5>(
                            *ORDERS_it1136);
                        string protect_O__CLERK = get<6>(*ORDERS_it1136);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it1136);
                        string protect_O__COMMENT = get<8>(*ORDERS_it1136);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1134 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1133 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1134 != LINEITEM_end1133; ++LINEITEM_it1134)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1134);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1134);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1134);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1134);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1134);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1134);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1134);
                            double protect_L__TAX = get<7>(*LINEITEM_it1134);
                            int64_t var1297 = 0;
                            var1297 += protect_C__NATIONKEY;
                            int64_t var1298 = 0;
                            var1298 += protect_N__NATIONKEY;
                            if ( var1297 == var1298 )
                            {
                                int64_t var1295 = 0;
                                var1295 += protect_C__NATIONKEY;
                                int64_t var1296 = 0;
                                var1296 += protect_S__NATIONKEY;
                                if ( var1295 == var1296 )
                                {
                                    int64_t var1293 = 0;
                                    var1293 += protect_C__CUSTKEY;
                                    int64_t var1294 = 0;
                                    var1294 += protect_O__CUSTKEY;
                                    if ( var1293 == var1294 )
                                    {
                                        int64_t var1291 = 0;
                                        var1291 += protect_L__SUPPKEY;
                                        int64_t var1292 = 0;
                                        var1292 += protect_S__SUPPKEY;
                                        if ( var1291 == var1292 )
                                        {
                                            int64_t var1289 = 0;
                                            var1289 += protect_L__ORDERKEY;
                                            int64_t var1290 = 0;
                                            var1290 += protect_O__ORDERKEY;
                                            if ( var1289 == var1290 )
                                            {
                                                int64_t var1287 = 0;
                                                var1287 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var1288 = 0;
                                                var1288 += protect_N__REGIONKEY;
                                                if ( var1287 == var1288 )
                                                {
                                                    string var1285 = 0;
                                                    var1285 += N__NAME;
                                                    string var1286 = 0;
                                                    var1286 += protect_N__NAME;
                                                    if ( var1285 == var1286 )
                                                    {
                                                        qREGION1[make_tuple(
                                                            N__NAME,x_qREGION_R__REGIONKEY)] += protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it1156 = 
        qREGION2.begin();
    map<tuple<string,int64_t>,double>::iterator qREGION2_end1155 = qREGION2.end(
        );
    for (; qREGION2_it1156 != qREGION2_end1155; ++qREGION2_it1156)
    {
        string N__NAME = get<0>(qREGION2_it1156->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it1156->first);
        qREGION2[make_tuple(N__NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it1154 
            = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_end1153 = NATION.end();
        for (; NATION_it1154 != NATION_end1153; ++NATION_it1154)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it1154);
            string protect_N__NAME = get<1>(*NATION_it1154);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it1154);
            string protect_N__COMMENT = get<3>(*NATION_it1154);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it1152 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end1151 = CUSTOMER.end();
            for (; CUSTOMER_it1152 != CUSTOMER_end1151; ++CUSTOMER_it1152)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1152);
                string protect_C__NAME = get<1>(*CUSTOMER_it1152);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it1152);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1152);
                string protect_C__PHONE = get<4>(*CUSTOMER_it1152);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1152);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1152);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it1152);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it1150 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end1149 = SUPPLIER.end();
                for (; SUPPLIER_it1150 != SUPPLIER_end1149; ++SUPPLIER_it1150)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1150);
                    string protect_S__NAME = get<1>(*SUPPLIER_it1150);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it1150);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1150);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it1150);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1150);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it1150);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it1148 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end1147 = ORDERS.end();
                    for (; ORDERS_it1148 != ORDERS_end1147; ++ORDERS_it1148)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it1148);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1148);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it1148);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it1148);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it1148);
                        string protect_O__ORDERPRIORITY = get<5>(
                            *ORDERS_it1148);
                        string protect_O__CLERK = get<6>(*ORDERS_it1148);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it1148);
                        string protect_O__COMMENT = get<8>(*ORDERS_it1148);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1146 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1145 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1146 != LINEITEM_end1145; ++LINEITEM_it1146)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1146);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1146);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1146);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1146);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1146);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1146);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1146);
                            double protect_L__TAX = get<7>(*LINEITEM_it1146);
                            int64_t var1312 = 0;
                            var1312 += protect_C__NATIONKEY;
                            int64_t var1313 = 0;
                            var1313 += protect_N__NATIONKEY;
                            if ( var1312 == var1313 )
                            {
                                int64_t var1310 = 0;
                                var1310 += protect_C__NATIONKEY;
                                int64_t var1311 = 0;
                                var1311 += protect_S__NATIONKEY;
                                if ( var1310 == var1311 )
                                {
                                    int64_t var1308 = 0;
                                    var1308 += protect_C__CUSTKEY;
                                    int64_t var1309 = 0;
                                    var1309 += protect_O__CUSTKEY;
                                    if ( var1308 == var1309 )
                                    {
                                        int64_t var1306 = 0;
                                        var1306 += protect_L__SUPPKEY;
                                        int64_t var1307 = 0;
                                        var1307 += protect_S__SUPPKEY;
                                        if ( var1306 == var1307 )
                                        {
                                            int64_t var1304 = 0;
                                            var1304 += protect_L__ORDERKEY;
                                            int64_t var1305 = 0;
                                            var1305 += protect_O__ORDERKEY;
                                            if ( var1304 == var1305 )
                                            {
                                                int64_t var1302 = 0;
                                                var1302 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var1303 = 0;
                                                var1303 += protect_N__REGIONKEY;
                                                if ( var1302 == var1303 )
                                                {
                                                    string var1300 = 0;
                                                    var1300 += N__NAME;
                                                    string var1301 = 0;
                                                    var1301 += protect_N__NAME;
                                                    if ( var1300 == var1301 )
                                                    {
                                                        double var1299 = 1;
                                                        var1299 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var1299 *= 
                                                            protect_L__DISCOUNT;
                                                        qREGION2[make_tuple(
                                                            N__NAME,x_qREGION_R__REGIONKEY)] += var1299;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it1168 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end1167 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it1168 != qSUPPLIER1_end1167; ++qSUPPLIER1_it1168)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it1168->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER1_it1168->first);
        string N__NAME = get<2>(qSUPPLIER1_it1168->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it1166 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end1165 = 
            REGION.end();
        for (; REGION_it1166 != REGION_end1165; ++REGION_it1166)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it1166);
            string protect_R__NAME = get<1>(*REGION_it1166);
            string protect_R__COMMENT = get<2>(*REGION_it1166);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it1164 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end1163 = NATION.end();
            for (; NATION_it1164 != NATION_end1163; ++NATION_it1164)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it1164);
                string protect_N__NAME = get<1>(*NATION_it1164);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it1164);
                string protect_N__COMMENT = get<3>(*NATION_it1164);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it1162 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end1161 = CUSTOMER.end();
                for (; CUSTOMER_it1162 != CUSTOMER_end1161; ++CUSTOMER_it1162)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1162);
                    string protect_C__NAME = get<1>(*CUSTOMER_it1162);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it1162);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1162);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it1162);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1162);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1162);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it1162);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it1160 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end1159 = ORDERS.end();
                    for (; ORDERS_it1160 != ORDERS_end1159; ++ORDERS_it1160)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it1160);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1160);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it1160);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it1160);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it1160);
                        string protect_O__ORDERPRIORITY = get<5>(
                            *ORDERS_it1160);
                        string protect_O__CLERK = get<6>(*ORDERS_it1160);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it1160);
                        string protect_O__COMMENT = get<8>(*ORDERS_it1160);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1158 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1157 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1158 != LINEITEM_end1157; ++LINEITEM_it1158)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1158);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1158);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1158);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1158);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1158);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1158);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1158);
                            double protect_L__TAX = get<7>(*LINEITEM_it1158);
                            int64_t var1326 = 0;
                            var1326 += protect_N__REGIONKEY;
                            int64_t var1327 = 0;
                            var1327 += protect_R__REGIONKEY;
                            if ( var1326 == var1327 )
                            {
                                int64_t var1324 = 0;
                                var1324 += protect_C__CUSTKEY;
                                int64_t var1325 = 0;
                                var1325 += protect_O__CUSTKEY;
                                if ( var1324 == var1325 )
                                {
                                    int64_t var1322 = 0;
                                    var1322 += protect_L__ORDERKEY;
                                    int64_t var1323 = 0;
                                    var1323 += protect_O__ORDERKEY;
                                    if ( var1322 == var1323 )
                                    {
                                        string var1320 = 0;
                                        var1320 += N__NAME;
                                        string var1321 = 0;
                                        var1321 += protect_N__NAME;
                                        if ( var1320 == var1321 )
                                        {
                                            int64_t var1318 = 0;
                                            var1318 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var1319 = 0;
                                            var1319 += protect_N__NATIONKEY;
                                            if ( var1318 == var1319 )
                                            {
                                                int64_t var1316 = 0;
                                                var1316 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var1317 = 0;
                                                var1317 += protect_C__NATIONKEY;
                                                if ( var1316 == var1317 )
                                                {
                                                    int64_t var1314 = 0;
                                                    var1314 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var1315 = 0;
                                                    var1315 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var1314 == var1315 )
                                                    {
                                                        qSUPPLIER1[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it1180 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end1179 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it1180 != qSUPPLIER2_end1179; ++qSUPPLIER2_it1180)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it1180->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER2_it1180->first);
        string N__NAME = get<2>(qSUPPLIER2_it1180->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it1178 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end1177 = 
            REGION.end();
        for (; REGION_it1178 != REGION_end1177; ++REGION_it1178)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it1178);
            string protect_R__NAME = get<1>(*REGION_it1178);
            string protect_R__COMMENT = get<2>(*REGION_it1178);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it1176 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end1175 = NATION.end();
            for (; NATION_it1176 != NATION_end1175; ++NATION_it1176)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it1176);
                string protect_N__NAME = get<1>(*NATION_it1176);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it1176);
                string protect_N__COMMENT = get<3>(*NATION_it1176);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it1174 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end1173 = CUSTOMER.end();
                for (; CUSTOMER_it1174 != CUSTOMER_end1173; ++CUSTOMER_it1174)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1174);
                    string protect_C__NAME = get<1>(*CUSTOMER_it1174);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it1174);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1174);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it1174);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1174);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1174);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it1174);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it1172 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end1171 = ORDERS.end();
                    for (; ORDERS_it1172 != ORDERS_end1171; ++ORDERS_it1172)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it1172);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1172);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it1172);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it1172);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it1172);
                        string protect_O__ORDERPRIORITY = get<5>(
                            *ORDERS_it1172);
                        string protect_O__CLERK = get<6>(*ORDERS_it1172);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it1172);
                        string protect_O__COMMENT = get<8>(*ORDERS_it1172);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1170 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1169 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1170 != LINEITEM_end1169; ++LINEITEM_it1170)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1170);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1170);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1170);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1170);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1170);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1170);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1170);
                            double protect_L__TAX = get<7>(*LINEITEM_it1170);
                            int64_t var1341 = 0;
                            var1341 += protect_N__REGIONKEY;
                            int64_t var1342 = 0;
                            var1342 += protect_R__REGIONKEY;
                            if ( var1341 == var1342 )
                            {
                                int64_t var1339 = 0;
                                var1339 += protect_C__CUSTKEY;
                                int64_t var1340 = 0;
                                var1340 += protect_O__CUSTKEY;
                                if ( var1339 == var1340 )
                                {
                                    int64_t var1337 = 0;
                                    var1337 += protect_L__ORDERKEY;
                                    int64_t var1338 = 0;
                                    var1338 += protect_O__ORDERKEY;
                                    if ( var1337 == var1338 )
                                    {
                                        string var1335 = 0;
                                        var1335 += N__NAME;
                                        string var1336 = 0;
                                        var1336 += protect_N__NAME;
                                        if ( var1335 == var1336 )
                                        {
                                            int64_t var1333 = 0;
                                            var1333 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var1334 = 0;
                                            var1334 += protect_N__NATIONKEY;
                                            if ( var1333 == var1334 )
                                            {
                                                int64_t var1331 = 0;
                                                var1331 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var1332 = 0;
                                                var1332 += protect_C__NATIONKEY;
                                                if ( var1331 == var1332 )
                                                {
                                                    int64_t var1329 = 0;
                                                    var1329 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var1330 = 0;
                                                    var1330 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var1329 == var1330 )
                                                    {
                                                        double var1328 = 1;
                                                        var1328 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var1328 *= 
                                                            protect_L__DISCOUNT;
                                                        qSUPPLIER2[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += var1328;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_CUSTOMER_sec_span, on_delete_CUSTOMER_usec_span);
}

void on_delete_NATION(
    ofstream* results, ofstream* log, ofstream* stats, int64_t NATIONKEY,string 
    NAME,int64_t REGIONKEY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    NATION.erase(make_tuple(NATIONKEY,NAME,REGIONKEY,COMMENT));
    q[NAME] += -1*qNATION1[NATIONKEY]*qNATION2[REGIONKEY];
    q[NAME] += -1*qNATION3[NATIONKEY]*qNATION2[REGIONKEY]*-1;
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it1192 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end1191 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it1192 != qCUSTOMER1_end1191; ++qCUSTOMER1_it1192)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it1192->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER1_it1192->first);
        string NAME = get<2>(qCUSTOMER1_it1192->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it1190 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end1189 = 
            REGION.end();
        for (; REGION_it1190 != REGION_end1189; ++REGION_it1190)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it1190);
            string protect_R__NAME = get<1>(*REGION_it1190);
            string protect_R__COMMENT = get<2>(*REGION_it1190);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it1188 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end1187 = NATION.end();
            for (; NATION_it1188 != NATION_end1187; ++NATION_it1188)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it1188);
                string protect_N__NAME = get<1>(*NATION_it1188);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it1188);
                string protect_N__COMMENT = get<3>(*NATION_it1188);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it1186 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end1185 = SUPPLIER.end();
                for (; SUPPLIER_it1186 != SUPPLIER_end1185; ++SUPPLIER_it1186)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1186);
                    string protect_S__NAME = get<1>(*SUPPLIER_it1186);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it1186);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1186);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it1186);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1186);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it1186);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it1184 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end1183 = ORDERS.end();
                    for (; ORDERS_it1184 != ORDERS_end1183; ++ORDERS_it1184)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it1184);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1184);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it1184);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it1184);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it1184);
                        string protect_O__ORDERPRIORITY = get<5>(
                            *ORDERS_it1184);
                        string protect_O__CLERK = get<6>(*ORDERS_it1184);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it1184);
                        string protect_O__COMMENT = get<8>(*ORDERS_it1184);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1182 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1181 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1182 != LINEITEM_end1181; ++LINEITEM_it1182)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1182);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1182);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1182);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1182);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1182);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1182);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1182);
                            double protect_L__TAX = get<7>(*LINEITEM_it1182);
                            int64_t var1355 = 0;
                            var1355 += protect_N__REGIONKEY;
                            int64_t var1356 = 0;
                            var1356 += protect_R__REGIONKEY;
                            if ( var1355 == var1356 )
                            {
                                int64_t var1353 = 0;
                                var1353 += protect_L__SUPPKEY;
                                int64_t var1354 = 0;
                                var1354 += protect_S__SUPPKEY;
                                if ( var1353 == var1354 )
                                {
                                    int64_t var1351 = 0;
                                    var1351 += protect_L__ORDERKEY;
                                    int64_t var1352 = 0;
                                    var1352 += protect_O__ORDERKEY;
                                    if ( var1351 == var1352 )
                                    {
                                        string var1349 = 0;
                                        var1349 += NAME;
                                        string var1350 = 0;
                                        var1350 += protect_N__NAME;
                                        if ( var1349 == var1350 )
                                        {
                                            int64_t var1347 = 0;
                                            var1347 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var1348 = 0;
                                            var1348 += protect_N__NATIONKEY;
                                            if ( var1347 == var1348 )
                                            {
                                                int64_t var1345 = 0;
                                                var1345 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var1346 = 0;
                                                var1346 += protect_S__NATIONKEY;
                                                if ( var1345 == var1346 )
                                                {
                                                    int64_t var1343 = 0;
                                                    var1343 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var1344 = 0;
                                                    var1344 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var1343 == var1344 )
                                                    {
                                                        qCUSTOMER1[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it1204 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end1203 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it1204 != qCUSTOMER2_end1203; ++qCUSTOMER2_it1204)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it1204->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER2_it1204->first);
        string NAME = get<2>(qCUSTOMER2_it1204->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it1202 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end1201 = 
            REGION.end();
        for (; REGION_it1202 != REGION_end1201; ++REGION_it1202)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it1202);
            string protect_R__NAME = get<1>(*REGION_it1202);
            string protect_R__COMMENT = get<2>(*REGION_it1202);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it1200 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end1199 = NATION.end();
            for (; NATION_it1200 != NATION_end1199; ++NATION_it1200)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it1200);
                string protect_N__NAME = get<1>(*NATION_it1200);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it1200);
                string protect_N__COMMENT = get<3>(*NATION_it1200);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it1198 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end1197 = SUPPLIER.end();
                for (; SUPPLIER_it1198 != SUPPLIER_end1197; ++SUPPLIER_it1198)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1198);
                    string protect_S__NAME = get<1>(*SUPPLIER_it1198);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it1198);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1198);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it1198);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1198);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it1198);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it1196 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end1195 = ORDERS.end();
                    for (; ORDERS_it1196 != ORDERS_end1195; ++ORDERS_it1196)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it1196);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1196);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it1196);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it1196);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it1196);
                        string protect_O__ORDERPRIORITY = get<5>(
                            *ORDERS_it1196);
                        string protect_O__CLERK = get<6>(*ORDERS_it1196);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it1196);
                        string protect_O__COMMENT = get<8>(*ORDERS_it1196);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1194 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1193 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1194 != LINEITEM_end1193; ++LINEITEM_it1194)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1194);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1194);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1194);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1194);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1194);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1194);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1194);
                            double protect_L__TAX = get<7>(*LINEITEM_it1194);
                            int64_t var1370 = 0;
                            var1370 += protect_N__REGIONKEY;
                            int64_t var1371 = 0;
                            var1371 += protect_R__REGIONKEY;
                            if ( var1370 == var1371 )
                            {
                                int64_t var1368 = 0;
                                var1368 += protect_L__SUPPKEY;
                                int64_t var1369 = 0;
                                var1369 += protect_S__SUPPKEY;
                                if ( var1368 == var1369 )
                                {
                                    int64_t var1366 = 0;
                                    var1366 += protect_L__ORDERKEY;
                                    int64_t var1367 = 0;
                                    var1367 += protect_O__ORDERKEY;
                                    if ( var1366 == var1367 )
                                    {
                                        string var1364 = 0;
                                        var1364 += NAME;
                                        string var1365 = 0;
                                        var1365 += protect_N__NAME;
                                        if ( var1364 == var1365 )
                                        {
                                            int64_t var1362 = 0;
                                            var1362 += x_qCUSTOMER_C__NATIONKEY;
                                            int64_t var1363 = 0;
                                            var1363 += protect_N__NATIONKEY;
                                            if ( var1362 == var1363 )
                                            {
                                                int64_t var1360 = 0;
                                                var1360 += 
                                                    x_qCUSTOMER_C__NATIONKEY;
                                                int64_t var1361 = 0;
                                                var1361 += protect_S__NATIONKEY;
                                                if ( var1360 == var1361 )
                                                {
                                                    int64_t var1358 = 0;
                                                    var1358 += 
                                                        x_qCUSTOMER_C__CUSTKEY;
                                                    int64_t var1359 = 0;
                                                    var1359 += 
                                                        protect_O__CUSTKEY;
                                                    if ( var1358 == var1359 )
                                                    {
                                                        double var1357 = 1;
                                                        var1357 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var1357 *= 
                                                            protect_L__DISCOUNT;
                                                        qCUSTOMER2[make_tuple(
                                                            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,NAME)] += var1357;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it1216 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end1215 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it1216 != qLINEITEM1_end1215; ++qLINEITEM1_it1216)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it1216->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it1216->first);
        string NAME = get<2>(qLINEITEM1_it1216->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it1214 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end1213 = 
            REGION.end();
        for (; REGION_it1214 != REGION_end1213; ++REGION_it1214)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it1214);
            string protect_R__NAME = get<1>(*REGION_it1214);
            string protect_R__COMMENT = get<2>(*REGION_it1214);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it1212 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end1211 = NATION.end();
            for (; NATION_it1212 != NATION_end1211; ++NATION_it1212)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it1212);
                string protect_N__NAME = get<1>(*NATION_it1212);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it1212);
                string protect_N__COMMENT = get<3>(*NATION_it1212);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it1210 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end1209 = SUPPLIER.end();
                for (; SUPPLIER_it1210 != SUPPLIER_end1209; ++SUPPLIER_it1210)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1210);
                    string protect_S__NAME = get<1>(*SUPPLIER_it1210);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it1210);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1210);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it1210);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1210);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it1210);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_it1208 = CUSTOMER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                        >::iterator CUSTOMER_end1207 = CUSTOMER.end();
                    for (
                        ; CUSTOMER_it1208 != CUSTOMER_end1207; ++CUSTOMER_it1208)
                    {
                        int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1208);
                        string protect_C__NAME = get<1>(*CUSTOMER_it1208);
                        string protect_C__ADDRESS = get<2>(*CUSTOMER_it1208);
                        int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1208);
                        string protect_C__PHONE = get<4>(*CUSTOMER_it1208);
                        double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1208);
                        string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1208);
                        string protect_C__COMMENT = get<7>(*CUSTOMER_it1208);
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it1206 = ORDERS.begin();
                        
                            multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end1205 = ORDERS.end();
                        for (; ORDERS_it1206 != ORDERS_end1205; ++ORDERS_it1206)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(
                                *ORDERS_it1206);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1206);
                            string protect_O__ORDERSTATUS = get<2>(
                                *ORDERS_it1206);
                            double protect_O__TOTALPRICE = get<3>(
                                *ORDERS_it1206);
                            string protect_O__ORDERDATE = get<4>(
                                *ORDERS_it1206);
                            string protect_O__ORDERPRIORITY = get<5>(
                                *ORDERS_it1206);
                            string protect_O__CLERK = get<6>(*ORDERS_it1206);
                            int protect_O__SHIPPRIORITY = get<7>(
                                *ORDERS_it1206);
                            string protect_O__COMMENT = get<8>(*ORDERS_it1206);
                            int64_t var1384 = 0;
                            var1384 += protect_N__REGIONKEY;
                            int64_t var1385 = 0;
                            var1385 += protect_R__REGIONKEY;
                            if ( var1384 == var1385 )
                            {
                                int64_t var1382 = 0;
                                var1382 += protect_C__NATIONKEY;
                                int64_t var1383 = 0;
                                var1383 += protect_N__NATIONKEY;
                                if ( var1382 == var1383 )
                                {
                                    int64_t var1380 = 0;
                                    var1380 += protect_C__NATIONKEY;
                                    int64_t var1381 = 0;
                                    var1381 += protect_S__NATIONKEY;
                                    if ( var1380 == var1381 )
                                    {
                                        int64_t var1378 = 0;
                                        var1378 += protect_C__CUSTKEY;
                                        int64_t var1379 = 0;
                                        var1379 += protect_O__CUSTKEY;
                                        if ( var1378 == var1379 )
                                        {
                                            string var1376 = 0;
                                            var1376 += NAME;
                                            string var1377 = 0;
                                            var1377 += protect_N__NAME;
                                            if ( var1376 == var1377 )
                                            {
                                                int64_t var1374 = 0;
                                                var1374 += 
                                                    x_qLINEITEM_L__SUPPKEY;
                                                int64_t var1375 = 0;
                                                var1375 += protect_S__SUPPKEY;
                                                if ( var1374 == var1375 )
                                                {
                                                    int64_t var1372 = 0;
                                                    var1372 += 
                                                        x_qLINEITEM_L__ORDERKEY;
                                                    int64_t var1373 = 0;
                                                    var1373 += 
                                                        protect_O__ORDERKEY;
                                                    if ( var1372 == var1373 )
                                                    {
                                                        qLINEITEM1[make_tuple(
                                                            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,NAME)] += 1;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it1228 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end1227 = 
        qORDERS1.end();
    for (; qORDERS1_it1228 != qORDERS1_end1227; ++qORDERS1_it1228)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it1228->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it1228->first);
        string NAME = get<2>(qORDERS1_it1228->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it1226 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end1225 = 
            REGION.end();
        for (; REGION_it1226 != REGION_end1225; ++REGION_it1226)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it1226);
            string protect_R__NAME = get<1>(*REGION_it1226);
            string protect_R__COMMENT = get<2>(*REGION_it1226);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it1224 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end1223 = NATION.end();
            for (; NATION_it1224 != NATION_end1223; ++NATION_it1224)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it1224);
                string protect_N__NAME = get<1>(*NATION_it1224);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it1224);
                string protect_N__COMMENT = get<3>(*NATION_it1224);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it1222 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end1221 = CUSTOMER.end();
                for (; CUSTOMER_it1222 != CUSTOMER_end1221; ++CUSTOMER_it1222)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1222);
                    string protect_C__NAME = get<1>(*CUSTOMER_it1222);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it1222);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1222);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it1222);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1222);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1222);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it1222);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it1220 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end1219 = SUPPLIER.end();
                    for (
                        ; SUPPLIER_it1220 != SUPPLIER_end1219; ++SUPPLIER_it1220)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1220);
                        string protect_S__NAME = get<1>(*SUPPLIER_it1220);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it1220);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1220);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it1220);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1220);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it1220);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1218 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1217 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1218 != LINEITEM_end1217; ++LINEITEM_it1218)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1218);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1218);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1218);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1218);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1218);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1218);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1218);
                            double protect_L__TAX = get<7>(*LINEITEM_it1218);
                            int64_t var1398 = 0;
                            var1398 += protect_N__REGIONKEY;
                            int64_t var1399 = 0;
                            var1399 += protect_R__REGIONKEY;
                            if ( var1398 == var1399 )
                            {
                                int64_t var1396 = 0;
                                var1396 += protect_C__NATIONKEY;
                                int64_t var1397 = 0;
                                var1397 += protect_N__NATIONKEY;
                                if ( var1396 == var1397 )
                                {
                                    int64_t var1394 = 0;
                                    var1394 += protect_C__NATIONKEY;
                                    int64_t var1395 = 0;
                                    var1395 += protect_S__NATIONKEY;
                                    if ( var1394 == var1395 )
                                    {
                                        int64_t var1392 = 0;
                                        var1392 += protect_L__SUPPKEY;
                                        int64_t var1393 = 0;
                                        var1393 += protect_S__SUPPKEY;
                                        if ( var1392 == var1393 )
                                        {
                                            string var1390 = 0;
                                            var1390 += NAME;
                                            string var1391 = 0;
                                            var1391 += protect_N__NAME;
                                            if ( var1390 == var1391 )
                                            {
                                                int64_t var1388 = 0;
                                                var1388 += x_qORDERS_O__CUSTKEY;
                                                int64_t var1389 = 0;
                                                var1389 += protect_C__CUSTKEY;
                                                if ( var1388 == var1389 )
                                                {
                                                    int64_t var1386 = 0;
                                                    var1386 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var1387 = 0;
                                                    var1387 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var1386 == var1387 )
                                                    {
                                                        qORDERS1[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,NAME)] += protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it1240 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end1239 = 
        qORDERS2.end();
    for (; qORDERS2_it1240 != qORDERS2_end1239; ++qORDERS2_it1240)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it1240->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it1240->first);
        string NAME = get<2>(qORDERS2_it1240->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it1238 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end1237 = 
            REGION.end();
        for (; REGION_it1238 != REGION_end1237; ++REGION_it1238)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it1238);
            string protect_R__NAME = get<1>(*REGION_it1238);
            string protect_R__COMMENT = get<2>(*REGION_it1238);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it1236 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end1235 = NATION.end();
            for (; NATION_it1236 != NATION_end1235; ++NATION_it1236)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it1236);
                string protect_N__NAME = get<1>(*NATION_it1236);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it1236);
                string protect_N__COMMENT = get<3>(*NATION_it1236);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it1234 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end1233 = CUSTOMER.end();
                for (; CUSTOMER_it1234 != CUSTOMER_end1233; ++CUSTOMER_it1234)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1234);
                    string protect_C__NAME = get<1>(*CUSTOMER_it1234);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it1234);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1234);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it1234);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1234);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1234);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it1234);
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it1232 = SUPPLIER.begin();
                    
                        multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end1231 = SUPPLIER.end();
                    for (
                        ; SUPPLIER_it1232 != SUPPLIER_end1231; ++SUPPLIER_it1232)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1232);
                        string protect_S__NAME = get<1>(*SUPPLIER_it1232);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it1232);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1232);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it1232);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1232);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it1232);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1230 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1229 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1230 != LINEITEM_end1229; ++LINEITEM_it1230)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1230);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1230);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1230);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1230);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1230);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1230);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1230);
                            double protect_L__TAX = get<7>(*LINEITEM_it1230);
                            int64_t var1413 = 0;
                            var1413 += protect_N__REGIONKEY;
                            int64_t var1414 = 0;
                            var1414 += protect_R__REGIONKEY;
                            if ( var1413 == var1414 )
                            {
                                int64_t var1411 = 0;
                                var1411 += protect_C__NATIONKEY;
                                int64_t var1412 = 0;
                                var1412 += protect_N__NATIONKEY;
                                if ( var1411 == var1412 )
                                {
                                    int64_t var1409 = 0;
                                    var1409 += protect_C__NATIONKEY;
                                    int64_t var1410 = 0;
                                    var1410 += protect_S__NATIONKEY;
                                    if ( var1409 == var1410 )
                                    {
                                        int64_t var1407 = 0;
                                        var1407 += protect_L__SUPPKEY;
                                        int64_t var1408 = 0;
                                        var1408 += protect_S__SUPPKEY;
                                        if ( var1407 == var1408 )
                                        {
                                            string var1405 = 0;
                                            var1405 += NAME;
                                            string var1406 = 0;
                                            var1406 += protect_N__NAME;
                                            if ( var1405 == var1406 )
                                            {
                                                int64_t var1403 = 0;
                                                var1403 += x_qORDERS_O__CUSTKEY;
                                                int64_t var1404 = 0;
                                                var1404 += protect_C__CUSTKEY;
                                                if ( var1403 == var1404 )
                                                {
                                                    int64_t var1401 = 0;
                                                    var1401 += 
                                                        x_qORDERS_O__ORDERKEY;
                                                    int64_t var1402 = 0;
                                                    var1402 += 
                                                        protect_L__ORDERKEY;
                                                    if ( var1401 == var1402 )
                                                    {
                                                        double var1400 = 1;
                                                        var1400 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var1400 *= 
                                                            protect_L__DISCOUNT;
                                                        qORDERS2[make_tuple(
                                                            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,NAME)] += var1400;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<string,int64_t>,double>::iterator qREGION1_it1252 = 
        qREGION1.begin();
    map<tuple<string,int64_t>,double>::iterator qREGION1_end1251 = qREGION1.end(
        );
    for (; qREGION1_it1252 != qREGION1_end1251; ++qREGION1_it1252)
    {
        string NAME = get<0>(qREGION1_it1252->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it1252->first);
        qREGION1[make_tuple(NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it1250 
            = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_end1249 = NATION.end();
        for (; NATION_it1250 != NATION_end1249; ++NATION_it1250)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it1250);
            string protect_N__NAME = get<1>(*NATION_it1250);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it1250);
            string protect_N__COMMENT = get<3>(*NATION_it1250);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it1248 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end1247 = CUSTOMER.end();
            for (; CUSTOMER_it1248 != CUSTOMER_end1247; ++CUSTOMER_it1248)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1248);
                string protect_C__NAME = get<1>(*CUSTOMER_it1248);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it1248);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1248);
                string protect_C__PHONE = get<4>(*CUSTOMER_it1248);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1248);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1248);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it1248);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it1246 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end1245 = SUPPLIER.end();
                for (; SUPPLIER_it1246 != SUPPLIER_end1245; ++SUPPLIER_it1246)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1246);
                    string protect_S__NAME = get<1>(*SUPPLIER_it1246);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it1246);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1246);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it1246);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1246);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it1246);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it1244 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end1243 = ORDERS.end();
                    for (; ORDERS_it1244 != ORDERS_end1243; ++ORDERS_it1244)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it1244);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1244);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it1244);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it1244);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it1244);
                        string protect_O__ORDERPRIORITY = get<5>(
                            *ORDERS_it1244);
                        string protect_O__CLERK = get<6>(*ORDERS_it1244);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it1244);
                        string protect_O__COMMENT = get<8>(*ORDERS_it1244);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1242 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1241 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1242 != LINEITEM_end1241; ++LINEITEM_it1242)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1242);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1242);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1242);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1242);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1242);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1242);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1242);
                            double protect_L__TAX = get<7>(*LINEITEM_it1242);
                            int64_t var1427 = 0;
                            var1427 += protect_C__NATIONKEY;
                            int64_t var1428 = 0;
                            var1428 += protect_N__NATIONKEY;
                            if ( var1427 == var1428 )
                            {
                                int64_t var1425 = 0;
                                var1425 += protect_C__NATIONKEY;
                                int64_t var1426 = 0;
                                var1426 += protect_S__NATIONKEY;
                                if ( var1425 == var1426 )
                                {
                                    int64_t var1423 = 0;
                                    var1423 += protect_C__CUSTKEY;
                                    int64_t var1424 = 0;
                                    var1424 += protect_O__CUSTKEY;
                                    if ( var1423 == var1424 )
                                    {
                                        int64_t var1421 = 0;
                                        var1421 += protect_L__SUPPKEY;
                                        int64_t var1422 = 0;
                                        var1422 += protect_S__SUPPKEY;
                                        if ( var1421 == var1422 )
                                        {
                                            int64_t var1419 = 0;
                                            var1419 += protect_L__ORDERKEY;
                                            int64_t var1420 = 0;
                                            var1420 += protect_O__ORDERKEY;
                                            if ( var1419 == var1420 )
                                            {
                                                int64_t var1417 = 0;
                                                var1417 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var1418 = 0;
                                                var1418 += protect_N__REGIONKEY;
                                                if ( var1417 == var1418 )
                                                {
                                                    string var1415 = 0;
                                                    var1415 += NAME;
                                                    string var1416 = 0;
                                                    var1416 += protect_N__NAME;
                                                    if ( var1415 == var1416 )
                                                    {
                                                        qREGION1[make_tuple(
                                                            NAME,x_qREGION_R__REGIONKEY)] += protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it1264 = 
        qREGION2.begin();
    map<tuple<string,int64_t>,double>::iterator qREGION2_end1263 = qREGION2.end(
        );
    for (; qREGION2_it1264 != qREGION2_end1263; ++qREGION2_it1264)
    {
        string NAME = get<0>(qREGION2_it1264->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it1264->first);
        qREGION2[make_tuple(NAME,x_qREGION_R__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it1262 
            = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_end1261 = NATION.end();
        for (; NATION_it1262 != NATION_end1261; ++NATION_it1262)
        {
            int64_t protect_N__NATIONKEY = get<0>(*NATION_it1262);
            string protect_N__NAME = get<1>(*NATION_it1262);
            int64_t protect_N__REGIONKEY = get<2>(*NATION_it1262);
            string protect_N__COMMENT = get<3>(*NATION_it1262);
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it1260 = CUSTOMER.begin();
            
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end1259 = CUSTOMER.end();
            for (; CUSTOMER_it1260 != CUSTOMER_end1259; ++CUSTOMER_it1260)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1260);
                string protect_C__NAME = get<1>(*CUSTOMER_it1260);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it1260);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1260);
                string protect_C__PHONE = get<4>(*CUSTOMER_it1260);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1260);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1260);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it1260);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it1258 = SUPPLIER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end1257 = SUPPLIER.end();
                for (; SUPPLIER_it1258 != SUPPLIER_end1257; ++SUPPLIER_it1258)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it1258);
                    string protect_S__NAME = get<1>(*SUPPLIER_it1258);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it1258);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it1258);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it1258);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it1258);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it1258);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it1256 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end1255 = ORDERS.end();
                    for (; ORDERS_it1256 != ORDERS_end1255; ++ORDERS_it1256)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it1256);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1256);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it1256);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it1256);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it1256);
                        string protect_O__ORDERPRIORITY = get<5>(
                            *ORDERS_it1256);
                        string protect_O__CLERK = get<6>(*ORDERS_it1256);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it1256);
                        string protect_O__COMMENT = get<8>(*ORDERS_it1256);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1254 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1253 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1254 != LINEITEM_end1253; ++LINEITEM_it1254)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1254);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1254);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1254);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1254);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1254);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1254);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1254);
                            double protect_L__TAX = get<7>(*LINEITEM_it1254);
                            int64_t var1442 = 0;
                            var1442 += protect_C__NATIONKEY;
                            int64_t var1443 = 0;
                            var1443 += protect_N__NATIONKEY;
                            if ( var1442 == var1443 )
                            {
                                int64_t var1440 = 0;
                                var1440 += protect_C__NATIONKEY;
                                int64_t var1441 = 0;
                                var1441 += protect_S__NATIONKEY;
                                if ( var1440 == var1441 )
                                {
                                    int64_t var1438 = 0;
                                    var1438 += protect_C__CUSTKEY;
                                    int64_t var1439 = 0;
                                    var1439 += protect_O__CUSTKEY;
                                    if ( var1438 == var1439 )
                                    {
                                        int64_t var1436 = 0;
                                        var1436 += protect_L__SUPPKEY;
                                        int64_t var1437 = 0;
                                        var1437 += protect_S__SUPPKEY;
                                        if ( var1436 == var1437 )
                                        {
                                            int64_t var1434 = 0;
                                            var1434 += protect_L__ORDERKEY;
                                            int64_t var1435 = 0;
                                            var1435 += protect_O__ORDERKEY;
                                            if ( var1434 == var1435 )
                                            {
                                                int64_t var1432 = 0;
                                                var1432 += 
                                                    x_qREGION_R__REGIONKEY;
                                                int64_t var1433 = 0;
                                                var1433 += protect_N__REGIONKEY;
                                                if ( var1432 == var1433 )
                                                {
                                                    string var1430 = 0;
                                                    var1430 += NAME;
                                                    string var1431 = 0;
                                                    var1431 += protect_N__NAME;
                                                    if ( var1430 == var1431 )
                                                    {
                                                        double var1429 = 1;
                                                        var1429 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var1429 *= 
                                                            protect_L__DISCOUNT;
                                                        qREGION2[make_tuple(
                                                            NAME,x_qREGION_R__REGIONKEY)] += var1429;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it1276 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end1275 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it1276 != qSUPPLIER1_end1275; ++qSUPPLIER1_it1276)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it1276->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER1_it1276->first);
        string NAME = get<2>(qSUPPLIER1_it1276->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it1274 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end1273 = 
            REGION.end();
        for (; REGION_it1274 != REGION_end1273; ++REGION_it1274)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it1274);
            string protect_R__NAME = get<1>(*REGION_it1274);
            string protect_R__COMMENT = get<2>(*REGION_it1274);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it1272 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end1271 = NATION.end();
            for (; NATION_it1272 != NATION_end1271; ++NATION_it1272)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it1272);
                string protect_N__NAME = get<1>(*NATION_it1272);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it1272);
                string protect_N__COMMENT = get<3>(*NATION_it1272);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it1270 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end1269 = CUSTOMER.end();
                for (; CUSTOMER_it1270 != CUSTOMER_end1269; ++CUSTOMER_it1270)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1270);
                    string protect_C__NAME = get<1>(*CUSTOMER_it1270);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it1270);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1270);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it1270);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1270);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1270);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it1270);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it1268 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end1267 = ORDERS.end();
                    for (; ORDERS_it1268 != ORDERS_end1267; ++ORDERS_it1268)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it1268);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1268);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it1268);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it1268);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it1268);
                        string protect_O__ORDERPRIORITY = get<5>(
                            *ORDERS_it1268);
                        string protect_O__CLERK = get<6>(*ORDERS_it1268);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it1268);
                        string protect_O__COMMENT = get<8>(*ORDERS_it1268);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1266 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1265 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1266 != LINEITEM_end1265; ++LINEITEM_it1266)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1266);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1266);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1266);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1266);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1266);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1266);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1266);
                            double protect_L__TAX = get<7>(*LINEITEM_it1266);
                            int64_t var1456 = 0;
                            var1456 += protect_N__REGIONKEY;
                            int64_t var1457 = 0;
                            var1457 += protect_R__REGIONKEY;
                            if ( var1456 == var1457 )
                            {
                                int64_t var1454 = 0;
                                var1454 += protect_C__CUSTKEY;
                                int64_t var1455 = 0;
                                var1455 += protect_O__CUSTKEY;
                                if ( var1454 == var1455 )
                                {
                                    int64_t var1452 = 0;
                                    var1452 += protect_L__ORDERKEY;
                                    int64_t var1453 = 0;
                                    var1453 += protect_O__ORDERKEY;
                                    if ( var1452 == var1453 )
                                    {
                                        string var1450 = 0;
                                        var1450 += NAME;
                                        string var1451 = 0;
                                        var1451 += protect_N__NAME;
                                        if ( var1450 == var1451 )
                                        {
                                            int64_t var1448 = 0;
                                            var1448 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var1449 = 0;
                                            var1449 += protect_N__NATIONKEY;
                                            if ( var1448 == var1449 )
                                            {
                                                int64_t var1446 = 0;
                                                var1446 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var1447 = 0;
                                                var1447 += protect_C__NATIONKEY;
                                                if ( var1446 == var1447 )
                                                {
                                                    int64_t var1444 = 0;
                                                    var1444 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var1445 = 0;
                                                    var1445 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var1444 == var1445 )
                                                    {
                                                        qSUPPLIER1[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,NAME)] += 
                                                            protect_L__EXTENDEDPRICE;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it1288 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end1287 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it1288 != qSUPPLIER2_end1287; ++qSUPPLIER2_it1288)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it1288->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER2_it1288->first);
        string NAME = get<2>(qSUPPLIER2_it1288->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,NAME)] = 0;
        multiset<tuple<int64_t,string,string> >::iterator REGION_it1286 = 
            REGION.begin();
        multiset<tuple<int64_t,string,string> >::iterator REGION_end1285 = 
            REGION.end();
        for (; REGION_it1286 != REGION_end1285; ++REGION_it1286)
        {
            int64_t protect_R__REGIONKEY = get<0>(*REGION_it1286);
            string protect_R__NAME = get<1>(*REGION_it1286);
            string protect_R__COMMENT = get<2>(*REGION_it1286);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_it1284 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator 
                NATION_end1283 = NATION.end();
            for (; NATION_it1284 != NATION_end1283; ++NATION_it1284)
            {
                int64_t protect_N__NATIONKEY = get<0>(*NATION_it1284);
                string protect_N__NAME = get<1>(*NATION_it1284);
                int64_t protect_N__REGIONKEY = get<2>(*NATION_it1284);
                string protect_N__COMMENT = get<3>(*NATION_it1284);
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it1282 = CUSTOMER.begin();
                
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end1281 = CUSTOMER.end();
                for (; CUSTOMER_it1282 != CUSTOMER_end1281; ++CUSTOMER_it1282)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it1282);
                    string protect_C__NAME = get<1>(*CUSTOMER_it1282);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it1282);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it1282);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it1282);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it1282);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it1282);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it1282);
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it1280 = ORDERS.begin();
                    
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end1279 = ORDERS.end();
                    for (; ORDERS_it1280 != ORDERS_end1279; ++ORDERS_it1280)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it1280);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it1280);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it1280);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it1280);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it1280);
                        string protect_O__ORDERPRIORITY = get<5>(
                            *ORDERS_it1280);
                        string protect_O__CLERK = get<6>(*ORDERS_it1280);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it1280);
                        string protect_O__COMMENT = get<8>(*ORDERS_it1280);
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it1278 = LINEITEM.begin();
                        
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end1277 = LINEITEM.end();
                        for (
                            ; LINEITEM_it1278 != LINEITEM_end1277; ++LINEITEM_it1278)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(
                                *LINEITEM_it1278);
                            int64_t protect_L__PARTKEY = get<1>(
                                *LINEITEM_it1278);
                            int64_t protect_L__SUPPKEY = get<2>(
                                *LINEITEM_it1278);
                            int protect_L__LINENUMBER = get<3>(
                                *LINEITEM_it1278);
                            double protect_L__QUANTITY = get<4>(
                                *LINEITEM_it1278);
                            double protect_L__EXTENDEDPRICE = get<5>(
                                *LINEITEM_it1278);
                            double protect_L__DISCOUNT = get<6>(
                                *LINEITEM_it1278);
                            double protect_L__TAX = get<7>(*LINEITEM_it1278);
                            int64_t var1471 = 0;
                            var1471 += protect_N__REGIONKEY;
                            int64_t var1472 = 0;
                            var1472 += protect_R__REGIONKEY;
                            if ( var1471 == var1472 )
                            {
                                int64_t var1469 = 0;
                                var1469 += protect_C__CUSTKEY;
                                int64_t var1470 = 0;
                                var1470 += protect_O__CUSTKEY;
                                if ( var1469 == var1470 )
                                {
                                    int64_t var1467 = 0;
                                    var1467 += protect_L__ORDERKEY;
                                    int64_t var1468 = 0;
                                    var1468 += protect_O__ORDERKEY;
                                    if ( var1467 == var1468 )
                                    {
                                        string var1465 = 0;
                                        var1465 += NAME;
                                        string var1466 = 0;
                                        var1466 += protect_N__NAME;
                                        if ( var1465 == var1466 )
                                        {
                                            int64_t var1463 = 0;
                                            var1463 += x_qSUPPLIER_S__NATIONKEY;
                                            int64_t var1464 = 0;
                                            var1464 += protect_N__NATIONKEY;
                                            if ( var1463 == var1464 )
                                            {
                                                int64_t var1461 = 0;
                                                var1461 += 
                                                    x_qSUPPLIER_S__NATIONKEY;
                                                int64_t var1462 = 0;
                                                var1462 += protect_C__NATIONKEY;
                                                if ( var1461 == var1462 )
                                                {
                                                    int64_t var1459 = 0;
                                                    var1459 += 
                                                        x_qSUPPLIER_S__SUPPKEY;
                                                    int64_t var1460 = 0;
                                                    var1460 += 
                                                        protect_L__SUPPKEY;
                                                    if ( var1459 == var1460 )
                                                    {
                                                        double var1458 = 1;
                                                        var1458 *= 
                                                            protect_L__EXTENDEDPRICE;
                                                        var1458 *= 
                                                            protect_L__DISCOUNT;
                                                        qSUPPLIER2[make_tuple(
                                                            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,NAME)] += var1458;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_NATION_sec_span, on_delete_NATION_usec_span);
}

DBToaster::DemoDatasets::RegionStream SSBRegion("/home/yanif/datasets/tpch/sf1/singlefile/region.tbl",&DBToaster::DemoDatasets::parseRegionField,3,100,512);

boost::shared_ptr<DBToaster::DemoDatasets::RegionTupleAdaptor> SSBRegion_adaptor(new DBToaster::DemoDatasets::RegionTupleAdaptor());
static int streamSSBRegionId = 0;

struct on_insert_REGION_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::RegionTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::RegionTupleAdaptor::Result>(data); 
        on_insert_REGION(results, log, stats, input.regionkey,input.name,input.comment);
    }
};

on_insert_REGION_fun_obj fo_on_insert_REGION_0;

DBToaster::DemoDatasets::LineitemStream SSBLineitem("/home/yanif/datasets/tpch/sf1/singlefile/lineitem.tbl.a",&DBToaster::DemoDatasets::parseLineitemField,16,65000000,512);

boost::shared_ptr<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor> SSBLineitem_adaptor(new DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor());
static int streamSSBLineitemId = 1;

struct on_insert_LINEITEM_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result>(data); 
        on_insert_LINEITEM(results, log, stats, input.orderkey,input.partkey,input.suppkey,input.linenumber,input.quantity,input.extendedprice,input.discount,input.tax);
    }
};

on_insert_LINEITEM_fun_obj fo_on_insert_LINEITEM_1;

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

DBToaster::DemoDatasets::SupplierStream SSBSupplier("/home/yanif/datasets/tpch/sf1/singlefile/supplier.tbl.a",&DBToaster::DemoDatasets::parseSupplierField,7,110000,512);

boost::shared_ptr<DBToaster::DemoDatasets::SupplierTupleAdaptor> SSBSupplier_adaptor(new DBToaster::DemoDatasets::SupplierTupleAdaptor());
static int streamSSBSupplierId = 3;

struct on_insert_SUPPLIER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::SupplierTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::SupplierTupleAdaptor::Result>(data); 
        on_insert_SUPPLIER(results, log, stats, input.suppkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.comment);
    }
};

on_insert_SUPPLIER_fun_obj fo_on_insert_SUPPLIER_3;

DBToaster::DemoDatasets::CustomerStream SSBCustomer("/home/yanif/datasets/tpch/sf1/singlefile/customer.tbl.a",&DBToaster::DemoDatasets::parseCustomerField,8,1600000, 512);

boost::shared_ptr<DBToaster::DemoDatasets::CustomerTupleAdaptor> SSBCustomer_adaptor(new DBToaster::DemoDatasets::CustomerTupleAdaptor());
static int streamSSBCustomerId = 4;

struct on_insert_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_insert_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_insert_CUSTOMER_fun_obj fo_on_insert_CUSTOMER_4;

DBToaster::DemoDatasets::NationStream SSBNation("/home/yanif/datasets/tpch/sf1/singlefile/nation.tbl",&DBToaster::DemoDatasets::parseNationField,4,100,512);

boost::shared_ptr<DBToaster::DemoDatasets::NationTupleAdaptor> SSBNation_adaptor(new DBToaster::DemoDatasets::NationTupleAdaptor());
static int streamSSBNationId = 5;

struct on_insert_NATION_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::NationTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::NationTupleAdaptor::Result>(data); 
        on_insert_NATION(results, log, stats, input.nationkey,input.name,input.regionkey,input.comment);
    }
};

on_insert_NATION_fun_obj fo_on_insert_NATION_5;

struct on_delete_REGION_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::RegionTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::RegionTupleAdaptor::Result>(data); 
        on_delete_REGION(results, log, stats, input.regionkey,input.name,input.comment);
    }
};

on_delete_REGION_fun_obj fo_on_delete_REGION_6;

struct on_delete_LINEITEM_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result>(data); 
        on_delete_LINEITEM(results, log, stats, input.orderkey,input.partkey,input.suppkey,input.linenumber,input.quantity,input.extendedprice,input.discount,input.tax);
    }
};

on_delete_LINEITEM_fun_obj fo_on_delete_LINEITEM_7;

struct on_delete_ORDERS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::OrderTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::OrderTupleAdaptor::Result>(data); 
        on_delete_ORDERS(results, log, stats, input.orderkey,input.custkey,input.orderstatus,input.totalprice,input.orderdate,input.orderpriority,input.clerk,input.shippriority,input.comment);
    }
};

on_delete_ORDERS_fun_obj fo_on_delete_ORDERS_8;

struct on_delete_SUPPLIER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::SupplierTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::SupplierTupleAdaptor::Result>(data); 
        on_delete_SUPPLIER(results, log, stats, input.suppkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.comment);
    }
};

on_delete_SUPPLIER_fun_obj fo_on_delete_SUPPLIER_9;

struct on_delete_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_delete_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_delete_CUSTOMER_fun_obj fo_on_delete_CUSTOMER_10;

struct on_delete_NATION_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::NationTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::NationTupleAdaptor::Result>(data); 
        on_delete_NATION(results, log, stats, input.nationkey,input.name,input.regionkey,input.comment);
    }
};

on_delete_NATION_fun_obj fo_on_delete_NATION_11;


void init(DBToaster::StandaloneEngine::FileMultiplexer& sources,
    DBToaster::StandaloneEngine::FileStreamDispatcher& router, 
    ofstream* results, ofstream* log, ofstream* stats)
{
    router.setFiles(results, log, stats);
    sources.addStream<DBToaster::DemoDatasets::region>(&SSBRegion, boost::ref(*SSBRegion_adaptor), streamSSBRegionId);
    router.addHandler(streamSSBRegionId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_REGION_0));
    sources.addStream<DBToaster::DemoDatasets::lineitem>(&SSBLineitem, boost::ref(*SSBLineitem_adaptor), streamSSBLineitemId);
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_LINEITEM_1));
    sources.addStream<DBToaster::DemoDatasets::order>(&SSBOrder, boost::ref(*SSBOrder_adaptor), streamSSBOrderId);
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_ORDERS_2));
    sources.addStream<DBToaster::DemoDatasets::supplier>(&SSBSupplier, boost::ref(*SSBSupplier_adaptor), streamSSBSupplierId);
    router.addHandler(streamSSBSupplierId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_SUPPLIER_3));
    sources.addStream<DBToaster::DemoDatasets::customer>(&SSBCustomer, boost::ref(*SSBCustomer_adaptor), streamSSBCustomerId);
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_CUSTOMER_4));
    sources.addStream<DBToaster::DemoDatasets::nation>(&SSBNation, boost::ref(*SSBNation_adaptor), streamSSBNationId);
    router.addHandler(streamSSBNationId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_NATION_5));
    router.addHandler(streamSSBRegionId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_REGION_6));
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_LINEITEM_7));
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_ORDERS_8));
    router.addHandler(streamSSBSupplierId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_SUPPLIER_9));
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_CUSTOMER_10));
    router.addHandler(streamSSBNationId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_NATION_11));
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
