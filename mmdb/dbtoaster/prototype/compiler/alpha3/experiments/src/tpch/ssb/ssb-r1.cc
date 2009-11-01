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
multiset<tuple<int64_t,string,string,string,string,int,string,double,string> > PARTS;
multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> > CUSTOMER;
multiset<tuple<int64_t,string,string,int64_t,string,double,string> > SUPPLIER;
multiset<tuple<int64_t,string,int64_t,string> > NATION;

map<tuple<string,int64_t,int64_t,int64_t>,double> q;

map<tuple<int64_t,int64_t,int64_t>,int> qLINEITEM1;
map<tuple<int64_t,string>,int> qLINEITEM2;
map<tuple<int64_t,int64_t>,int> qLINEITEM3;

map<tuple<int64_t,string,int64_t>,double> qORDERS1;
map<tuple<int64_t,int64_t,int64_t>,int> qORDERS2;
map<tuple<int64_t,string,int64_t>,double> qORDERS3;

map<tuple<int64_t,int64_t,int64_t,int64_t>,double> qPARTS1;
map<tuple<int64_t,int64_t,int64_t,int64_t>,double> qPARTS2;

map<tuple<int64_t,string,int64_t>,double> qCUSTOMER1;
map<tuple<int64_t,int64_t>,int> qCUSTOMER2;
map<tuple<int64_t,string,int64_t>,double> qCUSTOMER3;

map<tuple<int64_t,string,int64_t,int64_t>,double> qSUPPLIER1;
map<tuple<int64_t,int64_t>,int> qSUPPLIER2;
map<tuple<int64_t,string,int64_t,int64_t>,double> qSUPPLIER3;

map<tuple<int64_t,string,int64_t,int64_t>,double> qNATION1;
map<tuple<string,int64_t,int64_t>,double> qNATION2;
map<tuple<int64_t,string>,double> qNATION3;
map<tuple<int64_t,string,int64_t,int64_t>,double> qNATION4;
map<tuple<string,int64_t,int64_t>,double> qNATION5;
map<tuple<int64_t,string>,double> qNATION6;



double on_insert_NATION_sec_span = 0.0;
double on_insert_NATION_usec_span = 0.0;
double on_insert_LINEITEM_sec_span = 0.0;
double on_insert_LINEITEM_usec_span = 0.0;
double on_insert_ORDERS_sec_span = 0.0;
double on_insert_ORDERS_usec_span = 0.0;
double on_insert_SUPPLIER_sec_span = 0.0;
double on_insert_SUPPLIER_usec_span = 0.0;
double on_insert_PARTS_sec_span = 0.0;
double on_insert_PARTS_usec_span = 0.0;
double on_insert_CUSTOMER_sec_span = 0.0;
double on_insert_CUSTOMER_usec_span = 0.0;
double on_delete_NATION_sec_span = 0.0;
double on_delete_NATION_usec_span = 0.0;
double on_delete_LINEITEM_sec_span = 0.0;
double on_delete_LINEITEM_usec_span = 0.0;
double on_delete_ORDERS_sec_span = 0.0;
double on_delete_ORDERS_usec_span = 0.0;
double on_delete_SUPPLIER_sec_span = 0.0;
double on_delete_SUPPLIER_usec_span = 0.0;
double on_delete_PARTS_sec_span = 0.0;
double on_delete_PARTS_usec_span = 0.0;
double on_delete_CUSTOMER_sec_span = 0.0;
double on_delete_CUSTOMER_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
   cout << "SUPPLIER size: " << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * SUPPLIER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::key_compare>))) << endl;

   (*stats) << "m," << "SUPPLIER" << "," << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * SUPPLIER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::key_compare>))) << endl;

   cout << "qSUPPLIER1 size: " << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER1" << "," << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qSUPPLIER2 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER2" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qSUPPLIER3 size: " << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER3" << "," << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "LINEITEM size: " << (((sizeof(multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * LINEITEM.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_type, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type>, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_compare>))) << endl;

   (*stats) << "m," << "LINEITEM" << "," << (((sizeof(multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * LINEITEM.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_type, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::value_type>, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >::key_compare>))) << endl;

   cout << "qORDERS1 size: " << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1" << "," << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS2 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS2" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "ORDERS size: " << (((sizeof(multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * ORDERS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_type, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type>, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_compare>))) << endl;

   (*stats) << "m," << "ORDERS" << "," << (((sizeof(multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * ORDERS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_type, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type, _Identity<multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::value_type>, multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> >::key_compare>))) << endl;

   cout << "qORDERS3 size: " << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS3" << "," << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qPARTS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qPARTS2 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS2" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qLINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qLINEITEM2 size: " << (((sizeof(map<tuple<int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string>,int>::key_type, map<tuple<int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,string>,int>::value_type>, map<tuple<int64_t,string>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM2" << "," << (((sizeof(map<tuple<int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string>,int>::key_type, map<tuple<int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,string>,int>::value_type>, map<tuple<int64_t,string>,int>::key_compare>))) << endl;

   cout << "qLINEITEM3 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM3" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qNATION1 size: " << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION1" << "," << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "q size: " << (((sizeof(map<tuple<string,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<string,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<string,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "q" << "," << (((sizeof(map<tuple<string,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<string,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<string,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "CUSTOMER size: " << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * CUSTOMER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_compare>))) << endl;

   (*stats) << "m," << "CUSTOMER" << "," << (((sizeof(multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * CUSTOMER.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_type, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::value_type>, multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> >::key_compare>))) << endl;

   cout << "qNATION2 size: " << (((sizeof(map<tuple<string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION2.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t,int64_t>,double>::key_type, map<tuple<string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t,int64_t>,double>::value_type>, map<tuple<string,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION2" << "," << (((sizeof(map<tuple<string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION2.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t,int64_t>,double>::key_type, map<tuple<string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t,int64_t>,double>::value_type>, map<tuple<string,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER1 size: " << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1" << "," << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "NATION size: " << (((sizeof(multiset<tuple<int64_t,string,int64_t,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * NATION.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,int64_t,string> >::key_type, multiset<tuple<int64_t,string,int64_t,string> >::value_type, _Identity<multiset<tuple<int64_t,string,int64_t,string> >::value_type>, multiset<tuple<int64_t,string,int64_t,string> >::key_compare>))) << endl;

   (*stats) << "m," << "NATION" << "," << (((sizeof(multiset<tuple<int64_t,string,int64_t,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * NATION.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,int64_t,string> >::key_type, multiset<tuple<int64_t,string,int64_t,string> >::value_type, _Identity<multiset<tuple<int64_t,string,int64_t,string> >::value_type>, multiset<tuple<int64_t,string,int64_t,string> >::key_compare>))) << endl;

   cout << "qNATION3 size: " << (((sizeof(map<tuple<int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string>,double>::key_type, map<tuple<int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,string>,double>::value_type>, map<tuple<int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION3" << "," << (((sizeof(map<tuple<int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string>,double>::key_type, map<tuple<int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,string>,double>::value_type>, map<tuple<int64_t,string>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER2 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER2" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "PARTS size: " << (((sizeof(multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * PARTS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::key_type, multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type>, multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::key_compare>))) << endl;

   (*stats) << "m," << "PARTS" << "," << (((sizeof(multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * PARTS.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::key_type, multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type, _Identity<multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::value_type>, multiset<tuple<int64_t,string,string,string,string,int,string,double,string> >::key_compare>))) << endl;

   cout << "qNATION4 size: " << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION4.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION4" << "," << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION4.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER3 size: " << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER3" << "," << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qNATION5 size: " << (((sizeof(map<tuple<string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION5.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t,int64_t>,double>::key_type, map<tuple<string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t,int64_t>,double>::value_type>, map<tuple<string,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION5" << "," << (((sizeof(map<tuple<string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION5.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t,int64_t>,double>::key_type, map<tuple<string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t,int64_t>,double>::value_type>, map<tuple<string,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qNATION6 size: " << (((sizeof(map<tuple<int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION6.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string>,double>::key_type, map<tuple<int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,string>,double>::value_type>, map<tuple<int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION6" << "," << (((sizeof(map<tuple<int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION6.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string>,double>::key_type, map<tuple<int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,string>,double>::value_type>, map<tuple<int64_t,string>,double>::key_compare>))) << endl;
}

void analyse_handler_usage(ofstream* stats)
{
   cout << "on_insert_NATION cost: " << (on_insert_NATION_sec_span + (on_insert_NATION_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_NATION" << "," << (on_insert_NATION_sec_span + (on_insert_NATION_usec_span / 1000000.0)) << endl;
   cout << "on_insert_LINEITEM cost: " << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_LINEITEM" << "," << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_insert_ORDERS cost: " << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_ORDERS" << "," << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   cout << "on_insert_SUPPLIER cost: " << (on_insert_SUPPLIER_sec_span + (on_insert_SUPPLIER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_SUPPLIER" << "," << (on_insert_SUPPLIER_sec_span + (on_insert_SUPPLIER_usec_span / 1000000.0)) << endl;
   cout << "on_insert_PARTS cost: " << (on_insert_PARTS_sec_span + (on_insert_PARTS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_PARTS" << "," << (on_insert_PARTS_sec_span + (on_insert_PARTS_usec_span / 1000000.0)) << endl;
   cout << "on_insert_CUSTOMER cost: " << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_CUSTOMER" << "," << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   cout << "on_delete_NATION cost: " << (on_delete_NATION_sec_span + (on_delete_NATION_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_NATION" << "," << (on_delete_NATION_sec_span + (on_delete_NATION_usec_span / 1000000.0)) << endl;
   cout << "on_delete_LINEITEM cost: " << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_LINEITEM" << "," << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_delete_ORDERS cost: " << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_ORDERS" << "," << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_SUPPLIER cost: " << (on_delete_SUPPLIER_sec_span + (on_delete_SUPPLIER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_SUPPLIER" << "," << (on_delete_SUPPLIER_sec_span + (on_delete_SUPPLIER_usec_span / 1000000.0)) << endl;
   cout << "on_delete_PARTS cost: " << (on_delete_PARTS_sec_span + (on_delete_PARTS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_PARTS" << "," << (on_delete_PARTS_sec_span + (on_delete_PARTS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_CUSTOMER cost: " << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_CUSTOMER" << "," << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
}


//////////////////////////////
//
// qCUSTOMER* plans

void recompute_qCUSTOMER1()
{
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it24 = qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end23 = qCUSTOMER1.end();
    for (; qCUSTOMER1_it24 != qCUSTOMER1_end23; ++qCUSTOMER1_it24)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it24->first);
        string P__MFGR = get<1>(qCUSTOMER1_it24->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1_it24->first);
        qCUSTOMER1[make_tuple(x_qCUSTOMER_C__CUSTKEY,P__MFGR,N2__REGIONKEY)] = 0;

        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it22 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end21 = NATION.end();
        for (; NATION_it22 != NATION_end21; ++NATION_it22)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it22);
            string N2__NAME = get<1>(*NATION_it22);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it22);
            string N2__COMMENT = get<3>(*NATION_it22);
            
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                >::iterator PARTS_it20 = PARTS.begin();
            
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                >::iterator PARTS_end19 = PARTS.end();
            for (; PARTS_it20 != PARTS_end19; ++PARTS_it20)
            {
                int64_t protect_P__PARTKEY = get<0>(*PARTS_it20);
                string protect_P__NAME = get<1>(*PARTS_it20);
                string protect_P__MFGR = get<2>(*PARTS_it20);
                string protect_P__BRAND = get<3>(*PARTS_it20);
                string protect_P__TYPE = get<4>(*PARTS_it20);
                int protect_P__SIZE = get<5>(*PARTS_it20);
                string protect_P__CONTAINER = get<6>(*PARTS_it20);
                double protect_P__RETAILPRICE = get<7>(*PARTS_it20);
                string protect_P__COMMENT = get<8>(*PARTS_it20);
                
                multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it18 = SUPPLIER.begin();
                
                multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end17 = SUPPLIER.end();
                for (; SUPPLIER_it18 != SUPPLIER_end17; ++SUPPLIER_it18)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it18);
                    string protect_S__NAME = get<1>(*SUPPLIER_it18);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it18);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it18);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it18);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it18);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it18);
                    
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it16 = ORDERS.begin();
                    
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end15 = ORDERS.end();
                    for (; ORDERS_it16 != ORDERS_end15; ++ORDERS_it16)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it16);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it16);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it16);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it16);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it16);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it16);
                        string protect_O__CLERK = get<6>(*ORDERS_it16);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it16);
                        string protect_O__COMMENT = get<8>(*ORDERS_it16);
                        
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it14 = LINEITEM.begin();
                        
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end13 = LINEITEM.end();
                        for (; LINEITEM_it14 != LINEITEM_end13; ++LINEITEM_it14)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it14);
                            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it14);
                            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it14);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it14);
                            double protect_L__QUANTITY = get<4>(*LINEITEM_it14);
                            double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it14);
                            double protect_L__DISCOUNT = get<6>(*LINEITEM_it14);
                            double protect_L__TAX = get<7>(*LINEITEM_it14);
                            if ( protect_S__NATIONKEY == protect_N1__NATIONKEY
                                 && protect_L__PARTKEY == protect_P__PARTKEY
                                 && protect_L__SUPPKEY == protect_S__SUPPKEY
                                 && protect_L__ORDERKEY == protect_O__ORDERKEY
                                 && N2__REGIONKEY == protect_N1__REGIONKEY
                                 && P__MFGR == protect_P__MFGR
                                 && x_qCUSTOMER_C__CUSTKEY == protect_O__CUSTKEY )
                            {
                                qCUSTOMER1[make_tuple(x_qCUSTOMER_C__CUSTKEY,P__MFGR,N2__REGIONKEY)] +=
                                    protect_L__EXTENDEDPRICE;
                            }
                        }
                    }
                }
            }
        }
    }
}

void recompute_qCUSTOMER2()
{
    map<tuple<int64_t,int64_t>,int>::iterator qCUSTOMER2_it28 = qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qCUSTOMER2_end27 = qCUSTOMER2.end();
    for (; qCUSTOMER2_it28 != qCUSTOMER2_end27; ++qCUSTOMER2_it28)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<0>(qCUSTOMER2_it28->first);
        int64_t REGIONKEY = get<1>(qCUSTOMER2_it28->first);
        qCUSTOMER2[make_tuple(x_qCUSTOMER_C__NATIONKEY,REGIONKEY)] = 0;

        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it26 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end25 = NATION.end();
        for (; NATION_it26 != NATION_end25; ++NATION_it26)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it26);
            string protect_N1__NAME = get<1>(*NATION_it26);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it26);
            string protect_N1__COMMENT = get<3>(*NATION_it26);
            if ( REGIONKEY == protect_N1__REGIONKEY
                 && x_qCUSTOMER_C__NATIONKEY == protect_N1__NATIONKEY )
            {
                qCUSTOMER2[make_tuple(x_qCUSTOMER_C__NATIONKEY,REGIONKEY)] += 1;
            }
        }
    }
}

void recompute_qCUSTOMER3()
{
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_it40 = qCUSTOMER3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_end39 = qCUSTOMER3.end();
    for (; qCUSTOMER3_it40 != qCUSTOMER3_end39; ++qCUSTOMER3_it40)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER3_it40->first);
        string P__MFGR = get<1>(qCUSTOMER3_it40->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER3_it40->first);
        qCUSTOMER3[make_tuple(x_qCUSTOMER_C__CUSTKEY,P__MFGR,N2__REGIONKEY)] = 0;

        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it38 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end37 = NATION.end();
        for (; NATION_it38 != NATION_end37; ++NATION_it38)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it38);
            string N2__NAME = get<1>(*NATION_it38);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it38);
            string N2__COMMENT = get<3>(*NATION_it38);
            
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                >::iterator PARTS_it36 = PARTS.begin();
            
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                >::iterator PARTS_end35 = PARTS.end();
            for (; PARTS_it36 != PARTS_end35; ++PARTS_it36)
            {
                int64_t protect_P__PARTKEY = get<0>(*PARTS_it36);
                string protect_P__NAME = get<1>(*PARTS_it36);
                string protect_P__MFGR = get<2>(*PARTS_it36);
                string protect_P__BRAND = get<3>(*PARTS_it36);
                string protect_P__TYPE = get<4>(*PARTS_it36);
                int protect_P__SIZE = get<5>(*PARTS_it36);
                string protect_P__CONTAINER = get<6>(*PARTS_it36);
                double protect_P__RETAILPRICE = get<7>(*PARTS_it36);
                string protect_P__COMMENT = get<8>(*PARTS_it36);
                
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
                    
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it32 = ORDERS.begin();
                    
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end31 = ORDERS.end();
                    for (; ORDERS_it32 != ORDERS_end31; ++ORDERS_it32)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it32);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it32);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it32);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it32);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it32);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it32);
                        string protect_O__CLERK = get<6>(*ORDERS_it32);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it32);
                        string protect_O__COMMENT = get<8>(*ORDERS_it32);
                        
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it30 = LINEITEM.begin();
                        
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end29 = LINEITEM.end();
                        for (; LINEITEM_it30 != LINEITEM_end29; ++LINEITEM_it30)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it30);
                            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it30);
                            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it30);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it30);
                            double protect_L__QUANTITY = get<4>(*LINEITEM_it30);
                            double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it30);
                            double protect_L__DISCOUNT = get<6>(*LINEITEM_it30);
                            double protect_L__TAX = get<7>(*LINEITEM_it30);
                            if ( protect_S__NATIONKEY == protect_N1__NATIONKEY
                                 && protect_L__PARTKEY == protect_P__PARTKEY
                                 && protect_L__SUPPKEY == protect_S__SUPPKEY
                                 && protect_L__ORDERKEY == protect_O__ORDERKEY
                                 && N2__REGIONKEY == protect_N1__REGIONKEY
                                 && P__MFGR == protect_P__MFGR
                                 && x_qCUSTOMER_C__CUSTKEY == protect_O__CUSTKEY )
                            {
                                qCUSTOMER3[make_tuple(x_qCUSTOMER_C__CUSTKEY,P__MFGR,N2__REGIONKEY)] +=
                                    protect_L__EXTENDEDPRICE*protect_L__DISCOUNT;
                            }
                        }
                    }
                }
            }
        }
    }
}


//////////////////////////////
//
// qLINEITEM* plans

void recompute_qLINEITEM1()
{
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_it48 = qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_end47 = qLINEITEM1.end();
    for (; qLINEITEM1_it48 != qLINEITEM1_end47; ++qLINEITEM1_it48)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it48->first);
        int64_t C__NATIONKEY = get<1>(qLINEITEM1_it48->first);
        int64_t REGIONKEY = get<2>(qLINEITEM1_it48->first);
        qLINEITEM1[make_tuple(x_qLINEITEM_L__ORDERKEY,C__NATIONKEY,REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it46 = 
            NATION.begin();
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_end45 = NATION.end();
        for (; NATION_it46 != NATION_end45; ++NATION_it46)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it46);
            string protect_N1__NAME = get<1>(*NATION_it46);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it46);
            string protect_N1__COMMENT = get<3>(*NATION_it46);
            
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it44 = CUSTOMER.begin();
            
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end43 = CUSTOMER.end();
            for (; CUSTOMER_it44 != CUSTOMER_end43; ++CUSTOMER_it44)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it44);
                string protect_C__NAME = get<1>(*CUSTOMER_it44);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it44);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it44);
                string protect_C__PHONE = get<4>(*CUSTOMER_it44);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it44);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it44);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it44);
                
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_it42 = ORDERS.begin();
                
                multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                    >::iterator ORDERS_end41 = ORDERS.end();
                for (; ORDERS_it42 != ORDERS_end41; ++ORDERS_it42)
                {
                    int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it42);
                    int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it42);
                    string protect_O__ORDERSTATUS = get<2>(*ORDERS_it42);
                    double protect_O__TOTALPRICE = get<3>(*ORDERS_it42);
                    string protect_O__ORDERDATE = get<4>(*ORDERS_it42);
                    string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it42);
                    string protect_O__CLERK = get<6>(*ORDERS_it42);
                    int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it42);
                    string protect_O__COMMENT = get<8>(*ORDERS_it42);
                    if ( protect_O__CUSTKEY == protect_C__CUSTKEY
                         &&  REGIONKEY == protect_N1__REGIONKEY
                         &&  C__NATIONKEY == protect_N1__NATIONKEY
                         &&  C__NATIONKEY == protect_C__NATIONKEY
                         &&  x_qLINEITEM_L__ORDERKEY == protect_O__ORDERKEY )
                    {
                        qLINEITEM1[make_tuple(x_qLINEITEM_L__ORDERKEY,C__NATIONKEY,REGIONKEY)] += 1;
                    }
                }
            }
        }
    }
}


void recompute_qLINEITEM2()
{
    map<tuple<int64_t,string>,int>::iterator qLINEITEM2_it734 = qLINEITEM2.begin();
    map<tuple<int64_t,string>,int>::iterator qLINEITEM2_end733 = qLINEITEM2.end();
    for (; qLINEITEM2_it734 != qLINEITEM2_end733; ++qLINEITEM2_it734)
    {
        int64_t x_qLINEITEM_L__PARTKEY = get<0>(qLINEITEM2_it734->first);
        string MFGR = get<1>(qLINEITEM2_it734->first);
        qLINEITEM2[make_tuple(x_qLINEITEM_L__PARTKEY,MFGR)] = 0;
        
        multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
            >::iterator PARTS_it732 = PARTS.begin();
        
        multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
            >::iterator PARTS_end731 = PARTS.end();
        for (; PARTS_it732 != PARTS_end731; ++PARTS_it732)
        {
            int64_t protect_P__PARTKEY = get<0>(*PARTS_it732);
            string protect_P__NAME = get<1>(*PARTS_it732);
            string protect_P__MFGR = get<2>(*PARTS_it732);
            string protect_P__BRAND = get<3>(*PARTS_it732);
            string protect_P__TYPE = get<4>(*PARTS_it732);
            int protect_P__SIZE = get<5>(*PARTS_it732);
            string protect_P__CONTAINER = get<6>(*PARTS_it732);
            double protect_P__RETAILPRICE = get<7>(*PARTS_it732);
            string protect_P__COMMENT = get<8>(*PARTS_it732);
            if ( MFGR == protect_P__MFGR
                && x_qLINEITEM_L__PARTKEY == protect_P__PARTKEY )
            {
                qLINEITEM2[make_tuple(x_qLINEITEM_L__PARTKEY,MFGR)] += 1;
            }
        }
    }
}


void recompute_qLINEITEM3()
{
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_it54 = qLINEITEM3.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_end53 = qLINEITEM3.end();
    for (; qLINEITEM3_it54 != qLINEITEM3_end53; ++qLINEITEM3_it54)
    {
        int64_t x_qLINEITEM_L__SUPPKEY = get<0>(qLINEITEM3_it54->first);
        int64_t N2__REGIONKEY = get<1>(qLINEITEM3_it54->first);
        qLINEITEM3[make_tuple(x_qLINEITEM_L__SUPPKEY,N2__REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it52 = 
            NATION.begin();
        multiset<tuple<int64_t,string,int64_t,
            string> >::iterator NATION_end51 = NATION.end();
        for (; NATION_it52 != NATION_end51; ++NATION_it52)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it52);
            string N2__NAME = get<1>(*NATION_it52);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it52);
            string N2__COMMENT = get<3>(*NATION_it52);
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_it50 = SUPPLIER.begin();
            multiset<tuple<int64_t,string,string,int64_t,string,double,string> 
                >::iterator SUPPLIER_end49 = SUPPLIER.end();
            for (; SUPPLIER_it50 != SUPPLIER_end49; ++SUPPLIER_it50)
            {
                int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it50);
                string protect_S__NAME = get<1>(*SUPPLIER_it50);
                string protect_S__ADDRESS = get<2>(*SUPPLIER_it50);
                int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it50);
                string protect_S__PHONE = get<4>(*SUPPLIER_it50);
                double protect_S__ACCTBAL = get<5>(*SUPPLIER_it50);
                string protect_S__COMMENT = get<6>(*SUPPLIER_it50);
                if ( protect_S__NATIONKEY == protect_N1__NATIONKEY
                     &&  N2__REGIONKEY == protect_N1__REGIONKEY
                     &&  x_qLINEITEM_L__SUPPKEY == protect_S__SUPPKEY )
                {
                    qLINEITEM3[make_tuple(x_qLINEITEM_L__SUPPKEY,N2__REGIONKEY)] += 1;
                }
            }
        }
    }
}


//////////////////////////////
//
// qNATION* plans

void recompute_qNATION1()
{
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_it68 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end67 = qNATION1.end();
    for (; qNATION1_it68 != qNATION1_end67; ++qNATION1_it68)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it68->first);
        string P__MFGR = get<1>(qNATION1_it68->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it68->first);
        int64_t REGIONKEY = get<3>(qNATION1_it68->first);
        qNATION1[make_tuple(x_qNATION_N1__NATIONKEY,P__MFGR,C__NATIONKEY,REGIONKEY)] = 0;

        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it66 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end65 = NATION.end();
        for (; NATION_it66 != NATION_end65; ++NATION_it66)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it66);
            string protect_N1__NAME = get<1>(*NATION_it66);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it66);
            string protect_N1__COMMENT = get<3>(*NATION_it66);
            
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it64 = CUSTOMER.begin();
            
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end63 = CUSTOMER.end();
            for (; CUSTOMER_it64 != CUSTOMER_end63; ++CUSTOMER_it64)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it64);
                string protect_C__NAME = get<1>(*CUSTOMER_it64);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it64);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it64);
                string protect_C__PHONE = get<4>(*CUSTOMER_it64);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it64);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it64);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it64);
                
                multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                    >::iterator PARTS_it62 = PARTS.begin();
                
                multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                    >::iterator PARTS_end61 = PARTS.end();
                for (; PARTS_it62 != PARTS_end61; ++PARTS_it62)
                {
                    int64_t protect_P__PARTKEY = get<0>(*PARTS_it62);
                    string protect_P__NAME = get<1>(*PARTS_it62);
                    string protect_P__MFGR = get<2>(*PARTS_it62);
                    string protect_P__BRAND = get<3>(*PARTS_it62);
                    string protect_P__TYPE = get<4>(*PARTS_it62);
                    int protect_P__SIZE = get<5>(*PARTS_it62);
                    string protect_P__CONTAINER = get<6>(*PARTS_it62);
                    double protect_P__RETAILPRICE = get<7>(*PARTS_it62);
                    string protect_P__COMMENT = get<8>(*PARTS_it62);
                    
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
                        
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it58 = ORDERS.begin();
                        
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end57 = ORDERS.end();
                        for (; ORDERS_it58 != ORDERS_end57; ++ORDERS_it58)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it58);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it58);
                            string protect_O__ORDERSTATUS = get<2>(*ORDERS_it58);
                            double protect_O__TOTALPRICE = get<3>(*ORDERS_it58);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it58);
                            string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it58);
                            string protect_O__CLERK = get<6>(*ORDERS_it58);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it58);
                            string protect_O__COMMENT = get<8>(*ORDERS_it58);
                            
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it56 = LINEITEM.begin();
                            
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end55 = LINEITEM.end();
                            for (
                                ; LINEITEM_it56 != LINEITEM_end55; ++LINEITEM_it56)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it56);
                                int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it56);
                                int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it56);
                                int protect_L__LINENUMBER = get<3>(*LINEITEM_it56);
                                double protect_L__QUANTITY = get<4>(*LINEITEM_it56);
                                double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it56);
                                double protect_L__DISCOUNT = get<6>(*LINEITEM_it56);
                                double protect_L__TAX = get<7>(*LINEITEM_it56);
                                if ( protect_O__CUSTKEY == protect_C__CUSTKEY
                                     && protect_L__PARTKEY == protect_P__PARTKEY
                                     &&  protect_L__SUPPKEY == protect_S__SUPPKEY
                                     &&  protect_L__ORDERKEY == protect_O__ORDERKEY
                                     &&  REGIONKEY == protect_N1__REGIONKEY
                                     &&  C__NATIONKEY == protect_N1__NATIONKEY
                                     &&  C__NATIONKEY == protect_C__NATIONKEY
                                     &&  P__MFGR == protect_P__MFGR
                                     &&  x_qNATION_N1__NATIONKEY == protect_S__NATIONKEY )
                                {
                                    qNATION1[make_tuple(x_qNATION_N1__NATIONKEY,P__MFGR,C__NATIONKEY,REGIONKEY)] +=
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

void recompute_qNATION2()
{
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it82 = qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end81 = qNATION2.end();
    for (; qNATION2_it82 != qNATION2_end81; ++qNATION2_it82)
    {
        string P__MFGR = get<0>(qNATION2_it82->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it82->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it82->first);
        qNATION2[make_tuple(P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] = 0;

        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it80 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end79 = NATION.end();
        for (; NATION_it80 != NATION_end79; ++NATION_it80)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it80);
            string N2__NAME = get<1>(*NATION_it80);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it80);
            string N2__COMMENT = get<3>(*NATION_it80);
            
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it78 = CUSTOMER.begin();
            
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end77 = CUSTOMER.end();
            for (; CUSTOMER_it78 != CUSTOMER_end77; ++CUSTOMER_it78)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it78);
                string protect_C__NAME = get<1>(*CUSTOMER_it78);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it78);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it78);
                string protect_C__PHONE = get<4>(*CUSTOMER_it78);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it78);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it78);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it78);
                
                multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                    >::iterator PARTS_it76 = PARTS.begin();
                
                multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                    >::iterator PARTS_end75 = PARTS.end();
                for (; PARTS_it76 != PARTS_end75; ++PARTS_it76)
                {
                    int64_t protect_P__PARTKEY = get<0>(*PARTS_it76);
                    string protect_P__NAME = get<1>(*PARTS_it76);
                    string protect_P__MFGR = get<2>(*PARTS_it76);
                    string protect_P__BRAND = get<3>(*PARTS_it76);
                    string protect_P__TYPE = get<4>(*PARTS_it76);
                    int protect_P__SIZE = get<5>(*PARTS_it76);
                    string protect_P__CONTAINER = get<6>(*PARTS_it76);
                    double protect_P__RETAILPRICE = get<7>(*PARTS_it76);
                    string protect_P__COMMENT = get<8>(*PARTS_it76);
                    
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it74 = SUPPLIER.begin();
                    
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end73 = SUPPLIER.end();
                    for (; SUPPLIER_it74 != SUPPLIER_end73; ++SUPPLIER_it74)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it74);
                        string protect_S__NAME = get<1>(*SUPPLIER_it74);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it74);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it74);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it74);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it74);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it74);
                        
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
                            for (
                                ; LINEITEM_it70 != LINEITEM_end69; ++LINEITEM_it70)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it70);
                                int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it70);
                                int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it70);
                                int protect_L__LINENUMBER = get<3>(*LINEITEM_it70);
                                double protect_L__QUANTITY = get<4>(*LINEITEM_it70);
                                double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it70);
                                double protect_L__DISCOUNT = get<6>(*LINEITEM_it70);
                                double protect_L__TAX = get<7>(*LINEITEM_it70);
                                if ( protect_S__NATIONKEY == protect_N1__NATIONKEY
                                     &&  protect_O__CUSTKEY == protect_C__CUSTKEY
                                     &&  protect_L__PARTKEY == protect_P__PARTKEY
                                     &&  protect_L__SUPPKEY == protect_S__SUPPKEY
                                     &&  protect_L__ORDERKEY == protect_O__ORDERKEY
                                     &&  N2__REGIONKEY == protect_N1__REGIONKEY
                                     &&  x_qNATION_N1__NATIONKEY == protect_C__NATIONKEY
                                     &&  P__MFGR == protect_P__MFGR )
                                {
                                    qNATION2[make_tuple(P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] +=
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


void recompute_qNATION3()
{
    map<tuple<int64_t,string>,double>::iterator qNATION3_it260 = qNATION3.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION3_end259 = qNATION3.end();
    for (; qNATION3_it260 != qNATION3_end259; ++qNATION3_it260)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION3_it260->first);
        string P__MFGR = get<1>(qNATION3_it260->first);
        qNATION3[make_tuple(x_qNATION_N1__NATIONKEY,P__MFGR)] = 0;
        
        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it258 = CUSTOMER.begin();
        
        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end257 = CUSTOMER.end();
        for (; CUSTOMER_it258 != CUSTOMER_end257; ++CUSTOMER_it258)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it258);
            string protect_C__NAME = get<1>(*CUSTOMER_it258);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it258);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it258);
            string protect_C__PHONE = get<4>(*CUSTOMER_it258);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it258);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it258);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it258);
            
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                >::iterator PARTS_it256 = PARTS.begin();
            
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                >::iterator PARTS_end255 = PARTS.end();
            for (; PARTS_it256 != PARTS_end255; ++PARTS_it256)
            {
                int64_t protect_P__PARTKEY = get<0>(*PARTS_it256);
                string protect_P__NAME = get<1>(*PARTS_it256);
                string protect_P__MFGR = get<2>(*PARTS_it256);
                string protect_P__BRAND = get<3>(*PARTS_it256);
                string protect_P__TYPE = get<4>(*PARTS_it256);
                int protect_P__SIZE = get<5>(*PARTS_it256);
                string protect_P__CONTAINER = get<6>(*PARTS_it256);
                double protect_P__RETAILPRICE = get<7>(*PARTS_it256);
                string protect_P__COMMENT = get<8>(*PARTS_it256);
                
                multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it254 = SUPPLIER.begin();
                
                multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end253 = SUPPLIER.end();
                for (; SUPPLIER_it254 != SUPPLIER_end253; ++SUPPLIER_it254)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it254);
                    string protect_S__NAME = get<1>(*SUPPLIER_it254);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it254);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it254);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it254);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it254);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it254);
                    
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it252 = ORDERS.begin();
                    
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end251 = ORDERS.end();
                    for (; ORDERS_it252 != ORDERS_end251; ++ORDERS_it252)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it252);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it252);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it252);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it252);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it252);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it252);
                        string protect_O__CLERK = get<6>(*ORDERS_it252);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it252);
                        string protect_O__COMMENT = get<8>(*ORDERS_it252);
                        
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it250 = LINEITEM.begin();
                        
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end249 = LINEITEM.end();
                        for (
                            ; LINEITEM_it250 != LINEITEM_end249; ++LINEITEM_it250)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it250);
                            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it250);
                            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it250);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it250);
                            double protect_L__QUANTITY = get<4>(*LINEITEM_it250);
                            double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it250);
                            double protect_L__DISCOUNT = get<6>(*LINEITEM_it250);
                            double protect_L__TAX = get<7>(*LINEITEM_it250);
                            if ( protect_O__CUSTKEY == protect_C__CUSTKEY
                                && protect_L__PARTKEY == protect_P__PARTKEY 
                                && protect_L__SUPPKEY == protect_S__SUPPKEY 
                                && protect_L__ORDERKEY == protect_O__ORDERKEY 
                                && x_qNATION_N1__NATIONKEY == protect_C__NATIONKEY 
                                && P__MFGR == protect_P__MFGR 
                                && x_qNATION_N1__NATIONKEY == protect_S__NATIONKEY )
                            {
                                qNATION3[make_tuple(x_qNATION_N1__NATIONKEY,P__MFGR)] +=
                                    protect_L__EXTENDEDPRICE;
                            }
                        }
                    }
                }
            }
        }
    }
}


void recompute_qNATION4()
{
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION4_it96 = qNATION4.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION4_end95 = qNATION4.end();
    for (; qNATION4_it96 != qNATION4_end95; ++qNATION4_it96)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4_it96->first);
        string P__MFGR = get<1>(qNATION4_it96->first);
        int64_t C__NATIONKEY = get<2>(qNATION4_it96->first);
        int64_t REGIONKEY = get<3>(qNATION4_it96->first);
        qNATION4[make_tuple(x_qNATION_N1__NATIONKEY,P__MFGR,C__NATIONKEY,REGIONKEY)] = 0;

        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it94 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end93 = NATION.end();
        for (; NATION_it94 != NATION_end93; ++NATION_it94)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it94);
            string protect_N1__NAME = get<1>(*NATION_it94);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it94);
            string protect_N1__COMMENT = get<3>(*NATION_it94);
            
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
                
                multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                    >::iterator PARTS_it90 = PARTS.begin();
                
                multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                    >::iterator PARTS_end89 = PARTS.end();
                for (; PARTS_it90 != PARTS_end89; ++PARTS_it90)
                {
                    int64_t protect_P__PARTKEY = get<0>(*PARTS_it90);
                    string protect_P__NAME = get<1>(*PARTS_it90);
                    string protect_P__MFGR = get<2>(*PARTS_it90);
                    string protect_P__BRAND = get<3>(*PARTS_it90);
                    string protect_P__TYPE = get<4>(*PARTS_it90);
                    int protect_P__SIZE = get<5>(*PARTS_it90);
                    string protect_P__CONTAINER = get<6>(*PARTS_it90);
                    double protect_P__RETAILPRICE = get<7>(*PARTS_it90);
                    string protect_P__COMMENT = get<8>(*PARTS_it90);
                    
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it88 = SUPPLIER.begin();
                    
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end87 = SUPPLIER.end();
                    for (; SUPPLIER_it88 != SUPPLIER_end87; ++SUPPLIER_it88)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it88);
                        string protect_S__NAME = get<1>(*SUPPLIER_it88);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it88);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it88);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it88);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it88);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it88);
                        
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it86 = ORDERS.begin();
                        
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end85 = ORDERS.end();
                        for (; ORDERS_it86 != ORDERS_end85; ++ORDERS_it86)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it86);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it86);
                            string protect_O__ORDERSTATUS = get<2>(*ORDERS_it86);
                            double protect_O__TOTALPRICE = get<3>(*ORDERS_it86);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it86);
                            string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it86);
                            string protect_O__CLERK = get<6>(*ORDERS_it86);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it86);
                            string protect_O__COMMENT = get<8>(*ORDERS_it86);
                            
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it84 = LINEITEM.begin();
                            
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end83 = LINEITEM.end();
                            for (
                                ; LINEITEM_it84 != LINEITEM_end83; ++LINEITEM_it84)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it84);
                                int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it84);
                                int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it84);
                                int protect_L__LINENUMBER = get<3>(*LINEITEM_it84);
                                double protect_L__QUANTITY = get<4>(*LINEITEM_it84);
                                double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it84);
                                double protect_L__DISCOUNT = get<6>(*LINEITEM_it84);
                                double protect_L__TAX = get<7>(*LINEITEM_it84);
                                if ( protect_O__CUSTKEY == protect_C__CUSTKEY
                                    && protect_L__PARTKEY == protect_P__PARTKEY 
                                    && protect_L__SUPPKEY == protect_S__SUPPKEY 
                                    && protect_L__ORDERKEY == protect_O__ORDERKEY 
                                    && REGIONKEY == protect_N1__REGIONKEY 
                                    && C__NATIONKEY == protect_N1__NATIONKEY 
                                    && C__NATIONKEY == protect_C__NATIONKEY 
                                    && P__MFGR == protect_P__MFGR 
                                    && x_qNATION_N1__NATIONKEY == protect_S__NATIONKEY )
                                {
                                    double var84  = 1;
                                    var84 *= protect_L__EXTENDEDPRICE;
                                    var84 *= protect_L__DISCOUNT;
                                    
                                    qNATION4[make_tuple(x_qNATION_N1__NATIONKEY,P__MFGR,C__NATIONKEY,REGIONKEY)] += var84;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}


void recompute_qNATION5()
{
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_it110 = qNATION5.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_end109 = qNATION5.end();
    for (; qNATION5_it110 != qNATION5_end109; ++qNATION5_it110)
    {
        string P__MFGR = get<0>(qNATION5_it110->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION5_it110->first);
        int64_t N2__REGIONKEY = get<2>(qNATION5_it110->first);
        qNATION5[make_tuple(P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] = 0;

        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it108 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end107  = NATION.end();
        for (; NATION_it108 != NATION_end107; ++NATION_it108)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it108);
            string N2__NAME = get<1>(*NATION_it108);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it108);
            string N2__COMMENT = get<3>(*NATION_it108);
            
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it106 = CUSTOMER.begin();
            
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end105 = CUSTOMER.end();
            for (; CUSTOMER_it106 != CUSTOMER_end105; ++CUSTOMER_it106)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it106);
                string protect_C__NAME = get<1>(*CUSTOMER_it106);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it106);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it106);
                string protect_C__PHONE = get<4>(*CUSTOMER_it106);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it106);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it106);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it106);
                
                multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                    >::iterator PARTS_it104 = PARTS.begin();
                
                multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                    >::iterator PARTS_end103 = PARTS.end();
                for (; PARTS_it104 != PARTS_end103; ++PARTS_it104)
                {
                    int64_t protect_P__PARTKEY = get<0>(*PARTS_it104);
                    string protect_P__NAME = get<1>(*PARTS_it104);
                    string protect_P__MFGR = get<2>(*PARTS_it104);
                    string protect_P__BRAND = get<3>(*PARTS_it104);
                    string protect_P__TYPE = get<4>(*PARTS_it104);
                    int protect_P__SIZE = get<5>(*PARTS_it104);
                    string protect_P__CONTAINER = get<6>(*PARTS_it104);
                    double protect_P__RETAILPRICE = get<7>(*PARTS_it104);
                    string protect_P__COMMENT = get<8>(*PARTS_it104);
                    
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
                            for (
                                ; LINEITEM_it98 != LINEITEM_end97; ++LINEITEM_it98)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it98);
                                int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it98);
                                int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it98);
                                int protect_L__LINENUMBER = get<3>(*LINEITEM_it98);
                                double protect_L__QUANTITY = get<4>(*LINEITEM_it98);
                                double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it98);
                                double protect_L__DISCOUNT = get<6>(*LINEITEM_it98);
                                double protect_L__TAX = get<7>(*LINEITEM_it98);
                                if ( protect_S__NATIONKEY == protect_N1__NATIONKEY
                                    && protect_O__CUSTKEY == protect_C__CUSTKEY 
                                    && protect_L__PARTKEY == protect_P__PARTKEY 
                                    && protect_L__SUPPKEY == protect_S__SUPPKEY 
                                    && protect_L__ORDERKEY == protect_O__ORDERKEY 
                                    && N2__REGIONKEY == protect_N1__REGIONKEY 
                                    && x_qNATION_N1__NATIONKEY == protect_C__NATIONKEY 
                                    && P__MFGR == protect_P__MFGR )
                                {
                                    double var103 = 1;
                                    var103 *= protect_L__EXTENDEDPRICE;
                                    var103 *= protect_L__DISCOUNT;
                                    
                                    qNATION5[make_tuple(P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += var103;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

void recompute_qNATION6()
{
    map<tuple<int64_t,string>,double>::iterator qNATION6_it300 = qNATION6.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION6_end299 = qNATION6.end();
    for (; qNATION6_it300 != qNATION6_end299; ++qNATION6_it300)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION6_it300->first);
        string P__MFGR = get<1>(qNATION6_it300->first);
        qNATION6[make_tuple(x_qNATION_N1__NATIONKEY,P__MFGR)] = 0;
        
        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_it298 = CUSTOMER.begin();
        
        multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
            >::iterator CUSTOMER_end297 = CUSTOMER.end();
        for (; CUSTOMER_it298 != CUSTOMER_end297; ++CUSTOMER_it298)
        {
            int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it298);
            string protect_C__NAME = get<1>(*CUSTOMER_it298);
            string protect_C__ADDRESS = get<2>(*CUSTOMER_it298);
            int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it298);
            string protect_C__PHONE = get<4>(*CUSTOMER_it298);
            double protect_C__ACCTBAL = get<5>(*CUSTOMER_it298);
            string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it298);
            string protect_C__COMMENT = get<7>(*CUSTOMER_it298);
            
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                >::iterator PARTS_it296 = PARTS.begin();
            
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                >::iterator PARTS_end295 = PARTS.end();
            for (; PARTS_it296 != PARTS_end295; ++PARTS_it296)
            {
                int64_t protect_P__PARTKEY = get<0>(*PARTS_it296);
                string protect_P__NAME = get<1>(*PARTS_it296);
                string protect_P__MFGR = get<2>(*PARTS_it296);
                string protect_P__BRAND = get<3>(*PARTS_it296);
                string protect_P__TYPE = get<4>(*PARTS_it296);
                int protect_P__SIZE = get<5>(*PARTS_it296);
                string protect_P__CONTAINER = get<6>(*PARTS_it296);
                double protect_P__RETAILPRICE = get<7>(*PARTS_it296);
                string protect_P__COMMENT = get<8>(*PARTS_it296);
                
                multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it294 = SUPPLIER.begin();
                
                multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end293 = SUPPLIER.end();
                for (; SUPPLIER_it294 != SUPPLIER_end293; ++SUPPLIER_it294)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it294);
                    string protect_S__NAME = get<1>(*SUPPLIER_it294);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it294);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it294);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it294);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it294);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it294);
                    
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it292 = ORDERS.begin();
                    
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end291 = ORDERS.end();
                    for (; ORDERS_it292 != ORDERS_end291; ++ORDERS_it292)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it292);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it292);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it292);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it292);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it292);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it292);
                        string protect_O__CLERK = get<6>(*ORDERS_it292);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it292);
                        string protect_O__COMMENT = get<8>(*ORDERS_it292);
                        
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it290 = LINEITEM.begin();
                        
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end289 = LINEITEM.end();
                        for (
                            ; LINEITEM_it290 != LINEITEM_end289; ++LINEITEM_it290)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it290);
                            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it290);
                            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it290);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it290);
                            double protect_L__QUANTITY = get<4>(*LINEITEM_it290);
                            double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it290);
                            double protect_L__DISCOUNT = get<6>(*LINEITEM_it290);
                            double protect_L__TAX = get<7>(*LINEITEM_it290);
                            if ( protect_O__CUSTKEY == protect_C__CUSTKEY
                                && protect_L__PARTKEY == protect_P__PARTKEY 
                                && protect_L__SUPPKEY == protect_S__SUPPKEY 
                                && protect_L__ORDERKEY == protect_O__ORDERKEY 
                                && x_qNATION_N1__NATIONKEY == protect_C__NATIONKEY 
                                && P__MFGR == protect_P__MFGR 
                                && x_qNATION_N1__NATIONKEY == protect_S__NATIONKEY )
                            {
                                double var340 = 1;
                                var340 *= protect_L__EXTENDEDPRICE;
                                var340 *= protect_L__DISCOUNT;
                                qNATION6[make_tuple(x_qNATION_N1__NATIONKEY,P__MFGR)] += var340;
                            }
                        }
                    }
                }
            }
        }
    }
}

//////////////////////////////
//
// qORDERS* plans

void recompute_qORDERS1()
{
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it120 = qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end119 = qORDERS1.end();
    for (; qORDERS1_it120 != qORDERS1_end119; ++qORDERS1_it120)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it120->first);
        string P__MFGR = get<1>(qORDERS1_it120->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1_it120->first);
        qORDERS1[make_tuple(x_qORDERS_O__ORDERKEY,P__MFGR,N2__REGIONKEY)] = 0;

        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it118 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end117  = NATION.end();
        for (; NATION_it118 != NATION_end117; ++NATION_it118)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it118);
            string N2__NAME = get<1>(*NATION_it118);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it118);
            string N2__COMMENT = get<3>(*NATION_it118);
            
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                >::iterator PARTS_it116 = PARTS.begin();
            
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                >::iterator PARTS_end115 = PARTS.end();
            for (; PARTS_it116 != PARTS_end115; ++PARTS_it116)
            {
                int64_t protect_P__PARTKEY = get<0>(*PARTS_it116);
                string protect_P__NAME = get<1>(*PARTS_it116);
                string protect_P__MFGR = get<2>(*PARTS_it116);
                string protect_P__BRAND = get<3>(*PARTS_it116);
                string protect_P__TYPE = get<4>(*PARTS_it116);
                int protect_P__SIZE = get<5>(*PARTS_it116);
                string protect_P__CONTAINER = get<6>(*PARTS_it116);
                double protect_P__RETAILPRICE = get<7>(*PARTS_it116);
                string protect_P__COMMENT = get<8>(*PARTS_it116);
                
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
                    
                    multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it112 = LINEITEM.begin();
                    
                    multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end111 = LINEITEM.end();
                    for (; LINEITEM_it112 != LINEITEM_end111; ++LINEITEM_it112)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it112);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it112);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it112);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it112);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it112);
                        double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it112);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it112);
                        double protect_L__TAX = get<7>(*LINEITEM_it112);
                        if ( protect_S__NATIONKEY == protect_N1__NATIONKEY
                            && protect_L__PARTKEY == protect_P__PARTKEY 
                            && protect_L__SUPPKEY == protect_S__SUPPKEY 
                            && N2__REGIONKEY == protect_N1__REGIONKEY 
                            && P__MFGR == protect_P__MFGR 
                            && x_qORDERS_O__ORDERKEY == protect_L__ORDERKEY )
                        {
                            qORDERS1[make_tuple(x_qORDERS_O__ORDERKEY,P__MFGR,N2__REGIONKEY)] +=
                                protect_L__EXTENDEDPRICE;
                        }
                    }
                }
            }
        }
    }
}


void recompute_qORDERS2()
{
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_it126 = qORDERS2.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_end125 = qORDERS2.end();
    for (; qORDERS2_it126 != qORDERS2_end125; ++qORDERS2_it126)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<0>(qORDERS2_it126->first);
        int64_t C__NATIONKEY = get<1>(qORDERS2_it126->first);
        int64_t REGIONKEY = get<2>(qORDERS2_it126->first);
        qORDERS2[make_tuple(x_qORDERS_O__CUSTKEY,C__NATIONKEY,REGIONKEY)] = 0;

        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it124 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end123  = NATION.end();
        for (; NATION_it124 != NATION_end123; ++NATION_it124)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it124);
            string protect_N1__NAME = get<1>(*NATION_it124);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it124);
            string protect_N1__COMMENT = get<3>(*NATION_it124);
            
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it122 = CUSTOMER.begin();
            
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end121 = CUSTOMER.end();
            for (; CUSTOMER_it122 != CUSTOMER_end121; ++CUSTOMER_it122)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it122);
                string protect_C__NAME = get<1>(*CUSTOMER_it122);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it122);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it122);
                string protect_C__PHONE = get<4>(*CUSTOMER_it122);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it122);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it122);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it122);
                if ( REGIONKEY == protect_N1__REGIONKEY
                    && C__NATIONKEY == protect_N1__NATIONKEY 
                    && C__NATIONKEY == protect_C__NATIONKEY 
                    && x_qORDERS_O__CUSTKEY == protect_C__CUSTKEY )
                {
                    qORDERS2[make_tuple(x_qORDERS_O__CUSTKEY,C__NATIONKEY,REGIONKEY)] += 1;
                }
            }
        }
    }
}


void recompute_qORDERS3()
{
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_it136 = qORDERS3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_end135 = qORDERS3.end();
    for (; qORDERS3_it136 != qORDERS3_end135; ++qORDERS3_it136)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3_it136->first);
        string P__MFGR = get<1>(qORDERS3_it136->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS3_it136->first);
        qORDERS3[make_tuple(x_qORDERS_O__ORDERKEY,P__MFGR,N2__REGIONKEY)] = 0;

        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it134 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end133  = NATION.end();
        for (; NATION_it134 != NATION_end133; ++NATION_it134)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it134);
            string N2__NAME = get<1>(*NATION_it134);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it134);
            string N2__COMMENT = get<3>(*NATION_it134);
            
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                >::iterator PARTS_it132 = PARTS.begin();
            
            multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                >::iterator PARTS_end131 = PARTS.end();
            for (; PARTS_it132 != PARTS_end131; ++PARTS_it132)
            {
                int64_t protect_P__PARTKEY = get<0>(*PARTS_it132);
                string protect_P__NAME = get<1>(*PARTS_it132);
                string protect_P__MFGR = get<2>(*PARTS_it132);
                string protect_P__BRAND = get<3>(*PARTS_it132);
                string protect_P__TYPE = get<4>(*PARTS_it132);
                int protect_P__SIZE = get<5>(*PARTS_it132);
                string protect_P__CONTAINER = get<6>(*PARTS_it132);
                double protect_P__RETAILPRICE = get<7>(*PARTS_it132);
                string protect_P__COMMENT = get<8>(*PARTS_it132);
                
                multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_it130 = SUPPLIER.begin();
                
                multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                    SUPPLIER_end129 = SUPPLIER.end();
                for (; SUPPLIER_it130 != SUPPLIER_end129; ++SUPPLIER_it130)
                {
                    int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it130);
                    string protect_S__NAME = get<1>(*SUPPLIER_it130);
                    string protect_S__ADDRESS = get<2>(*SUPPLIER_it130);
                    int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it130);
                    string protect_S__PHONE = get<4>(*SUPPLIER_it130);
                    double protect_S__ACCTBAL = get<5>(*SUPPLIER_it130);
                    string protect_S__COMMENT = get<6>(*SUPPLIER_it130);
                    
                    multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_it128 = LINEITEM.begin();
                    
                    multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                        >::iterator LINEITEM_end127 = LINEITEM.end();
                    for (; LINEITEM_it128 != LINEITEM_end127; ++LINEITEM_it128)
                    {
                        int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it128);
                        int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it128);
                        int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it128);
                        int protect_L__LINENUMBER = get<3>(*LINEITEM_it128);
                        double protect_L__QUANTITY = get<4>(*LINEITEM_it128);
                        double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it128);
                        double protect_L__DISCOUNT = get<6>(*LINEITEM_it128);
                        double protect_L__TAX = get<7>(*LINEITEM_it128);
                        if ( protect_S__NATIONKEY == protect_N1__NATIONKEY
                            && protect_L__PARTKEY == protect_P__PARTKEY 
                            && protect_L__SUPPKEY == protect_S__SUPPKEY 
                            && N2__REGIONKEY == protect_N1__REGIONKEY 
                            && P__MFGR == protect_P__MFGR 
                            && x_qORDERS_O__ORDERKEY == protect_L__ORDERKEY )
                        {
                            double var140 = 1;
                            var140 *= protect_L__EXTENDEDPRICE;
                            var140 *= protect_L__DISCOUNT;
                            qORDERS3[make_tuple(x_qORDERS_O__ORDERKEY,P__MFGR,N2__REGIONKEY)] += var140;
                        }
                    }
                }
            }
        }
    }
}


//////////////////////////////
//
// qPARTS* plans

void recompute_qPARTS1()
{
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_it150 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end149  = qPARTS1.end();
    for (; qPARTS1_it150 != qPARTS1_end149; ++qPARTS1_it150)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it150->first);
        int64_t C__NATIONKEY = get<1>(qPARTS1_it150->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it150->first);
        int64_t REGIONKEY = get<3>(qPARTS1_it150->first);
        qPARTS1[make_tuple(x_qPARTS_P__PARTKEY,C__NATIONKEY,N2__REGIONKEY,REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it148 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end147  = NATION.end();
        for (; NATION_it148 != NATION_end147; ++NATION_it148)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it148);
            string protect_N1__NAME = get<1>(*NATION_it148);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it148);
            string protect_N1__COMMENT = get<3>(*NATION_it148);

            multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it146 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end145 = NATION.end();
            for (; NATION_it146 != NATION_end145; ++NATION_it146)
            {
                int64_t protect_N1__NATIONKEY = get<0>(*NATION_it146);
                string N2__NAME = get<1>(*NATION_it146);
                int64_t protect_N1__REGIONKEY = get<2>(*NATION_it146);
                string N2__COMMENT = get<3>(*NATION_it146);
                
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_it144 = CUSTOMER.begin();
                
                multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                    >::iterator CUSTOMER_end143 = CUSTOMER.end();
                for (; CUSTOMER_it144 != CUSTOMER_end143; ++CUSTOMER_it144)
                {
                    int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it144);
                    string protect_C__NAME = get<1>(*CUSTOMER_it144);
                    string protect_C__ADDRESS = get<2>(*CUSTOMER_it144);
                    int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it144);
                    string protect_C__PHONE = get<4>(*CUSTOMER_it144);
                    double protect_C__ACCTBAL = get<5>(*CUSTOMER_it144);
                    string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it144);
                    string protect_C__COMMENT = get<7>(*CUSTOMER_it144);
                    
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_it142 = SUPPLIER.begin();
                    
                    multiset<tuple<int64_t,string,string,int64_t,string,double,string> >::iterator 
                        SUPPLIER_end141 = SUPPLIER.end();
                    for (; SUPPLIER_it142 != SUPPLIER_end141; ++SUPPLIER_it142)
                    {
                        int64_t protect_S__SUPPKEY = get<0>(*SUPPLIER_it142);
                        string protect_S__NAME = get<1>(*SUPPLIER_it142);
                        string protect_S__ADDRESS = get<2>(*SUPPLIER_it142);
                        int64_t protect_S__NATIONKEY = get<3>(*SUPPLIER_it142);
                        string protect_S__PHONE = get<4>(*SUPPLIER_it142);
                        double protect_S__ACCTBAL = get<5>(*SUPPLIER_it142);
                        string protect_S__COMMENT = get<6>(*SUPPLIER_it142);
                        
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it140 = ORDERS.begin();
                        
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end139 = ORDERS.end();
                        for (; ORDERS_it140 != ORDERS_end139; ++ORDERS_it140)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it140);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it140);
                            string protect_O__ORDERSTATUS = get<2>(*ORDERS_it140);
                            double protect_O__TOTALPRICE = get<3>(*ORDERS_it140);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it140);
                            string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it140);
                            string protect_O__CLERK = get<6>(*ORDERS_it140);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it140);
                            string protect_O__COMMENT = get<8>(*ORDERS_it140);
                            
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it138 = LINEITEM.begin();
                            
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end137 = LINEITEM.end();
                            for (
                                ; LINEITEM_it138 != LINEITEM_end137; ++LINEITEM_it138)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it138);
                                int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it138);
                                int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it138);
                                int protect_L__LINENUMBER = get<3>(*LINEITEM_it138);
                                double protect_L__QUANTITY = get<4>(*LINEITEM_it138);
                                double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it138);
                                double protect_L__DISCOUNT = get<6>(*LINEITEM_it138);
                                double protect_L__TAX = get<7>(*LINEITEM_it138);
                                if ( protect_S__NATIONKEY == protect_N1__NATIONKEY
                                    && protect_O__CUSTKEY == protect_C__CUSTKEY 
                                    && protect_L__SUPPKEY == protect_S__SUPPKEY 
                                    && protect_L__ORDERKEY == protect_O__ORDERKEY 
                                    && REGIONKEY == protect_N1__REGIONKEY 
                                    && C__NATIONKEY == protect_N1__NATIONKEY 
                                    && N2__REGIONKEY == protect_N1__REGIONKEY 
                                    && C__NATIONKEY == protect_C__NATIONKEY 
                                    && x_qPARTS_P__PARTKEY == protect_L__PARTKEY )
                                {
                                    
                                    qPARTS1[make_tuple(x_qPARTS_P__PARTKEY,C__NATIONKEY,N2__REGIONKEY,REGIONKEY)] += protect_L__EXTENDEDPRICE;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

void recompute_qPARTS2()
{
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS2_it164 = qPARTS2.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS2_end163  = qPARTS2.end();
    for (; qPARTS2_it164 != qPARTS2_end163; ++qPARTS2_it164)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2_it164->first);
        int64_t C__NATIONKEY = get<1>(qPARTS2_it164->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2_it164->first);
        int64_t REGIONKEY = get<3>(qPARTS2_it164->first);
        qPARTS2[make_tuple(x_qPARTS_P__PARTKEY,C__NATIONKEY,N2__REGIONKEY,REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it162 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end161  = NATION.end();
        for (; NATION_it162 != NATION_end161; ++NATION_it162)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it162);
            string protect_N1__NAME = get<1>(*NATION_it162);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it162);
            string protect_N1__COMMENT = get<3>(*NATION_it162);
            multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it160 = NATION.begin();
            multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end159 = NATION.end();
            for (; NATION_it160 != NATION_end159; ++NATION_it160)
            {
                int64_t protect_N1__NATIONKEY = get<0>(*NATION_it160);
                string N2__NAME = get<1>(*NATION_it160);
                int64_t protect_N1__REGIONKEY = get<2>(*NATION_it160);
                string N2__COMMENT = get<3>(*NATION_it160);
                
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
                        
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_it154 = ORDERS.begin();
                        
                        multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                            >::iterator ORDERS_end153 = ORDERS.end();
                        for (; ORDERS_it154 != ORDERS_end153; ++ORDERS_it154)
                        {
                            int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it154);
                            int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it154);
                            string protect_O__ORDERSTATUS = get<2>(*ORDERS_it154);
                            double protect_O__TOTALPRICE = get<3>(*ORDERS_it154);
                            string protect_O__ORDERDATE = get<4>(*ORDERS_it154);
                            string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it154);
                            string protect_O__CLERK = get<6>(*ORDERS_it154);
                            int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it154);
                            string protect_O__COMMENT = get<8>(*ORDERS_it154);
                            
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_it152 = LINEITEM.begin();
                            
                            multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                                >::iterator LINEITEM_end151 = LINEITEM.end();
                            for (
                                ; LINEITEM_it152 != LINEITEM_end151; ++LINEITEM_it152)
                            {
                                int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it152);
                                int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it152);
                                int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it152);
                                int protect_L__LINENUMBER = get<3>(*LINEITEM_it152);
                                double protect_L__QUANTITY = get<4>(*LINEITEM_it152);
                                double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it152);
                                double protect_L__DISCOUNT = get<6>(*LINEITEM_it152);
                                double protect_L__TAX = get<7>(*LINEITEM_it152);
                                if ( protect_S__NATIONKEY == protect_N1__NATIONKEY
                                    && protect_O__CUSTKEY == protect_C__CUSTKEY 
                                    && protect_L__SUPPKEY == protect_S__SUPPKEY 
                                    && protect_L__ORDERKEY == protect_O__ORDERKEY 
                                    && REGIONKEY == protect_N1__REGIONKEY 
                                    && C__NATIONKEY == protect_N1__NATIONKEY 
                                    && N2__REGIONKEY == protect_N1__REGIONKEY 
                                    && C__NATIONKEY == protect_C__NATIONKEY 
                                    && x_qPARTS_P__PARTKEY == protect_L__PARTKEY )
                                {
                                    double var171 = 1;
                                    var171 *= protect_L__EXTENDEDPRICE;
                                    var171 *= protect_L__DISCOUNT;
                                    qPARTS2[make_tuple(x_qPARTS_P__PARTKEY,C__NATIONKEY,N2__REGIONKEY,REGIONKEY)] += var171;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}


////////////////////////////////////////
//
// qSUPPLIER* plans

void recompute_qSUPPLIER1()
{
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qSUPPLIER1_it176 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qSUPPLIER1_end175 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it176 != qSUPPLIER1_end175; ++qSUPPLIER1_it176)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it176->first);
        string P__MFGR = get<1>(qSUPPLIER1_it176->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1_it176->first);
        int64_t REGIONKEY = get<3>(qSUPPLIER1_it176->first);
        qSUPPLIER1[make_tuple(x_qSUPPLIER_S__SUPPKEY,P__MFGR,C__NATIONKEY,REGIONKEY)] = 0;

        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it174 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end173  = NATION.end();
        for (; NATION_it174 != NATION_end173; ++NATION_it174)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it174);
            string protect_N1__NAME = get<1>(*NATION_it174);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it174);
            string protect_N1__COMMENT = get<3>(*NATION_it174);
            
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
                
                multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                    >::iterator PARTS_it170 = PARTS.begin();
                
                multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                    >::iterator PARTS_end169 = PARTS.end();
                for (; PARTS_it170 != PARTS_end169; ++PARTS_it170)
                {
                    int64_t protect_P__PARTKEY = get<0>(*PARTS_it170);
                    string protect_P__NAME = get<1>(*PARTS_it170);
                    string protect_P__MFGR = get<2>(*PARTS_it170);
                    string protect_P__BRAND = get<3>(*PARTS_it170);
                    string protect_P__TYPE = get<4>(*PARTS_it170);
                    int protect_P__SIZE = get<5>(*PARTS_it170);
                    string protect_P__CONTAINER = get<6>(*PARTS_it170);
                    double protect_P__RETAILPRICE = get<7>(*PARTS_it170);
                    string protect_P__COMMENT = get<8>(*PARTS_it170);
                    
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
                            int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it166);
                            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it166);
                            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it166);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it166);
                            double protect_L__QUANTITY = get<4>(*LINEITEM_it166);
                            double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it166);
                            double protect_L__DISCOUNT = get<6>(*LINEITEM_it166);
                            double protect_L__TAX = get<7>(*LINEITEM_it166);
                            if ( protect_O__CUSTKEY == protect_C__CUSTKEY
                                && protect_L__PARTKEY == protect_P__PARTKEY 
                                && protect_L__ORDERKEY == protect_O__ORDERKEY 
                                && REGIONKEY == protect_N1__REGIONKEY 
                                && C__NATIONKEY == protect_N1__NATIONKEY 
                                && C__NATIONKEY == protect_C__NATIONKEY 
                                && P__MFGR == protect_P__MFGR 
                                && x_qSUPPLIER_S__SUPPKEY == protect_L__SUPPKEY )
                            {
                                                            
                                qSUPPLIER1[make_tuple(x_qSUPPLIER_S__SUPPKEY,P__MFGR,C__NATIONKEY,REGIONKEY)] += protect_L__EXTENDEDPRICE;
                            }
                        }
                    }
                }
            }
        }
    }
}


void recompute_qSUPPLIER2()
{
    map<tuple<int64_t,int64_t>,int>::iterator qSUPPLIER2_it180 = qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qSUPPLIER2_end179 = qSUPPLIER2.end();
    for (; qSUPPLIER2_it180 != qSUPPLIER2_end179; ++qSUPPLIER2_it180)
    {
        int64_t x_qSUPPLIER_S__NATIONKEY = get<0>(qSUPPLIER2_it180->first);
        int64_t N2__REGIONKEY = get<1>(qSUPPLIER2_it180->first);
        qSUPPLIER2[make_tuple(x_qSUPPLIER_S__NATIONKEY,N2__REGIONKEY)] = 0;

        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it178 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end177  = NATION.end();
        for (; NATION_it178 != NATION_end177; ++NATION_it178)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it178);
            string N2__NAME = get<1>(*NATION_it178);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it178);
            string N2__COMMENT = get<3>(*NATION_it178);
            if ( N2__REGIONKEY == protect_N1__REGIONKEY
                && x_qSUPPLIER_S__NATIONKEY == protect_N1__NATIONKEY )
            {
                qSUPPLIER2[make_tuple(x_qSUPPLIER_S__NATIONKEY,N2__REGIONKEY)] += 1;
            }
        }
    }
}

void recompute_qSUPPLIER3()
{
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qSUPPLIER3_it192 = qSUPPLIER3.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qSUPPLIER3_end191 = qSUPPLIER3.end();
    for (; qSUPPLIER3_it192 != qSUPPLIER3_end191; ++qSUPPLIER3_it192)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER3_it192->first);
        string P__MFGR = get<1>(qSUPPLIER3_it192->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER3_it192->first);
        int64_t REGIONKEY = get<3>(qSUPPLIER3_it192->first);
        qSUPPLIER3[make_tuple(x_qSUPPLIER_S__SUPPKEY,P__MFGR,C__NATIONKEY,REGIONKEY)] = 0;
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_it190 = NATION.begin();
        multiset<tuple<int64_t,string,int64_t,string> >::iterator NATION_end189  = NATION.end();
        for (; NATION_it190 != NATION_end189; ++NATION_it190)
        {
            int64_t protect_N1__NATIONKEY = get<0>(*NATION_it190);
            string protect_N1__NAME = get<1>(*NATION_it190);
            int64_t protect_N1__REGIONKEY = get<2>(*NATION_it190);
            string protect_N1__COMMENT = get<3>(*NATION_it190);
            
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_it188 = CUSTOMER.begin();
            
            multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
                >::iterator CUSTOMER_end187 = CUSTOMER.end();
            for (; CUSTOMER_it188 != CUSTOMER_end187; ++CUSTOMER_it188)
            {
                int64_t protect_C__CUSTKEY = get<0>(*CUSTOMER_it188);
                string protect_C__NAME = get<1>(*CUSTOMER_it188);
                string protect_C__ADDRESS = get<2>(*CUSTOMER_it188);
                int64_t protect_C__NATIONKEY = get<3>(*CUSTOMER_it188);
                string protect_C__PHONE = get<4>(*CUSTOMER_it188);
                double protect_C__ACCTBAL = get<5>(*CUSTOMER_it188);
                string protect_C__MKTSEGMENT = get<6>(*CUSTOMER_it188);
                string protect_C__COMMENT = get<7>(*CUSTOMER_it188);
                
                multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                    >::iterator PARTS_it186 = PARTS.begin();
                
                multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                    >::iterator PARTS_end185 = PARTS.end();
                for (; PARTS_it186 != PARTS_end185; ++PARTS_it186)
                {
                    int64_t protect_P__PARTKEY = get<0>(*PARTS_it186);
                    string protect_P__NAME = get<1>(*PARTS_it186);
                    string protect_P__MFGR = get<2>(*PARTS_it186);
                    string protect_P__BRAND = get<3>(*PARTS_it186);
                    string protect_P__TYPE = get<4>(*PARTS_it186);
                    int protect_P__SIZE = get<5>(*PARTS_it186);
                    string protect_P__CONTAINER = get<6>(*PARTS_it186);
                    double protect_P__RETAILPRICE = get<7>(*PARTS_it186);
                    string protect_P__COMMENT = get<8>(*PARTS_it186);
                    
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_it184 = ORDERS.begin();
                    
                    multiset<tuple<int64_t,int64_t,string,double,string,string,string,int,string> 
                        >::iterator ORDERS_end183 = ORDERS.end();
                    for (; ORDERS_it184 != ORDERS_end183; ++ORDERS_it184)
                    {
                        int64_t protect_O__ORDERKEY = get<0>(*ORDERS_it184);
                        int64_t protect_O__CUSTKEY = get<1>(*ORDERS_it184);
                        string protect_O__ORDERSTATUS = get<2>(*ORDERS_it184);
                        double protect_O__TOTALPRICE = get<3>(*ORDERS_it184);
                        string protect_O__ORDERDATE = get<4>(*ORDERS_it184);
                        string protect_O__ORDERPRIORITY = get<5>(*ORDERS_it184);
                        string protect_O__CLERK = get<6>(*ORDERS_it184);
                        int protect_O__SHIPPRIORITY = get<7>(*ORDERS_it184);
                        string protect_O__COMMENT = get<8>(*ORDERS_it184);
                        
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_it182 = LINEITEM.begin();
                        
                        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> 
                            >::iterator LINEITEM_end181 = LINEITEM.end();
                        for (
                            ; LINEITEM_it182 != LINEITEM_end181; ++LINEITEM_it182)
                        {
                            int64_t protect_L__ORDERKEY = get<0>(*LINEITEM_it182);
                            int64_t protect_L__PARTKEY = get<1>(*LINEITEM_it182);
                            int64_t protect_L__SUPPKEY = get<2>(*LINEITEM_it182);
                            int protect_L__LINENUMBER = get<3>(*LINEITEM_it182);
                            double protect_L__QUANTITY = get<4>(*LINEITEM_it182);
                            double protect_L__EXTENDEDPRICE = get<5>(*LINEITEM_it182);
                            double protect_L__DISCOUNT = get<6>(*LINEITEM_it182);
                            double protect_L__TAX = get<7>(*LINEITEM_it182);
                            if ( protect_O__CUSTKEY == protect_C__CUSTKEY
                                && protect_L__PARTKEY == protect_P__PARTKEY 
                                && protect_L__ORDERKEY == protect_O__ORDERKEY 
                                && REGIONKEY == protect_N1__REGIONKEY 
                                && C__NATIONKEY == protect_N1__NATIONKEY 
                                && C__NATIONKEY == protect_C__NATIONKEY 
                                && P__MFGR == protect_P__MFGR 
                                && x_qSUPPLIER_S__SUPPKEY == protect_L__SUPPKEY )
                            {
                                double var210 = 1;
                                var210 *= protect_L__EXTENDEDPRICE;
                                var210 *= protect_L__DISCOUNT;
                                
                                qSUPPLIER3[make_tuple(x_qSUPPLIER_S__SUPPKEY,P__MFGR,C__NATIONKEY,REGIONKEY)] += var210;
                            }
                        }
                    }
                }
            }
        }
    }
}


////////////////////////////////////////
//
// Triggers

void on_insert_NATION(
    ofstream* results, ofstream* log, ofstream* stats, int64_t NATIONKEY,string 
    NAME,int64_t REGIONKEY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    NATION.insert(make_tuple(NATIONKEY,NAME,REGIONKEY,COMMENT));
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it2 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1 = q.end();
    for (; q_it2 != q_end1; ++q_it2)
    {
        string P__MFGR = get<0>(q_it2->first);
        q[make_tuple(P__MFGR,REGIONKEY,REGIONKEY,NATIONKEY)] +=
            qNATION3[make_tuple(NATIONKEY,P__MFGR)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it4 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end3 = q.end();
    for (; q_it4 != q_end3; ++q_it4)
    {
        string P__MFGR = get<0>(q_it4->first);
        q[make_tuple(P__MFGR,REGIONKEY,REGIONKEY,NATIONKEY)] +=
            qNATION6[make_tuple(NATIONKEY,P__MFGR)]*-1;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it6 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end5 = q.end();
    for (; q_it6 != q_end5; ++q_it6)
    {
        string P__MFGR = get<0>(q_it6->first);
        int64_t N2__REGIONKEY = get<1>(q_it6->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,REGIONKEY,NATIONKEY)] +=
            qNATION2[make_tuple(P__MFGR,NATIONKEY,N2__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it8 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end7 = q.end();
    for (; q_it8 != q_end7; ++q_it8)
    {
        string P__MFGR = get<0>(q_it8->first);
        int64_t N2__REGIONKEY = get<1>(q_it8->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,REGIONKEY,NATIONKEY)] +=
            qNATION5[make_tuple(P__MFGR,NATIONKEY,N2__REGIONKEY)]*-1;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it10 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end9 = q.end();
    for (; q_it10 != q_end9; ++q_it10)
    {
        string P__MFGR = get<0>(q_it10->first);
        int64_t REGIONKEY = get<2>(q_it10->first);
        int64_t C__NATIONKEY = get<3>(q_it10->first);
        q[make_tuple(P__MFGR,REGIONKEY,REGIONKEY,C__NATIONKEY)] +=
            qNATION1[make_tuple(NATIONKEY,P__MFGR,C__NATIONKEY,REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it12 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end11 = q.end();
    for (; q_it12 != q_end11; ++q_it12)
    {
        string P__MFGR = get<0>(q_it12->first);
        int64_t REGIONKEY = get<2>(q_it12->first);
        int64_t C__NATIONKEY = get<3>(q_it12->first);
        q[make_tuple(P__MFGR,REGIONKEY,REGIONKEY,C__NATIONKEY)] +=
            qNATION4[make_tuple(NATIONKEY,P__MFGR,C__NATIONKEY,REGIONKEY)]*-1;
    }

    recompute_qCUSTOMER1();
    recompute_qCUSTOMER2();
    recompute_qCUSTOMER3();

    recompute_qLINEITEM1();
    recompute_qLINEITEM3();

    recompute_qNATION1();
    recompute_qNATION2();
    recompute_qNATION4();
    recompute_qNATION5();

    recompute_qORDERS1();
    recompute_qORDERS2();
    recompute_qORDERS3();

    recompute_qPARTS1();
    recompute_qPARTS2();

    recompute_qSUPPLIER1();
    recompute_qSUPPLIER2();
    recompute_qSUPPLIER3();

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_NATION_sec_span, on_insert_NATION_usec_span);
}

void on_insert_LINEITEM(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    PARTKEY,int64_t SUPPKEY,int LINENUMBER,double QUANTITY,double EXTENDEDPRICE,double DISCOUNT,double TAX)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    LINEITEM.insert(make_tuple(ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDEDPRICE,DISCOUNT,TAX));
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it194 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end193 = q.end();
    for (; q_it194 != q_end193; ++q_it194)
    {
        string P__MFGR = get<0>(q_it194->first);
        int64_t N2__REGIONKEY = get<1>(q_it194->first);
        int64_t N1__REGIONKEY = get<2>(q_it194->first);
        int64_t C__NATIONKEY = get<3>(q_it194->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] +=
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*
            qLINEITEM2[make_tuple(PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)]*-1;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it196 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end195 = q.end();
    for (; q_it196 != q_end195; ++q_it196)
    {
        string P__MFGR = get<0>(q_it196->first);
        int64_t N2__REGIONKEY = get<1>(q_it196->first);
        int64_t N1__REGIONKEY = get<2>(q_it196->first);
        int64_t C__NATIONKEY = get<3>(q_it196->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] +=
            EXTENDEDPRICE*qLINEITEM1[make_tuple(ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*
            qLINEITEM2[make_tuple(PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)]*100;
    }

    recompute_qCUSTOMER1();
    recompute_qCUSTOMER3();
    
    recompute_qNATION1();
    recompute_qNATION2();
    recompute_qNATION3();
    recompute_qNATION4();
    recompute_qNATION5();
    recompute_qNATION6();

    recompute_qORDERS1();
    recompute_qORDERS3();

    recompute_qPARTS1();
    recompute_qPARTS2();

    recompute_qSUPPLIER1();
    recompute_qSUPPLIER3();

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
    ORDERS.insert(make_tuple(ORDERKEY,CUSTKEY,ORDERSTATUS,TOTALPRICE,ORDERDATE,ORDERPRIORITY,CLERK,
        SHIPPRIORITY,COMMENT));
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it374 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end373 = q.end();
    for (; q_it374 != q_end373; ++q_it374)
    {
        string P__MFGR = get<0>(q_it374->first);
        int64_t N2__REGIONKEY = get<1>(q_it374->first);
        int64_t N1__REGIONKEY = get<2>(q_it374->first);
        int64_t C__NATIONKEY = get<3>(q_it374->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] +=
            qORDERS1[make_tuple(ORDERKEY,P__MFGR,N2__REGIONKEY)]*
            qORDERS2[make_tuple(CUSTKEY,C__NATIONKEY,N1__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it376 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end375 = q.end();
    for (; q_it376 != q_end375; ++q_it376)
    {
        string P__MFGR = get<0>(q_it376->first);
        int64_t N2__REGIONKEY = get<1>(q_it376->first);
        int64_t N1__REGIONKEY = get<2>(q_it376->first);
        int64_t C__NATIONKEY = get<3>(q_it376->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] +=
            qORDERS3[make_tuple(ORDERKEY,P__MFGR,N2__REGIONKEY)]*
            qORDERS2[make_tuple(CUSTKEY,C__NATIONKEY,N1__REGIONKEY)]*-1;
    }

    recompute_qCUSTOMER1();
    recompute_qCUSTOMER3();

    recompute_qLINEITEM1();

    recompute_qNATION1();
    recompute_qNATION2();
    recompute_qNATION3();
    recompute_qNATION4();
    recompute_qNATION5();
    recompute_qNATION6();

    recompute_qPARTS1();
    recompute_qPARTS2();

    recompute_qSUPPLIER1();
    recompute_qSUPPLIER3();
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
    SUPPLIER.insert(make_tuple(SUPPKEY,NAME,ADDRESS,NATIONKEY,PHONE,ACCTBAL,COMMENT));
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it542 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end541 = q.end();
    for (; q_it542 != q_end541; ++q_it542)
    {
        string P__MFGR = get<0>(q_it542->first);
        int64_t N2__REGIONKEY = get<1>(q_it542->first);
        int64_t N1__REGIONKEY = get<2>(q_it542->first);
        int64_t C__NATIONKEY = get<3>(q_it542->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] +=
            qSUPPLIER1[make_tuple(SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)]*
            qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it544 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end543 = q.end();
    for (; q_it544 != q_end543; ++q_it544)
    {
        string P__MFGR = get<0>(q_it544->first);
        int64_t N2__REGIONKEY = get<1>(q_it544->first);
        int64_t N1__REGIONKEY = get<2>(q_it544->first);
        int64_t C__NATIONKEY = get<3>(q_it544->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] +=
            qSUPPLIER3[make_tuple(SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)]*
            qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)]*-1;
    }
    
    recompute_qCUSTOMER1();
    recompute_qCUSTOMER3();

    recompute_qLINEITEM3();

    recompute_qNATION1();
    recompute_qNATION2();
    recompute_qNATION3();
    recompute_qNATION4();
    recompute_qNATION5();
    recompute_qNATION6();

    recompute_qORDERS1();
    recompute_qORDERS3();

    recompute_qPARTS1();
    recompute_qPARTS2();

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_SUPPLIER_sec_span, on_insert_SUPPLIER_usec_span);
}

void on_insert_PARTS(
    ofstream* results, ofstream* log, ofstream* stats, int64_t PARTKEY,string 
    NAME,string MFGR,string BRAND,string TYPE,int SIZE,string CONTAINER,double RETAILPRICE,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    PARTS.insert(make_tuple(PARTKEY,NAME,MFGR,BRAND,TYPE,SIZE,CONTAINER,RETAILPRICE,COMMENT));
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it704 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end703 = q.end();
    for (; q_it704 != q_end703; ++q_it704)
    {
        int64_t N2__REGIONKEY = get<1>(q_it704->first);
        int64_t N1__REGIONKEY = get<2>(q_it704->first);
        int64_t C__NATIONKEY = get<3>(q_it704->first);
        q[make_tuple(MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] +=
            qPARTS1[make_tuple(PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it706 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end705 = q.end();
    for (; q_it706 != q_end705; ++q_it706)
    {
        int64_t N2__REGIONKEY = get<1>(q_it706->first);
        int64_t N1__REGIONKEY = get<2>(q_it706->first);
        int64_t C__NATIONKEY = get<3>(q_it706->first);
        q[make_tuple(MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] +=
            qPARTS2[make_tuple(PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)]*-1;
    }

    recompute_qCUSTOMER1();
    recompute_qCUSTOMER3();

    recompute_qLINEITEM2();

    recompute_qNATION1();
    recompute_qNATION2();
    recompute_qNATION3();
    recompute_qNATION4();
    recompute_qNATION5();
    recompute_qNATION6();

    recompute_qORDERS1();
    recompute_qORDERS3();

    recompute_qSUPPLIER1();
    recompute_qSUPPLIER3();

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_PARTS_sec_span, on_insert_PARTS_usec_span);
}

void on_insert_CUSTOMER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t CUSTKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    MKTSEGMENT,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    CUSTOMER.insert(make_tuple(CUSTKEY,NAME,ADDRESS,NATIONKEY,PHONE,ACCTBAL,MKTSEGMENT,COMMENT));
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it860 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end859 = q.end();
    for (; q_it860 != q_end859; ++q_it860)
    {
        string P__MFGR = get<0>(q_it860->first);
        int64_t N2__REGIONKEY = get<1>(q_it860->first);
        int64_t N1__REGIONKEY = get<2>(q_it860->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,N1__REGIONKEY,NATIONKEY)] += 
            qCUSTOMER1[make_tuple(CUSTKEY,P__MFGR,N2__REGIONKEY)]*
            qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it862 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end861 = q.end();
    for (; q_it862 != q_end861; ++q_it862)
    {
        string P__MFGR = get<0>(q_it862->first);
        int64_t N2__REGIONKEY = get<1>(q_it862->first);
        int64_t N1__REGIONKEY = get<2>(q_it862->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,N1__REGIONKEY,NATIONKEY)] +=
            qCUSTOMER3[make_tuple(CUSTKEY,P__MFGR,N2__REGIONKEY)]*
            qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)]*-1;
    }

    recompute_qLINEITEM1();

    recompute_qNATION1();
    recompute_qNATION2();
    recompute_qNATION3();
    recompute_qNATION4();
    recompute_qNATION5();
    recompute_qNATION6();

    recompute_qORDERS2();

    recompute_qPARTS1();
    recompute_qPARTS2();

    recompute_qSUPPLIER1();
    recompute_qSUPPLIER3();

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_CUSTOMER_sec_span, on_insert_CUSTOMER_usec_span);
}

void on_delete_NATION(
    ofstream* results, ofstream* log, ofstream* stats, int64_t NATIONKEY,string 
    NAME,int64_t REGIONKEY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    NATION.erase(make_tuple(NATIONKEY,NAME,REGIONKEY,COMMENT));
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it1010 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1009 = q.end();
    for (; q_it1010 != q_end1009; ++q_it1010)
    {
        string P__MFGR = get<0>(q_it1010->first);
        q[make_tuple(P__MFGR,REGIONKEY,REGIONKEY,NATIONKEY)] +=
            -1*qNATION3[make_tuple(NATIONKEY,P__MFGR)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it1012 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1011 = q.end();
    for (; q_it1012 != q_end1011; ++q_it1012)
    {
        string P__MFGR = get<0>(q_it1012->first);
        q[make_tuple(P__MFGR,REGIONKEY,REGIONKEY,NATIONKEY)] +=
            -1*qNATION6[make_tuple(NATIONKEY,P__MFGR)]*-1;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it1014 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1013 = q.end();
    for (; q_it1014 != q_end1013; ++q_it1014)
    {
        string P__MFGR = get<0>(q_it1014->first);
        int64_t N2__REGIONKEY = get<1>(q_it1014->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,REGIONKEY,NATIONKEY)] +=
            -1*qNATION2[make_tuple(P__MFGR,NATIONKEY,N2__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it1016 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1015 = q.end();
    for (; q_it1016 != q_end1015; ++q_it1016)
    {
        string P__MFGR = get<0>(q_it1016->first);
        int64_t N2__REGIONKEY = get<1>(q_it1016->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,REGIONKEY,NATIONKEY)] +=
            -1*qNATION5[make_tuple(P__MFGR,NATIONKEY,N2__REGIONKEY)]*-1;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it1018 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1017 = q.end();
    for (; q_it1018 != q_end1017; ++q_it1018)
    {
        string P__MFGR = get<0>(q_it1018->first);
        int64_t REGIONKEY = get<2>(q_it1018->first);
        int64_t C__NATIONKEY = get<3>(q_it1018->first);
        q[make_tuple(P__MFGR,REGIONKEY,REGIONKEY,C__NATIONKEY)] += 
            -1*qNATION1[make_tuple(NATIONKEY,P__MFGR,C__NATIONKEY,REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it1020 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1019 = q.end();
    for (; q_it1020 != q_end1019; ++q_it1020)
    {
        string P__MFGR = get<0>(q_it1020->first);
        int64_t REGIONKEY = get<2>(q_it1020->first);
        int64_t C__NATIONKEY = get<3>(q_it1020->first);
        q[make_tuple(P__MFGR,REGIONKEY,REGIONKEY,C__NATIONKEY)] +=
            -1*qNATION4[make_tuple(NATIONKEY,P__MFGR,C__NATIONKEY,REGIONKEY)]*-1;
    }

    recompute_qCUSTOMER1();
    recompute_qCUSTOMER2();
    recompute_qCUSTOMER3();

    recompute_qLINEITEM1();
    recompute_qLINEITEM3();

    recompute_qNATION1();
    recompute_qNATION2();
    recompute_qNATION4();
    recompute_qNATION5();

    recompute_qORDERS1();
    recompute_qORDERS2();
    recompute_qORDERS3();

    recompute_qPARTS1();
    recompute_qPARTS2();

    recompute_qSUPPLIER1();
    recompute_qSUPPLIER2();

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_NATION_sec_span, on_delete_NATION_usec_span);
}

void on_delete_LINEITEM(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    PARTKEY,int64_t SUPPKEY,int LINENUMBER,double QUANTITY,double EXTENDEDPRICE,double DISCOUNT,double TAX)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    LINEITEM.erase(make_tuple(ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDEDPRICE,DISCOUNT,TAX));
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it1202 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1201 = q.end();
    for (; q_it1202 != q_end1201; ++q_it1202)
    {
        string P__MFGR = get<0>(q_it1202->first);
        int64_t N2__REGIONKEY = get<1>(q_it1202->first);
        int64_t N1__REGIONKEY = get<2>(q_it1202->first);
        int64_t C__NATIONKEY = get<3>(q_it1202->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*
            qLINEITEM1[make_tuple(ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*
            qLINEITEM2[make_tuple(PARTKEY,P__MFGR)]*
            qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)]*-1;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it1204 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1203 = 
        q.end();
    for (; q_it1204 != q_end1203; ++q_it1204)
    {
        string P__MFGR = get<0>(q_it1204->first);
        int64_t N2__REGIONKEY = get<1>(q_it1204->first);
        int64_t N1__REGIONKEY = get<2>(q_it1204->first);
        int64_t C__NATIONKEY = get<3>(q_it1204->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += 
            -1*EXTENDEDPRICE*
            qLINEITEM1[make_tuple(ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*
            qLINEITEM2[make_tuple(PARTKEY,P__MFGR)]*
            qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)]*100;
    }

    recompute_qCUSTOMER1();
    recompute_qCUSTOMER3();

    recompute_qNATION1();
    recompute_qNATION2();
    recompute_qNATION3();
    recompute_qNATION4();
    recompute_qNATION5();
    recompute_qNATION6();

    recompute_qORDERS1();
    recompute_qORDERS3();

    recompute_qPARTS1();
    recompute_qPARTS2();

    recompute_qSUPPLIER1();
    recompute_qSUPPLIER3();

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
    ORDERS.erase(make_tuple(ORDERKEY,CUSTKEY,ORDERSTATUS,TOTALPRICE,ORDERDATE,ORDERPRIORITY,CLERK,
        SHIPPRIORITY,COMMENT));
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it1382 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1381 = q.end();
    for (; q_it1382 != q_end1381; ++q_it1382)
    {
        string P__MFGR = get<0>(q_it1382->first);
        int64_t N2__REGIONKEY = get<1>(q_it1382->first);
        int64_t N1__REGIONKEY = get<2>(q_it1382->first);
        int64_t C__NATIONKEY = get<3>(q_it1382->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += 
            -1*qORDERS1[make_tuple(ORDERKEY,P__MFGR,N2__REGIONKEY)]*
            qORDERS2[make_tuple(CUSTKEY,C__NATIONKEY,N1__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it1384 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1383 = 
        q.end();
    for (; q_it1384 != q_end1383; ++q_it1384)
    {
        string P__MFGR = get<0>(q_it1384->first);
        int64_t N2__REGIONKEY = get<1>(q_it1384->first);
        int64_t N1__REGIONKEY = get<2>(q_it1384->first);
        int64_t C__NATIONKEY = get<3>(q_it1384->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += 
            -1*qORDERS3[make_tuple(ORDERKEY,P__MFGR,N2__REGIONKEY)]*
            qORDERS2[make_tuple(CUSTKEY,C__NATIONKEY,N1__REGIONKEY)]*-1;
    }

    recompute_qCUSTOMER1();
    recompute_qCUSTOMER3();
    
    recompute_qLINEITEM1();

    recompute_qNATION1();
    recompute_qNATION2();
    recompute_qNATION3();
    recompute_qNATION4();
    recompute_qNATION5();
    recompute_qNATION6();

    recompute_qPARTS1();
    recompute_qPARTS2();

    recompute_qSUPPLIER1();
    recompute_qSUPPLIER3();

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
    SUPPLIER.erase(make_tuple(SUPPKEY,NAME,ADDRESS,NATIONKEY,PHONE,ACCTBAL,COMMENT));
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it1550 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1549 = q.end();
    for (; q_it1550 != q_end1549; ++q_it1550)
    {
        string P__MFGR = get<0>(q_it1550->first);
        int64_t N2__REGIONKEY = get<1>(q_it1550->first);
        int64_t N1__REGIONKEY = get<2>(q_it1550->first);
        int64_t C__NATIONKEY = get<3>(q_it1550->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] +=
            -1*qSUPPLIER1[make_tuple(SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)]*
            qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it1552 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1551 = q.end();
    for (; q_it1552 != q_end1551; ++q_it1552)
    {
        string P__MFGR = get<0>(q_it1552->first);
        int64_t N2__REGIONKEY = get<1>(q_it1552->first);
        int64_t N1__REGIONKEY = get<2>(q_it1552->first);
        int64_t C__NATIONKEY = get<3>(q_it1552->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += 
            -1*qSUPPLIER3[make_tuple(SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)]*
            qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)]*-1;
    }

    recompute_qCUSTOMER1();
    recompute_qCUSTOMER3();

    recompute_qLINEITEM3();

    recompute_qNATION1();
    recompute_qNATION2();
    recompute_qNATION3();
    recompute_qNATION4();
    recompute_qNATION5();
    recompute_qNATION6();

    recompute_qORDERS1();
    recompute_qORDERS3();

    recompute_qPARTS1();
    recompute_qPARTS2();

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_SUPPLIER_sec_span, on_delete_SUPPLIER_usec_span);
}

void on_delete_PARTS(
    ofstream* results, ofstream* log, ofstream* stats, int64_t PARTKEY,string 
    NAME,string MFGR,string BRAND,string TYPE,int SIZE,string CONTAINER,double RETAILPRICE,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    PARTS.erase(make_tuple(PARTKEY,NAME,MFGR,BRAND,TYPE,SIZE,CONTAINER,RETAILPRICE,COMMENT));
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it1712 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1711 = q.end();
    for (; q_it1712 != q_end1711; ++q_it1712)
    {
        int64_t N2__REGIONKEY = get<1>(q_it1712->first);
        int64_t N1__REGIONKEY = get<2>(q_it1712->first);
        int64_t C__NATIONKEY = get<3>(q_it1712->first);
        q[make_tuple(MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += 
            -1*qPARTS1[make_tuple(PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it1714 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1713 = q.end();
    for (; q_it1714 != q_end1713; ++q_it1714)
    {
        int64_t N2__REGIONKEY = get<1>(q_it1714->first);
        int64_t N1__REGIONKEY = get<2>(q_it1714->first);
        int64_t C__NATIONKEY = get<3>(q_it1714->first);
        q[make_tuple(MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += 
            -1*qPARTS2[make_tuple(PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)]*-1;
    }

    recompute_qCUSTOMER1();
    recompute_qCUSTOMER3();

    recompute_qLINEITEM2();

    recompute_qNATION1();
    recompute_qNATION2();
    recompute_qNATION3();
    recompute_qNATION4();
    recompute_qNATION5();
    recompute_qNATION6();

    recompute_qORDERS1();
    recompute_qORDERS3();

    recompute_qSUPPLIER1();
    recompute_qSUPPLIER3();

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_PARTS_sec_span, on_delete_PARTS_usec_span);
}

void on_delete_CUSTOMER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t CUSTKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    MKTSEGMENT,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    CUSTOMER.erase(make_tuple(CUSTKEY,NAME,ADDRESS,NATIONKEY,PHONE,ACCTBAL,MKTSEGMENT,COMMENT));
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it1868 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1867 = q.end();
    for (; q_it1868 != q_end1867; ++q_it1868)
    {
        string P__MFGR = get<0>(q_it1868->first);
        int64_t N2__REGIONKEY = get<1>(q_it1868->first);
        int64_t N1__REGIONKEY = get<2>(q_it1868->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,N1__REGIONKEY,NATIONKEY)] += 
            -1*qCUSTOMER1[make_tuple(CUSTKEY,P__MFGR,N2__REGIONKEY)]*
            qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it1870 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1869 = q.end();
    for (; q_it1870 != q_end1869; ++q_it1870)
    {
        string P__MFGR = get<0>(q_it1870->first);
        int64_t N2__REGIONKEY = get<1>(q_it1870->first);
        int64_t N1__REGIONKEY = get<2>(q_it1870->first);
        q[make_tuple(P__MFGR,N2__REGIONKEY,N1__REGIONKEY,NATIONKEY)] +=
            -1*qCUSTOMER3[make_tuple(CUSTKEY,P__MFGR,N2__REGIONKEY)]*
            qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)]*-1;
    }

    recompute_qLINEITEM1();

    recompute_qNATION1();
    recompute_qNATION2();
    recompute_qNATION3();
    recompute_qNATION4();
    recompute_qNATION5();
    recompute_qNATION6();

    recompute_qORDERS2();

    recompute_qPARTS1();
    recompute_qPARTS2();

    recompute_qSUPPLIER1();
    recompute_qSUPPLIER3();

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_CUSTOMER_sec_span, on_delete_CUSTOMER_usec_span);
}

DBToaster::DemoDatasets::NationStream SSBNation("/Users/yanif/datasets/tpch/sf1/singlefile/nation.tbl",&DBToaster::DemoDatasets::parseNationField,4,100,512);

boost::shared_ptr<DBToaster::DemoDatasets::NationTupleAdaptor> SSBNation_adaptor(new DBToaster::DemoDatasets::NationTupleAdaptor());
static int streamSSBNationId = 0;

struct on_insert_NATION_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::NationTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::NationTupleAdaptor::Result>(data); 
        on_insert_NATION(results, log, stats, input.nationkey,input.name,input.regionkey,input.comment);
    }
};

on_insert_NATION_fun_obj fo_on_insert_NATION_0;

DBToaster::DemoDatasets::LineitemStream SSBLineitem("/Users/yanif/datasets/tpch/sf1/singlefile/lineitem.tbl.a",&DBToaster::DemoDatasets::parseLineitemField,16,65000000,512);

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

DBToaster::DemoDatasets::OrderStream SSBOrder("/Users/yanif/datasets/tpch/sf1/singlefile/orders.tbl.a",&DBToaster::DemoDatasets::parseOrderField,9,17000000,512);

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

DBToaster::DemoDatasets::SupplierStream SSBSupplier("/Users/yanif/datasets/tpch/sf1/singlefile/supplier.tbl.a",&DBToaster::DemoDatasets::parseSupplierField,7,110000,512);

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

DBToaster::DemoDatasets::PartStream SSBParts("/Users/yanif/datasets/tpch/sf1/singlefile/part.tbl.a",&DBToaster::DemoDatasets::parsePartField,9,2100000,512);

boost::shared_ptr<DBToaster::DemoDatasets::PartTupleAdaptor> SSBParts_adaptor(new DBToaster::DemoDatasets::PartTupleAdaptor());
static int streamSSBPartsId = 4;

struct on_insert_PARTS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::PartTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::PartTupleAdaptor::Result>(data); 
        on_insert_PARTS(results, log, stats, input.partkey,input.name,input.mfgr,input.brand,input.type,input.size,input.container,input.retailprice,input.comment);
    }
};

on_insert_PARTS_fun_obj fo_on_insert_PARTS_4;

DBToaster::DemoDatasets::CustomerStream SSBCustomer("/Users/yanif/datasets/tpch/sf1/singlefile/customer.tbl.a",&DBToaster::DemoDatasets::parseCustomerField,8,1600000, 512);

boost::shared_ptr<DBToaster::DemoDatasets::CustomerTupleAdaptor> SSBCustomer_adaptor(new DBToaster::DemoDatasets::CustomerTupleAdaptor());
static int streamSSBCustomerId = 5;

struct on_insert_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_insert_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_insert_CUSTOMER_fun_obj fo_on_insert_CUSTOMER_5;

struct on_delete_NATION_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::NationTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::NationTupleAdaptor::Result>(data); 
        on_delete_NATION(results, log, stats, input.nationkey,input.name,input.regionkey,input.comment);
    }
};

on_delete_NATION_fun_obj fo_on_delete_NATION_6;

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

struct on_delete_PARTS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::PartTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::PartTupleAdaptor::Result>(data); 
        on_delete_PARTS(results, log, stats, input.partkey,input.name,input.mfgr,input.brand,input.type,input.size,input.container,input.retailprice,input.comment);
    }
};

on_delete_PARTS_fun_obj fo_on_delete_PARTS_10;

struct on_delete_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_delete_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_delete_CUSTOMER_fun_obj fo_on_delete_CUSTOMER_11;


void init(DBToaster::StandaloneEngine::FileMultiplexer& sources,
    DBToaster::StandaloneEngine::FileStreamDispatcher& router, 
    ofstream* results, ofstream* log, ofstream* stats)
{
    router.setFiles(results, log, stats);
    sources.addStream<DBToaster::DemoDatasets::nation>(&SSBNation, boost::ref(*SSBNation_adaptor), streamSSBNationId);
    router.addHandler(streamSSBNationId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_NATION_0));
    sources.addStream<DBToaster::DemoDatasets::lineitem>(&SSBLineitem, boost::ref(*SSBLineitem_adaptor), streamSSBLineitemId);
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_LINEITEM_1));
    sources.addStream<DBToaster::DemoDatasets::order>(&SSBOrder, boost::ref(*SSBOrder_adaptor), streamSSBOrderId);
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_ORDERS_2));
    sources.addStream<DBToaster::DemoDatasets::supplier>(&SSBSupplier, boost::ref(*SSBSupplier_adaptor), streamSSBSupplierId);
    router.addHandler(streamSSBSupplierId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_SUPPLIER_3));
    sources.addStream<DBToaster::DemoDatasets::part>(&SSBParts, boost::ref(*SSBParts_adaptor), streamSSBPartsId);
    router.addHandler(streamSSBPartsId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_PARTS_4));
    sources.addStream<DBToaster::DemoDatasets::customer>(&SSBCustomer, boost::ref(*SSBCustomer_adaptor), streamSSBCustomerId);
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_CUSTOMER_5));
    router.addHandler(streamSSBNationId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_NATION_6));
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_LINEITEM_7));
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_ORDERS_8));
    router.addHandler(streamSSBSupplierId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_SUPPLIER_9));
    router.addHandler(streamSSBPartsId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_PARTS_10));
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_CUSTOMER_11));
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
