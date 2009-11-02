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
map<tuple<int64_t,int64_t,int64_t>,int> qLINEITEM1;
map<tuple<int64_t,string>,int> qLINEITEM2;
map<tuple<int64_t,int64_t>,int> qLINEITEM3;
map<tuple<int64_t,string,int64_t,int64_t>,double> qSUPPLIER1;
map<tuple<int64_t,int64_t,string>,double> qORDERS1SUPPLIER1;
map<tuple<int64_t,int64_t>,int> qSUPPLIER2;
map<tuple<int64_t,int64_t,int64_t>,double> qORDERS1PARTS1;
map<tuple<int64_t,int64_t,int64_t>,double> qORDERS1PARTS1NATION1;
map<tuple<int64_t,string,int64_t,int64_t>,double> qNATION1;
map<tuple<int64_t,int64_t,int64_t>,double> qCUSTOMER1PARTS1NATION1;
map<tuple<int64_t,int64_t,string,int64_t>,double> qNATION3ORDERS1;
map<tuple<int64_t,int64_t,int64_t,int64_t>,double> qSUPPLIER1PARTS1;
map<tuple<string,int64_t,int64_t>,double> qNATION2;
map<tuple<int64_t,string>,double> qNATION3;
map<tuple<int64_t,int64_t,int64_t,int64_t>,double> qPARTS1NATION1;
map<tuple<int64_t,int64_t,int64_t>,double> qPARTS1NATION2;
map<tuple<int64_t,int64_t>,double> qPARTS1NATION3;
map<tuple<int64_t,int64_t>,int> qLINEITEM1NATION1;
map<tuple<int64_t,int64_t,string>,double> qORDERS1NATION1;
map<tuple<int64_t,int64_t,string>,double> qCUSTOMER1SUPPLIER1;
map<tuple<int64_t,int64_t,int64_t>,int> qPARTS1NATION3LINEITEM1ORDERS1;
map<tuple<int64_t,int64_t,int64_t,int64_t>,double> qPARTS1;
map<tuple<int64_t,string,int64_t>,double> qNATION1NATION1;
map<tuple<int64_t,int64_t>,int> qLINEITEM1CUSTOMER1;
map<tuple<int64_t,int64_t,int64_t>,double> qCUSTOMER1SUPPLIER1PARTS1;
map<tuple<int64_t,string,int64_t>,double> qORDERS1;
map<tuple<int64_t,string,int64_t>,double> qCUSTOMER1;
map<tuple<int64_t,int64_t,int64_t>,int> qORDERS2;
map<tuple<int64_t,int64_t>,int> qCUSTOMER2;
map<tuple<int64_t,int64_t,int64_t>,int> qPARTS1NATION3LINEITEM1;
map<tuple<int64_t,int64_t,string>,double> qCUSTOMER1NATION1;
map<tuple<int64_t,int64_t>,int> qLINEITEM1NATION1ORDERS1;
map<tuple<string,int64_t,int64_t,int64_t>,double> q;
map<tuple<int64_t,int64_t,int64_t,int64_t>,double> qPARTS1NATION3ORDERS1;
map<tuple<int64_t,int64_t,int64_t>,double> qSUPPLIER1PARTS1NATION1;
map<tuple<int64_t,int64_t,int64_t>,double> qORDERS1SUPPLIER1PARTS1;
map<tuple<int64_t,string,int64_t>,double> qSUPPLIER1NATION1;
map<tuple<int64_t,int64_t,int64_t>,int> qPARTS1NATION3ORDERS1LINEITEM1;
map<tuple<int64_t,int64_t>,int> qLINEITEM3NATION1;
map<tuple<int64_t,int64_t,int64_t>,double> qCUSTOMER1PARTS1;
map<tuple<int64_t,int64_t,int64_t>,double> qPARTS1NATION1NATION1;

double on_insert_NATION_sec_span = 0.0;
double on_insert_NATION_usec_span = 0.0;
double on_insert_ORDERS_sec_span = 0.0;
double on_insert_ORDERS_usec_span = 0.0;
double on_insert_LINEITEM_sec_span = 0.0;
double on_insert_LINEITEM_usec_span = 0.0;
double on_insert_CUSTOMER_sec_span = 0.0;
double on_insert_CUSTOMER_usec_span = 0.0;
double on_insert_SUPPLIER_sec_span = 0.0;
double on_insert_SUPPLIER_usec_span = 0.0;
double on_insert_PARTS_sec_span = 0.0;
double on_insert_PARTS_usec_span = 0.0;
double on_delete_NATION_sec_span = 0.0;
double on_delete_NATION_usec_span = 0.0;
double on_delete_ORDERS_sec_span = 0.0;
double on_delete_ORDERS_usec_span = 0.0;
double on_delete_LINEITEM_sec_span = 0.0;
double on_delete_LINEITEM_usec_span = 0.0;
double on_delete_CUSTOMER_sec_span = 0.0;
double on_delete_CUSTOMER_usec_span = 0.0;
double on_delete_SUPPLIER_sec_span = 0.0;
double on_delete_SUPPLIER_usec_span = 0.0;
double on_delete_PARTS_sec_span = 0.0;
double on_delete_PARTS_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
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

   cout << "qSUPPLIER1 size: " << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER1" << "," << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS1SUPPLIER1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1SUPPLIER1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "qSUPPLIER2 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER2" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qORDERS1PARTS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1PARTS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS1PARTS1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1PARTS1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qNATION1 size: " << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION1" << "," << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER1PARTS1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1PARTS1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qNATION3ORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION3ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION3ORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION3ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qSUPPLIER1PARTS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER1PARTS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qNATION2 size: " << (((sizeof(map<tuple<string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION2.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t,int64_t>,double>::key_type, map<tuple<string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t,int64_t>,double>::value_type>, map<tuple<string,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION2" << "," << (((sizeof(map<tuple<string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION2.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t,int64_t>,double>::key_type, map<tuple<string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t,int64_t>,double>::value_type>, map<tuple<string,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qNATION3 size: " << (((sizeof(map<tuple<int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string>,double>::key_type, map<tuple<int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,string>,double>::value_type>, map<tuple<int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION3" << "," << (((sizeof(map<tuple<int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string>,double>::key_type, map<tuple<int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,string>,double>::value_type>, map<tuple<int64_t,string>,double>::key_compare>))) << endl;

   cout << "qPARTS1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qPARTS1NATION2 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS1NATION2" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qPARTS1NATION3 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS1NATION3" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qLINEITEM1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qORDERS1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER1SUPPLIER1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1SUPPLIER1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "qPARTS1NATION3LINEITEM1ORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION3LINEITEM1ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS1NATION3LINEITEM1ORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION3LINEITEM1ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qPARTS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qNATION1NATION1 size: " << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION1NATION1" << "," << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qLINEITEM1CUSTOMER1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1CUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1CUSTOMER1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1CUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER1SUPPLIER1PARTS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1SUPPLIER1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1SUPPLIER1PARTS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1SUPPLIER1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS1 size: " << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1" << "," << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER1 size: " << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1" << "," << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS2 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS2" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER2 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER2" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qPARTS1NATION3LINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION3LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS1NATION3LINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION3LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "qLINEITEM1NATION1ORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1NATION1ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1NATION1ORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1NATION1ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "q size: " << (((sizeof(map<tuple<string,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<string,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<string,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "q" << "," << (((sizeof(map<tuple<string,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<string,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<string,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qPARTS1NATION3ORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION3ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS1NATION3ORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION3ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qSUPPLIER1PARTS1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER1PARTS1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS1SUPPLIER1PARTS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1SUPPLIER1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1SUPPLIER1PARTS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1SUPPLIER1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qSUPPLIER1NATION1 size: " << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER1NATION1" << "," << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qPARTS1NATION3ORDERS1LINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION3ORDERS1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS1NATION3ORDERS1LINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION3ORDERS1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qLINEITEM3NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM3NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM3NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM3NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER1PARTS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1PARTS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qPARTS1NATION1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS1NATION1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

}

void analyse_handler_usage(ofstream* stats)
{
   cout << "on_insert_NATION cost: " << (on_insert_NATION_sec_span + (on_insert_NATION_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_NATION" << "," << (on_insert_NATION_sec_span + (on_insert_NATION_usec_span / 1000000.0)) << endl;
   cout << "on_insert_ORDERS cost: " << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_ORDERS" << "," << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   cout << "on_insert_LINEITEM cost: " << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_LINEITEM" << "," << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_insert_CUSTOMER cost: " << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_CUSTOMER" << "," << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   cout << "on_insert_SUPPLIER cost: " << (on_insert_SUPPLIER_sec_span + (on_insert_SUPPLIER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_SUPPLIER" << "," << (on_insert_SUPPLIER_sec_span + (on_insert_SUPPLIER_usec_span / 1000000.0)) << endl;
   cout << "on_insert_PARTS cost: " << (on_insert_PARTS_sec_span + (on_insert_PARTS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_PARTS" << "," << (on_insert_PARTS_sec_span + (on_insert_PARTS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_NATION cost: " << (on_delete_NATION_sec_span + (on_delete_NATION_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_NATION" << "," << (on_delete_NATION_sec_span + (on_delete_NATION_usec_span / 1000000.0)) << endl;
   cout << "on_delete_ORDERS cost: " << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_ORDERS" << "," << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_LINEITEM cost: " << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_LINEITEM" << "," << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_delete_CUSTOMER cost: " << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_CUSTOMER" << "," << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
   cout << "on_delete_SUPPLIER cost: " << (on_delete_SUPPLIER_sec_span + (on_delete_SUPPLIER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_SUPPLIER" << "," << (on_delete_SUPPLIER_sec_span + (on_delete_SUPPLIER_usec_span / 1000000.0)) << endl;
   cout << "on_delete_PARTS cost: " << (on_delete_PARTS_sec_span + (on_delete_PARTS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_PARTS" << "," << (on_delete_PARTS_sec_span + (on_delete_PARTS_usec_span / 1000000.0)) << endl;
}


void on_insert_NATION(
    ofstream* results, ofstream* log, ofstream* stats, int64_t NATIONKEY,string 
    NAME,int64_t REGIONKEY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it2 = q.begin(
        );
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1 = q.end(
        );
    for (; q_it2 != q_end1; ++q_it2)
    {
        string P__MFGR = get<0>(q_it2->first);
        q[make_tuple(
            P__MFGR,REGIONKEY,REGIONKEY,NATIONKEY)] += qNATION3[make_tuple(
            NATIONKEY,P__MFGR)];
    }
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it4 = q.begin(
        );
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end3 = q.end(
        );
    for (; q_it4 != q_end3; ++q_it4)
    {
        string P__MFGR = get<0>(q_it4->first);
        int64_t N2__REGIONKEY = get<1>(q_it4->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,REGIONKEY,NATIONKEY)] += qNATION2[make_tuple(
            P__MFGR,NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it6 = q.begin(
        );
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end5 = q.end(
        );
    for (; q_it6 != q_end5; ++q_it6)
    {
        string P__MFGR = get<0>(q_it6->first);
        int64_t REGIONKEY = get<2>(q_it6->first);
        int64_t C__NATIONKEY = get<3>(q_it6->first);
        q[make_tuple(
            P__MFGR,REGIONKEY,REGIONKEY,C__NATIONKEY)] += qNATION1[make_tuple(
            NATIONKEY,P__MFGR,C__NATIONKEY,REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it8 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end7 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it8 != qCUSTOMER1_end7; ++qCUSTOMER1_it8)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it8->first);
        string P__MFGR = get<1>(qCUSTOMER1_it8->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,REGIONKEY)] += qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_it10 = qCUSTOMER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_end9 = qCUSTOMER1PARTS1.end();
    for (
        ; qCUSTOMER1PARTS1_it10 != qCUSTOMER1PARTS1_end9; ++qCUSTOMER1PARTS1_it10)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1_it10->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1PARTS1_it10->first);
        qCUSTOMER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,REGIONKEY)] += 
            qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,NATIONKEY)];
    }
    qCUSTOMER2[make_tuple(NATIONKEY,REGIONKEY)] += 1;
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_it12 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_end11 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it12 != qLINEITEM1_end11; ++qLINEITEM1_it12)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it12->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,REGIONKEY)] += qLINEITEM1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_it14 = 
        qLINEITEM3.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_end13 = qLINEITEM3.end(
        );
    for (; qLINEITEM3_it14 != qLINEITEM3_end13; ++qLINEITEM3_it14)
    {
        int64_t x_qLINEITEM_L__SUPPKEY = get<0>(qLINEITEM3_it14->first);
        qLINEITEM3[make_tuple(
            x_qLINEITEM_L__SUPPKEY,REGIONKEY)] += qLINEITEM3NATION1[make_tuple(
            x_qLINEITEM_L__SUPPKEY,NATIONKEY)];
    }
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_it16 = 
        qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_end15 = qNATION1.end();
    for (; qNATION1_it16 != qNATION1_end15; ++qNATION1_it16)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it16->first);
        string P__MFGR = get<1>(qNATION1_it16->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY,REGIONKEY)] += 
            qNATION1NATION1[make_tuple(x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY)];
    }
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it18 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end17 = 
        qNATION2.end();
    for (; qNATION2_it18 != qNATION2_end17; ++qNATION2_it18)
    {
        string P__MFGR = get<0>(qNATION2_it18->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it18->first);
        qNATION2[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,REGIONKEY)] += qNATION1NATION1[make_tuple(
            NATIONKEY,P__MFGR,x_qNATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it20 = 
        qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end19 = 
        qORDERS1.end();
    for (; qORDERS1_it20 != qORDERS1_end19; ++qORDERS1_it20)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it20->first);
        string P__MFGR = get<1>(qORDERS1_it20->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,P__MFGR,REGIONKEY)] += qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1PARTS1_it22 = 
        qORDERS1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1PARTS1_end21 = 
        qORDERS1PARTS1.end();
    for (; qORDERS1PARTS1_it22 != qORDERS1PARTS1_end21; ++qORDERS1PARTS1_it22)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1PARTS1_it22->first);
        int64_t x_qORDERS1PARTS_P__PARTKEY = get<1>(qORDERS1PARTS1_it22->first);
        qORDERS1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,REGIONKEY)] += 
            qORDERS1PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_it24 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_end23 = 
        qORDERS2.end();
    for (; qORDERS2_it24 != qORDERS2_end23; ++qORDERS2_it24)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<0>(qORDERS2_it24->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,REGIONKEY)] += 
            qLINEITEM1NATION1ORDERS1[make_tuple(x_qORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_it26 = 
        qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_end25 = qPARTS1.end();
    for (; qPARTS1_it26 != qPARTS1_end25; ++qPARTS1_it26)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it26->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,REGIONKEY,REGIONKEY)] += 
            qPARTS1NATION3[make_tuple(x_qPARTS_P__PARTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_it28 = 
        qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_end27 = qPARTS1.end();
    for (; qPARTS1_it28 != qPARTS1_end27; ++qPARTS1_it28)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it28->first);
        int64_t C__NATIONKEY = get<1>(qPARTS1_it28->first);
        int64_t REGIONKEY = get<3>(qPARTS1_it28->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,REGIONKEY,REGIONKEY)] += 
            qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,C__NATIONKEY,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_it30 = 
        qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_end29 = qPARTS1.end();
    for (; qPARTS1_it30 != qPARTS1_end29; ++qPARTS1_it30)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it30->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it30->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY,REGIONKEY)] += 
            qPARTS1NATION2[make_tuple(x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it32 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end31 = qPARTS1NATION1.end();
    for (; qPARTS1NATION1_it32 != qPARTS1NATION1_end31; ++qPARTS1NATION1_it32)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1_it32->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1_it32->first);
        qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,NATIONKEY,REGIONKEY)] += 
            qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it34 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_end33 = 
        qPARTS1NATION2.end();
    for (; qPARTS1NATION2_it34 != qPARTS1NATION2_end33; ++qPARTS1NATION2_it34)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION2_it34->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION2_it34->first);
        qPARTS1NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,REGIONKEY)] += 
            qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qSUPPLIER1_it36 
        = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_end35 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it36 != qSUPPLIER1_end35; ++qSUPPLIER1_it36)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it36->first);
        string P__MFGR = get<1>(qSUPPLIER1_it36->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY,REGIONKEY)] += 
            qSUPPLIER1NATION1[make_tuple(x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_it38 = qSUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_end37 = qSUPPLIER1PARTS1.end();
    for (
        ; qSUPPLIER1PARTS1_it38 != qSUPPLIER1PARTS1_end37; ++qSUPPLIER1PARTS1_it38)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1_it38->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER1PARTS1_it38->first);
        qSUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY,REGIONKEY)] += 
            qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY)];
    }
    qSUPPLIER2[make_tuple(NATIONKEY,REGIONKEY)] += 1;
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_NATION_sec_span, on_insert_NATION_usec_span);
}

void on_insert_ORDERS(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    CUSTKEY,string ORDERSTATUS,double TOTALPRICE,string ORDERDATE,string 
    ORDERPRIORITY,string CLERK,int SHIPPRIORITY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it40 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end39 = q.end(
        );
    for (; q_it40 != q_end39; ++q_it40)
    {
        string P__MFGR = get<0>(q_it40->first);
        int64_t N2__REGIONKEY = get<1>(q_it40->first);
        int64_t N1__REGIONKEY = get<2>(q_it40->first);
        int64_t C__NATIONKEY = get<3>(q_it40->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += qORDERS1[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it42 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end41 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it42 != qCUSTOMER1_end41; ++qCUSTOMER1_it42)
    {
        string P__MFGR = get<1>(qCUSTOMER1_it42->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1_it42->first);
        qCUSTOMER1[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)] += qORDERS1[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1NATION1_it44 = qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1NATION1_end43 
        = qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it44 != qCUSTOMER1NATION1_end43; ++qCUSTOMER1NATION1_it44)
    {
        int64_t x_qCUSTOMER1NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER1NATION1_it44->first);
        string P__MFGR = get<2>(qCUSTOMER1NATION1_it44->first);
        qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,x_qCUSTOMER1NATION_N1__NATIONKEY,P__MFGR)] += 
            qORDERS1NATION1[make_tuple(ORDERKEY,x_qCUSTOMER1NATION_N1__NATIONKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_it46 = qCUSTOMER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER1PARTS1_end45 
        = qCUSTOMER1PARTS1.end();
    for (
        ; qCUSTOMER1PARTS1_it46 != qCUSTOMER1PARTS1_end45; ++qCUSTOMER1PARTS1_it46)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1_it46->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1PARTS1_it46->first);
        qCUSTOMER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)] += 
            qORDERS1PARTS1[make_tuple(ORDERKEY,x_qCUSTOMER1PARTS_P__PARTKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_it48 = qCUSTOMER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_end47 = qCUSTOMER1PARTS1NATION1.end();
    for (
        ; qCUSTOMER1PARTS1NATION1_it48 != qCUSTOMER1PARTS1NATION1_end47; 
        ++qCUSTOMER1PARTS1NATION1_it48)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1NATION1_it48->first);
        int64_t x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY = get<2>(
            qCUSTOMER1PARTS1NATION1_it48->first);
        qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,CUSTKEY,
            x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY)] += qORDERS1PARTS1NATION1[make_tuple(
            ORDERKEY,x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1SUPPLIER1_it50 = qCUSTOMER1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_end49 = qCUSTOMER1SUPPLIER1.end();
    for (
        ; qCUSTOMER1SUPPLIER1_it50 != qCUSTOMER1SUPPLIER1_end49; 
        ++qCUSTOMER1SUPPLIER1_it50)
    {
        int64_t x_qCUSTOMER1SUPPLIER_S__SUPPKEY = get<0>(
            qCUSTOMER1SUPPLIER1_it50->first);
        string P__MFGR = get<2>(qCUSTOMER1SUPPLIER1_it50->first);
        qCUSTOMER1SUPPLIER1[make_tuple(
            x_qCUSTOMER1SUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)] += 
            qORDERS1SUPPLIER1[make_tuple(ORDERKEY,x_qCUSTOMER1SUPPLIER_S__SUPPKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1SUPPLIER1PARTS1_it52 = qCUSTOMER1SUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1SUPPLIER1PARTS1_end51 = qCUSTOMER1SUPPLIER1PARTS1.end();
    for (
        ; qCUSTOMER1SUPPLIER1PARTS1_it52 != qCUSTOMER1SUPPLIER1PARTS1_end51; 
        ++qCUSTOMER1SUPPLIER1PARTS1_it52)
    {
        int64_t x_qCUSTOMER1SUPPLIER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1SUPPLIER1PARTS1_it52->first);
        int64_t x_qCUSTOMER1SUPPLIER_S__SUPPKEY = get<1>(
            qCUSTOMER1SUPPLIER1PARTS1_it52->first);
        qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER1SUPPLIER1PARTS_P__PARTKEY,x_qCUSTOMER1SUPPLIER_S__SUPPKEY,CUSTKEY)] 
            += qORDERS1SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qCUSTOMER1SUPPLIER1PARTS_P__PARTKEY,
            x_qCUSTOMER1SUPPLIER_S__SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_it54 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_end53 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it54 != qLINEITEM1_end53; ++qLINEITEM1_it54)
    {
        int64_t C__NATIONKEY = get<1>(qLINEITEM1_it54->first);
        int64_t N1__REGIONKEY = get<2>(qLINEITEM1_it54->first);
        qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)] += qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    qLINEITEM1CUSTOMER1[make_tuple(ORDERKEY,CUSTKEY)] += 1;
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_it56 = 
        qLINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_end55 = 
        qLINEITEM1NATION1.end();
    for (
        ; qLINEITEM1NATION1_it56 != qLINEITEM1NATION1_end55; ++qLINEITEM1NATION1_it56)
    {
        int64_t x_qLINEITEM1NATION_N1__NATIONKEY = get<1>(
            qLINEITEM1NATION1_it56->first);
        qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qLINEITEM1NATION_N1__NATIONKEY)] += 
            qLINEITEM1NATION1ORDERS1[make_tuple(CUSTKEY,x_qLINEITEM1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_it58 = 
        qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_end57 = qNATION1.end();
    for (; qNATION1_it58 != qNATION1_end57; ++qNATION1_it58)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it58->first);
        string P__MFGR = get<1>(qNATION1_it58->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it58->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it58->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it60 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_end59 = 
        qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it60 != qNATION1NATION1_end59; ++qNATION1NATION1_it60)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1NATION1_it60->first);
        string P__MFGR = get<1>(qNATION1NATION1_it60->first);
        int64_t x_qNATION1NATION_N1__NATIONKEY = get<2>(
            qNATION1NATION1_it60->first);
        qNATION1NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION1NATION_N1__NATIONKEY)] += 
            qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qNATION1NATION_N1__NATIONKEY)];
    }
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it62 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end61 = 
        qNATION2.end();
    for (; qNATION2_it62 != qNATION2_end61; ++qNATION2_it62)
    {
        string P__MFGR = get<0>(qNATION2_it62->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it62->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it62->first);
        qNATION2[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += qORDERS1[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,string>,double>::iterator qNATION3_it64 = qNATION3.begin(
        );
    map<tuple<int64_t,string>,double>::iterator qNATION3_end63 = qNATION3.end();
    for (; qNATION3_it64 != qNATION3_end63; ++qNATION3_it64)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION3_it64->first);
        string P__MFGR = get<1>(qNATION3_it64->first);
        qNATION3[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR)] += qNATION3ORDERS1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR,CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_it66 = 
        qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_end65 = qPARTS1.end();
    for (; qPARTS1_it66 != qPARTS1_end65; ++qPARTS1_it66)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it66->first);
        int64_t C__NATIONKEY = get<1>(qPARTS1_it66->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it66->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1_it66->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            qORDERS1PARTS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,N2__REGIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it68 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end67 = qPARTS1NATION1.end();
    for (; qPARTS1NATION1_it68 != qPARTS1NATION1_end67; ++qPARTS1NATION1_it68)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1_it68->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1_it68->first);
        int64_t C__NATIONKEY = get<2>(qPARTS1NATION1_it68->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1NATION1_it68->first);
        qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] 
            += qORDERS1PARTS1NATION1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            x_qPARTS1NATION_N1__NATIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_it70 = qPARTS1NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_end69 = qPARTS1NATION1NATION1.end();
    for (
        ; qPARTS1NATION1NATION1_it70 != qPARTS1NATION1NATION1_end69; 
        ++qPARTS1NATION1NATION1_it70)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1NATION1_it70->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1NATION1_it70->first);
        int64_t x_qPARTS1NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS1NATION1NATION1_it70->first);
        qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,
            x_qPARTS1NATION1NATION_N1__NATIONKEY)] += qORDERS1PARTS1NATION1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            x_qPARTS1NATION_N1__NATIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it72 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_end71 = 
        qPARTS1NATION2.end();
    for (; qPARTS1NATION2_it72 != qPARTS1NATION2_end71; ++qPARTS1NATION2_it72)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION2_it72->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION2_it72->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1NATION2_it72->first);
        qPARTS1NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            qORDERS1PARTS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            N2__REGIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_it74 = 
        qPARTS1NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_end73 = 
        qPARTS1NATION3.end();
    for (; qPARTS1NATION3_it74 != qPARTS1NATION3_end73; ++qPARTS1NATION3_it74)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION3_it74->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION3_it74->first);
        qPARTS1NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY)] += 
            qPARTS1NATION3ORDERS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_it76 = qPARTS1NATION3LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_end75 = qPARTS1NATION3LINEITEM1.end();
    for (
        ; qPARTS1NATION3LINEITEM1_it76 != qPARTS1NATION3LINEITEM1_end75; 
        ++qPARTS1NATION3LINEITEM1_it76)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION3LINEITEM1_it76->first);
        int64_t x_qPARTS1NATION3LINEITEM_L__SUPPKEY = get<2>(
            qPARTS1NATION3LINEITEM1_it76->first);
        qPARTS1NATION3LINEITEM1[make_tuple(
            ORDERKEY,x_qPARTS1NATION_N1__NATIONKEY,x_qPARTS1NATION3LINEITEM_L__SUPPKEY)] += 
            qPARTS1NATION3LINEITEM1ORDERS1[make_tuple(
            CUSTKEY,x_qPARTS1NATION_N1__NATIONKEY,x_qPARTS1NATION3LINEITEM_L__SUPPKEY)];
    }
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qSUPPLIER1_it78 
        = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_end77 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it78 != qSUPPLIER1_end77; ++qSUPPLIER1_it78)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it78->first);
        string P__MFGR = get<1>(qSUPPLIER1_it78->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1_it78->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1_it78->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            qORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY,P__MFGR)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER1NATION1_it80 = qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER1NATION1_end79 
        = qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it80 != qSUPPLIER1NATION1_end79; ++qSUPPLIER1NATION1_it80)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1NATION1_it80->first);
        string P__MFGR = get<1>(qSUPPLIER1NATION1_it80->first);
        int64_t x_qSUPPLIER1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1NATION1_it80->first);
        qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,x_qSUPPLIER1NATION_N1__NATIONKEY)] += 
            qORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY,P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qSUPPLIER1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_it82 = qSUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_end81 = qSUPPLIER1PARTS1.end();
    for (
        ; qSUPPLIER1PARTS1_it82 != qSUPPLIER1PARTS1_end81; ++qSUPPLIER1PARTS1_it82)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1_it82->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER1PARTS1_it82->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1PARTS1_it82->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1PARTS1_it82->first);
        qSUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,C__NATIONKEY,
            N1__REGIONKEY)] += qORDERS1SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qSUPPLIER1PARTS_P__PARTKEY,
            x_qSUPPLIER_S__SUPPKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_it84 = qSUPPLIER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_end83 = qSUPPLIER1PARTS1NATION1.end();
    for (
        ; qSUPPLIER1PARTS1NATION1_it84 != qSUPPLIER1PARTS1NATION1_end83; 
        ++qSUPPLIER1PARTS1NATION1_it84)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1NATION1_it84->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(
            qSUPPLIER1PARTS1NATION1_it84->first);
        int64_t x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1PARTS1NATION1_it84->first);
        qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,
            x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY)] += qORDERS1SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qSUPPLIER1PARTS_P__PARTKEY,
            x_qSUPPLIER_S__SUPPKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY)];
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_ORDERS_sec_span, on_insert_ORDERS_usec_span);
}

void on_insert_LINEITEM(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    PARTKEY,int64_t SUPPKEY,int LINENUMBER,double QUANTITY,double 
    EXTENDEDPRICE,double DISCOUNT,double TAX)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it86 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end85 = q.end(
        );
    for (; q_it86 != q_end85; ++q_it86)
    {
        string P__MFGR = get<0>(q_it86->first);
        int64_t N2__REGIONKEY = get<1>(q_it86->first);
        int64_t N1__REGIONKEY = get<2>(q_it86->first);
        int64_t C__NATIONKEY = get<3>(q_it86->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it88 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end87 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it88 != qCUSTOMER1_end87; ++qCUSTOMER1_it88)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it88->first);
        string P__MFGR = get<1>(qCUSTOMER1_it88->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1_it88->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,N2__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1NATION1_it90 = qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1NATION1_end89 
        = qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it90 != qCUSTOMER1NATION1_end89; ++qCUSTOMER1NATION1_it90)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1NATION1_it90->first);
        int64_t x_qCUSTOMER1NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER1NATION1_it90->first);
        string P__MFGR = get<2>(qCUSTOMER1NATION1_it90->first);
        qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1NATION_N1__NATIONKEY,P__MFGR)] += 
            EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qCUSTOMER1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_it92 = qCUSTOMER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER1PARTS1_end91 
        = qCUSTOMER1PARTS1.end();
    for (
        ; qCUSTOMER1PARTS1_it92 != qCUSTOMER1PARTS1_end91; ++qCUSTOMER1PARTS1_it92)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1PARTS1_it92->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1PARTS1_it92->first);
        qCUSTOMER1PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_it94 = qCUSTOMER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_end93 = qCUSTOMER1PARTS1NATION1.end();
    for (
        ; qCUSTOMER1PARTS1NATION1_it94 != qCUSTOMER1PARTS1NATION1_end93; 
        ++qCUSTOMER1PARTS1NATION1_it94)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1PARTS1NATION1_it94->first);
        int64_t x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY = get<2>(
            qCUSTOMER1PARTS1NATION1_it94->first);
        qCUSTOMER1PARTS1NATION1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1SUPPLIER1_it96 = qCUSTOMER1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_end95 = qCUSTOMER1SUPPLIER1.end();
    for (
        ; qCUSTOMER1SUPPLIER1_it96 != qCUSTOMER1SUPPLIER1_end95; 
        ++qCUSTOMER1SUPPLIER1_it96)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1SUPPLIER1_it96->first);
        string P__MFGR = get<2>(qCUSTOMER1SUPPLIER1_it96->first);
        qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)] += 
            EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(PARTKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1SUPPLIER1PARTS1_it98 = qCUSTOMER1SUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1SUPPLIER1PARTS1_end97 = qCUSTOMER1SUPPLIER1PARTS1.end();
    for (
        ; qCUSTOMER1SUPPLIER1PARTS1_it98 != qCUSTOMER1SUPPLIER1PARTS1_end97; 
        ++qCUSTOMER1SUPPLIER1PARTS1_it98)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<2>(
            qCUSTOMER1SUPPLIER1PARTS1_it98->first);
        qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            PARTKEY,SUPPKEY,x_qCUSTOMER_C__CUSTKEY)] += 
            EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(ORDERKEY,x_qCUSTOMER_C__CUSTKEY)];
    }
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it100 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_end99 = qNATION1.end();
    for (; qNATION1_it100 != qNATION1_end99; ++qNATION1_it100)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it100->first);
        string P__MFGR = get<1>(qNATION1_it100->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it100->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it100->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it102 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end101 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it102 != qNATION1NATION1_end101; ++qNATION1NATION1_it102)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1NATION1_it102->first);
        string P__MFGR = get<1>(qNATION1NATION1_it102->first);
        int64_t x_qNATION1NATION_N1__NATIONKEY = get<2>(
            qNATION1NATION1_it102->first);
        qNATION1NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qNATION1NATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY)];
    }
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it104 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end103 = 
        qNATION2.end();
    for (; qNATION2_it104 != qNATION2_end103; ++qNATION2_it104)
    {
        string P__MFGR = get<0>(qNATION2_it104->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it104->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it104->first);
        qNATION2[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,string>,double>::iterator qNATION3_it106 = qNATION3.begin(
        );
    map<tuple<int64_t,string>,double>::iterator qNATION3_end105 = qNATION3.end(
        );
    for (; qNATION3_it106 != qNATION3_end105; ++qNATION3_it106)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION3_it106->first);
        string P__MFGR = get<1>(qNATION3_it106->first);
        qNATION3[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR)] += 
            EXTENDEDPRICE*qPARTS1NATION3LINEITEM1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,SUPPKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_it108 = qNATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_end107 = qNATION3ORDERS1.end();
    for (
        ; qNATION3ORDERS1_it108 != qNATION3ORDERS1_end107; ++qNATION3ORDERS1_it108)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION3ORDERS1_it108->first);
        string P__MFGR = get<2>(qNATION3ORDERS1_it108->first);
        int64_t x_qNATION3ORDERS_O__CUSTKEY = get<3>(
            qNATION3ORDERS1_it108->first);
        qNATION3ORDERS1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION3ORDERS_O__CUSTKEY)] += 
            EXTENDEDPRICE*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY,x_qNATION3ORDERS_O__CUSTKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it110 = 
        qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end109 = 
        qORDERS1.end();
    for (; qORDERS1_it110 != qORDERS1_end109; ++qORDERS1_it110)
    {
        string P__MFGR = get<1>(qORDERS1_it110->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1_it110->first);
        qORDERS1[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)] += EXTENDEDPRICE*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1NATION1_it112 = 
        qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1NATION1_end111 = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it112 != qORDERS1NATION1_end111; ++qORDERS1NATION1_it112)
    {
        int64_t x_qORDERS1NATION_N1__NATIONKEY = get<1>(
            qORDERS1NATION1_it112->first);
        string P__MFGR = get<2>(qORDERS1NATION1_it112->first);
        qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qORDERS1NATION_N1__NATIONKEY,P__MFGR)] += 
            EXTENDEDPRICE*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qORDERS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1PARTS1_it114 = 
        qORDERS1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS1PARTS1_end113 = qORDERS1PARTS1.end();
    for (
        ; qORDERS1PARTS1_it114 != qORDERS1PARTS1_end113; ++qORDERS1PARTS1_it114)
    {
        int64_t N2__REGIONKEY = get<2>(qORDERS1PARTS1_it114->first);
        qORDERS1PARTS1[make_tuple(
            ORDERKEY,PARTKEY,N2__REGIONKEY)] += EXTENDEDPRICE*qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS1PARTS1NATION1_it116 = qORDERS1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS1PARTS1NATION1_end115 = qORDERS1PARTS1NATION1.end();
    for (
        ; qORDERS1PARTS1NATION1_it116 != qORDERS1PARTS1NATION1_end115; 
        ++qORDERS1PARTS1NATION1_it116)
    {
        int64_t x_qORDERS1PARTS1NATION_N1__NATIONKEY = get<2>(
            qORDERS1PARTS1NATION1_it116->first);
        qORDERS1PARTS1NATION1[make_tuple(
            ORDERKEY,PARTKEY,x_qORDERS1PARTS1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qORDERS1PARTS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1SUPPLIER1_it118 
        = qORDERS1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1SUPPLIER1_end117 = qORDERS1SUPPLIER1.end();
    for (
        ; qORDERS1SUPPLIER1_it118 != qORDERS1SUPPLIER1_end117; 
        ++qORDERS1SUPPLIER1_it118)
    {
        string P__MFGR = get<2>(qORDERS1SUPPLIER1_it118->first);
        qORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,SUPPKEY,P__MFGR)] += EXTENDEDPRICE*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }
    qORDERS1SUPPLIER1PARTS1[make_tuple(
        ORDERKEY,PARTKEY,SUPPKEY)] += EXTENDEDPRICE;
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it120 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end119 
        = qPARTS1.end();
    for (; qPARTS1_it120 != qPARTS1_end119; ++qPARTS1_it120)
    {
        int64_t C__NATIONKEY = get<1>(qPARTS1_it120->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it120->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1_it120->first);
        qPARTS1[make_tuple(
            PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it122 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end121 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it122 != qPARTS1NATION1_end121; ++qPARTS1NATION1_it122)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1_it122->first);
        int64_t C__NATIONKEY = get<2>(qPARTS1NATION1_it122->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1NATION1_it122->first);
        qPARTS1NATION1[make_tuple(
            PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_it124 = qPARTS1NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_end123 = qPARTS1NATION1NATION1.end();
    for (
        ; qPARTS1NATION1NATION1_it124 != qPARTS1NATION1NATION1_end123; 
        ++qPARTS1NATION1NATION1_it124)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1NATION1_it124->first);
        int64_t x_qPARTS1NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS1NATION1NATION1_it124->first);
        qPARTS1NATION1NATION1[make_tuple(
            PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it126 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end125 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it126 != qPARTS1NATION2_end125; ++qPARTS1NATION2_it126)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION2_it126->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1NATION2_it126->first);
        qPARTS1NATION2[make_tuple(
            PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qPARTS1NATION_N1__NATIONKEY)]*qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_it128 = 
        qPARTS1NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_end127 = 
        qPARTS1NATION3.end();
    for (
        ; qPARTS1NATION3_it128 != qPARTS1NATION3_end127; ++qPARTS1NATION3_it128)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION3_it128->first);
        qPARTS1NATION3[make_tuple(
            PARTKEY,x_qPARTS1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*qPARTS1NATION3LINEITEM1[make_tuple(
            ORDERKEY,x_qPARTS1NATION_N1__NATIONKEY,SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_it130 = qPARTS1NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_end129 = qPARTS1NATION3ORDERS1.end();
    for (
        ; qPARTS1NATION3ORDERS1_it130 != qPARTS1NATION3ORDERS1_end129; 
        ++qPARTS1NATION3ORDERS1_it130)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<2>(
            qPARTS1NATION3ORDERS1_it130->first);
        int64_t x_qPARTS1NATION3ORDERS_O__CUSTKEY = get<3>(
            qPARTS1NATION3ORDERS1_it130->first);
        qPARTS1NATION3ORDERS1[make_tuple(
            ORDERKEY,PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,
            x_qPARTS1NATION3ORDERS_O__CUSTKEY)] += 
            EXTENDEDPRICE*qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qPARTS1NATION_N1__NATIONKEY,x_qPARTS1NATION3ORDERS_O__CUSTKEY)];
    }
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it132 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end131 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it132 != qSUPPLIER1_end131; ++qSUPPLIER1_it132)
    {
        string P__MFGR = get<1>(qSUPPLIER1_it132->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1_it132->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1_it132->first);
        qSUPPLIER1[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(PARTKEY,P__MFGR)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER1NATION1_it134 
        = qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER1NATION1_end133 = qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it134 != qSUPPLIER1NATION1_end133; 
        ++qSUPPLIER1NATION1_it134)
    {
        string P__MFGR = get<1>(qSUPPLIER1NATION1_it134->first);
        int64_t x_qSUPPLIER1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1NATION1_it134->first);
        qSUPPLIER1NATION1[make_tuple(
            SUPPKEY,P__MFGR,x_qSUPPLIER1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qSUPPLIER1NATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_it136 = qSUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_end135 = qSUPPLIER1PARTS1.end();
    for (
        ; qSUPPLIER1PARTS1_it136 != qSUPPLIER1PARTS1_end135; ++qSUPPLIER1PARTS1_it136)
    {
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1PARTS1_it136->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1PARTS1_it136->first);
        qSUPPLIER1PARTS1[make_tuple(
            PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1[make_tuple(ORDERKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_it138 = qSUPPLIER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_end137 = qSUPPLIER1PARTS1NATION1.end();
    for (
        ; qSUPPLIER1PARTS1NATION1_it138 != qSUPPLIER1PARTS1NATION1_end137; 
        ++qSUPPLIER1PARTS1NATION1_it138)
    {
        int64_t x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1PARTS1NATION1_it138->first);
        qSUPPLIER1PARTS1NATION1[make_tuple(
            PARTKEY,SUPPKEY,x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY)];
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
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it140 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end139 = 
        q.end();
    for (; q_it140 != q_end139; ++q_it140)
    {
        string P__MFGR = get<0>(q_it140->first);
        int64_t N2__REGIONKEY = get<1>(q_it140->first);
        int64_t N1__REGIONKEY = get<2>(q_it140->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,NATIONKEY)] += qCUSTOMER1[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)]*qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_it142 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_end141 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it142 != qLINEITEM1_end141; ++qLINEITEM1_it142)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it142->first);
        int64_t N1__REGIONKEY = get<2>(qLINEITEM1_it142->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,N1__REGIONKEY)] += 
            qLINEITEM1CUSTOMER1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_it144 = 
        qLINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_end143 = 
        qLINEITEM1NATION1.end();
    for (
        ; qLINEITEM1NATION1_it144 != qLINEITEM1NATION1_end143; 
        ++qLINEITEM1NATION1_it144)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1NATION1_it144->first);
        qLINEITEM1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY)] += qLINEITEM1CUSTOMER1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)];
    }
    qLINEITEM1NATION1ORDERS1[make_tuple(CUSTKEY,NATIONKEY)] += 1;
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it146 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end145 
        = qNATION1.end();
    for (; qNATION1_it146 != qNATION1_end145; ++qNATION1_it146)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it146->first);
        string P__MFGR = get<1>(qNATION1_it146->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it146->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY,N1__REGIONKEY)] += 
            qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it148 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end147 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it148 != qNATION1NATION1_end147; ++qNATION1NATION1_it148)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1NATION1_it148->first);
        string P__MFGR = get<1>(qNATION1NATION1_it148->first);
        qNATION1NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY)] += qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY,P__MFGR)];
    }
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it150 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end149 = 
        qNATION2.end();
    for (; qNATION2_it150 != qNATION2_end149; ++qNATION2_it150)
    {
        string P__MFGR = get<0>(qNATION2_it150->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it150->first);
        qNATION2[make_tuple(
            P__MFGR,NATIONKEY,N2__REGIONKEY)] += qCUSTOMER1[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)];
    }
    map<tuple<int64_t,string>,double>::iterator qNATION3_it152 = qNATION3.begin(
        );
    map<tuple<int64_t,string>,double>::iterator qNATION3_end151 = qNATION3.end(
        );
    for (; qNATION3_it152 != qNATION3_end151; ++qNATION3_it152)
    {
        string P__MFGR = get<1>(qNATION3_it152->first);
        qNATION3[make_tuple(NATIONKEY,P__MFGR)] += qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,NATIONKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_it154 = qNATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_end153 = qNATION3ORDERS1.end();
    for (
        ; qNATION3ORDERS1_it154 != qNATION3ORDERS1_end153; ++qNATION3ORDERS1_it154)
    {
        int64_t x_qNATION3ORDERS_O__ORDERKEY = get<0>(
            qNATION3ORDERS1_it154->first);
        string P__MFGR = get<2>(qNATION3ORDERS1_it154->first);
        qNATION3ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR,CUSTKEY)] += 
            qORDERS1NATION1[make_tuple(x_qNATION3ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_it156 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_end155 = 
        qORDERS2.end();
    for (; qORDERS2_it156 != qORDERS2_end155; ++qORDERS2_it156)
    {
        int64_t N1__REGIONKEY = get<2>(qORDERS2_it156->first);
        qORDERS2[make_tuple(
            CUSTKEY,NATIONKEY,N1__REGIONKEY)] += qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it158 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end157 
        = qPARTS1.end();
    for (; qPARTS1_it158 != qPARTS1_end157; ++qPARTS1_it158)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it158->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it158->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1_it158->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            qCUSTOMER1PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it160 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end159 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it160 != qPARTS1NATION1_end159; ++qPARTS1NATION1_it160)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1_it160->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1_it160->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1NATION1_it160->first);
        qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,NATIONKEY,N1__REGIONKEY)] += 
            qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,
            x_qPARTS1NATION_N1__NATIONKEY)]*qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_it162 = qPARTS1NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_end161 = qPARTS1NATION1NATION1.end();
    for (
        ; qPARTS1NATION1NATION1_it162 != qPARTS1NATION1NATION1_end161; 
        ++qPARTS1NATION1NATION1_it162)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS1NATION1NATION1_it162->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1NATION1_it162->first);
        qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,NATIONKEY)] += 
            qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it164 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end163 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it164 != qPARTS1NATION2_end163; ++qPARTS1NATION2_it164)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION2_it164->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1NATION2_it164->first);
        qPARTS1NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY)] += qCUSTOMER1PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_it166 = 
        qPARTS1NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_end165 = 
        qPARTS1NATION3.end();
    for (
        ; qPARTS1NATION3_it166 != qPARTS1NATION3_end165; ++qPARTS1NATION3_it166)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION3_it166->first);
        qPARTS1NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY)] += qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_it168 = qPARTS1NATION3LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_end167 = qPARTS1NATION3LINEITEM1.end();
    for (
        ; qPARTS1NATION3LINEITEM1_it168 != qPARTS1NATION3LINEITEM1_end167; 
        ++qPARTS1NATION3LINEITEM1_it168)
    {
        int64_t x_qPARTS1NATION3LINEITEM_L__ORDERKEY = get<0>(
            qPARTS1NATION3LINEITEM1_it168->first);
        int64_t x_qPARTS1NATION3LINEITEM_L__SUPPKEY = get<2>(
            qPARTS1NATION3LINEITEM1_it168->first);
        qPARTS1NATION3LINEITEM1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__ORDERKEY,NATIONKEY,
            x_qPARTS1NATION3LINEITEM_L__SUPPKEY)] += qLINEITEM1CUSTOMER1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__ORDERKEY,CUSTKEY)]*qLINEITEM3NATION1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__SUPPKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1ORDERS1_it170 = qPARTS1NATION3LINEITEM1ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1ORDERS1_end169 = qPARTS1NATION3LINEITEM1ORDERS1.end();
    for (
        ; qPARTS1NATION3LINEITEM1ORDERS1_it170 != qPARTS1NATION3LINEITEM1ORDERS1_end169; 
        ++qPARTS1NATION3LINEITEM1ORDERS1_it170)
    {
        int64_t x_qPARTS1NATION3LINEITEM_L__SUPPKEY = get<2>(
            qPARTS1NATION3LINEITEM1ORDERS1_it170->first);
        qPARTS1NATION3LINEITEM1ORDERS1[make_tuple(
            CUSTKEY,NATIONKEY,x_qPARTS1NATION3LINEITEM_L__SUPPKEY)] += 
            qLINEITEM3NATION1[make_tuple(x_qPARTS1NATION3LINEITEM_L__SUPPKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_it172 = qPARTS1NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_end171 = qPARTS1NATION3ORDERS1.end();
    for (
        ; qPARTS1NATION3ORDERS1_it172 != qPARTS1NATION3ORDERS1_end171; 
        ++qPARTS1NATION3ORDERS1_it172)
    {
        int64_t x_qPARTS1NATION3ORDERS_O__ORDERKEY = get<0>(
            qPARTS1NATION3ORDERS1_it172->first);
        int64_t x_qPARTS_P__PARTKEY = get<1>(
            qPARTS1NATION3ORDERS1_it172->first);
        qPARTS1NATION3ORDERS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY,CUSTKEY)] += 
            qORDERS1PARTS1NATION1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3ORDERS1LINEITEM1_it174 = qPARTS1NATION3ORDERS1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3ORDERS1LINEITEM1_end173 = qPARTS1NATION3ORDERS1LINEITEM1.end();
    for (
        ; qPARTS1NATION3ORDERS1LINEITEM1_it174 != qPARTS1NATION3ORDERS1LINEITEM1_end173; 
        ++qPARTS1NATION3ORDERS1LINEITEM1_it174)
    {
        int64_t x_qPARTS1NATION3ORDERS1LINEITEM_L__SUPPKEY = get<0>(
            qPARTS1NATION3ORDERS1LINEITEM1_it174->first);
        qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            x_qPARTS1NATION3ORDERS1LINEITEM_L__SUPPKEY,NATIONKEY,CUSTKEY)] += 
            qLINEITEM3NATION1[make_tuple(
            x_qPARTS1NATION3ORDERS1LINEITEM_L__SUPPKEY,NATIONKEY)];
    }
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it176 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end175 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it176 != qSUPPLIER1_end175; ++qSUPPLIER1_it176)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it176->first);
        string P__MFGR = get<1>(qSUPPLIER1_it176->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1_it176->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY,N1__REGIONKEY)] += 
            qCUSTOMER1SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER1NATION1_it178 
        = qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER1NATION1_end177 = qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it178 != qSUPPLIER1NATION1_end177; 
        ++qSUPPLIER1NATION1_it178)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1NATION1_it178->first);
        string P__MFGR = get<1>(qSUPPLIER1NATION1_it178->first);
        qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY)] += qCUSTOMER1SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_it180 = qSUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_end179 = qSUPPLIER1PARTS1.end();
    for (
        ; qSUPPLIER1PARTS1_it180 != qSUPPLIER1PARTS1_end179; ++qSUPPLIER1PARTS1_it180)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1_it180->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER1PARTS1_it180->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1PARTS1_it180->first);
        qSUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY,
            N1__REGIONKEY)] += qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,
            CUSTKEY)]*qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_it182 = qSUPPLIER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_end181 = qSUPPLIER1PARTS1NATION1.end();
    for (
        ; qSUPPLIER1PARTS1NATION1_it182 != qSUPPLIER1PARTS1NATION1_end181; 
        ++qSUPPLIER1PARTS1NATION1_it182)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1NATION1_it182->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(
            qSUPPLIER1PARTS1NATION1_it182->first);
        qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY)] += 
            qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,CUSTKEY)];
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_CUSTOMER_sec_span, on_insert_CUSTOMER_usec_span);
}

void on_insert_SUPPLIER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t SUPPKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it184 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end183 = 
        q.end();
    for (; q_it184 != q_end183; ++q_it184)
    {
        string P__MFGR = get<0>(q_it184->first);
        int64_t N2__REGIONKEY = get<1>(q_it184->first);
        int64_t N1__REGIONKEY = get<2>(q_it184->first);
        int64_t C__NATIONKEY = get<3>(q_it184->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += qSUPPLIER1[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it186 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end185 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it186 != qCUSTOMER1_end185; ++qCUSTOMER1_it186)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it186->first);
        string P__MFGR = get<1>(qCUSTOMER1_it186->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1_it186->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,N2__REGIONKEY)] += 
            qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1NATION1_it188 
        = qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1NATION1_end187 = qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it188 != qCUSTOMER1NATION1_end187; 
        ++qCUSTOMER1NATION1_it188)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1NATION1_it188->first);
        string P__MFGR = get<2>(qCUSTOMER1NATION1_it188->first);
        qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,P__MFGR)] += qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER1PARTS1_it190 
        = qCUSTOMER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_end189 = qCUSTOMER1PARTS1.end();
    for (
        ; qCUSTOMER1PARTS1_it190 != qCUSTOMER1PARTS1_end189; ++qCUSTOMER1PARTS1_it190)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1_it190->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1PARTS1_it190->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1PARTS1_it190->first);
        qCUSTOMER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)] += 
            qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,SUPPKEY,
            x_qCUSTOMER_C__CUSTKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_it192 = qCUSTOMER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_end191 = qCUSTOMER1PARTS1NATION1.end();
    for (
        ; qCUSTOMER1PARTS1NATION1_it192 != qCUSTOMER1PARTS1NATION1_end191; 
        ++qCUSTOMER1PARTS1NATION1_it192)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1NATION1_it192->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1PARTS1NATION1_it192->first);
        qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,NATIONKEY)] += 
            qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,SUPPKEY,x_qCUSTOMER_C__CUSTKEY)];
    }
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_it194 = 
        qLINEITEM3.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_end193 = 
        qLINEITEM3.end();
    for (; qLINEITEM3_it194 != qLINEITEM3_end193; ++qLINEITEM3_it194)
    {
        int64_t N2__REGIONKEY = get<1>(qLINEITEM3_it194->first);
        qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)] += qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }
    qLINEITEM3NATION1[make_tuple(SUPPKEY,NATIONKEY)] += 1;
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it196 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end195 
        = qNATION1.end();
    for (; qNATION1_it196 != qNATION1_end195; ++qNATION1_it196)
    {
        string P__MFGR = get<1>(qNATION1_it196->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it196->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it196->first);
        qNATION1[make_tuple(
            NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += qSUPPLIER1[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it198 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end197 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it198 != qNATION1NATION1_end197; ++qNATION1NATION1_it198)
    {
        string P__MFGR = get<1>(qNATION1NATION1_it198->first);
        int64_t x_qNATION1NATION_N1__NATIONKEY = get<2>(
            qNATION1NATION1_it198->first);
        qNATION1NATION1[make_tuple(
            NATIONKEY,P__MFGR,x_qNATION1NATION_N1__NATIONKEY)] += 
            qSUPPLIER1NATION1[make_tuple(SUPPKEY,P__MFGR,x_qNATION1NATION_N1__NATIONKEY)];
    }
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it200 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end199 = 
        qNATION2.end();
    for (; qNATION2_it200 != qNATION2_end199; ++qNATION2_it200)
    {
        string P__MFGR = get<0>(qNATION2_it200->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it200->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it200->first);
        qNATION2[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += qSUPPLIER1NATION1[make_tuple(
            SUPPKEY,P__MFGR,x_qNATION_N1__NATIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,string>,double>::iterator qNATION3_it202 = qNATION3.begin(
        );
    map<tuple<int64_t,string>,double>::iterator qNATION3_end201 = qNATION3.end(
        );
    for (; qNATION3_it202 != qNATION3_end201; ++qNATION3_it202)
    {
        string P__MFGR = get<1>(qNATION3_it202->first);
        qNATION3[make_tuple(NATIONKEY,P__MFGR)] += qSUPPLIER1NATION1[make_tuple(
            SUPPKEY,P__MFGR,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_it204 = qNATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_end203 = qNATION3ORDERS1.end();
    for (
        ; qNATION3ORDERS1_it204 != qNATION3ORDERS1_end203; ++qNATION3ORDERS1_it204)
    {
        int64_t x_qNATION3ORDERS_O__ORDERKEY = get<0>(
            qNATION3ORDERS1_it204->first);
        string P__MFGR = get<2>(qNATION3ORDERS1_it204->first);
        int64_t x_qNATION3ORDERS_O__CUSTKEY = get<3>(
            qNATION3ORDERS1_it204->first);
        qNATION3ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR,x_qNATION3ORDERS_O__CUSTKEY)] += 
            qORDERS1SUPPLIER1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,SUPPKEY,
            P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it206 = 
        qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end205 = 
        qORDERS1.end();
    for (; qORDERS1_it206 != qORDERS1_end205; ++qORDERS1_it206)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it206->first);
        string P__MFGR = get<1>(qORDERS1_it206->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1_it206->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,P__MFGR,N2__REGIONKEY)] += qORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY,P__MFGR)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1NATION1_it208 = 
        qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1NATION1_end207 = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it208 != qORDERS1NATION1_end207; ++qORDERS1NATION1_it208)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1NATION1_it208->first);
        string P__MFGR = get<2>(qORDERS1NATION1_it208->first);
        qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)] += qORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1PARTS1_it210 = 
        qORDERS1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS1PARTS1_end209 = qORDERS1PARTS1.end();
    for (
        ; qORDERS1PARTS1_it210 != qORDERS1PARTS1_end209; ++qORDERS1PARTS1_it210)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1PARTS1_it210->first);
        int64_t x_qORDERS1PARTS_P__PARTKEY = get<1>(
            qORDERS1PARTS1_it210->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1PARTS1_it210->first);
        qORDERS1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,N2__REGIONKEY)] += 
            qORDERS1SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,
            SUPPKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS1PARTS1NATION1_it212 = qORDERS1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS1PARTS1NATION1_end211 = qORDERS1PARTS1NATION1.end();
    for (
        ; qORDERS1PARTS1NATION1_it212 != qORDERS1PARTS1NATION1_end211; 
        ++qORDERS1PARTS1NATION1_it212)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(
            qORDERS1PARTS1NATION1_it212->first);
        int64_t x_qORDERS1PARTS_P__PARTKEY = get<1>(
            qORDERS1PARTS1NATION1_it212->first);
        qORDERS1PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,NATIONKEY)] += 
            qORDERS1SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it214 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end213 
        = qPARTS1.end();
    for (; qPARTS1_it214 != qPARTS1_end213; ++qPARTS1_it214)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it214->first);
        int64_t C__NATIONKEY = get<1>(qPARTS1_it214->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it214->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1_it214->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            qSUPPLIER1PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it216 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end215 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it216 != qPARTS1NATION1_end215; ++qPARTS1NATION1_it216)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1_it216->first);
        int64_t C__NATIONKEY = get<2>(qPARTS1NATION1_it216->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1NATION1_it216->first);
        qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            qSUPPLIER1PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_it218 = qPARTS1NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_end217 = qPARTS1NATION1NATION1.end();
    for (
        ; qPARTS1NATION1NATION1_it218 != qPARTS1NATION1NATION1_end217; 
        ++qPARTS1NATION1NATION1_it218)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS1NATION1NATION1_it218->first);
        int64_t x_qPARTS1NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS1NATION1NATION1_it218->first);
        qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)] += 
            qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it220 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end219 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it220 != qPARTS1NATION2_end219; ++qPARTS1NATION2_it220)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION2_it220->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION2_it220->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1NATION2_it220->first);
        qPARTS1NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,
            x_qPARTS1NATION_N1__NATIONKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_it222 = 
        qPARTS1NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_end221 = 
        qPARTS1NATION3.end();
    for (
        ; qPARTS1NATION3_it222 != qPARTS1NATION3_end221; ++qPARTS1NATION3_it222)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION3_it222->first);
        qPARTS1NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY)] += qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_it224 = qPARTS1NATION3LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_end223 = qPARTS1NATION3LINEITEM1.end();
    for (
        ; qPARTS1NATION3LINEITEM1_it224 != qPARTS1NATION3LINEITEM1_end223; 
        ++qPARTS1NATION3LINEITEM1_it224)
    {
        int64_t x_qPARTS1NATION3LINEITEM_L__ORDERKEY = get<0>(
            qPARTS1NATION3LINEITEM1_it224->first);
        qPARTS1NATION3LINEITEM1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__ORDERKEY,NATIONKEY,SUPPKEY)] += 
            qLINEITEM1NATION1[make_tuple(x_qPARTS1NATION3LINEITEM_L__ORDERKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1ORDERS1_it226 = qPARTS1NATION3LINEITEM1ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1ORDERS1_end225 = qPARTS1NATION3LINEITEM1ORDERS1.end();
    for (
        ; qPARTS1NATION3LINEITEM1ORDERS1_it226 != qPARTS1NATION3LINEITEM1ORDERS1_end225; 
        ++qPARTS1NATION3LINEITEM1ORDERS1_it226)
    {
        int64_t x_qPARTS1NATION3LINEITEM1ORDERS_O__CUSTKEY = get<0>(
            qPARTS1NATION3LINEITEM1ORDERS1_it226->first);
        qPARTS1NATION3LINEITEM1ORDERS1[make_tuple(
            x_qPARTS1NATION3LINEITEM1ORDERS_O__CUSTKEY,NATIONKEY,SUPPKEY)] += 
            qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qPARTS1NATION3LINEITEM1ORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_it228 = qPARTS1NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_end227 = qPARTS1NATION3ORDERS1.end();
    for (
        ; qPARTS1NATION3ORDERS1_it228 != qPARTS1NATION3ORDERS1_end227; 
        ++qPARTS1NATION3ORDERS1_it228)
    {
        int64_t x_qPARTS1NATION3ORDERS_O__ORDERKEY = get<0>(
            qPARTS1NATION3ORDERS1_it228->first);
        int64_t x_qPARTS_P__PARTKEY = get<1>(
            qPARTS1NATION3ORDERS1_it228->first);
        int64_t x_qPARTS1NATION3ORDERS_O__CUSTKEY = get<3>(
            qPARTS1NATION3ORDERS1_it228->first);
        qPARTS1NATION3ORDERS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY,
            x_qPARTS1NATION3ORDERS_O__CUSTKEY)] += qORDERS1SUPPLIER1PARTS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,
            SUPPKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3ORDERS1LINEITEM1_it230 = qPARTS1NATION3ORDERS1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3ORDERS1LINEITEM1_end229 = qPARTS1NATION3ORDERS1LINEITEM1.end();
    for (
        ; qPARTS1NATION3ORDERS1LINEITEM1_it230 != qPARTS1NATION3ORDERS1LINEITEM1_end229; 
        ++qPARTS1NATION3ORDERS1LINEITEM1_it230)
    {
        int64_t x_qPARTS1NATION3ORDERS_O__CUSTKEY = get<2>(
            qPARTS1NATION3ORDERS1LINEITEM1_it230->first);
        qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            SUPPKEY,NATIONKEY,x_qPARTS1NATION3ORDERS_O__CUSTKEY)] += 
            qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__CUSTKEY,NATIONKEY)];
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_SUPPLIER_sec_span, on_insert_SUPPLIER_usec_span);
}

void on_insert_PARTS(
    ofstream* results, ofstream* log, ofstream* stats, int64_t PARTKEY,string 
    NAME,string MFGR,string BRAND,string TYPE,int SIZE,string CONTAINER,double 
    RETAILPRICE,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it232 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end231 = 
        q.end();
    for (; q_it232 != q_end231; ++q_it232)
    {
        int64_t N2__REGIONKEY = get<1>(q_it232->first);
        int64_t N1__REGIONKEY = get<2>(q_it232->first);
        int64_t C__NATIONKEY = get<3>(q_it232->first);
        q[make_tuple(
            MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += qPARTS1[make_tuple(
            PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it234 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end233 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it234 != qCUSTOMER1_end233; ++qCUSTOMER1_it234)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it234->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1_it234->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,MFGR,N2__REGIONKEY)] += qCUSTOMER1PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1NATION1_it236 
        = qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1NATION1_end235 = qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it236 != qCUSTOMER1NATION1_end235; 
        ++qCUSTOMER1NATION1_it236)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1NATION1_it236->first);
        int64_t x_qCUSTOMER1NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER1NATION1_it236->first);
        qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1NATION_N1__NATIONKEY,MFGR)] += 
            qCUSTOMER1PARTS1NATION1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_it238 = qCUSTOMER1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_end237 = qCUSTOMER1SUPPLIER1.end();
    for (
        ; qCUSTOMER1SUPPLIER1_it238 != qCUSTOMER1SUPPLIER1_end237; 
        ++qCUSTOMER1SUPPLIER1_it238)
    {
        int64_t x_qCUSTOMER1SUPPLIER_S__SUPPKEY = get<0>(
            qCUSTOMER1SUPPLIER1_it238->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1SUPPLIER1_it238->first);
        qCUSTOMER1SUPPLIER1[make_tuple(
            x_qCUSTOMER1SUPPLIER_S__SUPPKEY,x_qCUSTOMER_C__CUSTKEY,MFGR)] += 
            qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER1SUPPLIER_S__SUPPKEY,x_qCUSTOMER_C__CUSTKEY)];
    }
    qLINEITEM2[make_tuple(PARTKEY,MFGR)] += 1;
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it240 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end239 
        = qNATION1.end();
    for (; qNATION1_it240 != qNATION1_end239; ++qNATION1_it240)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it240->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it240->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it240->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            qPARTS1NATION1[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it242 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end241 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it242 != qNATION1NATION1_end241; ++qNATION1NATION1_it242)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1NATION1_it242->first);
        int64_t x_qNATION1NATION_N1__NATIONKEY = get<2>(
            qNATION1NATION1_it242->first);
        qNATION1NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR,x_qNATION1NATION_N1__NATIONKEY)] += 
            qPARTS1NATION1NATION1[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,x_qNATION1NATION_N1__NATIONKEY)];
    }
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it244 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end243 = 
        qNATION2.end();
    for (; qNATION2_it244 != qNATION2_end243; ++qNATION2_it244)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it244->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it244->first);
        qNATION2[make_tuple(
            MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += qPARTS1NATION2[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,string>,double>::iterator qNATION3_it246 = qNATION3.begin(
        );
    map<tuple<int64_t,string>,double>::iterator qNATION3_end245 = qNATION3.end(
        );
    for (; qNATION3_it246 != qNATION3_end245; ++qNATION3_it246)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION3_it246->first);
        qNATION3[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR)] += qPARTS1NATION3[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_it248 = qNATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_end247 = qNATION3ORDERS1.end();
    for (
        ; qNATION3ORDERS1_it248 != qNATION3ORDERS1_end247; ++qNATION3ORDERS1_it248)
    {
        int64_t x_qNATION3ORDERS_O__ORDERKEY = get<0>(
            qNATION3ORDERS1_it248->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION3ORDERS1_it248->first);
        int64_t x_qNATION3ORDERS_O__CUSTKEY = get<3>(
            qNATION3ORDERS1_it248->first);
        qNATION3ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,x_qNATION_N1__NATIONKEY,MFGR,
            x_qNATION3ORDERS_O__CUSTKEY)] += qPARTS1NATION3ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,PARTKEY,x_qNATION_N1__NATIONKEY,
            x_qNATION3ORDERS_O__CUSTKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it250 = 
        qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end249 = 
        qORDERS1.end();
    for (; qORDERS1_it250 != qORDERS1_end249; ++qORDERS1_it250)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it250->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1_it250->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,MFGR,N2__REGIONKEY)] += qORDERS1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1NATION1_it252 = 
        qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1NATION1_end251 = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it252 != qORDERS1NATION1_end251; ++qORDERS1NATION1_it252)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1NATION1_it252->first);
        int64_t x_qORDERS1NATION_N1__NATIONKEY = get<1>(
            qORDERS1NATION1_it252->first);
        qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1NATION_N1__NATIONKEY,MFGR)] += 
            qORDERS1PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,x_qORDERS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1SUPPLIER1_it254 
        = qORDERS1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1SUPPLIER1_end253 = qORDERS1SUPPLIER1.end();
    for (
        ; qORDERS1SUPPLIER1_it254 != qORDERS1SUPPLIER1_end253; 
        ++qORDERS1SUPPLIER1_it254)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1SUPPLIER1_it254->first);
        int64_t x_qORDERS1SUPPLIER_S__SUPPKEY = get<1>(
            qORDERS1SUPPLIER1_it254->first);
        qORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1SUPPLIER_S__SUPPKEY,MFGR)] += 
            qORDERS1SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,x_qORDERS1SUPPLIER_S__SUPPKEY)];
    }
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it256 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end255 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it256 != qSUPPLIER1_end255; ++qSUPPLIER1_it256)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it256->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1_it256->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1_it256->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            qSUPPLIER1PARTS1[make_tuple(
            PARTKEY,x_qSUPPLIER_S__SUPPKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER1NATION1_it258 
        = qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER1NATION1_end257 = qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it258 != qSUPPLIER1NATION1_end257; 
        ++qSUPPLIER1NATION1_it258)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1NATION1_it258->first);
        int64_t x_qSUPPLIER1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1NATION1_it258->first);
        qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,MFGR,x_qSUPPLIER1NATION_N1__NATIONKEY)] += 
            qSUPPLIER1PARTS1NATION1[make_tuple(
            PARTKEY,x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER1NATION_N1__NATIONKEY)];
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_PARTS_sec_span, on_insert_PARTS_usec_span);
}

void on_delete_NATION(
    ofstream* results, ofstream* log, ofstream* stats, int64_t NATIONKEY,string 
    NAME,int64_t REGIONKEY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it260 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end259 = 
        q.end();
    for (; q_it260 != q_end259; ++q_it260)
    {
        string P__MFGR = get<0>(q_it260->first);
        q[make_tuple(
            P__MFGR,REGIONKEY,REGIONKEY,NATIONKEY)] += -1*qNATION3[make_tuple(
            NATIONKEY,P__MFGR)];
    }
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it262 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end261 = 
        q.end();
    for (; q_it262 != q_end261; ++q_it262)
    {
        string P__MFGR = get<0>(q_it262->first);
        int64_t N2__REGIONKEY = get<1>(q_it262->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,REGIONKEY,NATIONKEY)] += -1*qNATION2[make_tuple(
            P__MFGR,NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it264 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end263 = 
        q.end();
    for (; q_it264 != q_end263; ++q_it264)
    {
        string P__MFGR = get<0>(q_it264->first);
        int64_t REGIONKEY = get<2>(q_it264->first);
        int64_t C__NATIONKEY = get<3>(q_it264->first);
        q[make_tuple(
            P__MFGR,REGIONKEY,REGIONKEY,C__NATIONKEY)] += -1*qNATION1[make_tuple(
            NATIONKEY,P__MFGR,C__NATIONKEY,REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it266 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end265 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it266 != qCUSTOMER1_end265; ++qCUSTOMER1_it266)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it266->first);
        string P__MFGR = get<1>(qCUSTOMER1_it266->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,REGIONKEY)] += -1*qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER1PARTS1_it268 
        = qCUSTOMER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_end267 = qCUSTOMER1PARTS1.end();
    for (
        ; qCUSTOMER1PARTS1_it268 != qCUSTOMER1PARTS1_end267; ++qCUSTOMER1PARTS1_it268)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1_it268->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1PARTS1_it268->first);
        qCUSTOMER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,REGIONKEY)] += 
            -1*qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,NATIONKEY)];
    }
    qCUSTOMER2[make_tuple(NATIONKEY,REGIONKEY)] += -1;
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_it270 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_end269 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it270 != qLINEITEM1_end269; ++qLINEITEM1_it270)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it270->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,REGIONKEY)] += 
            -1*qLINEITEM1NATION1[make_tuple(x_qLINEITEM_L__ORDERKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_it272 = 
        qLINEITEM3.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_end271 = 
        qLINEITEM3.end();
    for (; qLINEITEM3_it272 != qLINEITEM3_end271; ++qLINEITEM3_it272)
    {
        int64_t x_qLINEITEM_L__SUPPKEY = get<0>(qLINEITEM3_it272->first);
        qLINEITEM3[make_tuple(
            x_qLINEITEM_L__SUPPKEY,REGIONKEY)] += -1*qLINEITEM3NATION1[make_tuple(
            x_qLINEITEM_L__SUPPKEY,NATIONKEY)];
    }
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it274 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end273 
        = qNATION1.end();
    for (; qNATION1_it274 != qNATION1_end273; ++qNATION1_it274)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it274->first);
        string P__MFGR = get<1>(qNATION1_it274->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY,REGIONKEY)] += 
            -1*qNATION1NATION1[make_tuple(x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY)];
    }
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it276 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end275 = 
        qNATION2.end();
    for (; qNATION2_it276 != qNATION2_end275; ++qNATION2_it276)
    {
        string P__MFGR = get<0>(qNATION2_it276->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it276->first);
        qNATION2[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,REGIONKEY)] += -1*qNATION1NATION1[make_tuple(
            NATIONKEY,P__MFGR,x_qNATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it278 = 
        qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end277 = 
        qORDERS1.end();
    for (; qORDERS1_it278 != qORDERS1_end277; ++qORDERS1_it278)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it278->first);
        string P__MFGR = get<1>(qORDERS1_it278->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,P__MFGR,REGIONKEY)] += -1*qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1PARTS1_it280 = 
        qORDERS1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS1PARTS1_end279 = qORDERS1PARTS1.end();
    for (
        ; qORDERS1PARTS1_it280 != qORDERS1PARTS1_end279; ++qORDERS1PARTS1_it280)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1PARTS1_it280->first);
        int64_t x_qORDERS1PARTS_P__PARTKEY = get<1>(
            qORDERS1PARTS1_it280->first);
        qORDERS1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,REGIONKEY)] += 
            -1*qORDERS1PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_it282 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_end281 = 
        qORDERS2.end();
    for (; qORDERS2_it282 != qORDERS2_end281; ++qORDERS2_it282)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<0>(qORDERS2_it282->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,REGIONKEY)] += 
            -1*qLINEITEM1NATION1ORDERS1[make_tuple(x_qORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it284 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end283 
        = qPARTS1.end();
    for (; qPARTS1_it284 != qPARTS1_end283; ++qPARTS1_it284)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it284->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,REGIONKEY,REGIONKEY)] += 
            -1*qPARTS1NATION3[make_tuple(x_qPARTS_P__PARTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it286 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end285 
        = qPARTS1.end();
    for (; qPARTS1_it286 != qPARTS1_end285; ++qPARTS1_it286)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it286->first);
        int64_t C__NATIONKEY = get<1>(qPARTS1_it286->first);
        int64_t REGIONKEY = get<3>(qPARTS1_it286->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,REGIONKEY,REGIONKEY)] += 
            -1*qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,C__NATIONKEY,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it288 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end287 
        = qPARTS1.end();
    for (; qPARTS1_it288 != qPARTS1_end287; ++qPARTS1_it288)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it288->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it288->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY,REGIONKEY)] += 
            -1*qPARTS1NATION2[make_tuple(x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it290 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end289 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it290 != qPARTS1NATION1_end289; ++qPARTS1NATION1_it290)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1_it290->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1_it290->first);
        qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,NATIONKEY,REGIONKEY)] += 
            -1*qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it292 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end291 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it292 != qPARTS1NATION2_end291; ++qPARTS1NATION2_it292)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION2_it292->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION2_it292->first);
        qPARTS1NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,REGIONKEY)] += 
            -1*qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it294 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end293 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it294 != qSUPPLIER1_end293; ++qSUPPLIER1_it294)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it294->first);
        string P__MFGR = get<1>(qSUPPLIER1_it294->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY,REGIONKEY)] += 
            -1*qSUPPLIER1NATION1[make_tuple(x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_it296 = qSUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_end295 = qSUPPLIER1PARTS1.end();
    for (
        ; qSUPPLIER1PARTS1_it296 != qSUPPLIER1PARTS1_end295; ++qSUPPLIER1PARTS1_it296)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1_it296->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER1PARTS1_it296->first);
        qSUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY,REGIONKEY)] += 
            -1*qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY)];
    }
    qSUPPLIER2[make_tuple(NATIONKEY,REGIONKEY)] += -1;
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_NATION_sec_span, on_delete_NATION_usec_span);
}

void on_delete_ORDERS(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    CUSTKEY,string ORDERSTATUS,double TOTALPRICE,string ORDERDATE,string 
    ORDERPRIORITY,string CLERK,int SHIPPRIORITY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it298 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end297 = 
        q.end();
    for (; q_it298 != q_end297; ++q_it298)
    {
        string P__MFGR = get<0>(q_it298->first);
        int64_t N2__REGIONKEY = get<1>(q_it298->first);
        int64_t N1__REGIONKEY = get<2>(q_it298->first);
        int64_t C__NATIONKEY = get<3>(q_it298->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += -1*qORDERS1[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it300 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end299 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it300 != qCUSTOMER1_end299; ++qCUSTOMER1_it300)
    {
        string P__MFGR = get<1>(qCUSTOMER1_it300->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1_it300->first);
        qCUSTOMER1[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)] += -1*qORDERS1[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1NATION1_it302 
        = qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1NATION1_end301 = qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it302 != qCUSTOMER1NATION1_end301; 
        ++qCUSTOMER1NATION1_it302)
    {
        int64_t x_qCUSTOMER1NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER1NATION1_it302->first);
        string P__MFGR = get<2>(qCUSTOMER1NATION1_it302->first);
        qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,x_qCUSTOMER1NATION_N1__NATIONKEY,P__MFGR)] += 
            -1*qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qCUSTOMER1NATION_N1__NATIONKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER1PARTS1_it304 
        = qCUSTOMER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_end303 = qCUSTOMER1PARTS1.end();
    for (
        ; qCUSTOMER1PARTS1_it304 != qCUSTOMER1PARTS1_end303; ++qCUSTOMER1PARTS1_it304)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1_it304->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1PARTS1_it304->first);
        qCUSTOMER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)] += 
            -1*qORDERS1PARTS1[make_tuple(
            ORDERKEY,x_qCUSTOMER1PARTS_P__PARTKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_it306 = qCUSTOMER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_end305 = qCUSTOMER1PARTS1NATION1.end();
    for (
        ; qCUSTOMER1PARTS1NATION1_it306 != qCUSTOMER1PARTS1NATION1_end305; 
        ++qCUSTOMER1PARTS1NATION1_it306)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1NATION1_it306->first);
        int64_t x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY = get<2>(
            qCUSTOMER1PARTS1NATION1_it306->first);
        qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,CUSTKEY,
            x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY)] += -1*qORDERS1PARTS1NATION1[make_tuple(
            ORDERKEY,x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_it308 = qCUSTOMER1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_end307 = qCUSTOMER1SUPPLIER1.end();
    for (
        ; qCUSTOMER1SUPPLIER1_it308 != qCUSTOMER1SUPPLIER1_end307; 
        ++qCUSTOMER1SUPPLIER1_it308)
    {
        int64_t x_qCUSTOMER1SUPPLIER_S__SUPPKEY = get<0>(
            qCUSTOMER1SUPPLIER1_it308->first);
        string P__MFGR = get<2>(qCUSTOMER1SUPPLIER1_it308->first);
        qCUSTOMER1SUPPLIER1[make_tuple(
            x_qCUSTOMER1SUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)] += 
            -1*qORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qCUSTOMER1SUPPLIER_S__SUPPKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1SUPPLIER1PARTS1_it310 = qCUSTOMER1SUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1SUPPLIER1PARTS1_end309 = qCUSTOMER1SUPPLIER1PARTS1.end();
    for (
        ; qCUSTOMER1SUPPLIER1PARTS1_it310 != qCUSTOMER1SUPPLIER1PARTS1_end309; 
        ++qCUSTOMER1SUPPLIER1PARTS1_it310)
    {
        int64_t x_qCUSTOMER1SUPPLIER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1SUPPLIER1PARTS1_it310->first);
        int64_t x_qCUSTOMER1SUPPLIER_S__SUPPKEY = get<1>(
            qCUSTOMER1SUPPLIER1PARTS1_it310->first);
        qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER1SUPPLIER1PARTS_P__PARTKEY,x_qCUSTOMER1SUPPLIER_S__SUPPKEY,CUSTKEY)] 
            += -1*qORDERS1SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qCUSTOMER1SUPPLIER1PARTS_P__PARTKEY,
            x_qCUSTOMER1SUPPLIER_S__SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_it312 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_end311 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it312 != qLINEITEM1_end311; ++qLINEITEM1_it312)
    {
        int64_t C__NATIONKEY = get<1>(qLINEITEM1_it312->first);
        int64_t N1__REGIONKEY = get<2>(qLINEITEM1_it312->first);
        qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)] += -1*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    qLINEITEM1CUSTOMER1[make_tuple(ORDERKEY,CUSTKEY)] += -1;
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_it314 = 
        qLINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_end313 = 
        qLINEITEM1NATION1.end();
    for (
        ; qLINEITEM1NATION1_it314 != qLINEITEM1NATION1_end313; 
        ++qLINEITEM1NATION1_it314)
    {
        int64_t x_qLINEITEM1NATION_N1__NATIONKEY = get<1>(
            qLINEITEM1NATION1_it314->first);
        qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qLINEITEM1NATION_N1__NATIONKEY)] += 
            -1*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qLINEITEM1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it316 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end315 
        = qNATION1.end();
    for (; qNATION1_it316 != qNATION1_end315; ++qNATION1_it316)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it316->first);
        string P__MFGR = get<1>(qNATION1_it316->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it316->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it316->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it318 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end317 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it318 != qNATION1NATION1_end317; ++qNATION1NATION1_it318)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1NATION1_it318->first);
        string P__MFGR = get<1>(qNATION1NATION1_it318->first);
        int64_t x_qNATION1NATION_N1__NATIONKEY = get<2>(
            qNATION1NATION1_it318->first);
        qNATION1NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION1NATION_N1__NATIONKEY)] += 
            -1*qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qNATION1NATION_N1__NATIONKEY)];
    }
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it320 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end319 = 
        qNATION2.end();
    for (; qNATION2_it320 != qNATION2_end319; ++qNATION2_it320)
    {
        string P__MFGR = get<0>(qNATION2_it320->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it320->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it320->first);
        qNATION2[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += -1*qORDERS1[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,string>,double>::iterator qNATION3_it322 = qNATION3.begin(
        );
    map<tuple<int64_t,string>,double>::iterator qNATION3_end321 = qNATION3.end(
        );
    for (; qNATION3_it322 != qNATION3_end321; ++qNATION3_it322)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION3_it322->first);
        string P__MFGR = get<1>(qNATION3_it322->first);
        qNATION3[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR)] += -1*qNATION3ORDERS1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR,CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it324 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end323 
        = qPARTS1.end();
    for (; qPARTS1_it324 != qPARTS1_end323; ++qPARTS1_it324)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it324->first);
        int64_t C__NATIONKEY = get<1>(qPARTS1_it324->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it324->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1_it324->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            -1*qORDERS1PARTS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,N2__REGIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it326 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end325 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it326 != qPARTS1NATION1_end325; ++qPARTS1NATION1_it326)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1_it326->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1_it326->first);
        int64_t C__NATIONKEY = get<2>(qPARTS1NATION1_it326->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1NATION1_it326->first);
        qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] 
            += -1*qORDERS1PARTS1NATION1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            x_qPARTS1NATION_N1__NATIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_it328 = qPARTS1NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_end327 = qPARTS1NATION1NATION1.end();
    for (
        ; qPARTS1NATION1NATION1_it328 != qPARTS1NATION1NATION1_end327; 
        ++qPARTS1NATION1NATION1_it328)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS1NATION1NATION1_it328->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1NATION1_it328->first);
        int64_t x_qPARTS1NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS1NATION1NATION1_it328->first);
        qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,
            x_qPARTS1NATION1NATION_N1__NATIONKEY)] += -1*qORDERS1PARTS1NATION1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            x_qPARTS1NATION_N1__NATIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it330 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end329 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it330 != qPARTS1NATION2_end329; ++qPARTS1NATION2_it330)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION2_it330->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION2_it330->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1NATION2_it330->first);
        qPARTS1NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            -1*qORDERS1PARTS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            N2__REGIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_it332 = 
        qPARTS1NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_end331 = 
        qPARTS1NATION3.end();
    for (
        ; qPARTS1NATION3_it332 != qPARTS1NATION3_end331; ++qPARTS1NATION3_it332)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION3_it332->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION3_it332->first);
        qPARTS1NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY)] += 
            -1*qPARTS1NATION3ORDERS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_it334 = qPARTS1NATION3LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_end333 = qPARTS1NATION3LINEITEM1.end();
    for (
        ; qPARTS1NATION3LINEITEM1_it334 != qPARTS1NATION3LINEITEM1_end333; 
        ++qPARTS1NATION3LINEITEM1_it334)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION3LINEITEM1_it334->first);
        int64_t x_qPARTS1NATION3LINEITEM_L__SUPPKEY = get<2>(
            qPARTS1NATION3LINEITEM1_it334->first);
        qPARTS1NATION3LINEITEM1[make_tuple(
            ORDERKEY,x_qPARTS1NATION_N1__NATIONKEY,x_qPARTS1NATION3LINEITEM_L__SUPPKEY)] += 
            -1*qPARTS1NATION3LINEITEM1ORDERS1[make_tuple(
            CUSTKEY,x_qPARTS1NATION_N1__NATIONKEY,x_qPARTS1NATION3LINEITEM_L__SUPPKEY)];
    }
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it336 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end335 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it336 != qSUPPLIER1_end335; ++qSUPPLIER1_it336)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it336->first);
        string P__MFGR = get<1>(qSUPPLIER1_it336->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1_it336->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1_it336->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*qORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY,P__MFGR)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER1NATION1_it338 
        = qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER1NATION1_end337 = qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it338 != qSUPPLIER1NATION1_end337; 
        ++qSUPPLIER1NATION1_it338)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1NATION1_it338->first);
        string P__MFGR = get<1>(qSUPPLIER1NATION1_it338->first);
        int64_t x_qSUPPLIER1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1NATION1_it338->first);
        qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,x_qSUPPLIER1NATION_N1__NATIONKEY)] += 
            -1*qORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY,P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qSUPPLIER1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_it340 = qSUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_end339 = qSUPPLIER1PARTS1.end();
    for (
        ; qSUPPLIER1PARTS1_it340 != qSUPPLIER1PARTS1_end339; ++qSUPPLIER1PARTS1_it340)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1_it340->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER1PARTS1_it340->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1PARTS1_it340->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1PARTS1_it340->first);
        qSUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,C__NATIONKEY,
            N1__REGIONKEY)] += -1*qORDERS1SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qSUPPLIER1PARTS_P__PARTKEY,
            x_qSUPPLIER_S__SUPPKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_it342 = qSUPPLIER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_end341 = qSUPPLIER1PARTS1NATION1.end();
    for (
        ; qSUPPLIER1PARTS1NATION1_it342 != qSUPPLIER1PARTS1NATION1_end341; 
        ++qSUPPLIER1PARTS1NATION1_it342)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1NATION1_it342->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(
            qSUPPLIER1PARTS1NATION1_it342->first);
        int64_t x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1PARTS1NATION1_it342->first);
        qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,
            x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY)] += 
            -1*qORDERS1SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qSUPPLIER1PARTS_P__PARTKEY,
            x_qSUPPLIER_S__SUPPKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY)];
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_ORDERS_sec_span, on_delete_ORDERS_usec_span);
}

void on_delete_LINEITEM(
    ofstream* results, ofstream* log, ofstream* stats, int64_t ORDERKEY,int64_t 
    PARTKEY,int64_t SUPPKEY,int LINENUMBER,double QUANTITY,double 
    EXTENDEDPRICE,double DISCOUNT,double TAX)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it344 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end343 = 
        q.end();
    for (; q_it344 != q_end343; ++q_it344)
    {
        string P__MFGR = get<0>(q_it344->first);
        int64_t N2__REGIONKEY = get<1>(q_it344->first);
        int64_t N1__REGIONKEY = get<2>(q_it344->first);
        int64_t C__NATIONKEY = get<3>(q_it344->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it346 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end345 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it346 != qCUSTOMER1_end345; ++qCUSTOMER1_it346)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it346->first);
        string P__MFGR = get<1>(qCUSTOMER1_it346->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1_it346->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,N2__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1NATION1_it348 
        = qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1NATION1_end347 = qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it348 != qCUSTOMER1NATION1_end347; 
        ++qCUSTOMER1NATION1_it348)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1NATION1_it348->first);
        int64_t x_qCUSTOMER1NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER1NATION1_it348->first);
        string P__MFGR = get<2>(qCUSTOMER1NATION1_it348->first);
        qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1NATION_N1__NATIONKEY,P__MFGR)] += 
            -1*EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qCUSTOMER1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER1PARTS1_it350 
        = qCUSTOMER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_end349 = qCUSTOMER1PARTS1.end();
    for (
        ; qCUSTOMER1PARTS1_it350 != qCUSTOMER1PARTS1_end349; ++qCUSTOMER1PARTS1_it350)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1PARTS1_it350->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1PARTS1_it350->first);
        qCUSTOMER1PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_it352 = qCUSTOMER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_end351 = qCUSTOMER1PARTS1NATION1.end();
    for (
        ; qCUSTOMER1PARTS1NATION1_it352 != qCUSTOMER1PARTS1NATION1_end351; 
        ++qCUSTOMER1PARTS1NATION1_it352)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1PARTS1NATION1_it352->first);
        int64_t x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY = get<2>(
            qCUSTOMER1PARTS1NATION1_it352->first);
        qCUSTOMER1PARTS1NATION1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_it354 = qCUSTOMER1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_end353 = qCUSTOMER1SUPPLIER1.end();
    for (
        ; qCUSTOMER1SUPPLIER1_it354 != qCUSTOMER1SUPPLIER1_end353; 
        ++qCUSTOMER1SUPPLIER1_it354)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1SUPPLIER1_it354->first);
        string P__MFGR = get<2>(qCUSTOMER1SUPPLIER1_it354->first);
        qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)] += 
            -1*EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(PARTKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1SUPPLIER1PARTS1_it356 = qCUSTOMER1SUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1SUPPLIER1PARTS1_end355 = qCUSTOMER1SUPPLIER1PARTS1.end();
    for (
        ; qCUSTOMER1SUPPLIER1PARTS1_it356 != qCUSTOMER1SUPPLIER1PARTS1_end355; 
        ++qCUSTOMER1SUPPLIER1PARTS1_it356)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<2>(
            qCUSTOMER1SUPPLIER1PARTS1_it356->first);
        qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            PARTKEY,SUPPKEY,x_qCUSTOMER_C__CUSTKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)];
    }
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it358 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end357 
        = qNATION1.end();
    for (; qNATION1_it358 != qNATION1_end357; ++qNATION1_it358)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it358->first);
        string P__MFGR = get<1>(qNATION1_it358->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it358->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it358->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it360 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end359 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it360 != qNATION1NATION1_end359; ++qNATION1NATION1_it360)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1NATION1_it360->first);
        string P__MFGR = get<1>(qNATION1NATION1_it360->first);
        int64_t x_qNATION1NATION_N1__NATIONKEY = get<2>(
            qNATION1NATION1_it360->first);
        qNATION1NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qNATION1NATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY)];
    }
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it362 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end361 = 
        qNATION2.end();
    for (; qNATION2_it362 != qNATION2_end361; ++qNATION2_it362)
    {
        string P__MFGR = get<0>(qNATION2_it362->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it362->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it362->first);
        qNATION2[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,string>,double>::iterator qNATION3_it364 = qNATION3.begin(
        );
    map<tuple<int64_t,string>,double>::iterator qNATION3_end363 = qNATION3.end(
        );
    for (; qNATION3_it364 != qNATION3_end363; ++qNATION3_it364)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION3_it364->first);
        string P__MFGR = get<1>(qNATION3_it364->first);
        qNATION3[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR)] += 
            -1*EXTENDEDPRICE*qPARTS1NATION3LINEITEM1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,SUPPKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_it366 = qNATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_end365 = qNATION3ORDERS1.end();
    for (
        ; qNATION3ORDERS1_it366 != qNATION3ORDERS1_end365; ++qNATION3ORDERS1_it366)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION3ORDERS1_it366->first);
        string P__MFGR = get<2>(qNATION3ORDERS1_it366->first);
        int64_t x_qNATION3ORDERS_O__CUSTKEY = get<3>(
            qNATION3ORDERS1_it366->first);
        qNATION3ORDERS1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION3ORDERS_O__CUSTKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY,x_qNATION3ORDERS_O__CUSTKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it368 = 
        qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end367 = 
        qORDERS1.end();
    for (; qORDERS1_it368 != qORDERS1_end367; ++qORDERS1_it368)
    {
        string P__MFGR = get<1>(qORDERS1_it368->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1_it368->first);
        qORDERS1[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)] += -1*EXTENDEDPRICE*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1NATION1_it370 = 
        qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1NATION1_end369 = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it370 != qORDERS1NATION1_end369; ++qORDERS1NATION1_it370)
    {
        int64_t x_qORDERS1NATION_N1__NATIONKEY = get<1>(
            qORDERS1NATION1_it370->first);
        string P__MFGR = get<2>(qORDERS1NATION1_it370->first);
        qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qORDERS1NATION_N1__NATIONKEY,P__MFGR)] += 
            -1*EXTENDEDPRICE*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qORDERS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1PARTS1_it372 = 
        qORDERS1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS1PARTS1_end371 = qORDERS1PARTS1.end();
    for (
        ; qORDERS1PARTS1_it372 != qORDERS1PARTS1_end371; ++qORDERS1PARTS1_it372)
    {
        int64_t N2__REGIONKEY = get<2>(qORDERS1PARTS1_it372->first);
        qORDERS1PARTS1[make_tuple(
            ORDERKEY,PARTKEY,N2__REGIONKEY)] += -1*EXTENDEDPRICE*qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS1PARTS1NATION1_it374 = qORDERS1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS1PARTS1NATION1_end373 = qORDERS1PARTS1NATION1.end();
    for (
        ; qORDERS1PARTS1NATION1_it374 != qORDERS1PARTS1NATION1_end373; 
        ++qORDERS1PARTS1NATION1_it374)
    {
        int64_t x_qORDERS1PARTS1NATION_N1__NATIONKEY = get<2>(
            qORDERS1PARTS1NATION1_it374->first);
        qORDERS1PARTS1NATION1[make_tuple(
            ORDERKEY,PARTKEY,x_qORDERS1PARTS1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qORDERS1PARTS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1SUPPLIER1_it376 
        = qORDERS1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1SUPPLIER1_end375 = qORDERS1SUPPLIER1.end();
    for (
        ; qORDERS1SUPPLIER1_it376 != qORDERS1SUPPLIER1_end375; 
        ++qORDERS1SUPPLIER1_it376)
    {
        string P__MFGR = get<2>(qORDERS1SUPPLIER1_it376->first);
        qORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,SUPPKEY,P__MFGR)] += -1*EXTENDEDPRICE*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }
    qORDERS1SUPPLIER1PARTS1[make_tuple(
        ORDERKEY,PARTKEY,SUPPKEY)] += -1*EXTENDEDPRICE;
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it378 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end377 
        = qPARTS1.end();
    for (; qPARTS1_it378 != qPARTS1_end377; ++qPARTS1_it378)
    {
        int64_t C__NATIONKEY = get<1>(qPARTS1_it378->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it378->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1_it378->first);
        qPARTS1[make_tuple(
            PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it380 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end379 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it380 != qPARTS1NATION1_end379; ++qPARTS1NATION1_it380)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1_it380->first);
        int64_t C__NATIONKEY = get<2>(qPARTS1NATION1_it380->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1NATION1_it380->first);
        qPARTS1NATION1[make_tuple(
            PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_it382 = qPARTS1NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_end381 = qPARTS1NATION1NATION1.end();
    for (
        ; qPARTS1NATION1NATION1_it382 != qPARTS1NATION1NATION1_end381; 
        ++qPARTS1NATION1NATION1_it382)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1NATION1_it382->first);
        int64_t x_qPARTS1NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS1NATION1NATION1_it382->first);
        qPARTS1NATION1NATION1[make_tuple(
            PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it384 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end383 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it384 != qPARTS1NATION2_end383; ++qPARTS1NATION2_it384)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION2_it384->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1NATION2_it384->first);
        qPARTS1NATION2[make_tuple(
            PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qPARTS1NATION_N1__NATIONKEY)]*qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_it386 = 
        qPARTS1NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_end385 = 
        qPARTS1NATION3.end();
    for (
        ; qPARTS1NATION3_it386 != qPARTS1NATION3_end385; ++qPARTS1NATION3_it386)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION3_it386->first);
        qPARTS1NATION3[make_tuple(
            PARTKEY,x_qPARTS1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qPARTS1NATION3LINEITEM1[make_tuple(
            ORDERKEY,x_qPARTS1NATION_N1__NATIONKEY,SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_it388 = qPARTS1NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_end387 = qPARTS1NATION3ORDERS1.end();
    for (
        ; qPARTS1NATION3ORDERS1_it388 != qPARTS1NATION3ORDERS1_end387; 
        ++qPARTS1NATION3ORDERS1_it388)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<2>(
            qPARTS1NATION3ORDERS1_it388->first);
        int64_t x_qPARTS1NATION3ORDERS_O__CUSTKEY = get<3>(
            qPARTS1NATION3ORDERS1_it388->first);
        qPARTS1NATION3ORDERS1[make_tuple(
            ORDERKEY,PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,
            x_qPARTS1NATION3ORDERS_O__CUSTKEY)] += 
            -1*EXTENDEDPRICE*qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qPARTS1NATION_N1__NATIONKEY,x_qPARTS1NATION3ORDERS_O__CUSTKEY)];
    }
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it390 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end389 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it390 != qSUPPLIER1_end389; ++qSUPPLIER1_it390)
    {
        string P__MFGR = get<1>(qSUPPLIER1_it390->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1_it390->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1_it390->first);
        qSUPPLIER1[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(PARTKEY,P__MFGR)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER1NATION1_it392 
        = qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER1NATION1_end391 = qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it392 != qSUPPLIER1NATION1_end391; 
        ++qSUPPLIER1NATION1_it392)
    {
        string P__MFGR = get<1>(qSUPPLIER1NATION1_it392->first);
        int64_t x_qSUPPLIER1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1NATION1_it392->first);
        qSUPPLIER1NATION1[make_tuple(
            SUPPKEY,P__MFGR,x_qSUPPLIER1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qSUPPLIER1NATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_it394 = qSUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_end393 = qSUPPLIER1PARTS1.end();
    for (
        ; qSUPPLIER1PARTS1_it394 != qSUPPLIER1PARTS1_end393; ++qSUPPLIER1PARTS1_it394)
    {
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1PARTS1_it394->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1PARTS1_it394->first);
        qSUPPLIER1PARTS1[make_tuple(
            PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1[make_tuple(ORDERKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_it396 = qSUPPLIER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_end395 = qSUPPLIER1PARTS1NATION1.end();
    for (
        ; qSUPPLIER1PARTS1NATION1_it396 != qSUPPLIER1PARTS1NATION1_end395; 
        ++qSUPPLIER1PARTS1NATION1_it396)
    {
        int64_t x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1PARTS1NATION1_it396->first);
        qSUPPLIER1PARTS1NATION1[make_tuple(
            PARTKEY,SUPPKEY,x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY)];
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
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it398 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end397 = 
        q.end();
    for (; q_it398 != q_end397; ++q_it398)
    {
        string P__MFGR = get<0>(q_it398->first);
        int64_t N2__REGIONKEY = get<1>(q_it398->first);
        int64_t N1__REGIONKEY = get<2>(q_it398->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,NATIONKEY)] += -1*qCUSTOMER1[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)]*qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_it400 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_end399 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it400 != qLINEITEM1_end399; ++qLINEITEM1_it400)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it400->first);
        int64_t N1__REGIONKEY = get<2>(qLINEITEM1_it400->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,N1__REGIONKEY)] += 
            -1*qLINEITEM1CUSTOMER1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_it402 = 
        qLINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_end401 = 
        qLINEITEM1NATION1.end();
    for (
        ; qLINEITEM1NATION1_it402 != qLINEITEM1NATION1_end401; 
        ++qLINEITEM1NATION1_it402)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1NATION1_it402->first);
        qLINEITEM1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY)] += -1*qLINEITEM1CUSTOMER1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)];
    }
    qLINEITEM1NATION1ORDERS1[make_tuple(CUSTKEY,NATIONKEY)] += -1;
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it404 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end403 
        = qNATION1.end();
    for (; qNATION1_it404 != qNATION1_end403; ++qNATION1_it404)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it404->first);
        string P__MFGR = get<1>(qNATION1_it404->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it404->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY,N1__REGIONKEY)] += 
            -1*qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it406 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end405 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it406 != qNATION1NATION1_end405; ++qNATION1NATION1_it406)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1NATION1_it406->first);
        string P__MFGR = get<1>(qNATION1NATION1_it406->first);
        qNATION1NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY)] += -1*qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY,P__MFGR)];
    }
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it408 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end407 = 
        qNATION2.end();
    for (; qNATION2_it408 != qNATION2_end407; ++qNATION2_it408)
    {
        string P__MFGR = get<0>(qNATION2_it408->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it408->first);
        qNATION2[make_tuple(
            P__MFGR,NATIONKEY,N2__REGIONKEY)] += -1*qCUSTOMER1[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)];
    }
    map<tuple<int64_t,string>,double>::iterator qNATION3_it410 = qNATION3.begin(
        );
    map<tuple<int64_t,string>,double>::iterator qNATION3_end409 = qNATION3.end(
        );
    for (; qNATION3_it410 != qNATION3_end409; ++qNATION3_it410)
    {
        string P__MFGR = get<1>(qNATION3_it410->first);
        qNATION3[make_tuple(
            NATIONKEY,P__MFGR)] += -1*qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,NATIONKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_it412 = qNATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_end411 = qNATION3ORDERS1.end();
    for (
        ; qNATION3ORDERS1_it412 != qNATION3ORDERS1_end411; ++qNATION3ORDERS1_it412)
    {
        int64_t x_qNATION3ORDERS_O__ORDERKEY = get<0>(
            qNATION3ORDERS1_it412->first);
        string P__MFGR = get<2>(qNATION3ORDERS1_it412->first);
        qNATION3ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR,CUSTKEY)] += 
            -1*qORDERS1NATION1[make_tuple(x_qNATION3ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_it414 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_end413 = 
        qORDERS2.end();
    for (; qORDERS2_it414 != qORDERS2_end413; ++qORDERS2_it414)
    {
        int64_t N1__REGIONKEY = get<2>(qORDERS2_it414->first);
        qORDERS2[make_tuple(
            CUSTKEY,NATIONKEY,N1__REGIONKEY)] += -1*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it416 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end415 
        = qPARTS1.end();
    for (; qPARTS1_it416 != qPARTS1_end415; ++qPARTS1_it416)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it416->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it416->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1_it416->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            -1*qCUSTOMER1PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it418 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end417 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it418 != qPARTS1NATION1_end417; ++qPARTS1NATION1_it418)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1_it418->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1_it418->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1NATION1_it418->first);
        qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,NATIONKEY,N1__REGIONKEY)] += 
            -1*qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,
            x_qPARTS1NATION_N1__NATIONKEY)]*qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_it420 = qPARTS1NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_end419 = qPARTS1NATION1NATION1.end();
    for (
        ; qPARTS1NATION1NATION1_it420 != qPARTS1NATION1NATION1_end419; 
        ++qPARTS1NATION1NATION1_it420)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS1NATION1NATION1_it420->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1NATION1_it420->first);
        qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,NATIONKEY)] += 
            -1*qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it422 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end421 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it422 != qPARTS1NATION2_end421; ++qPARTS1NATION2_it422)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION2_it422->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1NATION2_it422->first);
        qPARTS1NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY)] += -1*qCUSTOMER1PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_it424 = 
        qPARTS1NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_end423 = 
        qPARTS1NATION3.end();
    for (
        ; qPARTS1NATION3_it424 != qPARTS1NATION3_end423; ++qPARTS1NATION3_it424)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION3_it424->first);
        qPARTS1NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY)] += -1*qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_it426 = qPARTS1NATION3LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_end425 = qPARTS1NATION3LINEITEM1.end();
    for (
        ; qPARTS1NATION3LINEITEM1_it426 != qPARTS1NATION3LINEITEM1_end425; 
        ++qPARTS1NATION3LINEITEM1_it426)
    {
        int64_t x_qPARTS1NATION3LINEITEM_L__ORDERKEY = get<0>(
            qPARTS1NATION3LINEITEM1_it426->first);
        int64_t x_qPARTS1NATION3LINEITEM_L__SUPPKEY = get<2>(
            qPARTS1NATION3LINEITEM1_it426->first);
        qPARTS1NATION3LINEITEM1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__ORDERKEY,NATIONKEY,
            x_qPARTS1NATION3LINEITEM_L__SUPPKEY)] += -1*qLINEITEM1CUSTOMER1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__ORDERKEY,CUSTKEY)]*qLINEITEM3NATION1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__SUPPKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1ORDERS1_it428 = qPARTS1NATION3LINEITEM1ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1ORDERS1_end427 = qPARTS1NATION3LINEITEM1ORDERS1.end();
    for (
        ; qPARTS1NATION3LINEITEM1ORDERS1_it428 != qPARTS1NATION3LINEITEM1ORDERS1_end427; 
        ++qPARTS1NATION3LINEITEM1ORDERS1_it428)
    {
        int64_t x_qPARTS1NATION3LINEITEM_L__SUPPKEY = get<2>(
            qPARTS1NATION3LINEITEM1ORDERS1_it428->first);
        qPARTS1NATION3LINEITEM1ORDERS1[make_tuple(
            CUSTKEY,NATIONKEY,x_qPARTS1NATION3LINEITEM_L__SUPPKEY)] += 
            -1*qLINEITEM3NATION1[make_tuple(x_qPARTS1NATION3LINEITEM_L__SUPPKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_it430 = qPARTS1NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_end429 = qPARTS1NATION3ORDERS1.end();
    for (
        ; qPARTS1NATION3ORDERS1_it430 != qPARTS1NATION3ORDERS1_end429; 
        ++qPARTS1NATION3ORDERS1_it430)
    {
        int64_t x_qPARTS1NATION3ORDERS_O__ORDERKEY = get<0>(
            qPARTS1NATION3ORDERS1_it430->first);
        int64_t x_qPARTS_P__PARTKEY = get<1>(
            qPARTS1NATION3ORDERS1_it430->first);
        qPARTS1NATION3ORDERS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY,CUSTKEY)] += 
            -1*qORDERS1PARTS1NATION1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3ORDERS1LINEITEM1_it432 = qPARTS1NATION3ORDERS1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3ORDERS1LINEITEM1_end431 = qPARTS1NATION3ORDERS1LINEITEM1.end();
    for (
        ; qPARTS1NATION3ORDERS1LINEITEM1_it432 != qPARTS1NATION3ORDERS1LINEITEM1_end431; 
        ++qPARTS1NATION3ORDERS1LINEITEM1_it432)
    {
        int64_t x_qPARTS1NATION3ORDERS1LINEITEM_L__SUPPKEY = get<0>(
            qPARTS1NATION3ORDERS1LINEITEM1_it432->first);
        qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            x_qPARTS1NATION3ORDERS1LINEITEM_L__SUPPKEY,NATIONKEY,CUSTKEY)] += 
            -1*qLINEITEM3NATION1[make_tuple(
            x_qPARTS1NATION3ORDERS1LINEITEM_L__SUPPKEY,NATIONKEY)];
    }
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it434 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end433 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it434 != qSUPPLIER1_end433; ++qSUPPLIER1_it434)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it434->first);
        string P__MFGR = get<1>(qSUPPLIER1_it434->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1_it434->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY,N1__REGIONKEY)] += 
            -1*qCUSTOMER1SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER1NATION1_it436 
        = qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER1NATION1_end435 = qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it436 != qSUPPLIER1NATION1_end435; 
        ++qSUPPLIER1NATION1_it436)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1NATION1_it436->first);
        string P__MFGR = get<1>(qSUPPLIER1NATION1_it436->first);
        qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY)] += -1*qCUSTOMER1SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_it438 = qSUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_end437 = qSUPPLIER1PARTS1.end();
    for (
        ; qSUPPLIER1PARTS1_it438 != qSUPPLIER1PARTS1_end437; ++qSUPPLIER1PARTS1_it438)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1_it438->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER1PARTS1_it438->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1PARTS1_it438->first);
        qSUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY,
            N1__REGIONKEY)] += -1*qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,
            CUSTKEY)]*qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_it440 = qSUPPLIER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_end439 = qSUPPLIER1PARTS1NATION1.end();
    for (
        ; qSUPPLIER1PARTS1NATION1_it440 != qSUPPLIER1PARTS1NATION1_end439; 
        ++qSUPPLIER1PARTS1NATION1_it440)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1NATION1_it440->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(
            qSUPPLIER1PARTS1NATION1_it440->first);
        qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY)] += 
            -1*qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,CUSTKEY)];
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_CUSTOMER_sec_span, on_delete_CUSTOMER_usec_span);
}

void on_delete_SUPPLIER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t SUPPKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it442 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end441 = 
        q.end();
    for (; q_it442 != q_end441; ++q_it442)
    {
        string P__MFGR = get<0>(q_it442->first);
        int64_t N2__REGIONKEY = get<1>(q_it442->first);
        int64_t N1__REGIONKEY = get<2>(q_it442->first);
        int64_t C__NATIONKEY = get<3>(q_it442->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += -1*qSUPPLIER1[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it444 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end443 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it444 != qCUSTOMER1_end443; ++qCUSTOMER1_it444)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it444->first);
        string P__MFGR = get<1>(qCUSTOMER1_it444->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1_it444->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,N2__REGIONKEY)] += 
            -1*qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1NATION1_it446 
        = qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1NATION1_end445 = qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it446 != qCUSTOMER1NATION1_end445; 
        ++qCUSTOMER1NATION1_it446)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1NATION1_it446->first);
        string P__MFGR = get<2>(qCUSTOMER1NATION1_it446->first);
        qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,P__MFGR)] += -1*qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER1PARTS1_it448 
        = qCUSTOMER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_end447 = qCUSTOMER1PARTS1.end();
    for (
        ; qCUSTOMER1PARTS1_it448 != qCUSTOMER1PARTS1_end447; ++qCUSTOMER1PARTS1_it448)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1_it448->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1PARTS1_it448->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1PARTS1_it448->first);
        qCUSTOMER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)] += 
            -1*qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,SUPPKEY,
            x_qCUSTOMER_C__CUSTKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_it450 = qCUSTOMER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_end449 = qCUSTOMER1PARTS1NATION1.end();
    for (
        ; qCUSTOMER1PARTS1NATION1_it450 != qCUSTOMER1PARTS1NATION1_end449; 
        ++qCUSTOMER1PARTS1NATION1_it450)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1NATION1_it450->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1PARTS1NATION1_it450->first);
        qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,NATIONKEY)] += 
            -1*qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,SUPPKEY,x_qCUSTOMER_C__CUSTKEY)];
    }
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_it452 = 
        qLINEITEM3.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_end451 = 
        qLINEITEM3.end();
    for (; qLINEITEM3_it452 != qLINEITEM3_end451; ++qLINEITEM3_it452)
    {
        int64_t N2__REGIONKEY = get<1>(qLINEITEM3_it452->first);
        qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)] += -1*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }
    qLINEITEM3NATION1[make_tuple(SUPPKEY,NATIONKEY)] += -1;
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it454 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end453 
        = qNATION1.end();
    for (; qNATION1_it454 != qNATION1_end453; ++qNATION1_it454)
    {
        string P__MFGR = get<1>(qNATION1_it454->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it454->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it454->first);
        qNATION1[make_tuple(
            NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += -1*qSUPPLIER1[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it456 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end455 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it456 != qNATION1NATION1_end455; ++qNATION1NATION1_it456)
    {
        string P__MFGR = get<1>(qNATION1NATION1_it456->first);
        int64_t x_qNATION1NATION_N1__NATIONKEY = get<2>(
            qNATION1NATION1_it456->first);
        qNATION1NATION1[make_tuple(
            NATIONKEY,P__MFGR,x_qNATION1NATION_N1__NATIONKEY)] += 
            -1*qSUPPLIER1NATION1[make_tuple(
            SUPPKEY,P__MFGR,x_qNATION1NATION_N1__NATIONKEY)];
    }
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it458 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end457 = 
        qNATION2.end();
    for (; qNATION2_it458 != qNATION2_end457; ++qNATION2_it458)
    {
        string P__MFGR = get<0>(qNATION2_it458->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it458->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it458->first);
        qNATION2[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            -1*qSUPPLIER1NATION1[make_tuple(
            SUPPKEY,P__MFGR,x_qNATION_N1__NATIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,string>,double>::iterator qNATION3_it460 = qNATION3.begin(
        );
    map<tuple<int64_t,string>,double>::iterator qNATION3_end459 = qNATION3.end(
        );
    for (; qNATION3_it460 != qNATION3_end459; ++qNATION3_it460)
    {
        string P__MFGR = get<1>(qNATION3_it460->first);
        qNATION3[make_tuple(
            NATIONKEY,P__MFGR)] += -1*qSUPPLIER1NATION1[make_tuple(
            SUPPKEY,P__MFGR,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_it462 = qNATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_end461 = qNATION3ORDERS1.end();
    for (
        ; qNATION3ORDERS1_it462 != qNATION3ORDERS1_end461; ++qNATION3ORDERS1_it462)
    {
        int64_t x_qNATION3ORDERS_O__ORDERKEY = get<0>(
            qNATION3ORDERS1_it462->first);
        string P__MFGR = get<2>(qNATION3ORDERS1_it462->first);
        int64_t x_qNATION3ORDERS_O__CUSTKEY = get<3>(
            qNATION3ORDERS1_it462->first);
        qNATION3ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR,x_qNATION3ORDERS_O__CUSTKEY)] += 
            -1*qORDERS1SUPPLIER1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,SUPPKEY,
            P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it464 = 
        qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end463 = 
        qORDERS1.end();
    for (; qORDERS1_it464 != qORDERS1_end463; ++qORDERS1_it464)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it464->first);
        string P__MFGR = get<1>(qORDERS1_it464->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1_it464->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,P__MFGR,N2__REGIONKEY)] += 
            -1*qORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY,P__MFGR)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1NATION1_it466 = 
        qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1NATION1_end465 = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it466 != qORDERS1NATION1_end465; ++qORDERS1NATION1_it466)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1NATION1_it466->first);
        string P__MFGR = get<2>(qORDERS1NATION1_it466->first);
        qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)] += -1*qORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY,P__MFGR)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1PARTS1_it468 = 
        qORDERS1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS1PARTS1_end467 = qORDERS1PARTS1.end();
    for (
        ; qORDERS1PARTS1_it468 != qORDERS1PARTS1_end467; ++qORDERS1PARTS1_it468)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1PARTS1_it468->first);
        int64_t x_qORDERS1PARTS_P__PARTKEY = get<1>(
            qORDERS1PARTS1_it468->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1PARTS1_it468->first);
        qORDERS1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,N2__REGIONKEY)] += 
            -1*qORDERS1SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,
            SUPPKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS1PARTS1NATION1_it470 = qORDERS1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS1PARTS1NATION1_end469 = qORDERS1PARTS1NATION1.end();
    for (
        ; qORDERS1PARTS1NATION1_it470 != qORDERS1PARTS1NATION1_end469; 
        ++qORDERS1PARTS1NATION1_it470)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(
            qORDERS1PARTS1NATION1_it470->first);
        int64_t x_qORDERS1PARTS_P__PARTKEY = get<1>(
            qORDERS1PARTS1NATION1_it470->first);
        qORDERS1PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,NATIONKEY)] += 
            -1*qORDERS1SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it472 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end471 
        = qPARTS1.end();
    for (; qPARTS1_it472 != qPARTS1_end471; ++qPARTS1_it472)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it472->first);
        int64_t C__NATIONKEY = get<1>(qPARTS1_it472->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it472->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1_it472->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            -1*qSUPPLIER1PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it474 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end473 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it474 != qPARTS1NATION1_end473; ++qPARTS1NATION1_it474)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1_it474->first);
        int64_t C__NATIONKEY = get<2>(qPARTS1NATION1_it474->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1NATION1_it474->first);
        qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*qSUPPLIER1PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_it476 = qPARTS1NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_end475 = qPARTS1NATION1NATION1.end();
    for (
        ; qPARTS1NATION1NATION1_it476 != qPARTS1NATION1NATION1_end475; 
        ++qPARTS1NATION1NATION1_it476)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS1NATION1NATION1_it476->first);
        int64_t x_qPARTS1NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS1NATION1NATION1_it476->first);
        qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)] += 
            -1*qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it478 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end477 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it478 != qPARTS1NATION2_end477; ++qPARTS1NATION2_it478)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION2_it478->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION2_it478->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1NATION2_it478->first);
        qPARTS1NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            -1*qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,
            x_qPARTS1NATION_N1__NATIONKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_it480 = 
        qPARTS1NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_end479 = 
        qPARTS1NATION3.end();
    for (
        ; qPARTS1NATION3_it480 != qPARTS1NATION3_end479; ++qPARTS1NATION3_it480)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION3_it480->first);
        qPARTS1NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY)] += -1*qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_it482 = qPARTS1NATION3LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_end481 = qPARTS1NATION3LINEITEM1.end();
    for (
        ; qPARTS1NATION3LINEITEM1_it482 != qPARTS1NATION3LINEITEM1_end481; 
        ++qPARTS1NATION3LINEITEM1_it482)
    {
        int64_t x_qPARTS1NATION3LINEITEM_L__ORDERKEY = get<0>(
            qPARTS1NATION3LINEITEM1_it482->first);
        qPARTS1NATION3LINEITEM1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__ORDERKEY,NATIONKEY,SUPPKEY)] += 
            -1*qLINEITEM1NATION1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__ORDERKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1ORDERS1_it484 = qPARTS1NATION3LINEITEM1ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1ORDERS1_end483 = qPARTS1NATION3LINEITEM1ORDERS1.end();
    for (
        ; qPARTS1NATION3LINEITEM1ORDERS1_it484 != qPARTS1NATION3LINEITEM1ORDERS1_end483; 
        ++qPARTS1NATION3LINEITEM1ORDERS1_it484)
    {
        int64_t x_qPARTS1NATION3LINEITEM1ORDERS_O__CUSTKEY = get<0>(
            qPARTS1NATION3LINEITEM1ORDERS1_it484->first);
        qPARTS1NATION3LINEITEM1ORDERS1[make_tuple(
            x_qPARTS1NATION3LINEITEM1ORDERS_O__CUSTKEY,NATIONKEY,SUPPKEY)] += 
            -1*qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qPARTS1NATION3LINEITEM1ORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_it486 = qPARTS1NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_end485 = qPARTS1NATION3ORDERS1.end();
    for (
        ; qPARTS1NATION3ORDERS1_it486 != qPARTS1NATION3ORDERS1_end485; 
        ++qPARTS1NATION3ORDERS1_it486)
    {
        int64_t x_qPARTS1NATION3ORDERS_O__ORDERKEY = get<0>(
            qPARTS1NATION3ORDERS1_it486->first);
        int64_t x_qPARTS_P__PARTKEY = get<1>(
            qPARTS1NATION3ORDERS1_it486->first);
        int64_t x_qPARTS1NATION3ORDERS_O__CUSTKEY = get<3>(
            qPARTS1NATION3ORDERS1_it486->first);
        qPARTS1NATION3ORDERS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY,
            x_qPARTS1NATION3ORDERS_O__CUSTKEY)] += -1*qORDERS1SUPPLIER1PARTS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,
            SUPPKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3ORDERS1LINEITEM1_it488 = qPARTS1NATION3ORDERS1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3ORDERS1LINEITEM1_end487 = qPARTS1NATION3ORDERS1LINEITEM1.end();
    for (
        ; qPARTS1NATION3ORDERS1LINEITEM1_it488 != qPARTS1NATION3ORDERS1LINEITEM1_end487; 
        ++qPARTS1NATION3ORDERS1LINEITEM1_it488)
    {
        int64_t x_qPARTS1NATION3ORDERS_O__CUSTKEY = get<2>(
            qPARTS1NATION3ORDERS1LINEITEM1_it488->first);
        qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            SUPPKEY,NATIONKEY,x_qPARTS1NATION3ORDERS_O__CUSTKEY)] += 
            -1*qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__CUSTKEY,NATIONKEY)];
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_SUPPLIER_sec_span, on_delete_SUPPLIER_usec_span);
}

void on_delete_PARTS(
    ofstream* results, ofstream* log, ofstream* stats, int64_t PARTKEY,string 
    NAME,string MFGR,string BRAND,string TYPE,int SIZE,string CONTAINER,double 
    RETAILPRICE,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it490 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end489 = 
        q.end();
    for (; q_it490 != q_end489; ++q_it490)
    {
        int64_t N2__REGIONKEY = get<1>(q_it490->first);
        int64_t N1__REGIONKEY = get<2>(q_it490->first);
        int64_t C__NATIONKEY = get<3>(q_it490->first);
        q[make_tuple(
            MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += -1*qPARTS1[make_tuple(
            PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it492 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end491 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it492 != qCUSTOMER1_end491; ++qCUSTOMER1_it492)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it492->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1_it492->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,MFGR,N2__REGIONKEY)] += -1*qCUSTOMER1PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1NATION1_it494 
        = qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1NATION1_end493 = qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it494 != qCUSTOMER1NATION1_end493; 
        ++qCUSTOMER1NATION1_it494)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1NATION1_it494->first);
        int64_t x_qCUSTOMER1NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER1NATION1_it494->first);
        qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1NATION_N1__NATIONKEY,MFGR)] += 
            -1*qCUSTOMER1PARTS1NATION1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_it496 = qCUSTOMER1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_end495 = qCUSTOMER1SUPPLIER1.end();
    for (
        ; qCUSTOMER1SUPPLIER1_it496 != qCUSTOMER1SUPPLIER1_end495; 
        ++qCUSTOMER1SUPPLIER1_it496)
    {
        int64_t x_qCUSTOMER1SUPPLIER_S__SUPPKEY = get<0>(
            qCUSTOMER1SUPPLIER1_it496->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1SUPPLIER1_it496->first);
        qCUSTOMER1SUPPLIER1[make_tuple(
            x_qCUSTOMER1SUPPLIER_S__SUPPKEY,x_qCUSTOMER_C__CUSTKEY,MFGR)] += 
            -1*qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER1SUPPLIER_S__SUPPKEY,x_qCUSTOMER_C__CUSTKEY)];
    }
    qLINEITEM2[make_tuple(PARTKEY,MFGR)] += -1;
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it498 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end497 
        = qNATION1.end();
    for (; qNATION1_it498 != qNATION1_end497; ++qNATION1_it498)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it498->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it498->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it498->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*qPARTS1NATION1[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it500 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end499 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it500 != qNATION1NATION1_end499; ++qNATION1NATION1_it500)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1NATION1_it500->first);
        int64_t x_qNATION1NATION_N1__NATIONKEY = get<2>(
            qNATION1NATION1_it500->first);
        qNATION1NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR,x_qNATION1NATION_N1__NATIONKEY)] += 
            -1*qPARTS1NATION1NATION1[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,x_qNATION1NATION_N1__NATIONKEY)];
    }
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it502 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end501 = 
        qNATION2.end();
    for (; qNATION2_it502 != qNATION2_end501; ++qNATION2_it502)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it502->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it502->first);
        qNATION2[make_tuple(
            MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += -1*qPARTS1NATION2[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,string>,double>::iterator qNATION3_it504 = qNATION3.begin(
        );
    map<tuple<int64_t,string>,double>::iterator qNATION3_end503 = qNATION3.end(
        );
    for (; qNATION3_it504 != qNATION3_end503; ++qNATION3_it504)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION3_it504->first);
        qNATION3[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR)] += -1*qPARTS1NATION3[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_it506 = qNATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_end505 = qNATION3ORDERS1.end();
    for (
        ; qNATION3ORDERS1_it506 != qNATION3ORDERS1_end505; ++qNATION3ORDERS1_it506)
    {
        int64_t x_qNATION3ORDERS_O__ORDERKEY = get<0>(
            qNATION3ORDERS1_it506->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION3ORDERS1_it506->first);
        int64_t x_qNATION3ORDERS_O__CUSTKEY = get<3>(
            qNATION3ORDERS1_it506->first);
        qNATION3ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,x_qNATION_N1__NATIONKEY,MFGR,
            x_qNATION3ORDERS_O__CUSTKEY)] += -1*qPARTS1NATION3ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,PARTKEY,x_qNATION_N1__NATIONKEY,
            x_qNATION3ORDERS_O__CUSTKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it508 = 
        qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end507 = 
        qORDERS1.end();
    for (; qORDERS1_it508 != qORDERS1_end507; ++qORDERS1_it508)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it508->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1_it508->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,MFGR,N2__REGIONKEY)] += -1*qORDERS1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,N2__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1NATION1_it510 = 
        qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1NATION1_end509 = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it510 != qORDERS1NATION1_end509; ++qORDERS1NATION1_it510)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1NATION1_it510->first);
        int64_t x_qORDERS1NATION_N1__NATIONKEY = get<1>(
            qORDERS1NATION1_it510->first);
        qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1NATION_N1__NATIONKEY,MFGR)] += 
            -1*qORDERS1PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,x_qORDERS1NATION_N1__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1SUPPLIER1_it512 
        = qORDERS1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1SUPPLIER1_end511 = qORDERS1SUPPLIER1.end();
    for (
        ; qORDERS1SUPPLIER1_it512 != qORDERS1SUPPLIER1_end511; 
        ++qORDERS1SUPPLIER1_it512)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1SUPPLIER1_it512->first);
        int64_t x_qORDERS1SUPPLIER_S__SUPPKEY = get<1>(
            qORDERS1SUPPLIER1_it512->first);
        qORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1SUPPLIER_S__SUPPKEY,MFGR)] += 
            -1*qORDERS1SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,x_qORDERS1SUPPLIER_S__SUPPKEY)];
    }
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it514 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end513 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it514 != qSUPPLIER1_end513; ++qSUPPLIER1_it514)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it514->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1_it514->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1_it514->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*qSUPPLIER1PARTS1[make_tuple(
            PARTKEY,x_qSUPPLIER_S__SUPPKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER1NATION1_it516 
        = qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER1NATION1_end515 = qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it516 != qSUPPLIER1NATION1_end515; 
        ++qSUPPLIER1NATION1_it516)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1NATION1_it516->first);
        int64_t x_qSUPPLIER1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1NATION1_it516->first);
        qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,MFGR,x_qSUPPLIER1NATION_N1__NATIONKEY)] += 
            -1*qSUPPLIER1PARTS1NATION1[make_tuple(
            PARTKEY,x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER1NATION_N1__NATIONKEY)];
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_PARTS_sec_span, on_delete_PARTS_usec_span);
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

DBToaster::DemoDatasets::LineitemStream SSBLineitem("/Users/yanif/datasets/tpch/sf1/singlefile/lineitem.tbl.a",&DBToaster::DemoDatasets::parseLineitemField,16,65000000,512);

boost::shared_ptr<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor> SSBLineitem_adaptor(new DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor());
static int streamSSBLineitemId = 2;

struct on_insert_LINEITEM_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result>(data); 
        on_insert_LINEITEM(results, log, stats, input.orderkey,input.partkey,input.suppkey,input.linenumber,input.quantity,input.extendedprice,input.discount,input.tax);
    }
};

on_insert_LINEITEM_fun_obj fo_on_insert_LINEITEM_2;

DBToaster::DemoDatasets::CustomerStream SSBCustomer("/Users/yanif/datasets/tpch/sf1/singlefile/customer.tbl.a",&DBToaster::DemoDatasets::parseCustomerField,8,1600000, 512);

boost::shared_ptr<DBToaster::DemoDatasets::CustomerTupleAdaptor> SSBCustomer_adaptor(new DBToaster::DemoDatasets::CustomerTupleAdaptor());
static int streamSSBCustomerId = 3;

struct on_insert_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_insert_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_insert_CUSTOMER_fun_obj fo_on_insert_CUSTOMER_3;

DBToaster::DemoDatasets::SupplierStream SSBSupplier("/Users/yanif/datasets/tpch/sf1/singlefile/supplier.tbl.a",&DBToaster::DemoDatasets::parseSupplierField,7,110000,512);

boost::shared_ptr<DBToaster::DemoDatasets::SupplierTupleAdaptor> SSBSupplier_adaptor(new DBToaster::DemoDatasets::SupplierTupleAdaptor());
static int streamSSBSupplierId = 4;

struct on_insert_SUPPLIER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::SupplierTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::SupplierTupleAdaptor::Result>(data); 
        on_insert_SUPPLIER(results, log, stats, input.suppkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.comment);
    }
};

on_insert_SUPPLIER_fun_obj fo_on_insert_SUPPLIER_4;

DBToaster::DemoDatasets::PartStream SSBParts("/Users/yanif/datasets/tpch/sf1/singlefile/part.tbl.a",&DBToaster::DemoDatasets::parsePartField,9,2100000,512);

boost::shared_ptr<DBToaster::DemoDatasets::PartTupleAdaptor> SSBParts_adaptor(new DBToaster::DemoDatasets::PartTupleAdaptor());
static int streamSSBPartsId = 5;

struct on_insert_PARTS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::PartTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::PartTupleAdaptor::Result>(data); 
        on_insert_PARTS(results, log, stats, input.partkey,input.name,input.mfgr,input.brand,input.type,input.size,input.container,input.retailprice,input.comment);
    }
};

on_insert_PARTS_fun_obj fo_on_insert_PARTS_5;

struct on_delete_NATION_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::NationTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::NationTupleAdaptor::Result>(data); 
        on_delete_NATION(results, log, stats, input.nationkey,input.name,input.regionkey,input.comment);
    }
};

on_delete_NATION_fun_obj fo_on_delete_NATION_6;

struct on_delete_ORDERS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::OrderTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::OrderTupleAdaptor::Result>(data); 
        on_delete_ORDERS(results, log, stats, input.orderkey,input.custkey,input.orderstatus,input.totalprice,input.orderdate,input.orderpriority,input.clerk,input.shippriority,input.comment);
    }
};

on_delete_ORDERS_fun_obj fo_on_delete_ORDERS_7;

struct on_delete_LINEITEM_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result>(data); 
        on_delete_LINEITEM(results, log, stats, input.orderkey,input.partkey,input.suppkey,input.linenumber,input.quantity,input.extendedprice,input.discount,input.tax);
    }
};

on_delete_LINEITEM_fun_obj fo_on_delete_LINEITEM_8;

struct on_delete_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_delete_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_delete_CUSTOMER_fun_obj fo_on_delete_CUSTOMER_9;

struct on_delete_SUPPLIER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::SupplierTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::SupplierTupleAdaptor::Result>(data); 
        on_delete_SUPPLIER(results, log, stats, input.suppkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.comment);
    }
};

on_delete_SUPPLIER_fun_obj fo_on_delete_SUPPLIER_10;

struct on_delete_PARTS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::PartTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::PartTupleAdaptor::Result>(data); 
        on_delete_PARTS(results, log, stats, input.partkey,input.name,input.mfgr,input.brand,input.type,input.size,input.container,input.retailprice,input.comment);
    }
};

on_delete_PARTS_fun_obj fo_on_delete_PARTS_11;


void init(DBToaster::StandaloneEngine::FileMultiplexer& sources,
    DBToaster::StandaloneEngine::FileStreamDispatcher& router, 
    ofstream* results, ofstream* log, ofstream* stats)
{
    router.setFiles(results, log, stats);
    sources.addStream<DBToaster::DemoDatasets::nation>(&SSBNation, boost::ref(*SSBNation_adaptor), streamSSBNationId);
    router.addHandler(streamSSBNationId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_NATION_0));
    sources.addStream<DBToaster::DemoDatasets::order>(&SSBOrder, boost::ref(*SSBOrder_adaptor), streamSSBOrderId);
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_ORDERS_1));
    sources.addStream<DBToaster::DemoDatasets::lineitem>(&SSBLineitem, boost::ref(*SSBLineitem_adaptor), streamSSBLineitemId);
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_LINEITEM_2));
    sources.addStream<DBToaster::DemoDatasets::customer>(&SSBCustomer, boost::ref(*SSBCustomer_adaptor), streamSSBCustomerId);
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_CUSTOMER_3));
    sources.addStream<DBToaster::DemoDatasets::supplier>(&SSBSupplier, boost::ref(*SSBSupplier_adaptor), streamSSBSupplierId);
    router.addHandler(streamSSBSupplierId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_SUPPLIER_4));
    sources.addStream<DBToaster::DemoDatasets::part>(&SSBParts, boost::ref(*SSBParts_adaptor), streamSSBPartsId);
    router.addHandler(streamSSBPartsId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_PARTS_5));
    router.addHandler(streamSSBNationId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_NATION_6));
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_ORDERS_7));
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_LINEITEM_8));
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_CUSTOMER_9));
    router.addHandler(streamSSBSupplierId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_SUPPLIER_10));
    router.addHandler(streamSSBPartsId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_PARTS_11));
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