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
map<tuple<int64_t,int64_t,int64_t,int64_t>,double> qPARTS2NATION1;
map<tuple<int64_t,string>,int> qLINEITEM2;
map<tuple<int64_t,int64_t,int64_t>,double> qPARTS2NATION2;
map<tuple<int64_t,int64_t,int64_t>,double> qSUPPLIER3PARTS1NATION1;
map<tuple<int64_t,int64_t>,int> qLINEITEM3;
map<tuple<int64_t,int64_t>,double> qPARTS2NATION3;
map<tuple<int64_t,int64_t,string>,double> qORDERS3SUPPLIER1;
map<tuple<int64_t,string,int64_t,int64_t>,double> qSUPPLIER1;
map<tuple<int64_t,int64_t,string>,double> qORDERS1SUPPLIER1;
map<tuple<int64_t,int64_t>,int> qSUPPLIER2;
map<tuple<int64_t,int64_t,int64_t,int64_t>,double> qPARTS2NATION3ORDERS1;
map<tuple<int64_t,string,int64_t,int64_t>,double> qSUPPLIER3;
map<tuple<int64_t,int64_t,string>,double> qCUSTOMER3NATION1;
map<tuple<int64_t,int64_t,int64_t>,double> qORDERS1PARTS1;
map<tuple<int64_t,int64_t,string,int64_t>,double> qNATION3ORDERS1;
map<tuple<int64_t,string,int64_t,int64_t>,double> qNATION1;
map<tuple<int64_t,int64_t,int64_t>,double> qORDERS1PARTS1NATION1;
map<tuple<int64_t,int64_t,int64_t>,double> qCUSTOMER1PARTS1NATION1;
map<tuple<int64_t,int64_t,int64_t>,double> qORDERS3PARTS1;
map<tuple<int64_t,int64_t,int64_t,int64_t>,double> qSUPPLIER1PARTS1;
map<tuple<string,int64_t,int64_t>,double> qNATION2;
map<tuple<int64_t,int64_t,int64_t,int64_t>,double> qSUPPLIER3PARTS1;
map<tuple<int64_t,int64_t,int64_t>,double> qORDERS3SUPPLIER1PARTS1;
map<tuple<int64_t,string>,double> qNATION3;
map<tuple<int64_t,string,int64_t,int64_t>,double> qNATION4;
map<tuple<int64_t,int64_t,int64_t,int64_t>,double> qPARTS1NATION1;
map<tuple<string,int64_t,int64_t>,double> qNATION5;
map<tuple<int64_t,int64_t,int64_t>,double> qPARTS1NATION2;
map<tuple<int64_t,string,int64_t>,double> qSUPPLIER3NATION1;
map<tuple<int64_t,int64_t,string>,double> qCUSTOMER3SUPPLIER1;
map<tuple<int64_t,string>,double> qNATION6;
map<tuple<int64_t,int64_t>,double> qPARTS1NATION3;
map<tuple<int64_t,int64_t>,int> qLINEITEM1NATION1;
map<tuple<int64_t,int64_t,string>,double> qORDERS1NATION1;
map<tuple<int64_t,int64_t,string>,double> qCUSTOMER1SUPPLIER1;
map<tuple<int64_t,int64_t,int64_t>,double> qPARTS2NATION1NATION1;
map<tuple<int64_t,int64_t,int64_t>,int> qPARTS1NATION3LINEITEM1ORDERS1;
map<tuple<int64_t,int64_t,int64_t,int64_t>,double> qPARTS1;
map<tuple<int64_t,string,int64_t>,double> qNATION1NATION1;
map<tuple<int64_t,int64_t,int64_t,int64_t>,double> qPARTS2;
map<tuple<int64_t,int64_t,string,int64_t>,double> qNATION6ORDERS1;
map<tuple<int64_t,int64_t>,int> qLINEITEM1CUSTOMER1;
map<tuple<int64_t,int64_t,int64_t>,double> qCUSTOMER1SUPPLIER1PARTS1;
map<tuple<int64_t,string,int64_t>,double> qORDERS1;
map<tuple<int64_t,string,int64_t>,double> qCUSTOMER1;
map<tuple<int64_t,int64_t,int64_t>,double> qORDERS3PARTS1NATION1;
map<tuple<int64_t,int64_t,int64_t>,int> qORDERS2;
map<tuple<int64_t,int64_t,int64_t>,double> qCUSTOMER3PARTS1NATION1;
map<tuple<int64_t,int64_t>,int> qCUSTOMER2;
map<tuple<int64_t,string,int64_t>,double> qORDERS3;
map<tuple<int64_t,string,int64_t>,double> qCUSTOMER3;
map<tuple<int64_t,int64_t,int64_t>,int> qPARTS1NATION3LINEITEM1;
map<tuple<int64_t,string,int64_t>,double> qNATION4NATION1;
map<tuple<int64_t,int64_t,string>,double> qCUSTOMER1NATION1;
map<tuple<int64_t,int64_t>,int> qLINEITEM1NATION1ORDERS1;
map<tuple<int64_t,int64_t,int64_t,int64_t>,double> qPARTS1NATION3ORDERS1;
map<tuple<string,int64_t,int64_t,int64_t>,double> q;
map<tuple<int64_t,int64_t,int64_t>,double> qSUPPLIER1PARTS1NATION1;
map<tuple<int64_t,int64_t,int64_t>,int> qPARTS1NATION3ORDERS1LINEITEM1;
map<tuple<int64_t,int64_t,int64_t>,double> qORDERS1SUPPLIER1PARTS1;
map<tuple<int64_t,string,int64_t>,double> qSUPPLIER1NATION1;
map<tuple<int64_t,int64_t>,int> qLINEITEM3NATION1;
map<tuple<int64_t,int64_t,string>,double> qORDERS3NATION1;
map<tuple<int64_t,int64_t,int64_t>,double> qCUSTOMER1PARTS1;
map<tuple<int64_t,int64_t,int64_t>,double> qCUSTOMER3PARTS1;
map<tuple<int64_t,int64_t,int64_t>,double> qCUSTOMER3SUPPLIER1PARTS1;
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

   cout << "qPARTS2NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS2NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS2NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS2NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qLINEITEM2 size: " << (((sizeof(map<tuple<int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string>,int>::key_type, map<tuple<int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,string>,int>::value_type>, map<tuple<int64_t,string>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM2" << "," << (((sizeof(map<tuple<int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string>,int>::key_type, map<tuple<int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,string>,int>::value_type>, map<tuple<int64_t,string>,int>::key_compare>))) << endl;

   cout << "qPARTS2NATION2 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS2NATION2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS2NATION2" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS2NATION2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qSUPPLIER3PARTS1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER3PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER3PARTS1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER3PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qLINEITEM3 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM3" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qPARTS2NATION3 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS2NATION3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS2NATION3" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS2NATION3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS3SUPPLIER1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS3SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS3SUPPLIER1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS3SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

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

   cout << "qPARTS2NATION3ORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS2NATION3ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS2NATION3ORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS2NATION3ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qSUPPLIER3 size: " << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER3" << "," << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER3NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER3NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "qORDERS1PARTS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1PARTS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qNATION3ORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION3ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION3ORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION3ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qNATION1 size: " << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION1" << "," << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS1PARTS1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1PARTS1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER1PARTS1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1PARTS1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS3PARTS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS3PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS3PARTS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS3PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

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

   cout << "qSUPPLIER3PARTS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER3PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER3PARTS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER3PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS3SUPPLIER1PARTS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS3SUPPLIER1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS3SUPPLIER1PARTS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS3SUPPLIER1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qNATION3 size: " << (((sizeof(map<tuple<int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string>,double>::key_type, map<tuple<int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,string>,double>::value_type>, map<tuple<int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION3" << "," << (((sizeof(map<tuple<int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string>,double>::key_type, map<tuple<int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,string>,double>::value_type>, map<tuple<int64_t,string>,double>::key_compare>))) << endl;

   cout << "qNATION4 size: " << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION4.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION4" << "," << (((sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION4.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qPARTS1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qNATION5 size: " << (((sizeof(map<tuple<string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION5.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t,int64_t>,double>::key_type, map<tuple<string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t,int64_t>,double>::value_type>, map<tuple<string,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION5" << "," << (((sizeof(map<tuple<string,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION5.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t,int64_t>,double>::key_type, map<tuple<string,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t,int64_t>,double>::value_type>, map<tuple<string,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qPARTS1NATION2 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS1NATION2" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qSUPPLIER3NATION1 size: " << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER3NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER3NATION1" << "," << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER3NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER3SUPPLIER1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER3SUPPLIER1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "qNATION6 size: " << (((sizeof(map<tuple<int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION6.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string>,double>::key_type, map<tuple<int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,string>,double>::value_type>, map<tuple<int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION6" << "," << (((sizeof(map<tuple<int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION6.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string>,double>::key_type, map<tuple<int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,string>,double>::value_type>, map<tuple<int64_t,string>,double>::key_compare>))) << endl;

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

   cout << "qPARTS2NATION1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS2NATION1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS2NATION1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS2NATION1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

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

   cout << "qPARTS2 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS2" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qNATION6ORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION6ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION6ORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION6ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

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

   cout << "qORDERS3PARTS1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS3PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS3PARTS1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS3PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS2 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS2" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER3PARTS1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER3PARTS1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER2 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER2" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qORDERS3 size: " << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS3" << "," << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER3 size: " << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER3" << "," << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qPARTS1NATION3LINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION3LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS1NATION3LINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION3LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qNATION4NATION1 size: " << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION4NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION4NATION1" << "," << (((sizeof(map<tuple<int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION4NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,string,int64_t>,double>::key_compare>))) << endl;

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

   cout << "qPARTS1NATION3ORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION3ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS1NATION3ORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION3ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "q size: " << (((sizeof(map<tuple<string,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<string,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<string,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "q" << "," << (((sizeof(map<tuple<string,int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t,int64_t,int64_t>,double>::key_type, map<tuple<string,int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<string,int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qSUPPLIER1PARTS1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER1PARTS1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1PARTS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qPARTS1NATION3ORDERS1LINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION3ORDERS1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qPARTS1NATION3ORDERS1LINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qPARTS1NATION3ORDERS1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

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

   cout << "qLINEITEM3NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM3NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM3NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM3NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qORDERS3NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS3NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS3NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS3NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER1PARTS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1PARTS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER3PARTS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER3PARTS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER3SUPPLIER1PARTS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3SUPPLIER1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER3SUPPLIER1PARTS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3SUPPLIER1PARTS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

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


multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> > CUSTOMER;
multiset<tuple<int64_t,string,string,string,string,int,string,double,string> > PARTS;

map<int64_t, list<tuple<int64_t, int64_t> > > PK_idx_OK_SK; // maintained by LINEITEM

map<int64_t, unordered_set<int64_t> > CK_idx_OK; // maintained by ORDERS

map<int64_t,int64_t> SK_idx_NK; // maintained by SUPPLIER

map<int64_t,int64_t> CK_idx_NK; // maintained by CUSTOMER

map<int64_t,int64_t> NK_idx_RK; // maintained by NATION

void indexed_result_domain_maintenance()
{
    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
        >::iterator CUSTOMER_it10 = CUSTOMER.begin();
                
    multiset<tuple<int64_t,string,string,int64_t,string,double,string,string> 
        >::iterator CUSTOMER_end9 = CUSTOMER.end();
    for (; CUSTOMER_it10 != CUSTOMER_end9; ++CUSTOMER_it10)
    {
        int64_t C__CUSTKEY = get<0>(*CUSTOMER_it10);
        int64_t C__NATIONKEY = get<3>(*CUSTOMER_it10);

        // (CK,NK) in CUSTOMER        
        map<int64_t,int64_t>::iterator nk_found = CK_idx_NK.find(C__CUSTKEY);
        if ( nk_found != CK_idx_NK.end() )
        {
            int64_t N_NATIONKEY = nk_found->second;

            // (NK,RK) in REGION
            map<int64_t,int64_t>::iterator rk_found = NK_idx_RK.find(N_NATIONKEY);
            if ( rk_found != NK_idx_RK.end() )
            {
                int64_t REGIONKEY = rk_found->second;

                // For all parts,
                multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                    >::iterator PARTS_it8 = PARTS.begin();
                    
                multiset<tuple<int64_t,string,string,string,string,int,string,double,string> 
                    >::iterator PARTS_end7 = PARTS.end();
                for (; PARTS_it8 != PARTS_end7; ++PARTS_it8)
                {
                    int64_t P__PARTKEY = get<0>(*PARTS_it8);
                    string P__MFGR = get<2>(*PARTS_it8);

                    tuple<string,int64_t,int64_t,int64_t> key =
                        make_tuple(P__MFGR,REGIONKEY,REGIONKEY,C__NATIONKEY);

                    // (CK,RK,RK,P_MFGR) not in q
                    if ( q.find(key) == q.end() )
                    {
                        // (OK,CK) in ORDERS
                        map<int64_t, unordered_set<int64_t> >::iterator ck_orders_found =
                            CK_idx_OK.find(C__CUSTKEY);

                        // (OK,SK,PK) in LINEITEM
                        map<int64_t,list<tuple<int64_t,int64_t> > >::iterator ok_sk_found =
                            PK_idx_OK_SK.find(P__PARTKEY);

                        if ( (ck_orders_found != CK_idx_OK.end()) && (ok_sk_found != PK_idx_OK_SK.end()) )
                        {
                            unordered_set<int64_t>& ck_orders = ck_orders_found->second;
                            list<tuple<int64_t,int64_t> >::iterator ok_sk_it = ok_sk_found->second.begin();
                            list<tuple<int64_t,int64_t> >::iterator ok_sk_end = ok_sk_found->second.end();

                            for (; ok_sk_it != ok_sk_end; ++ok_sk_it)
                            {
                                int64_t O__ORDERKEY = get<0>(*ok_sk_it);
                                int64_t S__SUPPKEY = get<1>(*ok_sk_it);

                                unordered_set<int64_t>::iterator ok_found = ck_orders.find(O__ORDERKEY);

                                // (SK,NK2) in SUPPLIER
                                map<int64_t, int64_t>::iterator nk_found = SK_idx_NK.find(S__SUPPKEY);

                                // (PK,CK) in LINEITEM join ORDERS
                                // && (NK=NK2)
                                if ( (ok_found != ck_orders.end())
                                     && (nk_found != SK_idx_NK.end() && nk_found->second == N_NATIONKEY) )
                                {
                                        q[make_tuple(P__MFGR,REGIONKEY,REGIONKEY,C__NATIONKEY)] = 0;
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
// Triggers

void on_insert_NATION(
    ofstream* results, ofstream* log, ofstream* stats, int64_t NATIONKEY,string 
    NAME,int64_t REGIONKEY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);

    NK_idx_RK[NATIONKEY] = REGIONKEY;

    indexed_result_domain_maintenance();

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it2 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end1 = q.end();
    for (; q_it2 != q_end1; ++q_it2)
    {
        string P__MFGR = get<0>(q_it2->first);
        q[make_tuple(
            P__MFGR,REGIONKEY,REGIONKEY,NATIONKEY)] += qNATION3[make_tuple(
            NATIONKEY,P__MFGR)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it4 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end3 = q.end();
    for (; q_it4 != q_end3; ++q_it4)
    {
        string P__MFGR = get<0>(q_it4->first);
        q[make_tuple(
            P__MFGR,REGIONKEY,REGIONKEY,NATIONKEY)] += qNATION6[make_tuple(
            NATIONKEY,P__MFGR)]*-1;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it6 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end5 = q.end();
    for (; q_it6 != q_end5; ++q_it6)
    {
        string P__MFGR = get<0>(q_it6->first);
        int64_t N2__REGIONKEY = get<1>(q_it6->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,REGIONKEY,NATIONKEY)] += qNATION2[make_tuple(
            P__MFGR,NATIONKEY,N2__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it8 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end7 = q.end();
    for (; q_it8 != q_end7; ++q_it8)
    {
        string P__MFGR = get<0>(q_it8->first);
        int64_t N2__REGIONKEY = get<1>(q_it8->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,REGIONKEY,NATIONKEY)] += qNATION5[make_tuple(
            P__MFGR,NATIONKEY,N2__REGIONKEY)]*-1;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it10 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end9 = q.end();
    for (; q_it10 != q_end9; ++q_it10)
    {
        string P__MFGR = get<0>(q_it10->first);
        int64_t REGIONKEY = get<2>(q_it10->first);
        int64_t C__NATIONKEY = get<3>(q_it10->first);
        q[make_tuple(
            P__MFGR,REGIONKEY,REGIONKEY,C__NATIONKEY)] += qNATION1[make_tuple(
            NATIONKEY,P__MFGR,C__NATIONKEY,REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it12 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end11 = q.end();
    for (; q_it12 != q_end11; ++q_it12)
    {
        string P__MFGR = get<0>(q_it12->first);
        int64_t REGIONKEY = get<2>(q_it12->first);
        int64_t C__NATIONKEY = get<3>(q_it12->first);
        q[make_tuple(
            P__MFGR,REGIONKEY,REGIONKEY,C__NATIONKEY)] += qNATION4[make_tuple(
            NATIONKEY,P__MFGR,C__NATIONKEY,REGIONKEY)]*-1;
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it14 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end13 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it14 != qCUSTOMER1_end13; ++qCUSTOMER1_it14)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it14->first);
        string P__MFGR = get<1>(qCUSTOMER1_it14->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,REGIONKEY)] += qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_it16 = qCUSTOMER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER1PARTS1_end15 
        = qCUSTOMER1PARTS1.end();
    for (
        ; qCUSTOMER1PARTS1_it16 != qCUSTOMER1PARTS1_end15; ++qCUSTOMER1PARTS1_it16)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1_it16->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1PARTS1_it16->first);
        qCUSTOMER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,REGIONKEY)] += 
            qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,NATIONKEY)];
    }
    qCUSTOMER2[make_tuple(NATIONKEY,REGIONKEY)] += 1;
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_it18 = 
        qCUSTOMER3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_end17 = 
        qCUSTOMER3.end();
    for (; qCUSTOMER3_it18 != qCUSTOMER3_end17; ++qCUSTOMER3_it18)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER3_it18->first);
        string P__MFGR = get<1>(qCUSTOMER3_it18->first);
        qCUSTOMER3[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,REGIONKEY)] += qCUSTOMER3NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER3PARTS1_it20 = qCUSTOMER3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER3PARTS1_end19 
        = qCUSTOMER3PARTS1.end();
    for (
        ; qCUSTOMER3PARTS1_it20 != qCUSTOMER3PARTS1_end19; ++qCUSTOMER3PARTS1_it20)
    {
        int64_t x_qCUSTOMER3PARTS_P__PARTKEY = get<0>(
            qCUSTOMER3PARTS1_it20->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER3PARTS1_it20->first);
        qCUSTOMER3PARTS1[make_tuple(
            x_qCUSTOMER3PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,REGIONKEY)] += 
            qCUSTOMER3PARTS1NATION1[make_tuple(
            x_qCUSTOMER3PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_it22 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_end21 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it22 != qLINEITEM1_end21; ++qLINEITEM1_it22)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it22->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,REGIONKEY)] += qLINEITEM1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_it24 = 
        qLINEITEM3.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_end23 = qLINEITEM3.end();
    for (; qLINEITEM3_it24 != qLINEITEM3_end23; ++qLINEITEM3_it24)
    {
        int64_t x_qLINEITEM_L__SUPPKEY = get<0>(qLINEITEM3_it24->first);
        qLINEITEM3[make_tuple(
            x_qLINEITEM_L__SUPPKEY,REGIONKEY)] += qLINEITEM3NATION1[make_tuple(
            x_qLINEITEM_L__SUPPKEY,NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_it26 = 
        qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_end25 = qNATION1.end();
    for (; qNATION1_it26 != qNATION1_end25; ++qNATION1_it26)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it26->first);
        string P__MFGR = get<1>(qNATION1_it26->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY,REGIONKEY)] += 
            qNATION1NATION1[make_tuple(x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it28 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end27 = 
        qNATION2.end();
    for (; qNATION2_it28 != qNATION2_end27; ++qNATION2_it28)
    {
        string P__MFGR = get<0>(qNATION2_it28->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it28->first);
        qNATION2[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,REGIONKEY)] += qNATION1NATION1[make_tuple(
            NATIONKEY,P__MFGR,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION4_it30 = 
        qNATION4.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION4_end29 = qNATION4.end();
    for (; qNATION4_it30 != qNATION4_end29; ++qNATION4_it30)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4_it30->first);
        string P__MFGR = get<1>(qNATION4_it30->first);
        qNATION4[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY,REGIONKEY)] += 
            qNATION4NATION1[make_tuple(x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_it32 = 
        qNATION5.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_end31 = 
        qNATION5.end();
    for (; qNATION5_it32 != qNATION5_end31; ++qNATION5_it32)
    {
        string P__MFGR = get<0>(qNATION5_it32->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION5_it32->first);
        qNATION5[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,REGIONKEY)] += qNATION4NATION1[make_tuple(
            NATIONKEY,P__MFGR,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it34 = 
        qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end33 = 
        qORDERS1.end();
    for (; qORDERS1_it34 != qORDERS1_end33; ++qORDERS1_it34)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it34->first);
        string P__MFGR = get<1>(qORDERS1_it34->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,P__MFGR,REGIONKEY)] += qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1PARTS1_it36 = 
        qORDERS1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1PARTS1_end35 = 
        qORDERS1PARTS1.end();
    for (; qORDERS1PARTS1_it36 != qORDERS1PARTS1_end35; ++qORDERS1PARTS1_it36)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1PARTS1_it36->first);
        int64_t x_qORDERS1PARTS_P__PARTKEY = get<1>(qORDERS1PARTS1_it36->first);
        qORDERS1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,REGIONKEY)] += 
            qORDERS1PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_it38 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_end37 = 
        qORDERS2.end();
    for (; qORDERS2_it38 != qORDERS2_end37; ++qORDERS2_it38)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<0>(qORDERS2_it38->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,REGIONKEY)] += 
            qLINEITEM1NATION1ORDERS1[make_tuple(x_qORDERS_O__CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_it40 = 
        qORDERS3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_end39 = 
        qORDERS3.end();
    for (; qORDERS3_it40 != qORDERS3_end39; ++qORDERS3_it40)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3_it40->first);
        string P__MFGR = get<1>(qORDERS3_it40->first);
        qORDERS3[make_tuple(
            x_qORDERS_O__ORDERKEY,P__MFGR,REGIONKEY)] += qORDERS3NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS3PARTS1_it42 = 
        qORDERS3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS3PARTS1_end41 = 
        qORDERS3PARTS1.end();
    for (; qORDERS3PARTS1_it42 != qORDERS3PARTS1_end41; ++qORDERS3PARTS1_it42)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3PARTS1_it42->first);
        int64_t x_qORDERS3PARTS_P__PARTKEY = get<1>(qORDERS3PARTS1_it42->first);
        qORDERS3PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS3PARTS_P__PARTKEY,REGIONKEY)] += 
            qORDERS3PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS3PARTS_P__PARTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_it44 = 
        qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_end43 = qPARTS1.end();
    for (; qPARTS1_it44 != qPARTS1_end43; ++qPARTS1_it44)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it44->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,REGIONKEY,REGIONKEY)] += 
            qPARTS1NATION3[make_tuple(x_qPARTS_P__PARTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_it46 = 
        qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_end45 = qPARTS1.end();
    for (; qPARTS1_it46 != qPARTS1_end45; ++qPARTS1_it46)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it46->first);
        int64_t C__NATIONKEY = get<1>(qPARTS1_it46->first);
        int64_t REGIONKEY = get<3>(qPARTS1_it46->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,REGIONKEY,REGIONKEY)] += 
            qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,C__NATIONKEY,REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_it48 = 
        qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_end47 = qPARTS1.end();
    for (; qPARTS1_it48 != qPARTS1_end47; ++qPARTS1_it48)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it48->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it48->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY,REGIONKEY)] += 
            qPARTS1NATION2[make_tuple(x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it50 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end49 = qPARTS1NATION1.end();
    for (; qPARTS1NATION1_it50 != qPARTS1NATION1_end49; ++qPARTS1NATION1_it50)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1_it50->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1_it50->first);
        qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,NATIONKEY,REGIONKEY)] += 
            qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it52 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_end51 = 
        qPARTS1NATION2.end();
    for (; qPARTS1NATION2_it52 != qPARTS1NATION2_end51; ++qPARTS1NATION2_it52)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION2_it52->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION2_it52->first);
        qPARTS1NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,REGIONKEY)] += 
            qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS2_it54 = 
        qPARTS2.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2_end53 = qPARTS2.end();
    for (; qPARTS2_it54 != qPARTS2_end53; ++qPARTS2_it54)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2_it54->first);
        qPARTS2[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,REGIONKEY,REGIONKEY)] += 
            qPARTS2NATION3[make_tuple(x_qPARTS_P__PARTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS2_it56 = 
        qPARTS2.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2_end55 = qPARTS2.end();
    for (; qPARTS2_it56 != qPARTS2_end55; ++qPARTS2_it56)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2_it56->first);
        int64_t C__NATIONKEY = get<1>(qPARTS2_it56->first);
        int64_t REGIONKEY = get<3>(qPARTS2_it56->first);
        qPARTS2[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,REGIONKEY,REGIONKEY)] += 
            qPARTS2NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,C__NATIONKEY,REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS2_it58 = 
        qPARTS2.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2_end57 = qPARTS2.end();
    for (; qPARTS2_it58 != qPARTS2_end57; ++qPARTS2_it58)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2_it58->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2_it58->first);
        qPARTS2[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY,REGIONKEY)] += 
            qPARTS2NATION2[make_tuple(x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_it60 = qPARTS2NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_end59 = qPARTS2NATION1.end();
    for (; qPARTS2NATION1_it60 != qPARTS2NATION1_end59; ++qPARTS2NATION1_it60)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION1_it60->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION1_it60->first);
        qPARTS2NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,NATIONKEY,REGIONKEY)] += 
            qPARTS2NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS2NATION2_it62 = 
        qPARTS2NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS2NATION2_end61 = 
        qPARTS2NATION2.end();
    for (; qPARTS2NATION2_it62 != qPARTS2NATION2_end61; ++qPARTS2NATION2_it62)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION2_it62->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION2_it62->first);
        qPARTS2NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,REGIONKEY)] += 
            qPARTS2NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,x_qPARTS2NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qSUPPLIER1_it64 
        = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_end63 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it64 != qSUPPLIER1_end63; ++qSUPPLIER1_it64)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it64->first);
        string P__MFGR = get<1>(qSUPPLIER1_it64->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY,REGIONKEY)] += 
            qSUPPLIER1NATION1[make_tuple(x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_it66 = qSUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_end65 = qSUPPLIER1PARTS1.end();
    for (
        ; qSUPPLIER1PARTS1_it66 != qSUPPLIER1PARTS1_end65; ++qSUPPLIER1PARTS1_it66)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1_it66->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER1PARTS1_it66->first);
        qSUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY,REGIONKEY)] += 
            qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY)];
    }
    qSUPPLIER2[make_tuple(NATIONKEY,REGIONKEY)] += 1;
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qSUPPLIER3_it68 
        = qSUPPLIER3.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER3_end67 = qSUPPLIER3.end();
    for (; qSUPPLIER3_it68 != qSUPPLIER3_end67; ++qSUPPLIER3_it68)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER3_it68->first);
        string P__MFGR = get<1>(qSUPPLIER3_it68->first);
        qSUPPLIER3[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY,REGIONKEY)] += 
            qSUPPLIER3NATION1[make_tuple(x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1_it70 = qSUPPLIER3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1_end69 = qSUPPLIER3PARTS1.end();
    for (
        ; qSUPPLIER3PARTS1_it70 != qSUPPLIER3PARTS1_end69; ++qSUPPLIER3PARTS1_it70)
    {
        int64_t x_qSUPPLIER3PARTS_P__PARTKEY = get<0>(
            qSUPPLIER3PARTS1_it70->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER3PARTS1_it70->first);
        qSUPPLIER3PARTS1[make_tuple(
            x_qSUPPLIER3PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY,REGIONKEY)] += 
            qSUPPLIER3PARTS1NATION1[make_tuple(
            x_qSUPPLIER3PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY)];
    }

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

    CK_idx_OK[CUSTKEY].insert(ORDERKEY);

    indexed_result_domain_maintenance();

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it72 = q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end71 = q.end();
    for (; q_it72 != q_end71; ++q_it72)
    {
        string P__MFGR = get<0>(q_it72->first);
        int64_t N2__REGIONKEY = get<1>(q_it72->first);
        int64_t N1__REGIONKEY = get<2>(q_it72->first);
        int64_t C__NATIONKEY = get<3>(q_it72->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += qORDERS1[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it74 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end73 = q.end();
    for (; q_it74 != q_end73; ++q_it74)
    {
        string P__MFGR = get<0>(q_it74->first);
        int64_t N2__REGIONKEY = get<1>(q_it74->first);
        int64_t N1__REGIONKEY = get<2>(q_it74->first);
        int64_t C__NATIONKEY = get<3>(q_it74->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += qORDERS3[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)]*-1;
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it76 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end75 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it76 != qCUSTOMER1_end75; ++qCUSTOMER1_it76)
    {
        string P__MFGR = get<1>(qCUSTOMER1_it76->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1_it76->first);
        qCUSTOMER1[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)] += qORDERS1[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1NATION1_it78 = qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1NATION1_end77 
        = qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it78 != qCUSTOMER1NATION1_end77; ++qCUSTOMER1NATION1_it78)
    {
        int64_t x_qCUSTOMER1NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER1NATION1_it78->first);
        string P__MFGR = get<2>(qCUSTOMER1NATION1_it78->first);
        qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,x_qCUSTOMER1NATION_N1__NATIONKEY,P__MFGR)] += 
            qORDERS1NATION1[make_tuple(ORDERKEY,x_qCUSTOMER1NATION_N1__NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_it80 = qCUSTOMER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER1PARTS1_end79 
        = qCUSTOMER1PARTS1.end();
    for (
        ; qCUSTOMER1PARTS1_it80 != qCUSTOMER1PARTS1_end79; ++qCUSTOMER1PARTS1_it80)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1_it80->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1PARTS1_it80->first);
        qCUSTOMER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)] += 
            qORDERS1PARTS1[make_tuple(ORDERKEY,x_qCUSTOMER1PARTS_P__PARTKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_it82 = qCUSTOMER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_end81 = qCUSTOMER1PARTS1NATION1.end();
    for (
        ; qCUSTOMER1PARTS1NATION1_it82 != qCUSTOMER1PARTS1NATION1_end81; 
        ++qCUSTOMER1PARTS1NATION1_it82)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1NATION1_it82->first);
        int64_t x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY = get<2>(
            qCUSTOMER1PARTS1NATION1_it82->first);
        qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,CUSTKEY,
            x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY)] += qORDERS1PARTS1NATION1[make_tuple(
            ORDERKEY,x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1SUPPLIER1_it84 = qCUSTOMER1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_end83 = qCUSTOMER1SUPPLIER1.end();
    for (
        ; qCUSTOMER1SUPPLIER1_it84 != qCUSTOMER1SUPPLIER1_end83; 
        ++qCUSTOMER1SUPPLIER1_it84)
    {
        int64_t x_qCUSTOMER1SUPPLIER_S__SUPPKEY = get<0>(
            qCUSTOMER1SUPPLIER1_it84->first);
        string P__MFGR = get<2>(qCUSTOMER1SUPPLIER1_it84->first);
        qCUSTOMER1SUPPLIER1[make_tuple(
            x_qCUSTOMER1SUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)] += 
            qORDERS1SUPPLIER1[make_tuple(ORDERKEY,x_qCUSTOMER1SUPPLIER_S__SUPPKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1SUPPLIER1PARTS1_it86 = qCUSTOMER1SUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1SUPPLIER1PARTS1_end85 = qCUSTOMER1SUPPLIER1PARTS1.end();
    for (
        ; qCUSTOMER1SUPPLIER1PARTS1_it86 != qCUSTOMER1SUPPLIER1PARTS1_end85; 
        ++qCUSTOMER1SUPPLIER1PARTS1_it86)
    {
        int64_t x_qCUSTOMER1SUPPLIER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1SUPPLIER1PARTS1_it86->first);
        int64_t x_qCUSTOMER1SUPPLIER_S__SUPPKEY = get<1>(
            qCUSTOMER1SUPPLIER1PARTS1_it86->first);
        qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER1SUPPLIER1PARTS_P__PARTKEY,x_qCUSTOMER1SUPPLIER_S__SUPPKEY,CUSTKEY)] 
            += qORDERS1SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qCUSTOMER1SUPPLIER1PARTS_P__PARTKEY,
            x_qCUSTOMER1SUPPLIER_S__SUPPKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_it88 = 
        qCUSTOMER3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_end87 = 
        qCUSTOMER3.end();
    for (; qCUSTOMER3_it88 != qCUSTOMER3_end87; ++qCUSTOMER3_it88)
    {
        string P__MFGR = get<1>(qCUSTOMER3_it88->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER3_it88->first);
        qCUSTOMER3[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)] += qORDERS3[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER3NATION1_it90 = qCUSTOMER3NATION1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER3NATION1_end89 
        = qCUSTOMER3NATION1.end();
    for (
        ; qCUSTOMER3NATION1_it90 != qCUSTOMER3NATION1_end89; ++qCUSTOMER3NATION1_it90)
    {
        int64_t x_qCUSTOMER3NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER3NATION1_it90->first);
        string P__MFGR = get<2>(qCUSTOMER3NATION1_it90->first);
        qCUSTOMER3NATION1[make_tuple(
            CUSTKEY,x_qCUSTOMER3NATION_N1__NATIONKEY,P__MFGR)] += 
            qORDERS3NATION1[make_tuple(ORDERKEY,x_qCUSTOMER3NATION_N1__NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER3PARTS1_it92 = qCUSTOMER3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER3PARTS1_end91 
        = qCUSTOMER3PARTS1.end();
    for (
        ; qCUSTOMER3PARTS1_it92 != qCUSTOMER3PARTS1_end91; ++qCUSTOMER3PARTS1_it92)
    {
        int64_t x_qCUSTOMER3PARTS_P__PARTKEY = get<0>(
            qCUSTOMER3PARTS1_it92->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER3PARTS1_it92->first);
        qCUSTOMER3PARTS1[make_tuple(
            x_qCUSTOMER3PARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)] += 
            qORDERS3PARTS1[make_tuple(ORDERKEY,x_qCUSTOMER3PARTS_P__PARTKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3PARTS1NATION1_it94 = qCUSTOMER3PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3PARTS1NATION1_end93 = qCUSTOMER3PARTS1NATION1.end();
    for (
        ; qCUSTOMER3PARTS1NATION1_it94 != qCUSTOMER3PARTS1NATION1_end93; 
        ++qCUSTOMER3PARTS1NATION1_it94)
    {
        int64_t x_qCUSTOMER3PARTS_P__PARTKEY = get<0>(
            qCUSTOMER3PARTS1NATION1_it94->first);
        int64_t x_qCUSTOMER3PARTS1NATION_N1__NATIONKEY = get<2>(
            qCUSTOMER3PARTS1NATION1_it94->first);
        qCUSTOMER3PARTS1NATION1[make_tuple(
            x_qCUSTOMER3PARTS_P__PARTKEY,CUSTKEY,
            x_qCUSTOMER3PARTS1NATION_N1__NATIONKEY)] += qORDERS3PARTS1NATION1[make_tuple(
            ORDERKEY,x_qCUSTOMER3PARTS_P__PARTKEY,x_qCUSTOMER3PARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER3SUPPLIER1_it96 = qCUSTOMER3SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER3SUPPLIER1_end95 = qCUSTOMER3SUPPLIER1.end();
    for (
        ; qCUSTOMER3SUPPLIER1_it96 != qCUSTOMER3SUPPLIER1_end95; 
        ++qCUSTOMER3SUPPLIER1_it96)
    {
        int64_t x_qCUSTOMER3SUPPLIER_S__SUPPKEY = get<0>(
            qCUSTOMER3SUPPLIER1_it96->first);
        string P__MFGR = get<2>(qCUSTOMER3SUPPLIER1_it96->first);
        qCUSTOMER3SUPPLIER1[make_tuple(
            x_qCUSTOMER3SUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)] += 
            qORDERS3SUPPLIER1[make_tuple(ORDERKEY,x_qCUSTOMER3SUPPLIER_S__SUPPKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3SUPPLIER1PARTS1_it98 = qCUSTOMER3SUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3SUPPLIER1PARTS1_end97 = qCUSTOMER3SUPPLIER1PARTS1.end();
    for (
        ; qCUSTOMER3SUPPLIER1PARTS1_it98 != qCUSTOMER3SUPPLIER1PARTS1_end97; 
        ++qCUSTOMER3SUPPLIER1PARTS1_it98)
    {
        int64_t x_qCUSTOMER3SUPPLIER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER3SUPPLIER1PARTS1_it98->first);
        int64_t x_qCUSTOMER3SUPPLIER_S__SUPPKEY = get<1>(
            qCUSTOMER3SUPPLIER1PARTS1_it98->first);
        qCUSTOMER3SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER3SUPPLIER1PARTS_P__PARTKEY,x_qCUSTOMER3SUPPLIER_S__SUPPKEY,CUSTKEY)] 
            += qORDERS3SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qCUSTOMER3SUPPLIER1PARTS_P__PARTKEY,
            x_qCUSTOMER3SUPPLIER_S__SUPPKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_it100 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_end99 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it100 != qLINEITEM1_end99; ++qLINEITEM1_it100)
    {
        int64_t C__NATIONKEY = get<1>(qLINEITEM1_it100->first);
        int64_t N1__REGIONKEY = get<2>(qLINEITEM1_it100->first);
        qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)] += qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    qLINEITEM1CUSTOMER1[make_tuple(ORDERKEY,CUSTKEY)] += 1;
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_it102 = 
        qLINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_end101 = 
        qLINEITEM1NATION1.end();
    for (
        ; qLINEITEM1NATION1_it102 != qLINEITEM1NATION1_end101; 
        ++qLINEITEM1NATION1_it102)
    {
        int64_t x_qLINEITEM1NATION_N1__NATIONKEY = get<1>(
            qLINEITEM1NATION1_it102->first);
        qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qLINEITEM1NATION_N1__NATIONKEY)] += 
            qLINEITEM1NATION1ORDERS1[make_tuple(CUSTKEY,x_qLINEITEM1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it104 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end103 
        = qNATION1.end();
    for (; qNATION1_it104 != qNATION1_end103; ++qNATION1_it104)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it104->first);
        string P__MFGR = get<1>(qNATION1_it104->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it104->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it104->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it106 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end105 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it106 != qNATION1NATION1_end105; ++qNATION1NATION1_it106)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1NATION1_it106->first);
        string P__MFGR = get<1>(qNATION1NATION1_it106->first);
        int64_t x_qNATION1NATION_N1__NATIONKEY = get<2>(
            qNATION1NATION1_it106->first);
        qNATION1NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION1NATION_N1__NATIONKEY)] += 
            qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qNATION1NATION_N1__NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it108 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end107 = 
        qNATION2.end();
    for (; qNATION2_it108 != qNATION2_end107; ++qNATION2_it108)
    {
        string P__MFGR = get<0>(qNATION2_it108->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it108->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it108->first);
        qNATION2[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += qORDERS1[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION3_it110 = qNATION3.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION3_end109 = qNATION3.end();
    for (; qNATION3_it110 != qNATION3_end109; ++qNATION3_it110)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION3_it110->first);
        string P__MFGR = get<1>(qNATION3_it110->first);
        qNATION3[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR)] += qNATION3ORDERS1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR,CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION4_it112 = qNATION4.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION4_end111 
        = qNATION4.end();
    for (; qNATION4_it112 != qNATION4_end111; ++qNATION4_it112)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4_it112->first);
        string P__MFGR = get<1>(qNATION4_it112->first);
        int64_t C__NATIONKEY = get<2>(qNATION4_it112->first);
        int64_t N1__REGIONKEY = get<3>(qNATION4_it112->first);
        qNATION4[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            qORDERS3NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION4NATION1_it114 = 
        qNATION4NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION4NATION1_end113 = qNATION4NATION1.end();
    for (
        ; qNATION4NATION1_it114 != qNATION4NATION1_end113; ++qNATION4NATION1_it114)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4NATION1_it114->first);
        string P__MFGR = get<1>(qNATION4NATION1_it114->first);
        int64_t x_qNATION4NATION_N1__NATIONKEY = get<2>(
            qNATION4NATION1_it114->first);
        qNATION4NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION4NATION_N1__NATIONKEY)] += 
            qORDERS3NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qNATION4NATION_N1__NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_it116 = 
        qNATION5.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_end115 = 
        qNATION5.end();
    for (; qNATION5_it116 != qNATION5_end115; ++qNATION5_it116)
    {
        string P__MFGR = get<0>(qNATION5_it116->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION5_it116->first);
        int64_t N2__REGIONKEY = get<2>(qNATION5_it116->first);
        qNATION5[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += qORDERS3[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION6_it118 = qNATION6.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION6_end117 = qNATION6.end();
    for (; qNATION6_it118 != qNATION6_end117; ++qNATION6_it118)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION6_it118->first);
        string P__MFGR = get<1>(qNATION6_it118->first);
        qNATION6[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR)] += qNATION6ORDERS1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR,CUSTKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it120 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end119 
        = qPARTS1.end();
    for (; qPARTS1_it120 != qPARTS1_end119; ++qPARTS1_it120)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it120->first);
        int64_t C__NATIONKEY = get<1>(qPARTS1_it120->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it120->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1_it120->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            qORDERS1PARTS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,N2__REGIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it122 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end121 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it122 != qPARTS1NATION1_end121; ++qPARTS1NATION1_it122)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1_it122->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1_it122->first);
        int64_t C__NATIONKEY = get<2>(qPARTS1NATION1_it122->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1NATION1_it122->first);
        qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] 
            += qORDERS1PARTS1NATION1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            x_qPARTS1NATION_N1__NATIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_it124 = qPARTS1NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_end123 = qPARTS1NATION1NATION1.end();
    for (
        ; qPARTS1NATION1NATION1_it124 != qPARTS1NATION1NATION1_end123; 
        ++qPARTS1NATION1NATION1_it124)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS1NATION1NATION1_it124->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1NATION1_it124->first);
        int64_t x_qPARTS1NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS1NATION1NATION1_it124->first);
        qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,
            x_qPARTS1NATION1NATION_N1__NATIONKEY)] += qORDERS1PARTS1NATION1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            x_qPARTS1NATION_N1__NATIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it126 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end125 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it126 != qPARTS1NATION2_end125; ++qPARTS1NATION2_it126)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION2_it126->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION2_it126->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1NATION2_it126->first);
        qPARTS1NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            qORDERS1PARTS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            N2__REGIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_it128 = 
        qPARTS1NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_end127 = 
        qPARTS1NATION3.end();
    for (
        ; qPARTS1NATION3_it128 != qPARTS1NATION3_end127; ++qPARTS1NATION3_it128)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION3_it128->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION3_it128->first);
        qPARTS1NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY)] += 
            qPARTS1NATION3ORDERS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,CUSTKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_it130 = qPARTS1NATION3LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_end129 = qPARTS1NATION3LINEITEM1.end();
    for (
        ; qPARTS1NATION3LINEITEM1_it130 != qPARTS1NATION3LINEITEM1_end129; 
        ++qPARTS1NATION3LINEITEM1_it130)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION3LINEITEM1_it130->first);
        int64_t x_qPARTS1NATION3LINEITEM_L__SUPPKEY = get<2>(
            qPARTS1NATION3LINEITEM1_it130->first);
        qPARTS1NATION3LINEITEM1[make_tuple(
            ORDERKEY,x_qPARTS1NATION_N1__NATIONKEY,x_qPARTS1NATION3LINEITEM_L__SUPPKEY)] += 
            qPARTS1NATION3LINEITEM1ORDERS1[make_tuple(
            CUSTKEY,x_qPARTS1NATION_N1__NATIONKEY,x_qPARTS1NATION3LINEITEM_L__SUPPKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2_it132 = qPARTS2.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS2_end131 
        = qPARTS2.end();
    for (; qPARTS2_it132 != qPARTS2_end131; ++qPARTS2_it132)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2_it132->first);
        int64_t C__NATIONKEY = get<1>(qPARTS2_it132->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2_it132->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS2_it132->first);
        qPARTS2[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            qORDERS3PARTS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,N2__REGIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_it134 = qPARTS2NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_end133 = qPARTS2NATION1.end();
    for (
        ; qPARTS2NATION1_it134 != qPARTS2NATION1_end133; ++qPARTS2NATION1_it134)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION1_it134->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION1_it134->first);
        int64_t C__NATIONKEY = get<2>(qPARTS2NATION1_it134->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS2NATION1_it134->first);
        qPARTS2NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] 
            += qORDERS3PARTS1NATION1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            x_qPARTS2NATION_N1__NATIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1NATION1_it136 = qPARTS2NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1NATION1_end135 = qPARTS2NATION1NATION1.end();
    for (
        ; qPARTS2NATION1NATION1_it136 != qPARTS2NATION1NATION1_end135; 
        ++qPARTS2NATION1NATION1_it136)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS2NATION1NATION1_it136->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION1NATION1_it136->first);
        int64_t x_qPARTS2NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS2NATION1NATION1_it136->first);
        qPARTS2NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,
            x_qPARTS2NATION1NATION_N1__NATIONKEY)] += qORDERS3PARTS1NATION1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            x_qPARTS2NATION_N1__NATIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qPARTS2NATION1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS2NATION2_it138 = 
        qPARTS2NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2NATION2_end137 = qPARTS2NATION2.end();
    for (
        ; qPARTS2NATION2_it138 != qPARTS2NATION2_end137; ++qPARTS2NATION2_it138)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION2_it138->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION2_it138->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2NATION2_it138->first);
        qPARTS2NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            qORDERS3PARTS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            N2__REGIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qPARTS2NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qPARTS2NATION3_it140 = 
        qPARTS2NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS2NATION3_end139 = 
        qPARTS2NATION3.end();
    for (
        ; qPARTS2NATION3_it140 != qPARTS2NATION3_end139; ++qPARTS2NATION3_it140)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION3_it140->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION3_it140->first);
        qPARTS2NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY)] += 
            qPARTS2NATION3ORDERS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it142 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end141 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it142 != qSUPPLIER1_end141; ++qSUPPLIER1_it142)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it142->first);
        string P__MFGR = get<1>(qSUPPLIER1_it142->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1_it142->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1_it142->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            qORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY,P__MFGR)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER1NATION1_it144 
        = qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER1NATION1_end143 = qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it144 != qSUPPLIER1NATION1_end143; 
        ++qSUPPLIER1NATION1_it144)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1NATION1_it144->first);
        string P__MFGR = get<1>(qSUPPLIER1NATION1_it144->first);
        int64_t x_qSUPPLIER1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1NATION1_it144->first);
        qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,x_qSUPPLIER1NATION_N1__NATIONKEY)] += 
            qORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY,P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qSUPPLIER1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_it146 = qSUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_end145 = qSUPPLIER1PARTS1.end();
    for (
        ; qSUPPLIER1PARTS1_it146 != qSUPPLIER1PARTS1_end145; ++qSUPPLIER1PARTS1_it146)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1_it146->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER1PARTS1_it146->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1PARTS1_it146->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1PARTS1_it146->first);
        qSUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,C__NATIONKEY,
            N1__REGIONKEY)] += qORDERS1SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qSUPPLIER1PARTS_P__PARTKEY,
            x_qSUPPLIER_S__SUPPKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_it148 = qSUPPLIER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_end147 = qSUPPLIER1PARTS1NATION1.end();
    for (
        ; qSUPPLIER1PARTS1NATION1_it148 != qSUPPLIER1PARTS1NATION1_end147; 
        ++qSUPPLIER1PARTS1NATION1_it148)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1NATION1_it148->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(
            qSUPPLIER1PARTS1NATION1_it148->first);
        int64_t x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1PARTS1NATION1_it148->first);
        qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,
            x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY)] += qORDERS1SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qSUPPLIER1PARTS_P__PARTKEY,
            x_qSUPPLIER_S__SUPPKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER3_it150 = qSUPPLIER3.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3_end149 = qSUPPLIER3.end();
    for (; qSUPPLIER3_it150 != qSUPPLIER3_end149; ++qSUPPLIER3_it150)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER3_it150->first);
        string P__MFGR = get<1>(qSUPPLIER3_it150->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER3_it150->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER3_it150->first);
        qSUPPLIER3[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            qORDERS3SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY,P__MFGR)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER3NATION1_it152 
        = qSUPPLIER3NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER3NATION1_end151 = qSUPPLIER3NATION1.end();
    for (
        ; qSUPPLIER3NATION1_it152 != qSUPPLIER3NATION1_end151; 
        ++qSUPPLIER3NATION1_it152)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER3NATION1_it152->first);
        string P__MFGR = get<1>(qSUPPLIER3NATION1_it152->first);
        int64_t x_qSUPPLIER3NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER3NATION1_it152->first);
        qSUPPLIER3NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,x_qSUPPLIER3NATION_N1__NATIONKEY)] += 
            qORDERS3SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY,P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qSUPPLIER3NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1_it154 = qSUPPLIER3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1_end153 = qSUPPLIER3PARTS1.end();
    for (
        ; qSUPPLIER3PARTS1_it154 != qSUPPLIER3PARTS1_end153; ++qSUPPLIER3PARTS1_it154)
    {
        int64_t x_qSUPPLIER3PARTS_P__PARTKEY = get<0>(
            qSUPPLIER3PARTS1_it154->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER3PARTS1_it154->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER3PARTS1_it154->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER3PARTS1_it154->first);
        qSUPPLIER3PARTS1[make_tuple(
            x_qSUPPLIER3PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,C__NATIONKEY,
            N1__REGIONKEY)] += qORDERS3SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qSUPPLIER3PARTS_P__PARTKEY,
            x_qSUPPLIER_S__SUPPKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1NATION1_it156 = qSUPPLIER3PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1NATION1_end155 = qSUPPLIER3PARTS1NATION1.end();
    for (
        ; qSUPPLIER3PARTS1NATION1_it156 != qSUPPLIER3PARTS1NATION1_end155; 
        ++qSUPPLIER3PARTS1NATION1_it156)
    {
        int64_t x_qSUPPLIER3PARTS_P__PARTKEY = get<0>(
            qSUPPLIER3PARTS1NATION1_it156->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(
            qSUPPLIER3PARTS1NATION1_it156->first);
        int64_t x_qSUPPLIER3PARTS1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER3PARTS1NATION1_it156->first);
        qSUPPLIER3PARTS1NATION1[make_tuple(
            x_qSUPPLIER3PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,
            x_qSUPPLIER3PARTS1NATION_N1__NATIONKEY)] += qORDERS3SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qSUPPLIER3PARTS_P__PARTKEY,
            x_qSUPPLIER_S__SUPPKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qSUPPLIER3PARTS1NATION_N1__NATIONKEY)];
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

    PK_idx_OK_SK[PARTKEY].push_back(make_tuple(ORDERKEY,SUPPKEY));

    indexed_result_domain_maintenance();

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it158 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end157 = 
        q.end();
    for (; q_it158 != q_end157; ++q_it158)
    {
        string P__MFGR = get<0>(q_it158->first);
        int64_t N2__REGIONKEY = get<1>(q_it158->first);
        int64_t N1__REGIONKEY = get<2>(q_it158->first);
        int64_t C__NATIONKEY = get<3>(q_it158->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)]*-1;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it160 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end159 = 
        q.end();
    for (; q_it160 != q_end159; ++q_it160)
    {
        string P__MFGR = get<0>(q_it160->first);
        int64_t N2__REGIONKEY = get<1>(q_it160->first);
        int64_t N1__REGIONKEY = get<2>(q_it160->first);
        int64_t C__NATIONKEY = get<3>(q_it160->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)]*100;
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it162 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end161 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it162 != qCUSTOMER1_end161; ++qCUSTOMER1_it162)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it162->first);
        string P__MFGR = get<1>(qCUSTOMER1_it162->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1_it162->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,N2__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1NATION1_it164 
        = qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1NATION1_end163 = qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it164 != qCUSTOMER1NATION1_end163; 
        ++qCUSTOMER1NATION1_it164)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1NATION1_it164->first);
        int64_t x_qCUSTOMER1NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER1NATION1_it164->first);
        string P__MFGR = get<2>(qCUSTOMER1NATION1_it164->first);
        qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1NATION_N1__NATIONKEY,P__MFGR)] += 
            EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qCUSTOMER1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER1PARTS1_it166 
        = qCUSTOMER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_end165 = qCUSTOMER1PARTS1.end();
    for (
        ; qCUSTOMER1PARTS1_it166 != qCUSTOMER1PARTS1_end165; ++qCUSTOMER1PARTS1_it166)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1PARTS1_it166->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1PARTS1_it166->first);
        qCUSTOMER1PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_it168 = qCUSTOMER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_end167 = qCUSTOMER1PARTS1NATION1.end();
    for (
        ; qCUSTOMER1PARTS1NATION1_it168 != qCUSTOMER1PARTS1NATION1_end167; 
        ++qCUSTOMER1PARTS1NATION1_it168)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1PARTS1NATION1_it168->first);
        int64_t x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY = get<2>(
            qCUSTOMER1PARTS1NATION1_it168->first);
        qCUSTOMER1PARTS1NATION1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_it170 = qCUSTOMER1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_end169 = qCUSTOMER1SUPPLIER1.end();
    for (
        ; qCUSTOMER1SUPPLIER1_it170 != qCUSTOMER1SUPPLIER1_end169; 
        ++qCUSTOMER1SUPPLIER1_it170)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1SUPPLIER1_it170->first);
        string P__MFGR = get<2>(qCUSTOMER1SUPPLIER1_it170->first);
        qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)] += 
            EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(PARTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1SUPPLIER1PARTS1_it172 = qCUSTOMER1SUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1SUPPLIER1PARTS1_end171 = qCUSTOMER1SUPPLIER1PARTS1.end();
    for (
        ; qCUSTOMER1SUPPLIER1PARTS1_it172 != qCUSTOMER1SUPPLIER1PARTS1_end171; 
        ++qCUSTOMER1SUPPLIER1PARTS1_it172)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<2>(
            qCUSTOMER1SUPPLIER1PARTS1_it172->first);
        qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            PARTKEY,SUPPKEY,x_qCUSTOMER_C__CUSTKEY)] += 
            EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(ORDERKEY,x_qCUSTOMER_C__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_it174 = 
        qCUSTOMER3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_end173 = 
        qCUSTOMER3.end();
    for (; qCUSTOMER3_it174 != qCUSTOMER3_end173; ++qCUSTOMER3_it174)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER3_it174->first);
        string P__MFGR = get<1>(qCUSTOMER3_it174->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER3_it174->first);
        qCUSTOMER3[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,N2__REGIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER3NATION1_it176 
        = qCUSTOMER3NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER3NATION1_end175 = qCUSTOMER3NATION1.end();
    for (
        ; qCUSTOMER3NATION1_it176 != qCUSTOMER3NATION1_end175; 
        ++qCUSTOMER3NATION1_it176)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER3NATION1_it176->first);
        int64_t x_qCUSTOMER3NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER3NATION1_it176->first);
        string P__MFGR = get<2>(qCUSTOMER3NATION1_it176->first);
        qCUSTOMER3NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER3NATION_N1__NATIONKEY,P__MFGR)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qCUSTOMER3NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER3PARTS1_it178 
        = qCUSTOMER3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER3PARTS1_end177 = qCUSTOMER3PARTS1.end();
    for (
        ; qCUSTOMER3PARTS1_it178 != qCUSTOMER3PARTS1_end177; ++qCUSTOMER3PARTS1_it178)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER3PARTS1_it178->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER3PARTS1_it178->first);
        qCUSTOMER3PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3PARTS1NATION1_it180 = qCUSTOMER3PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3PARTS1NATION1_end179 = qCUSTOMER3PARTS1NATION1.end();
    for (
        ; qCUSTOMER3PARTS1NATION1_it180 != qCUSTOMER3PARTS1NATION1_end179; 
        ++qCUSTOMER3PARTS1NATION1_it180)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER3PARTS1NATION1_it180->first);
        int64_t x_qCUSTOMER3PARTS1NATION_N1__NATIONKEY = get<2>(
            qCUSTOMER3PARTS1NATION1_it180->first);
        qCUSTOMER3PARTS1NATION1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER3PARTS1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qCUSTOMER3PARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER3SUPPLIER1_it182 = qCUSTOMER3SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER3SUPPLIER1_end181 = qCUSTOMER3SUPPLIER1.end();
    for (
        ; qCUSTOMER3SUPPLIER1_it182 != qCUSTOMER3SUPPLIER1_end181; 
        ++qCUSTOMER3SUPPLIER1_it182)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER3SUPPLIER1_it182->first);
        string P__MFGR = get<2>(qCUSTOMER3SUPPLIER1_it182->first);
        qCUSTOMER3SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(PARTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3SUPPLIER1PARTS1_it184 = qCUSTOMER3SUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3SUPPLIER1PARTS1_end183 = qCUSTOMER3SUPPLIER1PARTS1.end();
    for (
        ; qCUSTOMER3SUPPLIER1PARTS1_it184 != qCUSTOMER3SUPPLIER1PARTS1_end183; 
        ++qCUSTOMER3SUPPLIER1PARTS1_it184)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<2>(
            qCUSTOMER3SUPPLIER1PARTS1_it184->first);
        qCUSTOMER3SUPPLIER1PARTS1[make_tuple(
            PARTKEY,SUPPKEY,x_qCUSTOMER_C__CUSTKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it186 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end185 
        = qNATION1.end();
    for (; qNATION1_it186 != qNATION1_end185; ++qNATION1_it186)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it186->first);
        string P__MFGR = get<1>(qNATION1_it186->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it186->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it186->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it188 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end187 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it188 != qNATION1NATION1_end187; ++qNATION1NATION1_it188)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1NATION1_it188->first);
        string P__MFGR = get<1>(qNATION1NATION1_it188->first);
        int64_t x_qNATION1NATION_N1__NATIONKEY = get<2>(
            qNATION1NATION1_it188->first);
        qNATION1NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qNATION1NATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it190 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end189 = 
        qNATION2.end();
    for (; qNATION2_it190 != qNATION2_end189; ++qNATION2_it190)
    {
        string P__MFGR = get<0>(qNATION2_it190->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it190->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it190->first);
        qNATION2[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION3_it192 = qNATION3.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION3_end191 = qNATION3.end();
    for (; qNATION3_it192 != qNATION3_end191; ++qNATION3_it192)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION3_it192->first);
        string P__MFGR = get<1>(qNATION3_it192->first);
        qNATION3[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR)] += 
            EXTENDEDPRICE*qPARTS1NATION3LINEITEM1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,SUPPKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_it194 = qNATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_end193 = qNATION3ORDERS1.end();
    for (
        ; qNATION3ORDERS1_it194 != qNATION3ORDERS1_end193; ++qNATION3ORDERS1_it194)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION3ORDERS1_it194->first);
        string P__MFGR = get<2>(qNATION3ORDERS1_it194->first);
        int64_t x_qNATION3ORDERS_O__CUSTKEY = get<3>(
            qNATION3ORDERS1_it194->first);
        qNATION3ORDERS1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION3ORDERS_O__CUSTKEY)] += 
            EXTENDEDPRICE*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY,x_qNATION3ORDERS_O__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION4_it196 = qNATION4.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION4_end195 
        = qNATION4.end();
    for (; qNATION4_it196 != qNATION4_end195; ++qNATION4_it196)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4_it196->first);
        string P__MFGR = get<1>(qNATION4_it196->first);
        int64_t C__NATIONKEY = get<2>(qNATION4_it196->first);
        int64_t N1__REGIONKEY = get<3>(qNATION4_it196->first);
        qNATION4[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION4NATION1_it198 = 
        qNATION4NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION4NATION1_end197 = qNATION4NATION1.end();
    for (
        ; qNATION4NATION1_it198 != qNATION4NATION1_end197; ++qNATION4NATION1_it198)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4NATION1_it198->first);
        string P__MFGR = get<1>(qNATION4NATION1_it198->first);
        int64_t x_qNATION4NATION_N1__NATIONKEY = get<2>(
            qNATION4NATION1_it198->first);
        qNATION4NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION4NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qNATION4NATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_it200 = 
        qNATION5.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_end199 = 
        qNATION5.end();
    for (; qNATION5_it200 != qNATION5_end199; ++qNATION5_it200)
    {
        string P__MFGR = get<0>(qNATION5_it200->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION5_it200->first);
        int64_t N2__REGIONKEY = get<2>(qNATION5_it200->first);
        qNATION5[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION6_it202 = qNATION6.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION6_end201 = qNATION6.end();
    for (; qNATION6_it202 != qNATION6_end201; ++qNATION6_it202)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION6_it202->first);
        string P__MFGR = get<1>(qNATION6_it202->first);
        qNATION6[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR)] += 
            EXTENDEDPRICE*DISCOUNT*qPARTS1NATION3LINEITEM1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,SUPPKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION6ORDERS1_it204 = qNATION6ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION6ORDERS1_end203 = qNATION6ORDERS1.end();
    for (
        ; qNATION6ORDERS1_it204 != qNATION6ORDERS1_end203; ++qNATION6ORDERS1_it204)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION6ORDERS1_it204->first);
        string P__MFGR = get<2>(qNATION6ORDERS1_it204->first);
        int64_t x_qNATION6ORDERS_O__CUSTKEY = get<3>(
            qNATION6ORDERS1_it204->first);
        qNATION6ORDERS1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION6ORDERS_O__CUSTKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY,x_qNATION6ORDERS_O__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it206 = 
        qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end205 = 
        qORDERS1.end();
    for (; qORDERS1_it206 != qORDERS1_end205; ++qORDERS1_it206)
    {
        string P__MFGR = get<1>(qORDERS1_it206->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1_it206->first);
        qORDERS1[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)] += EXTENDEDPRICE*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1NATION1_it208 = 
        qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1NATION1_end207 = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it208 != qORDERS1NATION1_end207; ++qORDERS1NATION1_it208)
    {
        int64_t x_qORDERS1NATION_N1__NATIONKEY = get<1>(
            qORDERS1NATION1_it208->first);
        string P__MFGR = get<2>(qORDERS1NATION1_it208->first);
        qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qORDERS1NATION_N1__NATIONKEY,P__MFGR)] += 
            EXTENDEDPRICE*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qORDERS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1PARTS1_it210 = 
        qORDERS1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS1PARTS1_end209 = qORDERS1PARTS1.end();
    for (
        ; qORDERS1PARTS1_it210 != qORDERS1PARTS1_end209; ++qORDERS1PARTS1_it210)
    {
        int64_t N2__REGIONKEY = get<2>(qORDERS1PARTS1_it210->first);
        qORDERS1PARTS1[make_tuple(
            ORDERKEY,PARTKEY,N2__REGIONKEY)] += EXTENDEDPRICE*qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS1PARTS1NATION1_it212 = qORDERS1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS1PARTS1NATION1_end211 = qORDERS1PARTS1NATION1.end();
    for (
        ; qORDERS1PARTS1NATION1_it212 != qORDERS1PARTS1NATION1_end211; 
        ++qORDERS1PARTS1NATION1_it212)
    {
        int64_t x_qORDERS1PARTS1NATION_N1__NATIONKEY = get<2>(
            qORDERS1PARTS1NATION1_it212->first);
        qORDERS1PARTS1NATION1[make_tuple(
            ORDERKEY,PARTKEY,x_qORDERS1PARTS1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qORDERS1PARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1SUPPLIER1_it214 
        = qORDERS1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1SUPPLIER1_end213 = qORDERS1SUPPLIER1.end();
    for (
        ; qORDERS1SUPPLIER1_it214 != qORDERS1SUPPLIER1_end213; 
        ++qORDERS1SUPPLIER1_it214)
    {
        string P__MFGR = get<2>(qORDERS1SUPPLIER1_it214->first);
        qORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,SUPPKEY,P__MFGR)] += EXTENDEDPRICE*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }
    qORDERS1SUPPLIER1PARTS1[make_tuple(
        ORDERKEY,PARTKEY,SUPPKEY)] += EXTENDEDPRICE;
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_it216 = 
        qORDERS3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_end215 = 
        qORDERS3.end();
    for (; qORDERS3_it216 != qORDERS3_end215; ++qORDERS3_it216)
    {
        string P__MFGR = get<1>(qORDERS3_it216->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS3_it216->first);
        qORDERS3[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS3NATION1_it218 = 
        qORDERS3NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS3NATION1_end217 = qORDERS3NATION1.end();
    for (
        ; qORDERS3NATION1_it218 != qORDERS3NATION1_end217; ++qORDERS3NATION1_it218)
    {
        int64_t x_qORDERS3NATION_N1__NATIONKEY = get<1>(
            qORDERS3NATION1_it218->first);
        string P__MFGR = get<2>(qORDERS3NATION1_it218->first);
        qORDERS3NATION1[make_tuple(
            ORDERKEY,x_qORDERS3NATION_N1__NATIONKEY,P__MFGR)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qORDERS3NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS3PARTS1_it220 = 
        qORDERS3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS3PARTS1_end219 = qORDERS3PARTS1.end();
    for (
        ; qORDERS3PARTS1_it220 != qORDERS3PARTS1_end219; ++qORDERS3PARTS1_it220)
    {
        int64_t N2__REGIONKEY = get<2>(qORDERS3PARTS1_it220->first);
        qORDERS3PARTS1[make_tuple(
            ORDERKEY,PARTKEY,N2__REGIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS3PARTS1NATION1_it222 = qORDERS3PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS3PARTS1NATION1_end221 = qORDERS3PARTS1NATION1.end();
    for (
        ; qORDERS3PARTS1NATION1_it222 != qORDERS3PARTS1NATION1_end221; 
        ++qORDERS3PARTS1NATION1_it222)
    {
        int64_t x_qORDERS3PARTS1NATION_N1__NATIONKEY = get<2>(
            qORDERS3PARTS1NATION1_it222->first);
        qORDERS3PARTS1NATION1[make_tuple(
            ORDERKEY,PARTKEY,x_qORDERS3PARTS1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qORDERS3PARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS3SUPPLIER1_it224 
        = qORDERS3SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS3SUPPLIER1_end223 = qORDERS3SUPPLIER1.end();
    for (
        ; qORDERS3SUPPLIER1_it224 != qORDERS3SUPPLIER1_end223; 
        ++qORDERS3SUPPLIER1_it224)
    {
        string P__MFGR = get<2>(qORDERS3SUPPLIER1_it224->first);
        qORDERS3SUPPLIER1[make_tuple(
            ORDERKEY,SUPPKEY,P__MFGR)] += EXTENDEDPRICE*DISCOUNT*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }
    qORDERS3SUPPLIER1PARTS1[make_tuple(
        ORDERKEY,PARTKEY,SUPPKEY)] += EXTENDEDPRICE*DISCOUNT;
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it226 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end225 
        = qPARTS1.end();
    for (; qPARTS1_it226 != qPARTS1_end225; ++qPARTS1_it226)
    {
        int64_t C__NATIONKEY = get<1>(qPARTS1_it226->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it226->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1_it226->first);
        qPARTS1[make_tuple(
            PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it228 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end227 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it228 != qPARTS1NATION1_end227; ++qPARTS1NATION1_it228)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1_it228->first);
        int64_t C__NATIONKEY = get<2>(qPARTS1NATION1_it228->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1NATION1_it228->first);
        qPARTS1NATION1[make_tuple(
            PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_it230 = qPARTS1NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_end229 = qPARTS1NATION1NATION1.end();
    for (
        ; qPARTS1NATION1NATION1_it230 != qPARTS1NATION1NATION1_end229; 
        ++qPARTS1NATION1NATION1_it230)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1NATION1_it230->first);
        int64_t x_qPARTS1NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS1NATION1NATION1_it230->first);
        qPARTS1NATION1NATION1[make_tuple(
            PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it232 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end231 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it232 != qPARTS1NATION2_end231; ++qPARTS1NATION2_it232)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION2_it232->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1NATION2_it232->first);
        qPARTS1NATION2[make_tuple(
            PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qPARTS1NATION_N1__NATIONKEY)]*qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_it234 = 
        qPARTS1NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_end233 = 
        qPARTS1NATION3.end();
    for (
        ; qPARTS1NATION3_it234 != qPARTS1NATION3_end233; ++qPARTS1NATION3_it234)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION3_it234->first);
        qPARTS1NATION3[make_tuple(
            PARTKEY,x_qPARTS1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*qPARTS1NATION3LINEITEM1[make_tuple(
            ORDERKEY,x_qPARTS1NATION_N1__NATIONKEY,SUPPKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_it236 = qPARTS1NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_end235 = qPARTS1NATION3ORDERS1.end();
    for (
        ; qPARTS1NATION3ORDERS1_it236 != qPARTS1NATION3ORDERS1_end235; 
        ++qPARTS1NATION3ORDERS1_it236)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<2>(
            qPARTS1NATION3ORDERS1_it236->first);
        int64_t x_qPARTS1NATION3ORDERS_O__CUSTKEY = get<3>(
            qPARTS1NATION3ORDERS1_it236->first);
        qPARTS1NATION3ORDERS1[make_tuple(
            ORDERKEY,PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,
            x_qPARTS1NATION3ORDERS_O__CUSTKEY)] += 
            EXTENDEDPRICE*qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qPARTS1NATION_N1__NATIONKEY,x_qPARTS1NATION3ORDERS_O__CUSTKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2_it238 = qPARTS2.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS2_end237 
        = qPARTS2.end();
    for (; qPARTS2_it238 != qPARTS2_end237; ++qPARTS2_it238)
    {
        int64_t C__NATIONKEY = get<1>(qPARTS2_it238->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2_it238->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS2_it238->first);
        qPARTS2[make_tuple(
            PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_it240 = qPARTS2NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_end239 = qPARTS2NATION1.end();
    for (
        ; qPARTS2NATION1_it240 != qPARTS2NATION1_end239; ++qPARTS2NATION1_it240)
    {
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION1_it240->first);
        int64_t C__NATIONKEY = get<2>(qPARTS2NATION1_it240->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS2NATION1_it240->first);
        qPARTS2NATION1[make_tuple(
            PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qPARTS2NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1NATION1_it242 = qPARTS2NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1NATION1_end241 = qPARTS2NATION1NATION1.end();
    for (
        ; qPARTS2NATION1NATION1_it242 != qPARTS2NATION1NATION1_end241; 
        ++qPARTS2NATION1NATION1_it242)
    {
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION1NATION1_it242->first);
        int64_t x_qPARTS2NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS2NATION1NATION1_it242->first);
        qPARTS2NATION1NATION1[make_tuple(
            PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,x_qPARTS2NATION1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qPARTS2NATION1NATION_N1__NATIONKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qPARTS2NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS2NATION2_it244 = 
        qPARTS2NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2NATION2_end243 = qPARTS2NATION2.end();
    for (
        ; qPARTS2NATION2_it244 != qPARTS2NATION2_end243; ++qPARTS2NATION2_it244)
    {
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION2_it244->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2NATION2_it244->first);
        qPARTS2NATION2[make_tuple(
            PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qPARTS2NATION_N1__NATIONKEY)]*qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qPARTS2NATION3_it246 = 
        qPARTS2NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS2NATION3_end245 = 
        qPARTS2NATION3.end();
    for (
        ; qPARTS2NATION3_it246 != qPARTS2NATION3_end245; ++qPARTS2NATION3_it246)
    {
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION3_it246->first);
        qPARTS2NATION3[make_tuple(
            PARTKEY,x_qPARTS2NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qPARTS1NATION3LINEITEM1[make_tuple(
            ORDERKEY,x_qPARTS2NATION_N1__NATIONKEY,SUPPKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION3ORDERS1_it248 = qPARTS2NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION3ORDERS1_end247 = qPARTS2NATION3ORDERS1.end();
    for (
        ; qPARTS2NATION3ORDERS1_it248 != qPARTS2NATION3ORDERS1_end247; 
        ++qPARTS2NATION3ORDERS1_it248)
    {
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<2>(
            qPARTS2NATION3ORDERS1_it248->first);
        int64_t x_qPARTS2NATION3ORDERS_O__CUSTKEY = get<3>(
            qPARTS2NATION3ORDERS1_it248->first);
        qPARTS2NATION3ORDERS1[make_tuple(
            ORDERKEY,PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,
            x_qPARTS2NATION3ORDERS_O__CUSTKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qPARTS2NATION_N1__NATIONKEY,x_qPARTS2NATION3ORDERS_O__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it250 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end249 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it250 != qSUPPLIER1_end249; ++qSUPPLIER1_it250)
    {
        string P__MFGR = get<1>(qSUPPLIER1_it250->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1_it250->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1_it250->first);
        qSUPPLIER1[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(PARTKEY,P__MFGR)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER1NATION1_it252 
        = qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER1NATION1_end251 = qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it252 != qSUPPLIER1NATION1_end251; 
        ++qSUPPLIER1NATION1_it252)
    {
        string P__MFGR = get<1>(qSUPPLIER1NATION1_it252->first);
        int64_t x_qSUPPLIER1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1NATION1_it252->first);
        qSUPPLIER1NATION1[make_tuple(
            SUPPKEY,P__MFGR,x_qSUPPLIER1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qSUPPLIER1NATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_it254 = qSUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_end253 = qSUPPLIER1PARTS1.end();
    for (
        ; qSUPPLIER1PARTS1_it254 != qSUPPLIER1PARTS1_end253; ++qSUPPLIER1PARTS1_it254)
    {
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1PARTS1_it254->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1PARTS1_it254->first);
        qSUPPLIER1PARTS1[make_tuple(
            PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1[make_tuple(ORDERKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_it256 = qSUPPLIER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_end255 = qSUPPLIER1PARTS1NATION1.end();
    for (
        ; qSUPPLIER1PARTS1NATION1_it256 != qSUPPLIER1PARTS1NATION1_end255; 
        ++qSUPPLIER1PARTS1NATION1_it256)
    {
        int64_t x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1PARTS1NATION1_it256->first);
        qSUPPLIER1PARTS1NATION1[make_tuple(
            PARTKEY,SUPPKEY,x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER3_it258 = qSUPPLIER3.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3_end257 = qSUPPLIER3.end();
    for (; qSUPPLIER3_it258 != qSUPPLIER3_end257; ++qSUPPLIER3_it258)
    {
        string P__MFGR = get<1>(qSUPPLIER3_it258->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER3_it258->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER3_it258->first);
        qSUPPLIER3[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(PARTKEY,P__MFGR)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER3NATION1_it260 
        = qSUPPLIER3NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER3NATION1_end259 = qSUPPLIER3NATION1.end();
    for (
        ; qSUPPLIER3NATION1_it260 != qSUPPLIER3NATION1_end259; 
        ++qSUPPLIER3NATION1_it260)
    {
        string P__MFGR = get<1>(qSUPPLIER3NATION1_it260->first);
        int64_t x_qSUPPLIER3NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER3NATION1_it260->first);
        qSUPPLIER3NATION1[make_tuple(
            SUPPKEY,P__MFGR,x_qSUPPLIER3NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qSUPPLIER3NATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1_it262 = qSUPPLIER3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1_end261 = qSUPPLIER3PARTS1.end();
    for (
        ; qSUPPLIER3PARTS1_it262 != qSUPPLIER3PARTS1_end261; ++qSUPPLIER3PARTS1_it262)
    {
        int64_t C__NATIONKEY = get<2>(qSUPPLIER3PARTS1_it262->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER3PARTS1_it262->first);
        qSUPPLIER3PARTS1[make_tuple(
            PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1NATION1_it264 = qSUPPLIER3PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1NATION1_end263 = qSUPPLIER3PARTS1NATION1.end();
    for (
        ; qSUPPLIER3PARTS1NATION1_it264 != qSUPPLIER3PARTS1NATION1_end263; 
        ++qSUPPLIER3PARTS1NATION1_it264)
    {
        int64_t x_qSUPPLIER3PARTS1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER3PARTS1NATION1_it264->first);
        qSUPPLIER3PARTS1NATION1[make_tuple(
            PARTKEY,SUPPKEY,x_qSUPPLIER3PARTS1NATION_N1__NATIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qSUPPLIER3PARTS1NATION_N1__NATIONKEY)];
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

    CUSTOMER.insert(make_tuple(CUSTKEY,NAME,ADDRESS,NATIONKEY,PHONE,ACCTBAL,MKTSEGMENT,COMMENT));
    CK_idx_NK[CUSTKEY] = NATIONKEY;

    indexed_result_domain_maintenance();

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it266 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end265 = 
        q.end();
    for (; q_it266 != q_end265; ++q_it266)
    {
        string P__MFGR = get<0>(q_it266->first);
        int64_t N2__REGIONKEY = get<1>(q_it266->first);
        int64_t N1__REGIONKEY = get<2>(q_it266->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,NATIONKEY)] += qCUSTOMER1[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it268 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end267 = 
        q.end();
    for (; q_it268 != q_end267; ++q_it268)
    {
        string P__MFGR = get<0>(q_it268->first);
        int64_t N2__REGIONKEY = get<1>(q_it268->first);
        int64_t N1__REGIONKEY = get<2>(q_it268->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,NATIONKEY)] += qCUSTOMER3[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)]*-1;
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_it270 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_end269 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it270 != qLINEITEM1_end269; ++qLINEITEM1_it270)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it270->first);
        int64_t N1__REGIONKEY = get<2>(qLINEITEM1_it270->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,N1__REGIONKEY)] += 
            qLINEITEM1CUSTOMER1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_it272 = 
        qLINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_end271 = 
        qLINEITEM1NATION1.end();
    for (
        ; qLINEITEM1NATION1_it272 != qLINEITEM1NATION1_end271; 
        ++qLINEITEM1NATION1_it272)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1NATION1_it272->first);
        qLINEITEM1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY)] += qLINEITEM1CUSTOMER1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)];
    }
    qLINEITEM1NATION1ORDERS1[make_tuple(CUSTKEY,NATIONKEY)] += 1;
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it274 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end273 
        = qNATION1.end();
    for (; qNATION1_it274 != qNATION1_end273; ++qNATION1_it274)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it274->first);
        string P__MFGR = get<1>(qNATION1_it274->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it274->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY,N1__REGIONKEY)] += 
            qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it276 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end275 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it276 != qNATION1NATION1_end275; ++qNATION1NATION1_it276)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1NATION1_it276->first);
        string P__MFGR = get<1>(qNATION1NATION1_it276->first);
        qNATION1NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY)] += qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY,P__MFGR)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it278 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end277 = 
        qNATION2.end();
    for (; qNATION2_it278 != qNATION2_end277; ++qNATION2_it278)
    {
        string P__MFGR = get<0>(qNATION2_it278->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it278->first);
        qNATION2[make_tuple(
            P__MFGR,NATIONKEY,N2__REGIONKEY)] += qCUSTOMER1[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION3_it280 = qNATION3.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION3_end279 = qNATION3.end();
    for (; qNATION3_it280 != qNATION3_end279; ++qNATION3_it280)
    {
        string P__MFGR = get<1>(qNATION3_it280->first);
        qNATION3[make_tuple(NATIONKEY,P__MFGR)] += qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_it282 = qNATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_end281 = qNATION3ORDERS1.end();
    for (
        ; qNATION3ORDERS1_it282 != qNATION3ORDERS1_end281; ++qNATION3ORDERS1_it282)
    {
        int64_t x_qNATION3ORDERS_O__ORDERKEY = get<0>(
            qNATION3ORDERS1_it282->first);
        string P__MFGR = get<2>(qNATION3ORDERS1_it282->first);
        qNATION3ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR,CUSTKEY)] += 
            qORDERS1NATION1[make_tuple(x_qNATION3ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION4_it284 = qNATION4.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION4_end283 
        = qNATION4.end();
    for (; qNATION4_it284 != qNATION4_end283; ++qNATION4_it284)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4_it284->first);
        string P__MFGR = get<1>(qNATION4_it284->first);
        int64_t N1__REGIONKEY = get<3>(qNATION4_it284->first);
        qNATION4[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY,N1__REGIONKEY)] += 
            qCUSTOMER3NATION1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION4NATION1_it286 = 
        qNATION4NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION4NATION1_end285 = qNATION4NATION1.end();
    for (
        ; qNATION4NATION1_it286 != qNATION4NATION1_end285; ++qNATION4NATION1_it286)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4NATION1_it286->first);
        string P__MFGR = get<1>(qNATION4NATION1_it286->first);
        qNATION4NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY)] += qCUSTOMER3NATION1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY,P__MFGR)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_it288 = 
        qNATION5.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_end287 = 
        qNATION5.end();
    for (; qNATION5_it288 != qNATION5_end287; ++qNATION5_it288)
    {
        string P__MFGR = get<0>(qNATION5_it288->first);
        int64_t N2__REGIONKEY = get<2>(qNATION5_it288->first);
        qNATION5[make_tuple(
            P__MFGR,NATIONKEY,N2__REGIONKEY)] += qCUSTOMER3[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION6_it290 = qNATION6.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION6_end289 = qNATION6.end();
    for (; qNATION6_it290 != qNATION6_end289; ++qNATION6_it290)
    {
        string P__MFGR = get<1>(qNATION6_it290->first);
        qNATION6[make_tuple(NATIONKEY,P__MFGR)] += qCUSTOMER3NATION1[make_tuple(
            CUSTKEY,NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION6ORDERS1_it292 = qNATION6ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION6ORDERS1_end291 = qNATION6ORDERS1.end();
    for (
        ; qNATION6ORDERS1_it292 != qNATION6ORDERS1_end291; ++qNATION6ORDERS1_it292)
    {
        int64_t x_qNATION6ORDERS_O__ORDERKEY = get<0>(
            qNATION6ORDERS1_it292->first);
        string P__MFGR = get<2>(qNATION6ORDERS1_it292->first);
        qNATION6ORDERS1[make_tuple(
            x_qNATION6ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR,CUSTKEY)] += 
            qORDERS3NATION1[make_tuple(x_qNATION6ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_it294 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_end293 = 
        qORDERS2.end();
    for (; qORDERS2_it294 != qORDERS2_end293; ++qORDERS2_it294)
    {
        int64_t N1__REGIONKEY = get<2>(qORDERS2_it294->first);
        qORDERS2[make_tuple(
            CUSTKEY,NATIONKEY,N1__REGIONKEY)] += qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it296 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end295 
        = qPARTS1.end();
    for (; qPARTS1_it296 != qPARTS1_end295; ++qPARTS1_it296)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it296->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it296->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1_it296->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            qCUSTOMER1PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it298 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end297 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it298 != qPARTS1NATION1_end297; ++qPARTS1NATION1_it298)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1_it298->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1_it298->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1NATION1_it298->first);
        qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,NATIONKEY,N1__REGIONKEY)] += 
            qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,
            x_qPARTS1NATION_N1__NATIONKEY)]*qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_it300 = qPARTS1NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_end299 = qPARTS1NATION1NATION1.end();
    for (
        ; qPARTS1NATION1NATION1_it300 != qPARTS1NATION1NATION1_end299; 
        ++qPARTS1NATION1NATION1_it300)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS1NATION1NATION1_it300->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1NATION1_it300->first);
        qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,NATIONKEY)] += 
            qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it302 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end301 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it302 != qPARTS1NATION2_end301; ++qPARTS1NATION2_it302)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION2_it302->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1NATION2_it302->first);
        qPARTS1NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY)] += qCUSTOMER1PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_it304 = 
        qPARTS1NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_end303 = 
        qPARTS1NATION3.end();
    for (
        ; qPARTS1NATION3_it304 != qPARTS1NATION3_end303; ++qPARTS1NATION3_it304)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION3_it304->first);
        qPARTS1NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY)] += qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_it306 = qPARTS1NATION3LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_end305 = qPARTS1NATION3LINEITEM1.end();
    for (
        ; qPARTS1NATION3LINEITEM1_it306 != qPARTS1NATION3LINEITEM1_end305; 
        ++qPARTS1NATION3LINEITEM1_it306)
    {
        int64_t x_qPARTS1NATION3LINEITEM_L__ORDERKEY = get<0>(
            qPARTS1NATION3LINEITEM1_it306->first);
        int64_t x_qPARTS1NATION3LINEITEM_L__SUPPKEY = get<2>(
            qPARTS1NATION3LINEITEM1_it306->first);
        qPARTS1NATION3LINEITEM1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__ORDERKEY,NATIONKEY,
            x_qPARTS1NATION3LINEITEM_L__SUPPKEY)] += qLINEITEM1CUSTOMER1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__ORDERKEY,CUSTKEY)]*qLINEITEM3NATION1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__SUPPKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1ORDERS1_it308 = qPARTS1NATION3LINEITEM1ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1ORDERS1_end307 = qPARTS1NATION3LINEITEM1ORDERS1.end();
    for (
        ; qPARTS1NATION3LINEITEM1ORDERS1_it308 != qPARTS1NATION3LINEITEM1ORDERS1_end307; 
        ++qPARTS1NATION3LINEITEM1ORDERS1_it308)
    {
        int64_t x_qPARTS1NATION3LINEITEM_L__SUPPKEY = get<2>(
            qPARTS1NATION3LINEITEM1ORDERS1_it308->first);
        qPARTS1NATION3LINEITEM1ORDERS1[make_tuple(
            CUSTKEY,NATIONKEY,x_qPARTS1NATION3LINEITEM_L__SUPPKEY)] += 
            qLINEITEM3NATION1[make_tuple(x_qPARTS1NATION3LINEITEM_L__SUPPKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_it310 = qPARTS1NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_end309 = qPARTS1NATION3ORDERS1.end();
    for (
        ; qPARTS1NATION3ORDERS1_it310 != qPARTS1NATION3ORDERS1_end309; 
        ++qPARTS1NATION3ORDERS1_it310)
    {
        int64_t x_qPARTS1NATION3ORDERS_O__ORDERKEY = get<0>(
            qPARTS1NATION3ORDERS1_it310->first);
        int64_t x_qPARTS_P__PARTKEY = get<1>(
            qPARTS1NATION3ORDERS1_it310->first);
        qPARTS1NATION3ORDERS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY,CUSTKEY)] += 
            qORDERS1PARTS1NATION1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3ORDERS1LINEITEM1_it312 = qPARTS1NATION3ORDERS1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3ORDERS1LINEITEM1_end311 = qPARTS1NATION3ORDERS1LINEITEM1.end();
    for (
        ; qPARTS1NATION3ORDERS1LINEITEM1_it312 != qPARTS1NATION3ORDERS1LINEITEM1_end311; 
        ++qPARTS1NATION3ORDERS1LINEITEM1_it312)
    {
        int64_t x_qPARTS1NATION3ORDERS1LINEITEM_L__SUPPKEY = get<0>(
            qPARTS1NATION3ORDERS1LINEITEM1_it312->first);
        qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            x_qPARTS1NATION3ORDERS1LINEITEM_L__SUPPKEY,NATIONKEY,CUSTKEY)] += 
            qLINEITEM3NATION1[make_tuple(
            x_qPARTS1NATION3ORDERS1LINEITEM_L__SUPPKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2_it314 = qPARTS2.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS2_end313 
        = qPARTS2.end();
    for (; qPARTS2_it314 != qPARTS2_end313; ++qPARTS2_it314)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2_it314->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2_it314->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS2_it314->first);
        qPARTS2[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            qCUSTOMER3PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_it316 = qPARTS2NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_end315 = qPARTS2NATION1.end();
    for (
        ; qPARTS2NATION1_it316 != qPARTS2NATION1_end315; ++qPARTS2NATION1_it316)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION1_it316->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION1_it316->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS2NATION1_it316->first);
        qPARTS2NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,NATIONKEY,N1__REGIONKEY)] += 
            qCUSTOMER3PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,
            x_qPARTS2NATION_N1__NATIONKEY)]*qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1NATION1_it318 = qPARTS2NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1NATION1_end317 = qPARTS2NATION1NATION1.end();
    for (
        ; qPARTS2NATION1NATION1_it318 != qPARTS2NATION1NATION1_end317; 
        ++qPARTS2NATION1NATION1_it318)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS2NATION1NATION1_it318->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION1NATION1_it318->first);
        qPARTS2NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,NATIONKEY)] += 
            qCUSTOMER3PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,x_qPARTS2NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS2NATION2_it320 = 
        qPARTS2NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2NATION2_end319 = qPARTS2NATION2.end();
    for (
        ; qPARTS2NATION2_it320 != qPARTS2NATION2_end319; ++qPARTS2NATION2_it320)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION2_it320->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2NATION2_it320->first);
        qPARTS2NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY)] += qCUSTOMER3PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qPARTS2NATION3_it322 = 
        qPARTS2NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS2NATION3_end321 = 
        qPARTS2NATION3.end();
    for (
        ; qPARTS2NATION3_it322 != qPARTS2NATION3_end321; ++qPARTS2NATION3_it322)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION3_it322->first);
        qPARTS2NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY)] += qCUSTOMER3PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION3ORDERS1_it324 = qPARTS2NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION3ORDERS1_end323 = qPARTS2NATION3ORDERS1.end();
    for (
        ; qPARTS2NATION3ORDERS1_it324 != qPARTS2NATION3ORDERS1_end323; 
        ++qPARTS2NATION3ORDERS1_it324)
    {
        int64_t x_qPARTS2NATION3ORDERS_O__ORDERKEY = get<0>(
            qPARTS2NATION3ORDERS1_it324->first);
        int64_t x_qPARTS_P__PARTKEY = get<1>(
            qPARTS2NATION3ORDERS1_it324->first);
        qPARTS2NATION3ORDERS1[make_tuple(
            x_qPARTS2NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY,CUSTKEY)] += 
            qORDERS3PARTS1NATION1[make_tuple(
            x_qPARTS2NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it326 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end325 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it326 != qSUPPLIER1_end325; ++qSUPPLIER1_it326)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it326->first);
        string P__MFGR = get<1>(qSUPPLIER1_it326->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1_it326->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY,N1__REGIONKEY)] += 
            qCUSTOMER1SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER1NATION1_it328 
        = qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER1NATION1_end327 = qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it328 != qSUPPLIER1NATION1_end327; 
        ++qSUPPLIER1NATION1_it328)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1NATION1_it328->first);
        string P__MFGR = get<1>(qSUPPLIER1NATION1_it328->first);
        qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY)] += qCUSTOMER1SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_it330 = qSUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_end329 = qSUPPLIER1PARTS1.end();
    for (
        ; qSUPPLIER1PARTS1_it330 != qSUPPLIER1PARTS1_end329; ++qSUPPLIER1PARTS1_it330)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1_it330->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER1PARTS1_it330->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1PARTS1_it330->first);
        qSUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY,
            N1__REGIONKEY)] += qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,
            CUSTKEY)]*qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_it332 = qSUPPLIER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_end331 = qSUPPLIER1PARTS1NATION1.end();
    for (
        ; qSUPPLIER1PARTS1NATION1_it332 != qSUPPLIER1PARTS1NATION1_end331; 
        ++qSUPPLIER1PARTS1NATION1_it332)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1NATION1_it332->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(
            qSUPPLIER1PARTS1NATION1_it332->first);
        qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY)] += 
            qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER3_it334 = qSUPPLIER3.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3_end333 = qSUPPLIER3.end();
    for (; qSUPPLIER3_it334 != qSUPPLIER3_end333; ++qSUPPLIER3_it334)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER3_it334->first);
        string P__MFGR = get<1>(qSUPPLIER3_it334->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER3_it334->first);
        qSUPPLIER3[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY,N1__REGIONKEY)] += 
            qCUSTOMER3SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER3NATION1_it336 
        = qSUPPLIER3NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER3NATION1_end335 = qSUPPLIER3NATION1.end();
    for (
        ; qSUPPLIER3NATION1_it336 != qSUPPLIER3NATION1_end335; 
        ++qSUPPLIER3NATION1_it336)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER3NATION1_it336->first);
        string P__MFGR = get<1>(qSUPPLIER3NATION1_it336->first);
        qSUPPLIER3NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY)] += qCUSTOMER3SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1_it338 = qSUPPLIER3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1_end337 = qSUPPLIER3PARTS1.end();
    for (
        ; qSUPPLIER3PARTS1_it338 != qSUPPLIER3PARTS1_end337; ++qSUPPLIER3PARTS1_it338)
    {
        int64_t x_qSUPPLIER3PARTS_P__PARTKEY = get<0>(
            qSUPPLIER3PARTS1_it338->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER3PARTS1_it338->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER3PARTS1_it338->first);
        qSUPPLIER3PARTS1[make_tuple(
            x_qSUPPLIER3PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY,
            N1__REGIONKEY)] += qCUSTOMER3SUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER3PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,
            CUSTKEY)]*qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1NATION1_it340 = qSUPPLIER3PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1NATION1_end339 = qSUPPLIER3PARTS1NATION1.end();
    for (
        ; qSUPPLIER3PARTS1NATION1_it340 != qSUPPLIER3PARTS1NATION1_end339; 
        ++qSUPPLIER3PARTS1NATION1_it340)
    {
        int64_t x_qSUPPLIER3PARTS_P__PARTKEY = get<0>(
            qSUPPLIER3PARTS1NATION1_it340->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(
            qSUPPLIER3PARTS1NATION1_it340->first);
        qSUPPLIER3PARTS1NATION1[make_tuple(
            x_qSUPPLIER3PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY)] += 
            qCUSTOMER3SUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER3PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,CUSTKEY)];
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

    SK_idx_NK[SUPPKEY] = NATIONKEY;

    indexed_result_domain_maintenance();

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it342 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end341 = 
        q.end();
    for (; q_it342 != q_end341; ++q_it342)
    {
        string P__MFGR = get<0>(q_it342->first);
        int64_t N2__REGIONKEY = get<1>(q_it342->first);
        int64_t N1__REGIONKEY = get<2>(q_it342->first);
        int64_t C__NATIONKEY = get<3>(q_it342->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += qSUPPLIER1[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)]*100;
    }

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
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += qSUPPLIER3[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)]*-1;
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
            qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
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
        string P__MFGR = get<2>(qCUSTOMER1NATION1_it348->first);
        qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,P__MFGR)] += qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER1PARTS1_it350 
        = qCUSTOMER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_end349 = qCUSTOMER1PARTS1.end();
    for (
        ; qCUSTOMER1PARTS1_it350 != qCUSTOMER1PARTS1_end349; ++qCUSTOMER1PARTS1_it350)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1_it350->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1PARTS1_it350->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1PARTS1_it350->first);
        qCUSTOMER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)] += 
            qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,SUPPKEY,
            x_qCUSTOMER_C__CUSTKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_it352 = qCUSTOMER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_end351 = qCUSTOMER1PARTS1NATION1.end();
    for (
        ; qCUSTOMER1PARTS1NATION1_it352 != qCUSTOMER1PARTS1NATION1_end351; 
        ++qCUSTOMER1PARTS1NATION1_it352)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1NATION1_it352->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1PARTS1NATION1_it352->first);
        qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,NATIONKEY)] += 
            qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,SUPPKEY,x_qCUSTOMER_C__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_it354 = 
        qCUSTOMER3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_end353 = 
        qCUSTOMER3.end();
    for (; qCUSTOMER3_it354 != qCUSTOMER3_end353; ++qCUSTOMER3_it354)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER3_it354->first);
        string P__MFGR = get<1>(qCUSTOMER3_it354->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER3_it354->first);
        qCUSTOMER3[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,N2__REGIONKEY)] += 
            qCUSTOMER3SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER3NATION1_it356 
        = qCUSTOMER3NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER3NATION1_end355 = qCUSTOMER3NATION1.end();
    for (
        ; qCUSTOMER3NATION1_it356 != qCUSTOMER3NATION1_end355; 
        ++qCUSTOMER3NATION1_it356)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER3NATION1_it356->first);
        string P__MFGR = get<2>(qCUSTOMER3NATION1_it356->first);
        qCUSTOMER3NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,P__MFGR)] += qCUSTOMER3SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER3PARTS1_it358 
        = qCUSTOMER3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER3PARTS1_end357 = qCUSTOMER3PARTS1.end();
    for (
        ; qCUSTOMER3PARTS1_it358 != qCUSTOMER3PARTS1_end357; ++qCUSTOMER3PARTS1_it358)
    {
        int64_t x_qCUSTOMER3PARTS_P__PARTKEY = get<0>(
            qCUSTOMER3PARTS1_it358->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER3PARTS1_it358->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER3PARTS1_it358->first);
        qCUSTOMER3PARTS1[make_tuple(
            x_qCUSTOMER3PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)] += 
            qCUSTOMER3SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER3PARTS_P__PARTKEY,SUPPKEY,
            x_qCUSTOMER_C__CUSTKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3PARTS1NATION1_it360 = qCUSTOMER3PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3PARTS1NATION1_end359 = qCUSTOMER3PARTS1NATION1.end();
    for (
        ; qCUSTOMER3PARTS1NATION1_it360 != qCUSTOMER3PARTS1NATION1_end359; 
        ++qCUSTOMER3PARTS1NATION1_it360)
    {
        int64_t x_qCUSTOMER3PARTS_P__PARTKEY = get<0>(
            qCUSTOMER3PARTS1NATION1_it360->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER3PARTS1NATION1_it360->first);
        qCUSTOMER3PARTS1NATION1[make_tuple(
            x_qCUSTOMER3PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,NATIONKEY)] += 
            qCUSTOMER3SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER3PARTS_P__PARTKEY,SUPPKEY,x_qCUSTOMER_C__CUSTKEY)];
    }

    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_it362 = 
        qLINEITEM3.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_end361 = 
        qLINEITEM3.end();
    for (; qLINEITEM3_it362 != qLINEITEM3_end361; ++qLINEITEM3_it362)
    {
        int64_t N2__REGIONKEY = get<1>(qLINEITEM3_it362->first);
        qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)] += qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }
    qLINEITEM3NATION1[make_tuple(SUPPKEY,NATIONKEY)] += 1;
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it364 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end363 
        = qNATION1.end();
    for (; qNATION1_it364 != qNATION1_end363; ++qNATION1_it364)
    {
        string P__MFGR = get<1>(qNATION1_it364->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it364->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it364->first);
        qNATION1[make_tuple(
            NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += qSUPPLIER1[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it366 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end365 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it366 != qNATION1NATION1_end365; ++qNATION1NATION1_it366)
    {
        string P__MFGR = get<1>(qNATION1NATION1_it366->first);
        int64_t x_qNATION1NATION_N1__NATIONKEY = get<2>(
            qNATION1NATION1_it366->first);
        qNATION1NATION1[make_tuple(
            NATIONKEY,P__MFGR,x_qNATION1NATION_N1__NATIONKEY)] += 
            qSUPPLIER1NATION1[make_tuple(SUPPKEY,P__MFGR,x_qNATION1NATION_N1__NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it368 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end367 = 
        qNATION2.end();
    for (; qNATION2_it368 != qNATION2_end367; ++qNATION2_it368)
    {
        string P__MFGR = get<0>(qNATION2_it368->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it368->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it368->first);
        qNATION2[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += qSUPPLIER1NATION1[make_tuple(
            SUPPKEY,P__MFGR,x_qNATION_N1__NATIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION3_it370 = qNATION3.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION3_end369 = qNATION3.end();
    for (; qNATION3_it370 != qNATION3_end369; ++qNATION3_it370)
    {
        string P__MFGR = get<1>(qNATION3_it370->first);
        qNATION3[make_tuple(NATIONKEY,P__MFGR)] += qSUPPLIER1NATION1[make_tuple(
            SUPPKEY,P__MFGR,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_it372 = qNATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_end371 = qNATION3ORDERS1.end();
    for (
        ; qNATION3ORDERS1_it372 != qNATION3ORDERS1_end371; ++qNATION3ORDERS1_it372)
    {
        int64_t x_qNATION3ORDERS_O__ORDERKEY = get<0>(
            qNATION3ORDERS1_it372->first);
        string P__MFGR = get<2>(qNATION3ORDERS1_it372->first);
        int64_t x_qNATION3ORDERS_O__CUSTKEY = get<3>(
            qNATION3ORDERS1_it372->first);
        qNATION3ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR,x_qNATION3ORDERS_O__CUSTKEY)] += 
            qORDERS1SUPPLIER1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,SUPPKEY,
            P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION4_it374 = qNATION4.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION4_end373 
        = qNATION4.end();
    for (; qNATION4_it374 != qNATION4_end373; ++qNATION4_it374)
    {
        string P__MFGR = get<1>(qNATION4_it374->first);
        int64_t C__NATIONKEY = get<2>(qNATION4_it374->first);
        int64_t N1__REGIONKEY = get<3>(qNATION4_it374->first);
        qNATION4[make_tuple(
            NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += qSUPPLIER3[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION4NATION1_it376 = 
        qNATION4NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION4NATION1_end375 = qNATION4NATION1.end();
    for (
        ; qNATION4NATION1_it376 != qNATION4NATION1_end375; ++qNATION4NATION1_it376)
    {
        string P__MFGR = get<1>(qNATION4NATION1_it376->first);
        int64_t x_qNATION4NATION_N1__NATIONKEY = get<2>(
            qNATION4NATION1_it376->first);
        qNATION4NATION1[make_tuple(
            NATIONKEY,P__MFGR,x_qNATION4NATION_N1__NATIONKEY)] += 
            qSUPPLIER3NATION1[make_tuple(SUPPKEY,P__MFGR,x_qNATION4NATION_N1__NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_it378 = 
        qNATION5.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_end377 = 
        qNATION5.end();
    for (; qNATION5_it378 != qNATION5_end377; ++qNATION5_it378)
    {
        string P__MFGR = get<0>(qNATION5_it378->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION5_it378->first);
        int64_t N2__REGIONKEY = get<2>(qNATION5_it378->first);
        qNATION5[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += qSUPPLIER3NATION1[make_tuple(
            SUPPKEY,P__MFGR,x_qNATION_N1__NATIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION6_it380 = qNATION6.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION6_end379 = qNATION6.end();
    for (; qNATION6_it380 != qNATION6_end379; ++qNATION6_it380)
    {
        string P__MFGR = get<1>(qNATION6_it380->first);
        qNATION6[make_tuple(NATIONKEY,P__MFGR)] += qSUPPLIER3NATION1[make_tuple(
            SUPPKEY,P__MFGR,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION6ORDERS1_it382 = qNATION6ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION6ORDERS1_end381 = qNATION6ORDERS1.end();
    for (
        ; qNATION6ORDERS1_it382 != qNATION6ORDERS1_end381; ++qNATION6ORDERS1_it382)
    {
        int64_t x_qNATION6ORDERS_O__ORDERKEY = get<0>(
            qNATION6ORDERS1_it382->first);
        string P__MFGR = get<2>(qNATION6ORDERS1_it382->first);
        int64_t x_qNATION6ORDERS_O__CUSTKEY = get<3>(
            qNATION6ORDERS1_it382->first);
        qNATION6ORDERS1[make_tuple(
            x_qNATION6ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR,x_qNATION6ORDERS_O__CUSTKEY)] += 
            qORDERS3SUPPLIER1[make_tuple(
            x_qNATION6ORDERS_O__ORDERKEY,SUPPKEY,
            P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qNATION6ORDERS_O__CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it384 = 
        qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end383 = 
        qORDERS1.end();
    for (; qORDERS1_it384 != qORDERS1_end383; ++qORDERS1_it384)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it384->first);
        string P__MFGR = get<1>(qORDERS1_it384->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1_it384->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,P__MFGR,N2__REGIONKEY)] += qORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY,P__MFGR)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1NATION1_it386 = 
        qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1NATION1_end385 = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it386 != qORDERS1NATION1_end385; ++qORDERS1NATION1_it386)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1NATION1_it386->first);
        string P__MFGR = get<2>(qORDERS1NATION1_it386->first);
        qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)] += qORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1PARTS1_it388 = 
        qORDERS1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS1PARTS1_end387 = qORDERS1PARTS1.end();
    for (
        ; qORDERS1PARTS1_it388 != qORDERS1PARTS1_end387; ++qORDERS1PARTS1_it388)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1PARTS1_it388->first);
        int64_t x_qORDERS1PARTS_P__PARTKEY = get<1>(
            qORDERS1PARTS1_it388->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1PARTS1_it388->first);
        qORDERS1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,N2__REGIONKEY)] += 
            qORDERS1SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,
            SUPPKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS1PARTS1NATION1_it390 = qORDERS1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS1PARTS1NATION1_end389 = qORDERS1PARTS1NATION1.end();
    for (
        ; qORDERS1PARTS1NATION1_it390 != qORDERS1PARTS1NATION1_end389; 
        ++qORDERS1PARTS1NATION1_it390)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(
            qORDERS1PARTS1NATION1_it390->first);
        int64_t x_qORDERS1PARTS_P__PARTKEY = get<1>(
            qORDERS1PARTS1NATION1_it390->first);
        qORDERS1PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,NATIONKEY)] += 
            qORDERS1SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,SUPPKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_it392 = 
        qORDERS3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_end391 = 
        qORDERS3.end();
    for (; qORDERS3_it392 != qORDERS3_end391; ++qORDERS3_it392)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3_it392->first);
        string P__MFGR = get<1>(qORDERS3_it392->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS3_it392->first);
        qORDERS3[make_tuple(
            x_qORDERS_O__ORDERKEY,P__MFGR,N2__REGIONKEY)] += qORDERS3SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY,P__MFGR)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS3NATION1_it394 = 
        qORDERS3NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS3NATION1_end393 = qORDERS3NATION1.end();
    for (
        ; qORDERS3NATION1_it394 != qORDERS3NATION1_end393; ++qORDERS3NATION1_it394)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3NATION1_it394->first);
        string P__MFGR = get<2>(qORDERS3NATION1_it394->first);
        qORDERS3NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)] += qORDERS3SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS3PARTS1_it396 = 
        qORDERS3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS3PARTS1_end395 = qORDERS3PARTS1.end();
    for (
        ; qORDERS3PARTS1_it396 != qORDERS3PARTS1_end395; ++qORDERS3PARTS1_it396)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3PARTS1_it396->first);
        int64_t x_qORDERS3PARTS_P__PARTKEY = get<1>(
            qORDERS3PARTS1_it396->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS3PARTS1_it396->first);
        qORDERS3PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS3PARTS_P__PARTKEY,N2__REGIONKEY)] += 
            qORDERS3SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS3PARTS_P__PARTKEY,
            SUPPKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS3PARTS1NATION1_it398 = qORDERS3PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS3PARTS1NATION1_end397 = qORDERS3PARTS1NATION1.end();
    for (
        ; qORDERS3PARTS1NATION1_it398 != qORDERS3PARTS1NATION1_end397; 
        ++qORDERS3PARTS1NATION1_it398)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(
            qORDERS3PARTS1NATION1_it398->first);
        int64_t x_qORDERS3PARTS_P__PARTKEY = get<1>(
            qORDERS3PARTS1NATION1_it398->first);
        qORDERS3PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS3PARTS_P__PARTKEY,NATIONKEY)] += 
            qORDERS3SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS3PARTS_P__PARTKEY,SUPPKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it400 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end399 
        = qPARTS1.end();
    for (; qPARTS1_it400 != qPARTS1_end399; ++qPARTS1_it400)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it400->first);
        int64_t C__NATIONKEY = get<1>(qPARTS1_it400->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it400->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1_it400->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            qSUPPLIER1PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it402 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end401 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it402 != qPARTS1NATION1_end401; ++qPARTS1NATION1_it402)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1_it402->first);
        int64_t C__NATIONKEY = get<2>(qPARTS1NATION1_it402->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1NATION1_it402->first);
        qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            qSUPPLIER1PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_it404 = qPARTS1NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_end403 = qPARTS1NATION1NATION1.end();
    for (
        ; qPARTS1NATION1NATION1_it404 != qPARTS1NATION1NATION1_end403; 
        ++qPARTS1NATION1NATION1_it404)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS1NATION1NATION1_it404->first);
        int64_t x_qPARTS1NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS1NATION1NATION1_it404->first);
        qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)] += 
            qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it406 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end405 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it406 != qPARTS1NATION2_end405; ++qPARTS1NATION2_it406)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION2_it406->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION2_it406->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1NATION2_it406->first);
        qPARTS1NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,
            x_qPARTS1NATION_N1__NATIONKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_it408 = 
        qPARTS1NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_end407 = 
        qPARTS1NATION3.end();
    for (
        ; qPARTS1NATION3_it408 != qPARTS1NATION3_end407; ++qPARTS1NATION3_it408)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION3_it408->first);
        qPARTS1NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY)] += qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_it410 = qPARTS1NATION3LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_end409 = qPARTS1NATION3LINEITEM1.end();
    for (
        ; qPARTS1NATION3LINEITEM1_it410 != qPARTS1NATION3LINEITEM1_end409; 
        ++qPARTS1NATION3LINEITEM1_it410)
    {
        int64_t x_qPARTS1NATION3LINEITEM_L__ORDERKEY = get<0>(
            qPARTS1NATION3LINEITEM1_it410->first);
        qPARTS1NATION3LINEITEM1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__ORDERKEY,NATIONKEY,SUPPKEY)] += 
            qLINEITEM1NATION1[make_tuple(x_qPARTS1NATION3LINEITEM_L__ORDERKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1ORDERS1_it412 = qPARTS1NATION3LINEITEM1ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1ORDERS1_end411 = qPARTS1NATION3LINEITEM1ORDERS1.end();
    for (
        ; qPARTS1NATION3LINEITEM1ORDERS1_it412 != qPARTS1NATION3LINEITEM1ORDERS1_end411; 
        ++qPARTS1NATION3LINEITEM1ORDERS1_it412)
    {
        int64_t x_qPARTS1NATION3LINEITEM1ORDERS_O__CUSTKEY = get<0>(
            qPARTS1NATION3LINEITEM1ORDERS1_it412->first);
        qPARTS1NATION3LINEITEM1ORDERS1[make_tuple(
            x_qPARTS1NATION3LINEITEM1ORDERS_O__CUSTKEY,NATIONKEY,SUPPKEY)] += 
            qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qPARTS1NATION3LINEITEM1ORDERS_O__CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_it414 = qPARTS1NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_end413 = qPARTS1NATION3ORDERS1.end();
    for (
        ; qPARTS1NATION3ORDERS1_it414 != qPARTS1NATION3ORDERS1_end413; 
        ++qPARTS1NATION3ORDERS1_it414)
    {
        int64_t x_qPARTS1NATION3ORDERS_O__ORDERKEY = get<0>(
            qPARTS1NATION3ORDERS1_it414->first);
        int64_t x_qPARTS_P__PARTKEY = get<1>(
            qPARTS1NATION3ORDERS1_it414->first);
        int64_t x_qPARTS1NATION3ORDERS_O__CUSTKEY = get<3>(
            qPARTS1NATION3ORDERS1_it414->first);
        qPARTS1NATION3ORDERS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY,
            x_qPARTS1NATION3ORDERS_O__CUSTKEY)] += qORDERS1SUPPLIER1PARTS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,
            SUPPKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3ORDERS1LINEITEM1_it416 = qPARTS1NATION3ORDERS1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3ORDERS1LINEITEM1_end415 = qPARTS1NATION3ORDERS1LINEITEM1.end();
    for (
        ; qPARTS1NATION3ORDERS1LINEITEM1_it416 != qPARTS1NATION3ORDERS1LINEITEM1_end415; 
        ++qPARTS1NATION3ORDERS1LINEITEM1_it416)
    {
        int64_t x_qPARTS1NATION3ORDERS_O__CUSTKEY = get<2>(
            qPARTS1NATION3ORDERS1LINEITEM1_it416->first);
        qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            SUPPKEY,NATIONKEY,x_qPARTS1NATION3ORDERS_O__CUSTKEY)] += 
            qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2_it418 = qPARTS2.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS2_end417 
        = qPARTS2.end();
    for (; qPARTS2_it418 != qPARTS2_end417; ++qPARTS2_it418)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2_it418->first);
        int64_t C__NATIONKEY = get<1>(qPARTS2_it418->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2_it418->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS2_it418->first);
        qPARTS2[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            qSUPPLIER3PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_it420 = qPARTS2NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_end419 = qPARTS2NATION1.end();
    for (
        ; qPARTS2NATION1_it420 != qPARTS2NATION1_end419; ++qPARTS2NATION1_it420)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION1_it420->first);
        int64_t C__NATIONKEY = get<2>(qPARTS2NATION1_it420->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS2NATION1_it420->first);
        qPARTS2NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            qSUPPLIER3PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1NATION1_it422 = qPARTS2NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1NATION1_end421 = qPARTS2NATION1NATION1.end();
    for (
        ; qPARTS2NATION1NATION1_it422 != qPARTS2NATION1NATION1_end421; 
        ++qPARTS2NATION1NATION1_it422)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS2NATION1NATION1_it422->first);
        int64_t x_qPARTS2NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS2NATION1NATION1_it422->first);
        qPARTS2NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,x_qPARTS2NATION1NATION_N1__NATIONKEY)] += 
            qSUPPLIER3PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,x_qPARTS2NATION1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS2NATION2_it424 = 
        qPARTS2NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2NATION2_end423 = qPARTS2NATION2.end();
    for (
        ; qPARTS2NATION2_it424 != qPARTS2NATION2_end423; ++qPARTS2NATION2_it424)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION2_it424->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION2_it424->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2NATION2_it424->first);
        qPARTS2NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            qSUPPLIER3PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,
            x_qPARTS2NATION_N1__NATIONKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qPARTS2NATION3_it426 = 
        qPARTS2NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS2NATION3_end425 = 
        qPARTS2NATION3.end();
    for (
        ; qPARTS2NATION3_it426 != qPARTS2NATION3_end425; ++qPARTS2NATION3_it426)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION3_it426->first);
        qPARTS2NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY)] += qSUPPLIER3PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION3ORDERS1_it428 = qPARTS2NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION3ORDERS1_end427 = qPARTS2NATION3ORDERS1.end();
    for (
        ; qPARTS2NATION3ORDERS1_it428 != qPARTS2NATION3ORDERS1_end427; 
        ++qPARTS2NATION3ORDERS1_it428)
    {
        int64_t x_qPARTS2NATION3ORDERS_O__ORDERKEY = get<0>(
            qPARTS2NATION3ORDERS1_it428->first);
        int64_t x_qPARTS_P__PARTKEY = get<1>(
            qPARTS2NATION3ORDERS1_it428->first);
        int64_t x_qPARTS2NATION3ORDERS_O__CUSTKEY = get<3>(
            qPARTS2NATION3ORDERS1_it428->first);
        qPARTS2NATION3ORDERS1[make_tuple(
            x_qPARTS2NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY,
            x_qPARTS2NATION3ORDERS_O__CUSTKEY)] += qORDERS3SUPPLIER1PARTS1[make_tuple(
            x_qPARTS2NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,
            SUPPKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qPARTS2NATION3ORDERS_O__CUSTKEY,NATIONKEY)];
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

    PARTS.insert(make_tuple(
        PARTKEY,NAME,MFGR,BRAND,TYPE,SIZE,CONTAINER,RETAILPRICE,COMMENT));

    indexed_result_domain_maintenance();

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it430 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end429 = 
        q.end();
    for (; q_it430 != q_end429; ++q_it430)
    {
        int64_t N2__REGIONKEY = get<1>(q_it430->first);
        int64_t N1__REGIONKEY = get<2>(q_it430->first);
        int64_t C__NATIONKEY = get<3>(q_it430->first);
        q[make_tuple(
            MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += qPARTS1[make_tuple(
            PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it432 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end431 = 
        q.end();
    for (; q_it432 != q_end431; ++q_it432)
    {
        int64_t N2__REGIONKEY = get<1>(q_it432->first);
        int64_t N1__REGIONKEY = get<2>(q_it432->first);
        int64_t C__NATIONKEY = get<3>(q_it432->first);
        q[make_tuple(
            MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += qPARTS2[make_tuple(
            PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)]*-1;
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it434 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end433 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it434 != qCUSTOMER1_end433; ++qCUSTOMER1_it434)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it434->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1_it434->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,MFGR,N2__REGIONKEY)] += qCUSTOMER1PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1NATION1_it436 
        = qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1NATION1_end435 = qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it436 != qCUSTOMER1NATION1_end435; 
        ++qCUSTOMER1NATION1_it436)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1NATION1_it436->first);
        int64_t x_qCUSTOMER1NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER1NATION1_it436->first);
        qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1NATION_N1__NATIONKEY,MFGR)] += 
            qCUSTOMER1PARTS1NATION1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_it438 = qCUSTOMER1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_end437 = qCUSTOMER1SUPPLIER1.end();
    for (
        ; qCUSTOMER1SUPPLIER1_it438 != qCUSTOMER1SUPPLIER1_end437; 
        ++qCUSTOMER1SUPPLIER1_it438)
    {
        int64_t x_qCUSTOMER1SUPPLIER_S__SUPPKEY = get<0>(
            qCUSTOMER1SUPPLIER1_it438->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1SUPPLIER1_it438->first);
        qCUSTOMER1SUPPLIER1[make_tuple(
            x_qCUSTOMER1SUPPLIER_S__SUPPKEY,x_qCUSTOMER_C__CUSTKEY,MFGR)] += 
            qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER1SUPPLIER_S__SUPPKEY,x_qCUSTOMER_C__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_it440 = 
        qCUSTOMER3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_end439 = 
        qCUSTOMER3.end();
    for (; qCUSTOMER3_it440 != qCUSTOMER3_end439; ++qCUSTOMER3_it440)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER3_it440->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER3_it440->first);
        qCUSTOMER3[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,MFGR,N2__REGIONKEY)] += qCUSTOMER3PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER3NATION1_it442 
        = qCUSTOMER3NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER3NATION1_end441 = qCUSTOMER3NATION1.end();
    for (
        ; qCUSTOMER3NATION1_it442 != qCUSTOMER3NATION1_end441; 
        ++qCUSTOMER3NATION1_it442)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER3NATION1_it442->first);
        int64_t x_qCUSTOMER3NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER3NATION1_it442->first);
        qCUSTOMER3NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER3NATION_N1__NATIONKEY,MFGR)] += 
            qCUSTOMER3PARTS1NATION1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER3NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER3SUPPLIER1_it444 = qCUSTOMER3SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER3SUPPLIER1_end443 = qCUSTOMER3SUPPLIER1.end();
    for (
        ; qCUSTOMER3SUPPLIER1_it444 != qCUSTOMER3SUPPLIER1_end443; 
        ++qCUSTOMER3SUPPLIER1_it444)
    {
        int64_t x_qCUSTOMER3SUPPLIER_S__SUPPKEY = get<0>(
            qCUSTOMER3SUPPLIER1_it444->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER3SUPPLIER1_it444->first);
        qCUSTOMER3SUPPLIER1[make_tuple(
            x_qCUSTOMER3SUPPLIER_S__SUPPKEY,x_qCUSTOMER_C__CUSTKEY,MFGR)] += 
            qCUSTOMER3SUPPLIER1PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER3SUPPLIER_S__SUPPKEY,x_qCUSTOMER_C__CUSTKEY)];
    }
    qLINEITEM2[make_tuple(PARTKEY,MFGR)] += 1;
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it446 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end445 
        = qNATION1.end();
    for (; qNATION1_it446 != qNATION1_end445; ++qNATION1_it446)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it446->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it446->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it446->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            qPARTS1NATION1[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it448 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end447 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it448 != qNATION1NATION1_end447; ++qNATION1NATION1_it448)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1NATION1_it448->first);
        int64_t x_qNATION1NATION_N1__NATIONKEY = get<2>(
            qNATION1NATION1_it448->first);
        qNATION1NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR,x_qNATION1NATION_N1__NATIONKEY)] += 
            qPARTS1NATION1NATION1[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,x_qNATION1NATION_N1__NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it450 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end449 = 
        qNATION2.end();
    for (; qNATION2_it450 != qNATION2_end449; ++qNATION2_it450)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it450->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it450->first);
        qNATION2[make_tuple(
            MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += qPARTS1NATION2[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION3_it452 = qNATION3.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION3_end451 = qNATION3.end();
    for (; qNATION3_it452 != qNATION3_end451; ++qNATION3_it452)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION3_it452->first);
        qNATION3[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR)] += qPARTS1NATION3[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_it454 = qNATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_end453 = qNATION3ORDERS1.end();
    for (
        ; qNATION3ORDERS1_it454 != qNATION3ORDERS1_end453; ++qNATION3ORDERS1_it454)
    {
        int64_t x_qNATION3ORDERS_O__ORDERKEY = get<0>(
            qNATION3ORDERS1_it454->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION3ORDERS1_it454->first);
        int64_t x_qNATION3ORDERS_O__CUSTKEY = get<3>(
            qNATION3ORDERS1_it454->first);
        qNATION3ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,x_qNATION_N1__NATIONKEY,MFGR,
            x_qNATION3ORDERS_O__CUSTKEY)] += qPARTS1NATION3ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,PARTKEY,x_qNATION_N1__NATIONKEY,
            x_qNATION3ORDERS_O__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION4_it456 = qNATION4.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION4_end455 
        = qNATION4.end();
    for (; qNATION4_it456 != qNATION4_end455; ++qNATION4_it456)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4_it456->first);
        int64_t C__NATIONKEY = get<2>(qNATION4_it456->first);
        int64_t N1__REGIONKEY = get<3>(qNATION4_it456->first);
        qNATION4[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            qPARTS2NATION1[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION4NATION1_it458 = 
        qNATION4NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION4NATION1_end457 = qNATION4NATION1.end();
    for (
        ; qNATION4NATION1_it458 != qNATION4NATION1_end457; ++qNATION4NATION1_it458)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4NATION1_it458->first);
        int64_t x_qNATION4NATION_N1__NATIONKEY = get<2>(
            qNATION4NATION1_it458->first);
        qNATION4NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR,x_qNATION4NATION_N1__NATIONKEY)] += 
            qPARTS2NATION1NATION1[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,x_qNATION4NATION_N1__NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_it460 = 
        qNATION5.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_end459 = 
        qNATION5.end();
    for (; qNATION5_it460 != qNATION5_end459; ++qNATION5_it460)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION5_it460->first);
        int64_t N2__REGIONKEY = get<2>(qNATION5_it460->first);
        qNATION5[make_tuple(
            MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += qPARTS2NATION2[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION6_it462 = qNATION6.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION6_end461 = qNATION6.end();
    for (; qNATION6_it462 != qNATION6_end461; ++qNATION6_it462)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION6_it462->first);
        qNATION6[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR)] += qPARTS2NATION3[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION6ORDERS1_it464 = qNATION6ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION6ORDERS1_end463 = qNATION6ORDERS1.end();
    for (
        ; qNATION6ORDERS1_it464 != qNATION6ORDERS1_end463; ++qNATION6ORDERS1_it464)
    {
        int64_t x_qNATION6ORDERS_O__ORDERKEY = get<0>(
            qNATION6ORDERS1_it464->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION6ORDERS1_it464->first);
        int64_t x_qNATION6ORDERS_O__CUSTKEY = get<3>(
            qNATION6ORDERS1_it464->first);
        qNATION6ORDERS1[make_tuple(
            x_qNATION6ORDERS_O__ORDERKEY,x_qNATION_N1__NATIONKEY,MFGR,
            x_qNATION6ORDERS_O__CUSTKEY)] += qPARTS2NATION3ORDERS1[make_tuple(
            x_qNATION6ORDERS_O__ORDERKEY,PARTKEY,x_qNATION_N1__NATIONKEY,
            x_qNATION6ORDERS_O__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it466 = 
        qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end465 = 
        qORDERS1.end();
    for (; qORDERS1_it466 != qORDERS1_end465; ++qORDERS1_it466)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it466->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1_it466->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,MFGR,N2__REGIONKEY)] += qORDERS1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1NATION1_it468 = 
        qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1NATION1_end467 = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it468 != qORDERS1NATION1_end467; ++qORDERS1NATION1_it468)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1NATION1_it468->first);
        int64_t x_qORDERS1NATION_N1__NATIONKEY = get<1>(
            qORDERS1NATION1_it468->first);
        qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1NATION_N1__NATIONKEY,MFGR)] += 
            qORDERS1PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,x_qORDERS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1SUPPLIER1_it470 
        = qORDERS1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1SUPPLIER1_end469 = qORDERS1SUPPLIER1.end();
    for (
        ; qORDERS1SUPPLIER1_it470 != qORDERS1SUPPLIER1_end469; 
        ++qORDERS1SUPPLIER1_it470)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1SUPPLIER1_it470->first);
        int64_t x_qORDERS1SUPPLIER_S__SUPPKEY = get<1>(
            qORDERS1SUPPLIER1_it470->first);
        qORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1SUPPLIER_S__SUPPKEY,MFGR)] += 
            qORDERS1SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,x_qORDERS1SUPPLIER_S__SUPPKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_it472 = 
        qORDERS3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_end471 = 
        qORDERS3.end();
    for (; qORDERS3_it472 != qORDERS3_end471; ++qORDERS3_it472)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3_it472->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS3_it472->first);
        qORDERS3[make_tuple(
            x_qORDERS_O__ORDERKEY,MFGR,N2__REGIONKEY)] += qORDERS3PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS3NATION1_it474 = 
        qORDERS3NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS3NATION1_end473 = qORDERS3NATION1.end();
    for (
        ; qORDERS3NATION1_it474 != qORDERS3NATION1_end473; ++qORDERS3NATION1_it474)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3NATION1_it474->first);
        int64_t x_qORDERS3NATION_N1__NATIONKEY = get<1>(
            qORDERS3NATION1_it474->first);
        qORDERS3NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS3NATION_N1__NATIONKEY,MFGR)] += 
            qORDERS3PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,x_qORDERS3NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS3SUPPLIER1_it476 
        = qORDERS3SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS3SUPPLIER1_end475 = qORDERS3SUPPLIER1.end();
    for (
        ; qORDERS3SUPPLIER1_it476 != qORDERS3SUPPLIER1_end475; 
        ++qORDERS3SUPPLIER1_it476)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3SUPPLIER1_it476->first);
        int64_t x_qORDERS3SUPPLIER_S__SUPPKEY = get<1>(
            qORDERS3SUPPLIER1_it476->first);
        qORDERS3SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS3SUPPLIER_S__SUPPKEY,MFGR)] += 
            qORDERS3SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,x_qORDERS3SUPPLIER_S__SUPPKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it478 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end477 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it478 != qSUPPLIER1_end477; ++qSUPPLIER1_it478)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it478->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1_it478->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1_it478->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            qSUPPLIER1PARTS1[make_tuple(
            PARTKEY,x_qSUPPLIER_S__SUPPKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER1NATION1_it480 
        = qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER1NATION1_end479 = qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it480 != qSUPPLIER1NATION1_end479; 
        ++qSUPPLIER1NATION1_it480)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1NATION1_it480->first);
        int64_t x_qSUPPLIER1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1NATION1_it480->first);
        qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,MFGR,x_qSUPPLIER1NATION_N1__NATIONKEY)] += 
            qSUPPLIER1PARTS1NATION1[make_tuple(
            PARTKEY,x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER3_it482 = qSUPPLIER3.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3_end481 = qSUPPLIER3.end();
    for (; qSUPPLIER3_it482 != qSUPPLIER3_end481; ++qSUPPLIER3_it482)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER3_it482->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER3_it482->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER3_it482->first);
        qSUPPLIER3[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            qSUPPLIER3PARTS1[make_tuple(
            PARTKEY,x_qSUPPLIER_S__SUPPKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER3NATION1_it484 
        = qSUPPLIER3NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER3NATION1_end483 = qSUPPLIER3NATION1.end();
    for (
        ; qSUPPLIER3NATION1_it484 != qSUPPLIER3NATION1_end483; 
        ++qSUPPLIER3NATION1_it484)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER3NATION1_it484->first);
        int64_t x_qSUPPLIER3NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER3NATION1_it484->first);
        qSUPPLIER3NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,MFGR,x_qSUPPLIER3NATION_N1__NATIONKEY)] += 
            qSUPPLIER3PARTS1NATION1[make_tuple(
            PARTKEY,x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER3NATION_N1__NATIONKEY)];
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
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it486 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end485 = 
        q.end();
    for (; q_it486 != q_end485; ++q_it486)
    {
        string P__MFGR = get<0>(q_it486->first);
        q[make_tuple(
            P__MFGR,REGIONKEY,REGIONKEY,NATIONKEY)] += -1*qNATION3[make_tuple(
            NATIONKEY,P__MFGR)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it488 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end487 = 
        q.end();
    for (; q_it488 != q_end487; ++q_it488)
    {
        string P__MFGR = get<0>(q_it488->first);
        q[make_tuple(
            P__MFGR,REGIONKEY,REGIONKEY,NATIONKEY)] += -1*qNATION6[make_tuple(
            NATIONKEY,P__MFGR)]*-1;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it490 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end489 = 
        q.end();
    for (; q_it490 != q_end489; ++q_it490)
    {
        string P__MFGR = get<0>(q_it490->first);
        int64_t N2__REGIONKEY = get<1>(q_it490->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,REGIONKEY,NATIONKEY)] += -1*qNATION2[make_tuple(
            P__MFGR,NATIONKEY,N2__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it492 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end491 = 
        q.end();
    for (; q_it492 != q_end491; ++q_it492)
    {
        string P__MFGR = get<0>(q_it492->first);
        int64_t N2__REGIONKEY = get<1>(q_it492->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,REGIONKEY,NATIONKEY)] += -1*qNATION5[make_tuple(
            P__MFGR,NATIONKEY,N2__REGIONKEY)]*-1;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it494 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end493 = 
        q.end();
    for (; q_it494 != q_end493; ++q_it494)
    {
        string P__MFGR = get<0>(q_it494->first);
        int64_t REGIONKEY = get<2>(q_it494->first);
        int64_t C__NATIONKEY = get<3>(q_it494->first);
        q[make_tuple(
            P__MFGR,REGIONKEY,REGIONKEY,C__NATIONKEY)] += -1*qNATION1[make_tuple(
            NATIONKEY,P__MFGR,C__NATIONKEY,REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it496 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end495 = 
        q.end();
    for (; q_it496 != q_end495; ++q_it496)
    {
        string P__MFGR = get<0>(q_it496->first);
        int64_t REGIONKEY = get<2>(q_it496->first);
        int64_t C__NATIONKEY = get<3>(q_it496->first);
        q[make_tuple(
            P__MFGR,REGIONKEY,REGIONKEY,C__NATIONKEY)] += -1*qNATION4[make_tuple(
            NATIONKEY,P__MFGR,C__NATIONKEY,REGIONKEY)]*-1;
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it498 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end497 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it498 != qCUSTOMER1_end497; ++qCUSTOMER1_it498)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it498->first);
        string P__MFGR = get<1>(qCUSTOMER1_it498->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,REGIONKEY)] += -1*qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER1PARTS1_it500 
        = qCUSTOMER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_end499 = qCUSTOMER1PARTS1.end();
    for (
        ; qCUSTOMER1PARTS1_it500 != qCUSTOMER1PARTS1_end499; ++qCUSTOMER1PARTS1_it500)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1_it500->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1PARTS1_it500->first);
        qCUSTOMER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,REGIONKEY)] += 
            -1*qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,NATIONKEY)];
    }
    qCUSTOMER2[make_tuple(NATIONKEY,REGIONKEY)] += -1;
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_it502 = 
        qCUSTOMER3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_end501 = 
        qCUSTOMER3.end();
    for (; qCUSTOMER3_it502 != qCUSTOMER3_end501; ++qCUSTOMER3_it502)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER3_it502->first);
        string P__MFGR = get<1>(qCUSTOMER3_it502->first);
        qCUSTOMER3[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,REGIONKEY)] += -1*qCUSTOMER3NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER3PARTS1_it504 
        = qCUSTOMER3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER3PARTS1_end503 = qCUSTOMER3PARTS1.end();
    for (
        ; qCUSTOMER3PARTS1_it504 != qCUSTOMER3PARTS1_end503; ++qCUSTOMER3PARTS1_it504)
    {
        int64_t x_qCUSTOMER3PARTS_P__PARTKEY = get<0>(
            qCUSTOMER3PARTS1_it504->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER3PARTS1_it504->first);
        qCUSTOMER3PARTS1[make_tuple(
            x_qCUSTOMER3PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,REGIONKEY)] += 
            -1*qCUSTOMER3PARTS1NATION1[make_tuple(
            x_qCUSTOMER3PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_it506 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_end505 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it506 != qLINEITEM1_end505; ++qLINEITEM1_it506)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it506->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,REGIONKEY)] += 
            -1*qLINEITEM1NATION1[make_tuple(x_qLINEITEM_L__ORDERKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_it508 = 
        qLINEITEM3.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_end507 = 
        qLINEITEM3.end();
    for (; qLINEITEM3_it508 != qLINEITEM3_end507; ++qLINEITEM3_it508)
    {
        int64_t x_qLINEITEM_L__SUPPKEY = get<0>(qLINEITEM3_it508->first);
        qLINEITEM3[make_tuple(
            x_qLINEITEM_L__SUPPKEY,REGIONKEY)] += -1*qLINEITEM3NATION1[make_tuple(
            x_qLINEITEM_L__SUPPKEY,NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it510 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end509 
        = qNATION1.end();
    for (; qNATION1_it510 != qNATION1_end509; ++qNATION1_it510)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it510->first);
        string P__MFGR = get<1>(qNATION1_it510->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY,REGIONKEY)] += 
            -1*qNATION1NATION1[make_tuple(x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it512 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end511 = 
        qNATION2.end();
    for (; qNATION2_it512 != qNATION2_end511; ++qNATION2_it512)
    {
        string P__MFGR = get<0>(qNATION2_it512->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it512->first);
        qNATION2[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,REGIONKEY)] += -1*qNATION1NATION1[make_tuple(
            NATIONKEY,P__MFGR,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION4_it514 = qNATION4.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION4_end513 
        = qNATION4.end();
    for (; qNATION4_it514 != qNATION4_end513; ++qNATION4_it514)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4_it514->first);
        string P__MFGR = get<1>(qNATION4_it514->first);
        qNATION4[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY,REGIONKEY)] += 
            -1*qNATION4NATION1[make_tuple(x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_it516 = 
        qNATION5.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_end515 = 
        qNATION5.end();
    for (; qNATION5_it516 != qNATION5_end515; ++qNATION5_it516)
    {
        string P__MFGR = get<0>(qNATION5_it516->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION5_it516->first);
        qNATION5[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,REGIONKEY)] += -1*qNATION4NATION1[make_tuple(
            NATIONKEY,P__MFGR,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it518 = 
        qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end517 = 
        qORDERS1.end();
    for (; qORDERS1_it518 != qORDERS1_end517; ++qORDERS1_it518)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it518->first);
        string P__MFGR = get<1>(qORDERS1_it518->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,P__MFGR,REGIONKEY)] += -1*qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1PARTS1_it520 = 
        qORDERS1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS1PARTS1_end519 = qORDERS1PARTS1.end();
    for (
        ; qORDERS1PARTS1_it520 != qORDERS1PARTS1_end519; ++qORDERS1PARTS1_it520)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1PARTS1_it520->first);
        int64_t x_qORDERS1PARTS_P__PARTKEY = get<1>(
            qORDERS1PARTS1_it520->first);
        qORDERS1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,REGIONKEY)] += 
            -1*qORDERS1PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_it522 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_end521 = 
        qORDERS2.end();
    for (; qORDERS2_it522 != qORDERS2_end521; ++qORDERS2_it522)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<0>(qORDERS2_it522->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,REGIONKEY)] += 
            -1*qLINEITEM1NATION1ORDERS1[make_tuple(x_qORDERS_O__CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_it524 = 
        qORDERS3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_end523 = 
        qORDERS3.end();
    for (; qORDERS3_it524 != qORDERS3_end523; ++qORDERS3_it524)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3_it524->first);
        string P__MFGR = get<1>(qORDERS3_it524->first);
        qORDERS3[make_tuple(
            x_qORDERS_O__ORDERKEY,P__MFGR,REGIONKEY)] += -1*qORDERS3NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS3PARTS1_it526 = 
        qORDERS3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS3PARTS1_end525 = qORDERS3PARTS1.end();
    for (
        ; qORDERS3PARTS1_it526 != qORDERS3PARTS1_end525; ++qORDERS3PARTS1_it526)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3PARTS1_it526->first);
        int64_t x_qORDERS3PARTS_P__PARTKEY = get<1>(
            qORDERS3PARTS1_it526->first);
        qORDERS3PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS3PARTS_P__PARTKEY,REGIONKEY)] += 
            -1*qORDERS3PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS3PARTS_P__PARTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it528 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end527 
        = qPARTS1.end();
    for (; qPARTS1_it528 != qPARTS1_end527; ++qPARTS1_it528)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it528->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,REGIONKEY,REGIONKEY)] += 
            -1*qPARTS1NATION3[make_tuple(x_qPARTS_P__PARTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it530 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end529 
        = qPARTS1.end();
    for (; qPARTS1_it530 != qPARTS1_end529; ++qPARTS1_it530)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it530->first);
        int64_t C__NATIONKEY = get<1>(qPARTS1_it530->first);
        int64_t REGIONKEY = get<3>(qPARTS1_it530->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,REGIONKEY,REGIONKEY)] += 
            -1*qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,C__NATIONKEY,REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it532 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end531 
        = qPARTS1.end();
    for (; qPARTS1_it532 != qPARTS1_end531; ++qPARTS1_it532)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it532->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it532->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY,REGIONKEY)] += 
            -1*qPARTS1NATION2[make_tuple(x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it534 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end533 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it534 != qPARTS1NATION1_end533; ++qPARTS1NATION1_it534)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1_it534->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1_it534->first);
        qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,NATIONKEY,REGIONKEY)] += 
            -1*qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it536 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end535 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it536 != qPARTS1NATION2_end535; ++qPARTS1NATION2_it536)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION2_it536->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION2_it536->first);
        qPARTS1NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,REGIONKEY)] += 
            -1*qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2_it538 = qPARTS2.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS2_end537 
        = qPARTS2.end();
    for (; qPARTS2_it538 != qPARTS2_end537; ++qPARTS2_it538)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2_it538->first);
        qPARTS2[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,REGIONKEY,REGIONKEY)] += 
            -1*qPARTS2NATION3[make_tuple(x_qPARTS_P__PARTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2_it540 = qPARTS2.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS2_end539 
        = qPARTS2.end();
    for (; qPARTS2_it540 != qPARTS2_end539; ++qPARTS2_it540)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2_it540->first);
        int64_t C__NATIONKEY = get<1>(qPARTS2_it540->first);
        int64_t REGIONKEY = get<3>(qPARTS2_it540->first);
        qPARTS2[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,REGIONKEY,REGIONKEY)] += 
            -1*qPARTS2NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,C__NATIONKEY,REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2_it542 = qPARTS2.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS2_end541 
        = qPARTS2.end();
    for (; qPARTS2_it542 != qPARTS2_end541; ++qPARTS2_it542)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2_it542->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2_it542->first);
        qPARTS2[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY,REGIONKEY)] += 
            -1*qPARTS2NATION2[make_tuple(x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_it544 = qPARTS2NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_end543 = qPARTS2NATION1.end();
    for (
        ; qPARTS2NATION1_it544 != qPARTS2NATION1_end543; ++qPARTS2NATION1_it544)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION1_it544->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION1_it544->first);
        qPARTS2NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,NATIONKEY,REGIONKEY)] += 
            -1*qPARTS2NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS2NATION2_it546 = 
        qPARTS2NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2NATION2_end545 = qPARTS2NATION2.end();
    for (
        ; qPARTS2NATION2_it546 != qPARTS2NATION2_end545; ++qPARTS2NATION2_it546)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION2_it546->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION2_it546->first);
        qPARTS2NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,REGIONKEY)] += 
            -1*qPARTS2NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,x_qPARTS2NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it548 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end547 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it548 != qSUPPLIER1_end547; ++qSUPPLIER1_it548)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it548->first);
        string P__MFGR = get<1>(qSUPPLIER1_it548->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY,REGIONKEY)] += 
            -1*qSUPPLIER1NATION1[make_tuple(x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_it550 = qSUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_end549 = qSUPPLIER1PARTS1.end();
    for (
        ; qSUPPLIER1PARTS1_it550 != qSUPPLIER1PARTS1_end549; ++qSUPPLIER1PARTS1_it550)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1_it550->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER1PARTS1_it550->first);
        qSUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY,REGIONKEY)] += 
            -1*qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY)];
    }
    qSUPPLIER2[make_tuple(NATIONKEY,REGIONKEY)] += -1;
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER3_it552 = qSUPPLIER3.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3_end551 = qSUPPLIER3.end();
    for (; qSUPPLIER3_it552 != qSUPPLIER3_end551; ++qSUPPLIER3_it552)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER3_it552->first);
        string P__MFGR = get<1>(qSUPPLIER3_it552->first);
        qSUPPLIER3[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY,REGIONKEY)] += 
            -1*qSUPPLIER3NATION1[make_tuple(x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1_it554 = qSUPPLIER3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1_end553 = qSUPPLIER3PARTS1.end();
    for (
        ; qSUPPLIER3PARTS1_it554 != qSUPPLIER3PARTS1_end553; ++qSUPPLIER3PARTS1_it554)
    {
        int64_t x_qSUPPLIER3PARTS_P__PARTKEY = get<0>(
            qSUPPLIER3PARTS1_it554->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER3PARTS1_it554->first);
        qSUPPLIER3PARTS1[make_tuple(
            x_qSUPPLIER3PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY,REGIONKEY)] += 
            -1*qSUPPLIER3PARTS1NATION1[make_tuple(
            x_qSUPPLIER3PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY)];
    }

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
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it556 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end555 = 
        q.end();
    for (; q_it556 != q_end555; ++q_it556)
    {
        string P__MFGR = get<0>(q_it556->first);
        int64_t N2__REGIONKEY = get<1>(q_it556->first);
        int64_t N1__REGIONKEY = get<2>(q_it556->first);
        int64_t C__NATIONKEY = get<3>(q_it556->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += -1*qORDERS1[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it558 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end557 = 
        q.end();
    for (; q_it558 != q_end557; ++q_it558)
    {
        string P__MFGR = get<0>(q_it558->first);
        int64_t N2__REGIONKEY = get<1>(q_it558->first);
        int64_t N1__REGIONKEY = get<2>(q_it558->first);
        int64_t C__NATIONKEY = get<3>(q_it558->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += -1*qORDERS3[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)]*-1;
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it560 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end559 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it560 != qCUSTOMER1_end559; ++qCUSTOMER1_it560)
    {
        string P__MFGR = get<1>(qCUSTOMER1_it560->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1_it560->first);
        qCUSTOMER1[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)] += -1*qORDERS1[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1NATION1_it562 
        = qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1NATION1_end561 = qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it562 != qCUSTOMER1NATION1_end561; 
        ++qCUSTOMER1NATION1_it562)
    {
        int64_t x_qCUSTOMER1NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER1NATION1_it562->first);
        string P__MFGR = get<2>(qCUSTOMER1NATION1_it562->first);
        qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,x_qCUSTOMER1NATION_N1__NATIONKEY,P__MFGR)] += 
            -1*qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qCUSTOMER1NATION_N1__NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER1PARTS1_it564 
        = qCUSTOMER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_end563 = qCUSTOMER1PARTS1.end();
    for (
        ; qCUSTOMER1PARTS1_it564 != qCUSTOMER1PARTS1_end563; ++qCUSTOMER1PARTS1_it564)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1_it564->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1PARTS1_it564->first);
        qCUSTOMER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)] += 
            -1*qORDERS1PARTS1[make_tuple(
            ORDERKEY,x_qCUSTOMER1PARTS_P__PARTKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_it566 = qCUSTOMER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_end565 = qCUSTOMER1PARTS1NATION1.end();
    for (
        ; qCUSTOMER1PARTS1NATION1_it566 != qCUSTOMER1PARTS1NATION1_end565; 
        ++qCUSTOMER1PARTS1NATION1_it566)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1NATION1_it566->first);
        int64_t x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY = get<2>(
            qCUSTOMER1PARTS1NATION1_it566->first);
        qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,CUSTKEY,
            x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY)] += -1*qORDERS1PARTS1NATION1[make_tuple(
            ORDERKEY,x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_it568 = qCUSTOMER1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_end567 = qCUSTOMER1SUPPLIER1.end();
    for (
        ; qCUSTOMER1SUPPLIER1_it568 != qCUSTOMER1SUPPLIER1_end567; 
        ++qCUSTOMER1SUPPLIER1_it568)
    {
        int64_t x_qCUSTOMER1SUPPLIER_S__SUPPKEY = get<0>(
            qCUSTOMER1SUPPLIER1_it568->first);
        string P__MFGR = get<2>(qCUSTOMER1SUPPLIER1_it568->first);
        qCUSTOMER1SUPPLIER1[make_tuple(
            x_qCUSTOMER1SUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)] += 
            -1*qORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qCUSTOMER1SUPPLIER_S__SUPPKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1SUPPLIER1PARTS1_it570 = qCUSTOMER1SUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1SUPPLIER1PARTS1_end569 = qCUSTOMER1SUPPLIER1PARTS1.end();
    for (
        ; qCUSTOMER1SUPPLIER1PARTS1_it570 != qCUSTOMER1SUPPLIER1PARTS1_end569; 
        ++qCUSTOMER1SUPPLIER1PARTS1_it570)
    {
        int64_t x_qCUSTOMER1SUPPLIER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1SUPPLIER1PARTS1_it570->first);
        int64_t x_qCUSTOMER1SUPPLIER_S__SUPPKEY = get<1>(
            qCUSTOMER1SUPPLIER1PARTS1_it570->first);
        qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER1SUPPLIER1PARTS_P__PARTKEY,x_qCUSTOMER1SUPPLIER_S__SUPPKEY,CUSTKEY)] 
            += -1*qORDERS1SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qCUSTOMER1SUPPLIER1PARTS_P__PARTKEY,
            x_qCUSTOMER1SUPPLIER_S__SUPPKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_it572 = 
        qCUSTOMER3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_end571 = 
        qCUSTOMER3.end();
    for (; qCUSTOMER3_it572 != qCUSTOMER3_end571; ++qCUSTOMER3_it572)
    {
        string P__MFGR = get<1>(qCUSTOMER3_it572->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER3_it572->first);
        qCUSTOMER3[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)] += -1*qORDERS3[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER3NATION1_it574 
        = qCUSTOMER3NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER3NATION1_end573 = qCUSTOMER3NATION1.end();
    for (
        ; qCUSTOMER3NATION1_it574 != qCUSTOMER3NATION1_end573; 
        ++qCUSTOMER3NATION1_it574)
    {
        int64_t x_qCUSTOMER3NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER3NATION1_it574->first);
        string P__MFGR = get<2>(qCUSTOMER3NATION1_it574->first);
        qCUSTOMER3NATION1[make_tuple(
            CUSTKEY,x_qCUSTOMER3NATION_N1__NATIONKEY,P__MFGR)] += 
            -1*qORDERS3NATION1[make_tuple(
            ORDERKEY,x_qCUSTOMER3NATION_N1__NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER3PARTS1_it576 
        = qCUSTOMER3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER3PARTS1_end575 = qCUSTOMER3PARTS1.end();
    for (
        ; qCUSTOMER3PARTS1_it576 != qCUSTOMER3PARTS1_end575; ++qCUSTOMER3PARTS1_it576)
    {
        int64_t x_qCUSTOMER3PARTS_P__PARTKEY = get<0>(
            qCUSTOMER3PARTS1_it576->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER3PARTS1_it576->first);
        qCUSTOMER3PARTS1[make_tuple(
            x_qCUSTOMER3PARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)] += 
            -1*qORDERS3PARTS1[make_tuple(
            ORDERKEY,x_qCUSTOMER3PARTS_P__PARTKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3PARTS1NATION1_it578 = qCUSTOMER3PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3PARTS1NATION1_end577 = qCUSTOMER3PARTS1NATION1.end();
    for (
        ; qCUSTOMER3PARTS1NATION1_it578 != qCUSTOMER3PARTS1NATION1_end577; 
        ++qCUSTOMER3PARTS1NATION1_it578)
    {
        int64_t x_qCUSTOMER3PARTS_P__PARTKEY = get<0>(
            qCUSTOMER3PARTS1NATION1_it578->first);
        int64_t x_qCUSTOMER3PARTS1NATION_N1__NATIONKEY = get<2>(
            qCUSTOMER3PARTS1NATION1_it578->first);
        qCUSTOMER3PARTS1NATION1[make_tuple(
            x_qCUSTOMER3PARTS_P__PARTKEY,CUSTKEY,
            x_qCUSTOMER3PARTS1NATION_N1__NATIONKEY)] += -1*qORDERS3PARTS1NATION1[make_tuple(
            ORDERKEY,x_qCUSTOMER3PARTS_P__PARTKEY,x_qCUSTOMER3PARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER3SUPPLIER1_it580 = qCUSTOMER3SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER3SUPPLIER1_end579 = qCUSTOMER3SUPPLIER1.end();
    for (
        ; qCUSTOMER3SUPPLIER1_it580 != qCUSTOMER3SUPPLIER1_end579; 
        ++qCUSTOMER3SUPPLIER1_it580)
    {
        int64_t x_qCUSTOMER3SUPPLIER_S__SUPPKEY = get<0>(
            qCUSTOMER3SUPPLIER1_it580->first);
        string P__MFGR = get<2>(qCUSTOMER3SUPPLIER1_it580->first);
        qCUSTOMER3SUPPLIER1[make_tuple(
            x_qCUSTOMER3SUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)] += 
            -1*qORDERS3SUPPLIER1[make_tuple(
            ORDERKEY,x_qCUSTOMER3SUPPLIER_S__SUPPKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3SUPPLIER1PARTS1_it582 = qCUSTOMER3SUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3SUPPLIER1PARTS1_end581 = qCUSTOMER3SUPPLIER1PARTS1.end();
    for (
        ; qCUSTOMER3SUPPLIER1PARTS1_it582 != qCUSTOMER3SUPPLIER1PARTS1_end581; 
        ++qCUSTOMER3SUPPLIER1PARTS1_it582)
    {
        int64_t x_qCUSTOMER3SUPPLIER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER3SUPPLIER1PARTS1_it582->first);
        int64_t x_qCUSTOMER3SUPPLIER_S__SUPPKEY = get<1>(
            qCUSTOMER3SUPPLIER1PARTS1_it582->first);
        qCUSTOMER3SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER3SUPPLIER1PARTS_P__PARTKEY,x_qCUSTOMER3SUPPLIER_S__SUPPKEY,CUSTKEY)] 
            += -1*qORDERS3SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qCUSTOMER3SUPPLIER1PARTS_P__PARTKEY,
            x_qCUSTOMER3SUPPLIER_S__SUPPKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_it584 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_end583 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it584 != qLINEITEM1_end583; ++qLINEITEM1_it584)
    {
        int64_t C__NATIONKEY = get<1>(qLINEITEM1_it584->first);
        int64_t N1__REGIONKEY = get<2>(qLINEITEM1_it584->first);
        qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)] += -1*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }
    qLINEITEM1CUSTOMER1[make_tuple(ORDERKEY,CUSTKEY)] += -1;
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_it586 = 
        qLINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_end585 = 
        qLINEITEM1NATION1.end();
    for (
        ; qLINEITEM1NATION1_it586 != qLINEITEM1NATION1_end585; 
        ++qLINEITEM1NATION1_it586)
    {
        int64_t x_qLINEITEM1NATION_N1__NATIONKEY = get<1>(
            qLINEITEM1NATION1_it586->first);
        qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qLINEITEM1NATION_N1__NATIONKEY)] += 
            -1*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qLINEITEM1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it588 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end587 
        = qNATION1.end();
    for (; qNATION1_it588 != qNATION1_end587; ++qNATION1_it588)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it588->first);
        string P__MFGR = get<1>(qNATION1_it588->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it588->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it588->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it590 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end589 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it590 != qNATION1NATION1_end589; ++qNATION1NATION1_it590)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1NATION1_it590->first);
        string P__MFGR = get<1>(qNATION1NATION1_it590->first);
        int64_t x_qNATION1NATION_N1__NATIONKEY = get<2>(
            qNATION1NATION1_it590->first);
        qNATION1NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION1NATION_N1__NATIONKEY)] += 
            -1*qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qNATION1NATION_N1__NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it592 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end591 = 
        qNATION2.end();
    for (; qNATION2_it592 != qNATION2_end591; ++qNATION2_it592)
    {
        string P__MFGR = get<0>(qNATION2_it592->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it592->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it592->first);
        qNATION2[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += -1*qORDERS1[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION3_it594 = qNATION3.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION3_end593 = qNATION3.end();
    for (; qNATION3_it594 != qNATION3_end593; ++qNATION3_it594)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION3_it594->first);
        string P__MFGR = get<1>(qNATION3_it594->first);
        qNATION3[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR)] += -1*qNATION3ORDERS1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR,CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION4_it596 = qNATION4.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION4_end595 
        = qNATION4.end();
    for (; qNATION4_it596 != qNATION4_end595; ++qNATION4_it596)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4_it596->first);
        string P__MFGR = get<1>(qNATION4_it596->first);
        int64_t C__NATIONKEY = get<2>(qNATION4_it596->first);
        int64_t N1__REGIONKEY = get<3>(qNATION4_it596->first);
        qNATION4[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*qORDERS3NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION4NATION1_it598 = 
        qNATION4NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION4NATION1_end597 = qNATION4NATION1.end();
    for (
        ; qNATION4NATION1_it598 != qNATION4NATION1_end597; ++qNATION4NATION1_it598)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4NATION1_it598->first);
        string P__MFGR = get<1>(qNATION4NATION1_it598->first);
        int64_t x_qNATION4NATION_N1__NATIONKEY = get<2>(
            qNATION4NATION1_it598->first);
        qNATION4NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION4NATION_N1__NATIONKEY)] += 
            -1*qORDERS3NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qNATION4NATION_N1__NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_it600 = 
        qNATION5.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_end599 = 
        qNATION5.end();
    for (; qNATION5_it600 != qNATION5_end599; ++qNATION5_it600)
    {
        string P__MFGR = get<0>(qNATION5_it600->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION5_it600->first);
        int64_t N2__REGIONKEY = get<2>(qNATION5_it600->first);
        qNATION5[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += -1*qORDERS3[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION6_it602 = qNATION6.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION6_end601 = qNATION6.end();
    for (; qNATION6_it602 != qNATION6_end601; ++qNATION6_it602)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION6_it602->first);
        string P__MFGR = get<1>(qNATION6_it602->first);
        qNATION6[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR)] += -1*qNATION6ORDERS1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR,CUSTKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it604 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end603 
        = qPARTS1.end();
    for (; qPARTS1_it604 != qPARTS1_end603; ++qPARTS1_it604)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it604->first);
        int64_t C__NATIONKEY = get<1>(qPARTS1_it604->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it604->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1_it604->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            -1*qORDERS1PARTS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,N2__REGIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it606 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end605 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it606 != qPARTS1NATION1_end605; ++qPARTS1NATION1_it606)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1_it606->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1_it606->first);
        int64_t C__NATIONKEY = get<2>(qPARTS1NATION1_it606->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1NATION1_it606->first);
        qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] 
            += -1*qORDERS1PARTS1NATION1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            x_qPARTS1NATION_N1__NATIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_it608 = qPARTS1NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_end607 = qPARTS1NATION1NATION1.end();
    for (
        ; qPARTS1NATION1NATION1_it608 != qPARTS1NATION1NATION1_end607; 
        ++qPARTS1NATION1NATION1_it608)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS1NATION1NATION1_it608->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1NATION1_it608->first);
        int64_t x_qPARTS1NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS1NATION1NATION1_it608->first);
        qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,
            x_qPARTS1NATION1NATION_N1__NATIONKEY)] += -1*qORDERS1PARTS1NATION1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            x_qPARTS1NATION_N1__NATIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it610 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end609 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it610 != qPARTS1NATION2_end609; ++qPARTS1NATION2_it610)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION2_it610->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION2_it610->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1NATION2_it610->first);
        qPARTS1NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            -1*qORDERS1PARTS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            N2__REGIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_it612 = 
        qPARTS1NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_end611 = 
        qPARTS1NATION3.end();
    for (
        ; qPARTS1NATION3_it612 != qPARTS1NATION3_end611; ++qPARTS1NATION3_it612)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION3_it612->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION3_it612->first);
        qPARTS1NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY)] += 
            -1*qPARTS1NATION3ORDERS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,CUSTKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_it614 = qPARTS1NATION3LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_end613 = qPARTS1NATION3LINEITEM1.end();
    for (
        ; qPARTS1NATION3LINEITEM1_it614 != qPARTS1NATION3LINEITEM1_end613; 
        ++qPARTS1NATION3LINEITEM1_it614)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION3LINEITEM1_it614->first);
        int64_t x_qPARTS1NATION3LINEITEM_L__SUPPKEY = get<2>(
            qPARTS1NATION3LINEITEM1_it614->first);
        qPARTS1NATION3LINEITEM1[make_tuple(
            ORDERKEY,x_qPARTS1NATION_N1__NATIONKEY,x_qPARTS1NATION3LINEITEM_L__SUPPKEY)] += 
            -1*qPARTS1NATION3LINEITEM1ORDERS1[make_tuple(
            CUSTKEY,x_qPARTS1NATION_N1__NATIONKEY,x_qPARTS1NATION3LINEITEM_L__SUPPKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2_it616 = qPARTS2.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS2_end615 
        = qPARTS2.end();
    for (; qPARTS2_it616 != qPARTS2_end615; ++qPARTS2_it616)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2_it616->first);
        int64_t C__NATIONKEY = get<1>(qPARTS2_it616->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2_it616->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS2_it616->first);
        qPARTS2[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            -1*qORDERS3PARTS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,N2__REGIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_it618 = qPARTS2NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_end617 = qPARTS2NATION1.end();
    for (
        ; qPARTS2NATION1_it618 != qPARTS2NATION1_end617; ++qPARTS2NATION1_it618)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION1_it618->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION1_it618->first);
        int64_t C__NATIONKEY = get<2>(qPARTS2NATION1_it618->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS2NATION1_it618->first);
        qPARTS2NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] 
            += -1*qORDERS3PARTS1NATION1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            x_qPARTS2NATION_N1__NATIONKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1NATION1_it620 = qPARTS2NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1NATION1_end619 = qPARTS2NATION1NATION1.end();
    for (
        ; qPARTS2NATION1NATION1_it620 != qPARTS2NATION1NATION1_end619; 
        ++qPARTS2NATION1NATION1_it620)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS2NATION1NATION1_it620->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION1NATION1_it620->first);
        int64_t x_qPARTS2NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS2NATION1NATION1_it620->first);
        qPARTS2NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,
            x_qPARTS2NATION1NATION_N1__NATIONKEY)] += -1*qORDERS3PARTS1NATION1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            x_qPARTS2NATION_N1__NATIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qPARTS2NATION1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS2NATION2_it622 = 
        qPARTS2NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2NATION2_end621 = qPARTS2NATION2.end();
    for (
        ; qPARTS2NATION2_it622 != qPARTS2NATION2_end621; ++qPARTS2NATION2_it622)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION2_it622->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION2_it622->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2NATION2_it622->first);
        qPARTS2NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            -1*qORDERS3PARTS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,
            N2__REGIONKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qPARTS2NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qPARTS2NATION3_it624 = 
        qPARTS2NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS2NATION3_end623 = 
        qPARTS2NATION3.end();
    for (
        ; qPARTS2NATION3_it624 != qPARTS2NATION3_end623; ++qPARTS2NATION3_it624)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION3_it624->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION3_it624->first);
        qPARTS2NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY)] += 
            -1*qPARTS2NATION3ORDERS1[make_tuple(
            ORDERKEY,x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it626 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end625 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it626 != qSUPPLIER1_end625; ++qSUPPLIER1_it626)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it626->first);
        string P__MFGR = get<1>(qSUPPLIER1_it626->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1_it626->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1_it626->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*qORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY,P__MFGR)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER1NATION1_it628 
        = qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER1NATION1_end627 = qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it628 != qSUPPLIER1NATION1_end627; 
        ++qSUPPLIER1NATION1_it628)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1NATION1_it628->first);
        string P__MFGR = get<1>(qSUPPLIER1NATION1_it628->first);
        int64_t x_qSUPPLIER1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1NATION1_it628->first);
        qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,x_qSUPPLIER1NATION_N1__NATIONKEY)] += 
            -1*qORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY,P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qSUPPLIER1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_it630 = qSUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_end629 = qSUPPLIER1PARTS1.end();
    for (
        ; qSUPPLIER1PARTS1_it630 != qSUPPLIER1PARTS1_end629; ++qSUPPLIER1PARTS1_it630)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1_it630->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER1PARTS1_it630->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1PARTS1_it630->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1PARTS1_it630->first);
        qSUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,C__NATIONKEY,
            N1__REGIONKEY)] += -1*qORDERS1SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qSUPPLIER1PARTS_P__PARTKEY,
            x_qSUPPLIER_S__SUPPKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_it632 = qSUPPLIER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_end631 = qSUPPLIER1PARTS1NATION1.end();
    for (
        ; qSUPPLIER1PARTS1NATION1_it632 != qSUPPLIER1PARTS1NATION1_end631; 
        ++qSUPPLIER1PARTS1NATION1_it632)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1NATION1_it632->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(
            qSUPPLIER1PARTS1NATION1_it632->first);
        int64_t x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1PARTS1NATION1_it632->first);
        qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,
            x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY)] += 
            -1*qORDERS1SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qSUPPLIER1PARTS_P__PARTKEY,
            x_qSUPPLIER_S__SUPPKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER3_it634 = qSUPPLIER3.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3_end633 = qSUPPLIER3.end();
    for (; qSUPPLIER3_it634 != qSUPPLIER3_end633; ++qSUPPLIER3_it634)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER3_it634->first);
        string P__MFGR = get<1>(qSUPPLIER3_it634->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER3_it634->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER3_it634->first);
        qSUPPLIER3[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*qORDERS3SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY,P__MFGR)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER3NATION1_it636 
        = qSUPPLIER3NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER3NATION1_end635 = qSUPPLIER3NATION1.end();
    for (
        ; qSUPPLIER3NATION1_it636 != qSUPPLIER3NATION1_end635; 
        ++qSUPPLIER3NATION1_it636)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER3NATION1_it636->first);
        string P__MFGR = get<1>(qSUPPLIER3NATION1_it636->first);
        int64_t x_qSUPPLIER3NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER3NATION1_it636->first);
        qSUPPLIER3NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,x_qSUPPLIER3NATION_N1__NATIONKEY)] += 
            -1*qORDERS3SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY,P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qSUPPLIER3NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1_it638 = qSUPPLIER3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1_end637 = qSUPPLIER3PARTS1.end();
    for (
        ; qSUPPLIER3PARTS1_it638 != qSUPPLIER3PARTS1_end637; ++qSUPPLIER3PARTS1_it638)
    {
        int64_t x_qSUPPLIER3PARTS_P__PARTKEY = get<0>(
            qSUPPLIER3PARTS1_it638->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER3PARTS1_it638->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER3PARTS1_it638->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER3PARTS1_it638->first);
        qSUPPLIER3PARTS1[make_tuple(
            x_qSUPPLIER3PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,C__NATIONKEY,
            N1__REGIONKEY)] += -1*qORDERS3SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qSUPPLIER3PARTS_P__PARTKEY,
            x_qSUPPLIER_S__SUPPKEY)]*qORDERS2[make_tuple(
            CUSTKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1NATION1_it640 = qSUPPLIER3PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1NATION1_end639 = qSUPPLIER3PARTS1NATION1.end();
    for (
        ; qSUPPLIER3PARTS1NATION1_it640 != qSUPPLIER3PARTS1NATION1_end639; 
        ++qSUPPLIER3PARTS1NATION1_it640)
    {
        int64_t x_qSUPPLIER3PARTS_P__PARTKEY = get<0>(
            qSUPPLIER3PARTS1NATION1_it640->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(
            qSUPPLIER3PARTS1NATION1_it640->first);
        int64_t x_qSUPPLIER3PARTS1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER3PARTS1NATION1_it640->first);
        qSUPPLIER3PARTS1NATION1[make_tuple(
            x_qSUPPLIER3PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,
            x_qSUPPLIER3PARTS1NATION_N1__NATIONKEY)] += 
            -1*qORDERS3SUPPLIER1PARTS1[make_tuple(
            ORDERKEY,x_qSUPPLIER3PARTS_P__PARTKEY,
            x_qSUPPLIER_S__SUPPKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            CUSTKEY,x_qSUPPLIER3PARTS1NATION_N1__NATIONKEY)];
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
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it642 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end641 = 
        q.end();
    for (; q_it642 != q_end641; ++q_it642)
    {
        string P__MFGR = get<0>(q_it642->first);
        int64_t N2__REGIONKEY = get<1>(q_it642->first);
        int64_t N1__REGIONKEY = get<2>(q_it642->first);
        int64_t C__NATIONKEY = get<3>(q_it642->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)]*-1;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it644 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end643 = 
        q.end();
    for (; q_it644 != q_end643; ++q_it644)
    {
        string P__MFGR = get<0>(q_it644->first);
        int64_t N2__REGIONKEY = get<1>(q_it644->first);
        int64_t N1__REGIONKEY = get<2>(q_it644->first);
        int64_t C__NATIONKEY = get<3>(q_it644->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)]*100;
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it646 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end645 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it646 != qCUSTOMER1_end645; ++qCUSTOMER1_it646)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it646->first);
        string P__MFGR = get<1>(qCUSTOMER1_it646->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1_it646->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,N2__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1NATION1_it648 
        = qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1NATION1_end647 = qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it648 != qCUSTOMER1NATION1_end647; 
        ++qCUSTOMER1NATION1_it648)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1NATION1_it648->first);
        int64_t x_qCUSTOMER1NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER1NATION1_it648->first);
        string P__MFGR = get<2>(qCUSTOMER1NATION1_it648->first);
        qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1NATION_N1__NATIONKEY,P__MFGR)] += 
            -1*EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qCUSTOMER1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER1PARTS1_it650 
        = qCUSTOMER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_end649 = qCUSTOMER1PARTS1.end();
    for (
        ; qCUSTOMER1PARTS1_it650 != qCUSTOMER1PARTS1_end649; ++qCUSTOMER1PARTS1_it650)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1PARTS1_it650->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1PARTS1_it650->first);
        qCUSTOMER1PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_it652 = qCUSTOMER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_end651 = qCUSTOMER1PARTS1NATION1.end();
    for (
        ; qCUSTOMER1PARTS1NATION1_it652 != qCUSTOMER1PARTS1NATION1_end651; 
        ++qCUSTOMER1PARTS1NATION1_it652)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1PARTS1NATION1_it652->first);
        int64_t x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY = get<2>(
            qCUSTOMER1PARTS1NATION1_it652->first);
        qCUSTOMER1PARTS1NATION1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qCUSTOMER1PARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_it654 = qCUSTOMER1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_end653 = qCUSTOMER1SUPPLIER1.end();
    for (
        ; qCUSTOMER1SUPPLIER1_it654 != qCUSTOMER1SUPPLIER1_end653; 
        ++qCUSTOMER1SUPPLIER1_it654)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1SUPPLIER1_it654->first);
        string P__MFGR = get<2>(qCUSTOMER1SUPPLIER1_it654->first);
        qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)] += 
            -1*EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(PARTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1SUPPLIER1PARTS1_it656 = qCUSTOMER1SUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1SUPPLIER1PARTS1_end655 = qCUSTOMER1SUPPLIER1PARTS1.end();
    for (
        ; qCUSTOMER1SUPPLIER1PARTS1_it656 != qCUSTOMER1SUPPLIER1PARTS1_end655; 
        ++qCUSTOMER1SUPPLIER1PARTS1_it656)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<2>(
            qCUSTOMER1SUPPLIER1PARTS1_it656->first);
        qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            PARTKEY,SUPPKEY,x_qCUSTOMER_C__CUSTKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_it658 = 
        qCUSTOMER3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_end657 = 
        qCUSTOMER3.end();
    for (; qCUSTOMER3_it658 != qCUSTOMER3_end657; ++qCUSTOMER3_it658)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER3_it658->first);
        string P__MFGR = get<1>(qCUSTOMER3_it658->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER3_it658->first);
        qCUSTOMER3[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,N2__REGIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER3NATION1_it660 
        = qCUSTOMER3NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER3NATION1_end659 = qCUSTOMER3NATION1.end();
    for (
        ; qCUSTOMER3NATION1_it660 != qCUSTOMER3NATION1_end659; 
        ++qCUSTOMER3NATION1_it660)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER3NATION1_it660->first);
        int64_t x_qCUSTOMER3NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER3NATION1_it660->first);
        string P__MFGR = get<2>(qCUSTOMER3NATION1_it660->first);
        qCUSTOMER3NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER3NATION_N1__NATIONKEY,P__MFGR)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qCUSTOMER3NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER3PARTS1_it662 
        = qCUSTOMER3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER3PARTS1_end661 = qCUSTOMER3PARTS1.end();
    for (
        ; qCUSTOMER3PARTS1_it662 != qCUSTOMER3PARTS1_end661; ++qCUSTOMER3PARTS1_it662)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER3PARTS1_it662->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER3PARTS1_it662->first);
        qCUSTOMER3PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3PARTS1NATION1_it664 = qCUSTOMER3PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3PARTS1NATION1_end663 = qCUSTOMER3PARTS1NATION1.end();
    for (
        ; qCUSTOMER3PARTS1NATION1_it664 != qCUSTOMER3PARTS1NATION1_end663; 
        ++qCUSTOMER3PARTS1NATION1_it664)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER3PARTS1NATION1_it664->first);
        int64_t x_qCUSTOMER3PARTS1NATION_N1__NATIONKEY = get<2>(
            qCUSTOMER3PARTS1NATION1_it664->first);
        qCUSTOMER3PARTS1NATION1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER3PARTS1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qCUSTOMER3PARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER3SUPPLIER1_it666 = qCUSTOMER3SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER3SUPPLIER1_end665 = qCUSTOMER3SUPPLIER1.end();
    for (
        ; qCUSTOMER3SUPPLIER1_it666 != qCUSTOMER3SUPPLIER1_end665; 
        ++qCUSTOMER3SUPPLIER1_it666)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER3SUPPLIER1_it666->first);
        string P__MFGR = get<2>(qCUSTOMER3SUPPLIER1_it666->first);
        qCUSTOMER3SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qLINEITEM2[make_tuple(PARTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3SUPPLIER1PARTS1_it668 = qCUSTOMER3SUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3SUPPLIER1PARTS1_end667 = qCUSTOMER3SUPPLIER1PARTS1.end();
    for (
        ; qCUSTOMER3SUPPLIER1PARTS1_it668 != qCUSTOMER3SUPPLIER1PARTS1_end667; 
        ++qCUSTOMER3SUPPLIER1PARTS1_it668)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<2>(
            qCUSTOMER3SUPPLIER1PARTS1_it668->first);
        qCUSTOMER3SUPPLIER1PARTS1[make_tuple(
            PARTKEY,SUPPKEY,x_qCUSTOMER_C__CUSTKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1CUSTOMER1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it670 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end669 
        = qNATION1.end();
    for (; qNATION1_it670 != qNATION1_end669; ++qNATION1_it670)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it670->first);
        string P__MFGR = get<1>(qNATION1_it670->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it670->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it670->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it672 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end671 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it672 != qNATION1NATION1_end671; ++qNATION1NATION1_it672)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1NATION1_it672->first);
        string P__MFGR = get<1>(qNATION1NATION1_it672->first);
        int64_t x_qNATION1NATION_N1__NATIONKEY = get<2>(
            qNATION1NATION1_it672->first);
        qNATION1NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qNATION1NATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it674 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end673 = 
        qNATION2.end();
    for (; qNATION2_it674 != qNATION2_end673; ++qNATION2_it674)
    {
        string P__MFGR = get<0>(qNATION2_it674->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it674->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it674->first);
        qNATION2[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION3_it676 = qNATION3.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION3_end675 = qNATION3.end();
    for (; qNATION3_it676 != qNATION3_end675; ++qNATION3_it676)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION3_it676->first);
        string P__MFGR = get<1>(qNATION3_it676->first);
        qNATION3[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR)] += 
            -1*EXTENDEDPRICE*qPARTS1NATION3LINEITEM1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,SUPPKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_it678 = qNATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_end677 = qNATION3ORDERS1.end();
    for (
        ; qNATION3ORDERS1_it678 != qNATION3ORDERS1_end677; ++qNATION3ORDERS1_it678)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION3ORDERS1_it678->first);
        string P__MFGR = get<2>(qNATION3ORDERS1_it678->first);
        int64_t x_qNATION3ORDERS_O__CUSTKEY = get<3>(
            qNATION3ORDERS1_it678->first);
        qNATION3ORDERS1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION3ORDERS_O__CUSTKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY,x_qNATION3ORDERS_O__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION4_it680 = qNATION4.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION4_end679 
        = qNATION4.end();
    for (; qNATION4_it680 != qNATION4_end679; ++qNATION4_it680)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4_it680->first);
        string P__MFGR = get<1>(qNATION4_it680->first);
        int64_t C__NATIONKEY = get<2>(qNATION4_it680->first);
        int64_t N1__REGIONKEY = get<3>(qNATION4_it680->first);
        qNATION4[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION4NATION1_it682 = 
        qNATION4NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION4NATION1_end681 = qNATION4NATION1.end();
    for (
        ; qNATION4NATION1_it682 != qNATION4NATION1_end681; ++qNATION4NATION1_it682)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4NATION1_it682->first);
        string P__MFGR = get<1>(qNATION4NATION1_it682->first);
        int64_t x_qNATION4NATION_N1__NATIONKEY = get<2>(
            qNATION4NATION1_it682->first);
        qNATION4NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION4NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qNATION4NATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_it684 = 
        qNATION5.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_end683 = 
        qNATION5.end();
    for (; qNATION5_it684 != qNATION5_end683; ++qNATION5_it684)
    {
        string P__MFGR = get<0>(qNATION5_it684->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION5_it684->first);
        int64_t N2__REGIONKEY = get<2>(qNATION5_it684->first);
        qNATION5[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION6_it686 = qNATION6.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION6_end685 = qNATION6.end();
    for (; qNATION6_it686 != qNATION6_end685; ++qNATION6_it686)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION6_it686->first);
        string P__MFGR = get<1>(qNATION6_it686->first);
        qNATION6[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qPARTS1NATION3LINEITEM1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,SUPPKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION6ORDERS1_it688 = qNATION6ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION6ORDERS1_end687 = qNATION6ORDERS1.end();
    for (
        ; qNATION6ORDERS1_it688 != qNATION6ORDERS1_end687; ++qNATION6ORDERS1_it688)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION6ORDERS1_it688->first);
        string P__MFGR = get<2>(qNATION6ORDERS1_it688->first);
        int64_t x_qNATION6ORDERS_O__CUSTKEY = get<3>(
            qNATION6ORDERS1_it688->first);
        qNATION6ORDERS1[make_tuple(
            ORDERKEY,x_qNATION_N1__NATIONKEY,P__MFGR,x_qNATION6ORDERS_O__CUSTKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qNATION_N1__NATIONKEY,x_qNATION6ORDERS_O__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it690 = 
        qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end689 = 
        qORDERS1.end();
    for (; qORDERS1_it690 != qORDERS1_end689; ++qORDERS1_it690)
    {
        string P__MFGR = get<1>(qORDERS1_it690->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1_it690->first);
        qORDERS1[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)] += -1*EXTENDEDPRICE*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1NATION1_it692 = 
        qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1NATION1_end691 = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it692 != qORDERS1NATION1_end691; ++qORDERS1NATION1_it692)
    {
        int64_t x_qORDERS1NATION_N1__NATIONKEY = get<1>(
            qORDERS1NATION1_it692->first);
        string P__MFGR = get<2>(qORDERS1NATION1_it692->first);
        qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qORDERS1NATION_N1__NATIONKEY,P__MFGR)] += 
            -1*EXTENDEDPRICE*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qORDERS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1PARTS1_it694 = 
        qORDERS1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS1PARTS1_end693 = qORDERS1PARTS1.end();
    for (
        ; qORDERS1PARTS1_it694 != qORDERS1PARTS1_end693; ++qORDERS1PARTS1_it694)
    {
        int64_t N2__REGIONKEY = get<2>(qORDERS1PARTS1_it694->first);
        qORDERS1PARTS1[make_tuple(
            ORDERKEY,PARTKEY,N2__REGIONKEY)] += -1*EXTENDEDPRICE*qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS1PARTS1NATION1_it696 = qORDERS1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS1PARTS1NATION1_end695 = qORDERS1PARTS1NATION1.end();
    for (
        ; qORDERS1PARTS1NATION1_it696 != qORDERS1PARTS1NATION1_end695; 
        ++qORDERS1PARTS1NATION1_it696)
    {
        int64_t x_qORDERS1PARTS1NATION_N1__NATIONKEY = get<2>(
            qORDERS1PARTS1NATION1_it696->first);
        qORDERS1PARTS1NATION1[make_tuple(
            ORDERKEY,PARTKEY,x_qORDERS1PARTS1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qORDERS1PARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1SUPPLIER1_it698 
        = qORDERS1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1SUPPLIER1_end697 = qORDERS1SUPPLIER1.end();
    for (
        ; qORDERS1SUPPLIER1_it698 != qORDERS1SUPPLIER1_end697; 
        ++qORDERS1SUPPLIER1_it698)
    {
        string P__MFGR = get<2>(qORDERS1SUPPLIER1_it698->first);
        qORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,SUPPKEY,P__MFGR)] += -1*EXTENDEDPRICE*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }
    qORDERS1SUPPLIER1PARTS1[make_tuple(
        ORDERKEY,PARTKEY,SUPPKEY)] += -1*EXTENDEDPRICE;
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_it700 = 
        qORDERS3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_end699 = 
        qORDERS3.end();
    for (; qORDERS3_it700 != qORDERS3_end699; ++qORDERS3_it700)
    {
        string P__MFGR = get<1>(qORDERS3_it700->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS3_it700->first);
        qORDERS3[make_tuple(
            ORDERKEY,P__MFGR,N2__REGIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS3NATION1_it702 = 
        qORDERS3NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS3NATION1_end701 = qORDERS3NATION1.end();
    for (
        ; qORDERS3NATION1_it702 != qORDERS3NATION1_end701; ++qORDERS3NATION1_it702)
    {
        int64_t x_qORDERS3NATION_N1__NATIONKEY = get<1>(
            qORDERS3NATION1_it702->first);
        string P__MFGR = get<2>(qORDERS3NATION1_it702->first);
        qORDERS3NATION1[make_tuple(
            ORDERKEY,x_qORDERS3NATION_N1__NATIONKEY,P__MFGR)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qORDERS3NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS3PARTS1_it704 = 
        qORDERS3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS3PARTS1_end703 = qORDERS3PARTS1.end();
    for (
        ; qORDERS3PARTS1_it704 != qORDERS3PARTS1_end703; ++qORDERS3PARTS1_it704)
    {
        int64_t N2__REGIONKEY = get<2>(qORDERS3PARTS1_it704->first);
        qORDERS3PARTS1[make_tuple(
            ORDERKEY,PARTKEY,N2__REGIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM3[make_tuple(SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS3PARTS1NATION1_it706 = qORDERS3PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS3PARTS1NATION1_end705 = qORDERS3PARTS1NATION1.end();
    for (
        ; qORDERS3PARTS1NATION1_it706 != qORDERS3PARTS1NATION1_end705; 
        ++qORDERS3PARTS1NATION1_it706)
    {
        int64_t x_qORDERS3PARTS1NATION_N1__NATIONKEY = get<2>(
            qORDERS3PARTS1NATION1_it706->first);
        qORDERS3PARTS1NATION1[make_tuple(
            ORDERKEY,PARTKEY,x_qORDERS3PARTS1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qORDERS3PARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS3SUPPLIER1_it708 
        = qORDERS3SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS3SUPPLIER1_end707 = qORDERS3SUPPLIER1.end();
    for (
        ; qORDERS3SUPPLIER1_it708 != qORDERS3SUPPLIER1_end707; 
        ++qORDERS3SUPPLIER1_it708)
    {
        string P__MFGR = get<2>(qORDERS3SUPPLIER1_it708->first);
        qORDERS3SUPPLIER1[make_tuple(
            ORDERKEY,SUPPKEY,P__MFGR)] += -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }
    qORDERS3SUPPLIER1PARTS1[make_tuple(
        ORDERKEY,PARTKEY,SUPPKEY)] += -1*EXTENDEDPRICE*DISCOUNT;
    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it710 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end709 
        = qPARTS1.end();
    for (; qPARTS1_it710 != qPARTS1_end709; ++qPARTS1_it710)
    {
        int64_t C__NATIONKEY = get<1>(qPARTS1_it710->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it710->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1_it710->first);
        qPARTS1[make_tuple(
            PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it712 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end711 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it712 != qPARTS1NATION1_end711; ++qPARTS1NATION1_it712)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1_it712->first);
        int64_t C__NATIONKEY = get<2>(qPARTS1NATION1_it712->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1NATION1_it712->first);
        qPARTS1NATION1[make_tuple(
            PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_it714 = qPARTS1NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_end713 = qPARTS1NATION1NATION1.end();
    for (
        ; qPARTS1NATION1NATION1_it714 != qPARTS1NATION1NATION1_end713; 
        ++qPARTS1NATION1NATION1_it714)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1NATION1_it714->first);
        int64_t x_qPARTS1NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS1NATION1NATION1_it714->first);
        qPARTS1NATION1NATION1[make_tuple(
            PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it716 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end715 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it716 != qPARTS1NATION2_end715; ++qPARTS1NATION2_it716)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION2_it716->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1NATION2_it716->first);
        qPARTS1NATION2[make_tuple(
            PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qPARTS1NATION_N1__NATIONKEY)]*qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_it718 = 
        qPARTS1NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_end717 = 
        qPARTS1NATION3.end();
    for (
        ; qPARTS1NATION3_it718 != qPARTS1NATION3_end717; ++qPARTS1NATION3_it718)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION3_it718->first);
        qPARTS1NATION3[make_tuple(
            PARTKEY,x_qPARTS1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qPARTS1NATION3LINEITEM1[make_tuple(
            ORDERKEY,x_qPARTS1NATION_N1__NATIONKEY,SUPPKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_it720 = qPARTS1NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_end719 = qPARTS1NATION3ORDERS1.end();
    for (
        ; qPARTS1NATION3ORDERS1_it720 != qPARTS1NATION3ORDERS1_end719; 
        ++qPARTS1NATION3ORDERS1_it720)
    {
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<2>(
            qPARTS1NATION3ORDERS1_it720->first);
        int64_t x_qPARTS1NATION3ORDERS_O__CUSTKEY = get<3>(
            qPARTS1NATION3ORDERS1_it720->first);
        qPARTS1NATION3ORDERS1[make_tuple(
            ORDERKEY,PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,
            x_qPARTS1NATION3ORDERS_O__CUSTKEY)] += 
            -1*EXTENDEDPRICE*qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qPARTS1NATION_N1__NATIONKEY,x_qPARTS1NATION3ORDERS_O__CUSTKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2_it722 = qPARTS2.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS2_end721 
        = qPARTS2.end();
    for (; qPARTS2_it722 != qPARTS2_end721; ++qPARTS2_it722)
    {
        int64_t C__NATIONKEY = get<1>(qPARTS2_it722->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2_it722->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS2_it722->first);
        qPARTS2[make_tuple(
            PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_it724 = qPARTS2NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_end723 = qPARTS2NATION1.end();
    for (
        ; qPARTS2NATION1_it724 != qPARTS2NATION1_end723; ++qPARTS2NATION1_it724)
    {
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION1_it724->first);
        int64_t C__NATIONKEY = get<2>(qPARTS2NATION1_it724->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS2NATION1_it724->first);
        qPARTS2NATION1[make_tuple(
            PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qPARTS2NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1NATION1_it726 = qPARTS2NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1NATION1_end725 = qPARTS2NATION1NATION1.end();
    for (
        ; qPARTS2NATION1NATION1_it726 != qPARTS2NATION1NATION1_end725; 
        ++qPARTS2NATION1NATION1_it726)
    {
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION1NATION1_it726->first);
        int64_t x_qPARTS2NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS2NATION1NATION1_it726->first);
        qPARTS2NATION1NATION1[make_tuple(
            PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,x_qPARTS2NATION1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qPARTS2NATION1NATION_N1__NATIONKEY)]*qLINEITEM3NATION1[make_tuple(
            SUPPKEY,x_qPARTS2NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS2NATION2_it728 = 
        qPARTS2NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2NATION2_end727 = qPARTS2NATION2.end();
    for (
        ; qPARTS2NATION2_it728 != qPARTS2NATION2_end727; ++qPARTS2NATION2_it728)
    {
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION2_it728->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2NATION2_it728->first);
        qPARTS2NATION2[make_tuple(
            PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qPARTS2NATION_N1__NATIONKEY)]*qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qPARTS2NATION3_it730 = 
        qPARTS2NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS2NATION3_end729 = 
        qPARTS2NATION3.end();
    for (
        ; qPARTS2NATION3_it730 != qPARTS2NATION3_end729; ++qPARTS2NATION3_it730)
    {
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION3_it730->first);
        qPARTS2NATION3[make_tuple(
            PARTKEY,x_qPARTS2NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qPARTS1NATION3LINEITEM1[make_tuple(
            ORDERKEY,x_qPARTS2NATION_N1__NATIONKEY,SUPPKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION3ORDERS1_it732 = qPARTS2NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION3ORDERS1_end731 = qPARTS2NATION3ORDERS1.end();
    for (
        ; qPARTS2NATION3ORDERS1_it732 != qPARTS2NATION3ORDERS1_end731; 
        ++qPARTS2NATION3ORDERS1_it732)
    {
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<2>(
            qPARTS2NATION3ORDERS1_it732->first);
        int64_t x_qPARTS2NATION3ORDERS_O__CUSTKEY = get<3>(
            qPARTS2NATION3ORDERS1_it732->first);
        qPARTS2NATION3ORDERS1[make_tuple(
            ORDERKEY,PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,
            x_qPARTS2NATION3ORDERS_O__CUSTKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qPARTS2NATION_N1__NATIONKEY,x_qPARTS2NATION3ORDERS_O__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it734 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end733 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it734 != qSUPPLIER1_end733; ++qSUPPLIER1_it734)
    {
        string P__MFGR = get<1>(qSUPPLIER1_it734->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1_it734->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1_it734->first);
        qSUPPLIER1[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(PARTKEY,P__MFGR)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER1NATION1_it736 
        = qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER1NATION1_end735 = qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it736 != qSUPPLIER1NATION1_end735; 
        ++qSUPPLIER1NATION1_it736)
    {
        string P__MFGR = get<1>(qSUPPLIER1NATION1_it736->first);
        int64_t x_qSUPPLIER1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1NATION1_it736->first);
        qSUPPLIER1NATION1[make_tuple(
            SUPPKEY,P__MFGR,x_qSUPPLIER1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qSUPPLIER1NATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_it738 = qSUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_end737 = qSUPPLIER1PARTS1.end();
    for (
        ; qSUPPLIER1PARTS1_it738 != qSUPPLIER1PARTS1_end737; ++qSUPPLIER1PARTS1_it738)
    {
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1PARTS1_it738->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1PARTS1_it738->first);
        qSUPPLIER1PARTS1[make_tuple(
            PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1[make_tuple(ORDERKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_it740 = qSUPPLIER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_end739 = qSUPPLIER1PARTS1NATION1.end();
    for (
        ; qSUPPLIER1PARTS1NATION1_it740 != qSUPPLIER1PARTS1NATION1_end739; 
        ++qSUPPLIER1PARTS1NATION1_it740)
    {
        int64_t x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1PARTS1NATION1_it740->first);
        qSUPPLIER1PARTS1NATION1[make_tuple(
            PARTKEY,SUPPKEY,x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qSUPPLIER1PARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER3_it742 = qSUPPLIER3.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3_end741 = qSUPPLIER3.end();
    for (; qSUPPLIER3_it742 != qSUPPLIER3_end741; ++qSUPPLIER3_it742)
    {
        string P__MFGR = get<1>(qSUPPLIER3_it742->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER3_it742->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER3_it742->first);
        qSUPPLIER3[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)]*qLINEITEM2[make_tuple(PARTKEY,P__MFGR)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER3NATION1_it744 
        = qSUPPLIER3NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER3NATION1_end743 = qSUPPLIER3NATION1.end();
    for (
        ; qSUPPLIER3NATION1_it744 != qSUPPLIER3NATION1_end743; 
        ++qSUPPLIER3NATION1_it744)
    {
        string P__MFGR = get<1>(qSUPPLIER3NATION1_it744->first);
        int64_t x_qSUPPLIER3NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER3NATION1_it744->first);
        qSUPPLIER3NATION1[make_tuple(
            SUPPKEY,P__MFGR,x_qSUPPLIER3NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qSUPPLIER3NATION_N1__NATIONKEY)]*qLINEITEM2[make_tuple(
            PARTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1_it746 = qSUPPLIER3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1_end745 = qSUPPLIER3PARTS1.end();
    for (
        ; qSUPPLIER3PARTS1_it746 != qSUPPLIER3PARTS1_end745; ++qSUPPLIER3PARTS1_it746)
    {
        int64_t C__NATIONKEY = get<2>(qSUPPLIER3PARTS1_it746->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER3PARTS1_it746->first);
        qSUPPLIER3PARTS1[make_tuple(
            PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(
            ORDERKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1NATION1_it748 = qSUPPLIER3PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1NATION1_end747 = qSUPPLIER3PARTS1NATION1.end();
    for (
        ; qSUPPLIER3PARTS1NATION1_it748 != qSUPPLIER3PARTS1NATION1_end747; 
        ++qSUPPLIER3PARTS1NATION1_it748)
    {
        int64_t x_qSUPPLIER3PARTS1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER3PARTS1NATION1_it748->first);
        qSUPPLIER3PARTS1NATION1[make_tuple(
            PARTKEY,SUPPKEY,x_qSUPPLIER3PARTS1NATION_N1__NATIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qSUPPLIER3PARTS1NATION_N1__NATIONKEY)];
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
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it750 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end749 = 
        q.end();
    for (; q_it750 != q_end749; ++q_it750)
    {
        string P__MFGR = get<0>(q_it750->first);
        int64_t N2__REGIONKEY = get<1>(q_it750->first);
        int64_t N1__REGIONKEY = get<2>(q_it750->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,NATIONKEY)] += -1*qCUSTOMER1[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it752 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end751 = 
        q.end();
    for (; q_it752 != q_end751; ++q_it752)
    {
        string P__MFGR = get<0>(q_it752->first);
        int64_t N2__REGIONKEY = get<1>(q_it752->first);
        int64_t N1__REGIONKEY = get<2>(q_it752->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,NATIONKEY)] += -1*qCUSTOMER3[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)]*-1;
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_it754 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1_end753 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it754 != qLINEITEM1_end753; ++qLINEITEM1_it754)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it754->first);
        int64_t N1__REGIONKEY = get<2>(qLINEITEM1_it754->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,N1__REGIONKEY)] += 
            -1*qLINEITEM1CUSTOMER1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_it756 = 
        qLINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_end755 = 
        qLINEITEM1NATION1.end();
    for (
        ; qLINEITEM1NATION1_it756 != qLINEITEM1NATION1_end755; 
        ++qLINEITEM1NATION1_it756)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1NATION1_it756->first);
        qLINEITEM1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY)] += -1*qLINEITEM1CUSTOMER1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)];
    }
    qLINEITEM1NATION1ORDERS1[make_tuple(CUSTKEY,NATIONKEY)] += -1;
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it758 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end757 
        = qNATION1.end();
    for (; qNATION1_it758 != qNATION1_end757; ++qNATION1_it758)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it758->first);
        string P__MFGR = get<1>(qNATION1_it758->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it758->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY,N1__REGIONKEY)] += 
            -1*qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it760 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end759 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it760 != qNATION1NATION1_end759; ++qNATION1NATION1_it760)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1NATION1_it760->first);
        string P__MFGR = get<1>(qNATION1NATION1_it760->first);
        qNATION1NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY)] += -1*qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY,P__MFGR)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it762 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end761 = 
        qNATION2.end();
    for (; qNATION2_it762 != qNATION2_end761; ++qNATION2_it762)
    {
        string P__MFGR = get<0>(qNATION2_it762->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it762->first);
        qNATION2[make_tuple(
            P__MFGR,NATIONKEY,N2__REGIONKEY)] += -1*qCUSTOMER1[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION3_it764 = qNATION3.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION3_end763 = qNATION3.end();
    for (; qNATION3_it764 != qNATION3_end763; ++qNATION3_it764)
    {
        string P__MFGR = get<1>(qNATION3_it764->first);
        qNATION3[make_tuple(
            NATIONKEY,P__MFGR)] += -1*qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_it766 = qNATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_end765 = qNATION3ORDERS1.end();
    for (
        ; qNATION3ORDERS1_it766 != qNATION3ORDERS1_end765; ++qNATION3ORDERS1_it766)
    {
        int64_t x_qNATION3ORDERS_O__ORDERKEY = get<0>(
            qNATION3ORDERS1_it766->first);
        string P__MFGR = get<2>(qNATION3ORDERS1_it766->first);
        qNATION3ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR,CUSTKEY)] += 
            -1*qORDERS1NATION1[make_tuple(x_qNATION3ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION4_it768 = qNATION4.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION4_end767 
        = qNATION4.end();
    for (; qNATION4_it768 != qNATION4_end767; ++qNATION4_it768)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4_it768->first);
        string P__MFGR = get<1>(qNATION4_it768->first);
        int64_t N1__REGIONKEY = get<3>(qNATION4_it768->first);
        qNATION4[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY,N1__REGIONKEY)] += 
            -1*qCUSTOMER3NATION1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY,P__MFGR)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION4NATION1_it770 = 
        qNATION4NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION4NATION1_end769 = qNATION4NATION1.end();
    for (
        ; qNATION4NATION1_it770 != qNATION4NATION1_end769; ++qNATION4NATION1_it770)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4NATION1_it770->first);
        string P__MFGR = get<1>(qNATION4NATION1_it770->first);
        qNATION4NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,P__MFGR,NATIONKEY)] += -1*qCUSTOMER3NATION1[make_tuple(
            CUSTKEY,x_qNATION_N1__NATIONKEY,P__MFGR)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_it772 = 
        qNATION5.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_end771 = 
        qNATION5.end();
    for (; qNATION5_it772 != qNATION5_end771; ++qNATION5_it772)
    {
        string P__MFGR = get<0>(qNATION5_it772->first);
        int64_t N2__REGIONKEY = get<2>(qNATION5_it772->first);
        qNATION5[make_tuple(
            P__MFGR,NATIONKEY,N2__REGIONKEY)] += -1*qCUSTOMER3[make_tuple(
            CUSTKEY,P__MFGR,N2__REGIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION6_it774 = qNATION6.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION6_end773 = qNATION6.end();
    for (; qNATION6_it774 != qNATION6_end773; ++qNATION6_it774)
    {
        string P__MFGR = get<1>(qNATION6_it774->first);
        qNATION6[make_tuple(
            NATIONKEY,P__MFGR)] += -1*qCUSTOMER3NATION1[make_tuple(
            CUSTKEY,NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION6ORDERS1_it776 = qNATION6ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION6ORDERS1_end775 = qNATION6ORDERS1.end();
    for (
        ; qNATION6ORDERS1_it776 != qNATION6ORDERS1_end775; ++qNATION6ORDERS1_it776)
    {
        int64_t x_qNATION6ORDERS_O__ORDERKEY = get<0>(
            qNATION6ORDERS1_it776->first);
        string P__MFGR = get<2>(qNATION6ORDERS1_it776->first);
        qNATION6ORDERS1[make_tuple(
            x_qNATION6ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR,CUSTKEY)] += 
            -1*qORDERS3NATION1[make_tuple(x_qNATION6ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_it778 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qORDERS2_end777 = 
        qORDERS2.end();
    for (; qORDERS2_it778 != qORDERS2_end777; ++qORDERS2_it778)
    {
        int64_t N1__REGIONKEY = get<2>(qORDERS2_it778->first);
        qORDERS2[make_tuple(
            CUSTKEY,NATIONKEY,N1__REGIONKEY)] += -1*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it780 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end779 
        = qPARTS1.end();
    for (; qPARTS1_it780 != qPARTS1_end779; ++qPARTS1_it780)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it780->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it780->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1_it780->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            -1*qCUSTOMER1PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it782 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end781 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it782 != qPARTS1NATION1_end781; ++qPARTS1NATION1_it782)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1_it782->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1_it782->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1NATION1_it782->first);
        qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,NATIONKEY,N1__REGIONKEY)] += 
            -1*qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,
            x_qPARTS1NATION_N1__NATIONKEY)]*qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_it784 = qPARTS1NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_end783 = qPARTS1NATION1NATION1.end();
    for (
        ; qPARTS1NATION1NATION1_it784 != qPARTS1NATION1NATION1_end783; 
        ++qPARTS1NATION1NATION1_it784)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS1NATION1NATION1_it784->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION1NATION1_it784->first);
        qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,NATIONKEY)] += 
            -1*qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,x_qPARTS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it786 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end785 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it786 != qPARTS1NATION2_end785; ++qPARTS1NATION2_it786)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION2_it786->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1NATION2_it786->first);
        qPARTS1NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY)] += -1*qCUSTOMER1PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_it788 = 
        qPARTS1NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_end787 = 
        qPARTS1NATION3.end();
    for (
        ; qPARTS1NATION3_it788 != qPARTS1NATION3_end787; ++qPARTS1NATION3_it788)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION3_it788->first);
        qPARTS1NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY)] += -1*qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_it790 = qPARTS1NATION3LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_end789 = qPARTS1NATION3LINEITEM1.end();
    for (
        ; qPARTS1NATION3LINEITEM1_it790 != qPARTS1NATION3LINEITEM1_end789; 
        ++qPARTS1NATION3LINEITEM1_it790)
    {
        int64_t x_qPARTS1NATION3LINEITEM_L__ORDERKEY = get<0>(
            qPARTS1NATION3LINEITEM1_it790->first);
        int64_t x_qPARTS1NATION3LINEITEM_L__SUPPKEY = get<2>(
            qPARTS1NATION3LINEITEM1_it790->first);
        qPARTS1NATION3LINEITEM1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__ORDERKEY,NATIONKEY,
            x_qPARTS1NATION3LINEITEM_L__SUPPKEY)] += -1*qLINEITEM1CUSTOMER1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__ORDERKEY,CUSTKEY)]*qLINEITEM3NATION1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__SUPPKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1ORDERS1_it792 = qPARTS1NATION3LINEITEM1ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1ORDERS1_end791 = qPARTS1NATION3LINEITEM1ORDERS1.end();
    for (
        ; qPARTS1NATION3LINEITEM1ORDERS1_it792 != qPARTS1NATION3LINEITEM1ORDERS1_end791; 
        ++qPARTS1NATION3LINEITEM1ORDERS1_it792)
    {
        int64_t x_qPARTS1NATION3LINEITEM_L__SUPPKEY = get<2>(
            qPARTS1NATION3LINEITEM1ORDERS1_it792->first);
        qPARTS1NATION3LINEITEM1ORDERS1[make_tuple(
            CUSTKEY,NATIONKEY,x_qPARTS1NATION3LINEITEM_L__SUPPKEY)] += 
            -1*qLINEITEM3NATION1[make_tuple(x_qPARTS1NATION3LINEITEM_L__SUPPKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_it794 = qPARTS1NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_end793 = qPARTS1NATION3ORDERS1.end();
    for (
        ; qPARTS1NATION3ORDERS1_it794 != qPARTS1NATION3ORDERS1_end793; 
        ++qPARTS1NATION3ORDERS1_it794)
    {
        int64_t x_qPARTS1NATION3ORDERS_O__ORDERKEY = get<0>(
            qPARTS1NATION3ORDERS1_it794->first);
        int64_t x_qPARTS_P__PARTKEY = get<1>(
            qPARTS1NATION3ORDERS1_it794->first);
        qPARTS1NATION3ORDERS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY,CUSTKEY)] += 
            -1*qORDERS1PARTS1NATION1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3ORDERS1LINEITEM1_it796 = qPARTS1NATION3ORDERS1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3ORDERS1LINEITEM1_end795 = qPARTS1NATION3ORDERS1LINEITEM1.end();
    for (
        ; qPARTS1NATION3ORDERS1LINEITEM1_it796 != qPARTS1NATION3ORDERS1LINEITEM1_end795; 
        ++qPARTS1NATION3ORDERS1LINEITEM1_it796)
    {
        int64_t x_qPARTS1NATION3ORDERS1LINEITEM_L__SUPPKEY = get<0>(
            qPARTS1NATION3ORDERS1LINEITEM1_it796->first);
        qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            x_qPARTS1NATION3ORDERS1LINEITEM_L__SUPPKEY,NATIONKEY,CUSTKEY)] += 
            -1*qLINEITEM3NATION1[make_tuple(
            x_qPARTS1NATION3ORDERS1LINEITEM_L__SUPPKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2_it798 = qPARTS2.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS2_end797 
        = qPARTS2.end();
    for (; qPARTS2_it798 != qPARTS2_end797; ++qPARTS2_it798)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2_it798->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2_it798->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS2_it798->first);
        qPARTS2[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            -1*qCUSTOMER3PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_it800 = qPARTS2NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_end799 = qPARTS2NATION1.end();
    for (
        ; qPARTS2NATION1_it800 != qPARTS2NATION1_end799; ++qPARTS2NATION1_it800)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION1_it800->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION1_it800->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS2NATION1_it800->first);
        qPARTS2NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,NATIONKEY,N1__REGIONKEY)] += 
            -1*qCUSTOMER3PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,
            x_qPARTS2NATION_N1__NATIONKEY)]*qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1NATION1_it802 = qPARTS2NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1NATION1_end801 = qPARTS2NATION1NATION1.end();
    for (
        ; qPARTS2NATION1NATION1_it802 != qPARTS2NATION1NATION1_end801; 
        ++qPARTS2NATION1NATION1_it802)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS2NATION1NATION1_it802->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION1NATION1_it802->first);
        qPARTS2NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,NATIONKEY)] += 
            -1*qCUSTOMER3PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,x_qPARTS2NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS2NATION2_it804 = 
        qPARTS2NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2NATION2_end803 = qPARTS2NATION2.end();
    for (
        ; qPARTS2NATION2_it804 != qPARTS2NATION2_end803; ++qPARTS2NATION2_it804)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION2_it804->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2NATION2_it804->first);
        qPARTS2NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,N2__REGIONKEY)] += -1*qCUSTOMER3PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qPARTS2NATION3_it806 = 
        qPARTS2NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS2NATION3_end805 = 
        qPARTS2NATION3.end();
    for (
        ; qPARTS2NATION3_it806 != qPARTS2NATION3_end805; ++qPARTS2NATION3_it806)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION3_it806->first);
        qPARTS2NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY)] += -1*qCUSTOMER3PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION3ORDERS1_it808 = qPARTS2NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION3ORDERS1_end807 = qPARTS2NATION3ORDERS1.end();
    for (
        ; qPARTS2NATION3ORDERS1_it808 != qPARTS2NATION3ORDERS1_end807; 
        ++qPARTS2NATION3ORDERS1_it808)
    {
        int64_t x_qPARTS2NATION3ORDERS_O__ORDERKEY = get<0>(
            qPARTS2NATION3ORDERS1_it808->first);
        int64_t x_qPARTS_P__PARTKEY = get<1>(
            qPARTS2NATION3ORDERS1_it808->first);
        qPARTS2NATION3ORDERS1[make_tuple(
            x_qPARTS2NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY,CUSTKEY)] += 
            -1*qORDERS3PARTS1NATION1[make_tuple(
            x_qPARTS2NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it810 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end809 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it810 != qSUPPLIER1_end809; ++qSUPPLIER1_it810)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it810->first);
        string P__MFGR = get<1>(qSUPPLIER1_it810->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1_it810->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY,N1__REGIONKEY)] += 
            -1*qCUSTOMER1SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER1NATION1_it812 
        = qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER1NATION1_end811 = qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it812 != qSUPPLIER1NATION1_end811; 
        ++qSUPPLIER1NATION1_it812)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1NATION1_it812->first);
        string P__MFGR = get<1>(qSUPPLIER1NATION1_it812->first);
        qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY)] += -1*qCUSTOMER1SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_it814 = qSUPPLIER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1_end813 = qSUPPLIER1PARTS1.end();
    for (
        ; qSUPPLIER1PARTS1_it814 != qSUPPLIER1PARTS1_end813; ++qSUPPLIER1PARTS1_it814)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1_it814->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER1PARTS1_it814->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1PARTS1_it814->first);
        qSUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY,
            N1__REGIONKEY)] += -1*qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,
            CUSTKEY)]*qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_it816 = qSUPPLIER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1PARTS1NATION1_end815 = qSUPPLIER1PARTS1NATION1.end();
    for (
        ; qSUPPLIER1PARTS1NATION1_it816 != qSUPPLIER1PARTS1NATION1_end815; 
        ++qSUPPLIER1PARTS1NATION1_it816)
    {
        int64_t x_qSUPPLIER1PARTS_P__PARTKEY = get<0>(
            qSUPPLIER1PARTS1NATION1_it816->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(
            qSUPPLIER1PARTS1NATION1_it816->first);
        qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY)] += 
            -1*qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER1PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER3_it818 = qSUPPLIER3.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3_end817 = qSUPPLIER3.end();
    for (; qSUPPLIER3_it818 != qSUPPLIER3_end817; ++qSUPPLIER3_it818)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER3_it818->first);
        string P__MFGR = get<1>(qSUPPLIER3_it818->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER3_it818->first);
        qSUPPLIER3[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY,N1__REGIONKEY)] += 
            -1*qCUSTOMER3SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)]*qCUSTOMER2[make_tuple(
            NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER3NATION1_it820 
        = qSUPPLIER3NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER3NATION1_end819 = qSUPPLIER3NATION1.end();
    for (
        ; qSUPPLIER3NATION1_it820 != qSUPPLIER3NATION1_end819; 
        ++qSUPPLIER3NATION1_it820)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER3NATION1_it820->first);
        string P__MFGR = get<1>(qSUPPLIER3NATION1_it820->first);
        qSUPPLIER3NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,P__MFGR,NATIONKEY)] += -1*qCUSTOMER3SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1_it822 = qSUPPLIER3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1_end821 = qSUPPLIER3PARTS1.end();
    for (
        ; qSUPPLIER3PARTS1_it822 != qSUPPLIER3PARTS1_end821; ++qSUPPLIER3PARTS1_it822)
    {
        int64_t x_qSUPPLIER3PARTS_P__PARTKEY = get<0>(
            qSUPPLIER3PARTS1_it822->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(qSUPPLIER3PARTS1_it822->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER3PARTS1_it822->first);
        qSUPPLIER3PARTS1[make_tuple(
            x_qSUPPLIER3PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY,
            N1__REGIONKEY)] += -1*qCUSTOMER3SUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER3PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,
            CUSTKEY)]*qCUSTOMER2[make_tuple(NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1NATION1_it824 = qSUPPLIER3PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3PARTS1NATION1_end823 = qSUPPLIER3PARTS1NATION1.end();
    for (
        ; qSUPPLIER3PARTS1NATION1_it824 != qSUPPLIER3PARTS1NATION1_end823; 
        ++qSUPPLIER3PARTS1NATION1_it824)
    {
        int64_t x_qSUPPLIER3PARTS_P__PARTKEY = get<0>(
            qSUPPLIER3PARTS1NATION1_it824->first);
        int64_t x_qSUPPLIER_S__SUPPKEY = get<1>(
            qSUPPLIER3PARTS1NATION1_it824->first);
        qSUPPLIER3PARTS1NATION1[make_tuple(
            x_qSUPPLIER3PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,NATIONKEY)] += 
            -1*qCUSTOMER3SUPPLIER1PARTS1[make_tuple(
            x_qSUPPLIER3PARTS_P__PARTKEY,x_qSUPPLIER_S__SUPPKEY,CUSTKEY)];
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
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it826 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end825 = 
        q.end();
    for (; q_it826 != q_end825; ++q_it826)
    {
        string P__MFGR = get<0>(q_it826->first);
        int64_t N2__REGIONKEY = get<1>(q_it826->first);
        int64_t N1__REGIONKEY = get<2>(q_it826->first);
        int64_t C__NATIONKEY = get<3>(q_it826->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += -1*qSUPPLIER1[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it828 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end827 = 
        q.end();
    for (; q_it828 != q_end827; ++q_it828)
    {
        string P__MFGR = get<0>(q_it828->first);
        int64_t N2__REGIONKEY = get<1>(q_it828->first);
        int64_t N1__REGIONKEY = get<2>(q_it828->first);
        int64_t C__NATIONKEY = get<3>(q_it828->first);
        q[make_tuple(
            P__MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += -1*qSUPPLIER3[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)]*-1;
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it830 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end829 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it830 != qCUSTOMER1_end829; ++qCUSTOMER1_it830)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it830->first);
        string P__MFGR = get<1>(qCUSTOMER1_it830->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1_it830->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,N2__REGIONKEY)] += 
            -1*qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1NATION1_it832 
        = qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1NATION1_end831 = qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it832 != qCUSTOMER1NATION1_end831; 
        ++qCUSTOMER1NATION1_it832)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1NATION1_it832->first);
        string P__MFGR = get<2>(qCUSTOMER1NATION1_it832->first);
        qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,P__MFGR)] += -1*qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER1PARTS1_it834 
        = qCUSTOMER1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER1PARTS1_end833 = qCUSTOMER1PARTS1.end();
    for (
        ; qCUSTOMER1PARTS1_it834 != qCUSTOMER1PARTS1_end833; ++qCUSTOMER1PARTS1_it834)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1_it834->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1PARTS1_it834->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1PARTS1_it834->first);
        qCUSTOMER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)] += 
            -1*qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,SUPPKEY,
            x_qCUSTOMER_C__CUSTKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_it836 = qCUSTOMER1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER1PARTS1NATION1_end835 = qCUSTOMER1PARTS1NATION1.end();
    for (
        ; qCUSTOMER1PARTS1NATION1_it836 != qCUSTOMER1PARTS1NATION1_end835; 
        ++qCUSTOMER1PARTS1NATION1_it836)
    {
        int64_t x_qCUSTOMER1PARTS_P__PARTKEY = get<0>(
            qCUSTOMER1PARTS1NATION1_it836->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1PARTS1NATION1_it836->first);
        qCUSTOMER1PARTS1NATION1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,NATIONKEY)] += 
            -1*qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER1PARTS_P__PARTKEY,SUPPKEY,x_qCUSTOMER_C__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_it838 = 
        qCUSTOMER3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_end837 = 
        qCUSTOMER3.end();
    for (; qCUSTOMER3_it838 != qCUSTOMER3_end837; ++qCUSTOMER3_it838)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER3_it838->first);
        string P__MFGR = get<1>(qCUSTOMER3_it838->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER3_it838->first);
        qCUSTOMER3[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,P__MFGR,N2__REGIONKEY)] += 
            -1*qCUSTOMER3SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER3NATION1_it840 
        = qCUSTOMER3NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER3NATION1_end839 = qCUSTOMER3NATION1.end();
    for (
        ; qCUSTOMER3NATION1_it840 != qCUSTOMER3NATION1_end839; 
        ++qCUSTOMER3NATION1_it840)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER3NATION1_it840->first);
        string P__MFGR = get<2>(qCUSTOMER3NATION1_it840->first);
        qCUSTOMER3NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,P__MFGR)] += -1*qCUSTOMER3SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qCUSTOMER3PARTS1_it842 
        = qCUSTOMER3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qCUSTOMER3PARTS1_end841 = qCUSTOMER3PARTS1.end();
    for (
        ; qCUSTOMER3PARTS1_it842 != qCUSTOMER3PARTS1_end841; ++qCUSTOMER3PARTS1_it842)
    {
        int64_t x_qCUSTOMER3PARTS_P__PARTKEY = get<0>(
            qCUSTOMER3PARTS1_it842->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER3PARTS1_it842->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER3PARTS1_it842->first);
        qCUSTOMER3PARTS1[make_tuple(
            x_qCUSTOMER3PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)] += 
            -1*qCUSTOMER3SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER3PARTS_P__PARTKEY,SUPPKEY,
            x_qCUSTOMER_C__CUSTKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3PARTS1NATION1_it844 = qCUSTOMER3PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qCUSTOMER3PARTS1NATION1_end843 = qCUSTOMER3PARTS1NATION1.end();
    for (
        ; qCUSTOMER3PARTS1NATION1_it844 != qCUSTOMER3PARTS1NATION1_end843; 
        ++qCUSTOMER3PARTS1NATION1_it844)
    {
        int64_t x_qCUSTOMER3PARTS_P__PARTKEY = get<0>(
            qCUSTOMER3PARTS1NATION1_it844->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER3PARTS1NATION1_it844->first);
        qCUSTOMER3PARTS1NATION1[make_tuple(
            x_qCUSTOMER3PARTS_P__PARTKEY,x_qCUSTOMER_C__CUSTKEY,NATIONKEY)] += 
            -1*qCUSTOMER3SUPPLIER1PARTS1[make_tuple(
            x_qCUSTOMER3PARTS_P__PARTKEY,SUPPKEY,x_qCUSTOMER_C__CUSTKEY)];
    }

    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_it846 = 
        qLINEITEM3.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM3_end845 = 
        qLINEITEM3.end();
    for (; qLINEITEM3_it846 != qLINEITEM3_end845; ++qLINEITEM3_it846)
    {
        int64_t N2__REGIONKEY = get<1>(qLINEITEM3_it846->first);
        qLINEITEM3[make_tuple(
            SUPPKEY,N2__REGIONKEY)] += -1*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }
    qLINEITEM3NATION1[make_tuple(SUPPKEY,NATIONKEY)] += -1;
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it848 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end847 
        = qNATION1.end();
    for (; qNATION1_it848 != qNATION1_end847; ++qNATION1_it848)
    {
        string P__MFGR = get<1>(qNATION1_it848->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it848->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it848->first);
        qNATION1[make_tuple(
            NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += -1*qSUPPLIER1[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it850 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end849 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it850 != qNATION1NATION1_end849; ++qNATION1NATION1_it850)
    {
        string P__MFGR = get<1>(qNATION1NATION1_it850->first);
        int64_t x_qNATION1NATION_N1__NATIONKEY = get<2>(
            qNATION1NATION1_it850->first);
        qNATION1NATION1[make_tuple(
            NATIONKEY,P__MFGR,x_qNATION1NATION_N1__NATIONKEY)] += 
            -1*qSUPPLIER1NATION1[make_tuple(
            SUPPKEY,P__MFGR,x_qNATION1NATION_N1__NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it852 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end851 = 
        qNATION2.end();
    for (; qNATION2_it852 != qNATION2_end851; ++qNATION2_it852)
    {
        string P__MFGR = get<0>(qNATION2_it852->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it852->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it852->first);
        qNATION2[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            -1*qSUPPLIER1NATION1[make_tuple(
            SUPPKEY,P__MFGR,x_qNATION_N1__NATIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION3_it854 = qNATION3.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION3_end853 = qNATION3.end();
    for (; qNATION3_it854 != qNATION3_end853; ++qNATION3_it854)
    {
        string P__MFGR = get<1>(qNATION3_it854->first);
        qNATION3[make_tuple(
            NATIONKEY,P__MFGR)] += -1*qSUPPLIER1NATION1[make_tuple(
            SUPPKEY,P__MFGR,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_it856 = qNATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_end855 = qNATION3ORDERS1.end();
    for (
        ; qNATION3ORDERS1_it856 != qNATION3ORDERS1_end855; ++qNATION3ORDERS1_it856)
    {
        int64_t x_qNATION3ORDERS_O__ORDERKEY = get<0>(
            qNATION3ORDERS1_it856->first);
        string P__MFGR = get<2>(qNATION3ORDERS1_it856->first);
        int64_t x_qNATION3ORDERS_O__CUSTKEY = get<3>(
            qNATION3ORDERS1_it856->first);
        qNATION3ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR,x_qNATION3ORDERS_O__CUSTKEY)] += 
            -1*qORDERS1SUPPLIER1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,SUPPKEY,
            P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION4_it858 = qNATION4.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION4_end857 
        = qNATION4.end();
    for (; qNATION4_it858 != qNATION4_end857; ++qNATION4_it858)
    {
        string P__MFGR = get<1>(qNATION4_it858->first);
        int64_t C__NATIONKEY = get<2>(qNATION4_it858->first);
        int64_t N1__REGIONKEY = get<3>(qNATION4_it858->first);
        qNATION4[make_tuple(
            NATIONKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)] += -1*qSUPPLIER3[make_tuple(
            SUPPKEY,P__MFGR,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION4NATION1_it860 = 
        qNATION4NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION4NATION1_end859 = qNATION4NATION1.end();
    for (
        ; qNATION4NATION1_it860 != qNATION4NATION1_end859; ++qNATION4NATION1_it860)
    {
        string P__MFGR = get<1>(qNATION4NATION1_it860->first);
        int64_t x_qNATION4NATION_N1__NATIONKEY = get<2>(
            qNATION4NATION1_it860->first);
        qNATION4NATION1[make_tuple(
            NATIONKEY,P__MFGR,x_qNATION4NATION_N1__NATIONKEY)] += 
            -1*qSUPPLIER3NATION1[make_tuple(
            SUPPKEY,P__MFGR,x_qNATION4NATION_N1__NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_it862 = 
        qNATION5.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_end861 = 
        qNATION5.end();
    for (; qNATION5_it862 != qNATION5_end861; ++qNATION5_it862)
    {
        string P__MFGR = get<0>(qNATION5_it862->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION5_it862->first);
        int64_t N2__REGIONKEY = get<2>(qNATION5_it862->first);
        qNATION5[make_tuple(
            P__MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            -1*qSUPPLIER3NATION1[make_tuple(
            SUPPKEY,P__MFGR,x_qNATION_N1__NATIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION6_it864 = qNATION6.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION6_end863 = qNATION6.end();
    for (; qNATION6_it864 != qNATION6_end863; ++qNATION6_it864)
    {
        string P__MFGR = get<1>(qNATION6_it864->first);
        qNATION6[make_tuple(
            NATIONKEY,P__MFGR)] += -1*qSUPPLIER3NATION1[make_tuple(
            SUPPKEY,P__MFGR,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION6ORDERS1_it866 = qNATION6ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION6ORDERS1_end865 = qNATION6ORDERS1.end();
    for (
        ; qNATION6ORDERS1_it866 != qNATION6ORDERS1_end865; ++qNATION6ORDERS1_it866)
    {
        int64_t x_qNATION6ORDERS_O__ORDERKEY = get<0>(
            qNATION6ORDERS1_it866->first);
        string P__MFGR = get<2>(qNATION6ORDERS1_it866->first);
        int64_t x_qNATION6ORDERS_O__CUSTKEY = get<3>(
            qNATION6ORDERS1_it866->first);
        qNATION6ORDERS1[make_tuple(
            x_qNATION6ORDERS_O__ORDERKEY,NATIONKEY,P__MFGR,x_qNATION6ORDERS_O__CUSTKEY)] += 
            -1*qORDERS3SUPPLIER1[make_tuple(
            x_qNATION6ORDERS_O__ORDERKEY,SUPPKEY,
            P__MFGR)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qNATION6ORDERS_O__CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it868 = 
        qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end867 = 
        qORDERS1.end();
    for (; qORDERS1_it868 != qORDERS1_end867; ++qORDERS1_it868)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it868->first);
        string P__MFGR = get<1>(qORDERS1_it868->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1_it868->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,P__MFGR,N2__REGIONKEY)] += 
            -1*qORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY,P__MFGR)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1NATION1_it870 = 
        qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1NATION1_end869 = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it870 != qORDERS1NATION1_end869; ++qORDERS1NATION1_it870)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1NATION1_it870->first);
        string P__MFGR = get<2>(qORDERS1NATION1_it870->first);
        qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)] += -1*qORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1PARTS1_it872 = 
        qORDERS1PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS1PARTS1_end871 = qORDERS1PARTS1.end();
    for (
        ; qORDERS1PARTS1_it872 != qORDERS1PARTS1_end871; ++qORDERS1PARTS1_it872)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1PARTS1_it872->first);
        int64_t x_qORDERS1PARTS_P__PARTKEY = get<1>(
            qORDERS1PARTS1_it872->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1PARTS1_it872->first);
        qORDERS1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,N2__REGIONKEY)] += 
            -1*qORDERS1SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,
            SUPPKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS1PARTS1NATION1_it874 = qORDERS1PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS1PARTS1NATION1_end873 = qORDERS1PARTS1NATION1.end();
    for (
        ; qORDERS1PARTS1NATION1_it874 != qORDERS1PARTS1NATION1_end873; 
        ++qORDERS1PARTS1NATION1_it874)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(
            qORDERS1PARTS1NATION1_it874->first);
        int64_t x_qORDERS1PARTS_P__PARTKEY = get<1>(
            qORDERS1PARTS1NATION1_it874->first);
        qORDERS1PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,NATIONKEY)] += 
            -1*qORDERS1SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1PARTS_P__PARTKEY,SUPPKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_it876 = 
        qORDERS3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_end875 = 
        qORDERS3.end();
    for (; qORDERS3_it876 != qORDERS3_end875; ++qORDERS3_it876)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3_it876->first);
        string P__MFGR = get<1>(qORDERS3_it876->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS3_it876->first);
        qORDERS3[make_tuple(
            x_qORDERS_O__ORDERKEY,P__MFGR,N2__REGIONKEY)] += 
            -1*qORDERS3SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY,P__MFGR)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS3NATION1_it878 = 
        qORDERS3NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS3NATION1_end877 = qORDERS3NATION1.end();
    for (
        ; qORDERS3NATION1_it878 != qORDERS3NATION1_end877; ++qORDERS3NATION1_it878)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3NATION1_it878->first);
        string P__MFGR = get<2>(qORDERS3NATION1_it878->first);
        qORDERS3NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,P__MFGR)] += -1*qORDERS3SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY,P__MFGR)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS3PARTS1_it880 = 
        qORDERS3PARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS3PARTS1_end879 = qORDERS3PARTS1.end();
    for (
        ; qORDERS3PARTS1_it880 != qORDERS3PARTS1_end879; ++qORDERS3PARTS1_it880)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3PARTS1_it880->first);
        int64_t x_qORDERS3PARTS_P__PARTKEY = get<1>(
            qORDERS3PARTS1_it880->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS3PARTS1_it880->first);
        qORDERS3PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS3PARTS_P__PARTKEY,N2__REGIONKEY)] += 
            -1*qORDERS3SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS3PARTS_P__PARTKEY,
            SUPPKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS3PARTS1NATION1_it882 = qORDERS3PARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qORDERS3PARTS1NATION1_end881 = qORDERS3PARTS1NATION1.end();
    for (
        ; qORDERS3PARTS1NATION1_it882 != qORDERS3PARTS1NATION1_end881; 
        ++qORDERS3PARTS1NATION1_it882)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(
            qORDERS3PARTS1NATION1_it882->first);
        int64_t x_qORDERS3PARTS_P__PARTKEY = get<1>(
            qORDERS3PARTS1NATION1_it882->first);
        qORDERS3PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS3PARTS_P__PARTKEY,NATIONKEY)] += 
            -1*qORDERS3SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS3PARTS_P__PARTKEY,SUPPKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1_it884 = qPARTS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS1_end883 
        = qPARTS1.end();
    for (; qPARTS1_it884 != qPARTS1_end883; ++qPARTS1_it884)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1_it884->first);
        int64_t C__NATIONKEY = get<1>(qPARTS1_it884->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1_it884->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1_it884->first);
        qPARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            -1*qSUPPLIER1PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_it886 = qPARTS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1_end885 = qPARTS1NATION1.end();
    for (
        ; qPARTS1NATION1_it886 != qPARTS1NATION1_end885; ++qPARTS1NATION1_it886)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION1_it886->first);
        int64_t C__NATIONKEY = get<2>(qPARTS1NATION1_it886->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS1NATION1_it886->first);
        qPARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*qSUPPLIER1PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_it888 = qPARTS1NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION1NATION1_end887 = qPARTS1NATION1NATION1.end();
    for (
        ; qPARTS1NATION1NATION1_it888 != qPARTS1NATION1NATION1_end887; 
        ++qPARTS1NATION1NATION1_it888)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS1NATION1NATION1_it888->first);
        int64_t x_qPARTS1NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS1NATION1NATION1_it888->first);
        qPARTS1NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)] += 
            -1*qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,x_qPARTS1NATION1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS1NATION2_it890 = 
        qPARTS1NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS1NATION2_end889 = qPARTS1NATION2.end();
    for (
        ; qPARTS1NATION2_it890 != qPARTS1NATION2_end889; ++qPARTS1NATION2_it890)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION2_it890->first);
        int64_t x_qPARTS1NATION_N1__NATIONKEY = get<1>(
            qPARTS1NATION2_it890->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS1NATION2_it890->first);
        qPARTS1NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS1NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            -1*qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,
            x_qPARTS1NATION_N1__NATIONKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_it892 = 
        qPARTS1NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS1NATION3_end891 = 
        qPARTS1NATION3.end();
    for (
        ; qPARTS1NATION3_it892 != qPARTS1NATION3_end891; ++qPARTS1NATION3_it892)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS1NATION3_it892->first);
        qPARTS1NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY)] += -1*qSUPPLIER1PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_it894 = qPARTS1NATION3LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1_end893 = qPARTS1NATION3LINEITEM1.end();
    for (
        ; qPARTS1NATION3LINEITEM1_it894 != qPARTS1NATION3LINEITEM1_end893; 
        ++qPARTS1NATION3LINEITEM1_it894)
    {
        int64_t x_qPARTS1NATION3LINEITEM_L__ORDERKEY = get<0>(
            qPARTS1NATION3LINEITEM1_it894->first);
        qPARTS1NATION3LINEITEM1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__ORDERKEY,NATIONKEY,SUPPKEY)] += 
            -1*qLINEITEM1NATION1[make_tuple(
            x_qPARTS1NATION3LINEITEM_L__ORDERKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1ORDERS1_it896 = qPARTS1NATION3LINEITEM1ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3LINEITEM1ORDERS1_end895 = qPARTS1NATION3LINEITEM1ORDERS1.end();
    for (
        ; qPARTS1NATION3LINEITEM1ORDERS1_it896 != qPARTS1NATION3LINEITEM1ORDERS1_end895; 
        ++qPARTS1NATION3LINEITEM1ORDERS1_it896)
    {
        int64_t x_qPARTS1NATION3LINEITEM1ORDERS_O__CUSTKEY = get<0>(
            qPARTS1NATION3LINEITEM1ORDERS1_it896->first);
        qPARTS1NATION3LINEITEM1ORDERS1[make_tuple(
            x_qPARTS1NATION3LINEITEM1ORDERS_O__CUSTKEY,NATIONKEY,SUPPKEY)] += 
            -1*qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qPARTS1NATION3LINEITEM1ORDERS_O__CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_it898 = qPARTS1NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS1NATION3ORDERS1_end897 = qPARTS1NATION3ORDERS1.end();
    for (
        ; qPARTS1NATION3ORDERS1_it898 != qPARTS1NATION3ORDERS1_end897; 
        ++qPARTS1NATION3ORDERS1_it898)
    {
        int64_t x_qPARTS1NATION3ORDERS_O__ORDERKEY = get<0>(
            qPARTS1NATION3ORDERS1_it898->first);
        int64_t x_qPARTS_P__PARTKEY = get<1>(
            qPARTS1NATION3ORDERS1_it898->first);
        int64_t x_qPARTS1NATION3ORDERS_O__CUSTKEY = get<3>(
            qPARTS1NATION3ORDERS1_it898->first);
        qPARTS1NATION3ORDERS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY,
            x_qPARTS1NATION3ORDERS_O__CUSTKEY)] += -1*qORDERS1SUPPLIER1PARTS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,
            SUPPKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3ORDERS1LINEITEM1_it900 = qPARTS1NATION3ORDERS1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qPARTS1NATION3ORDERS1LINEITEM1_end899 = qPARTS1NATION3ORDERS1LINEITEM1.end();
    for (
        ; qPARTS1NATION3ORDERS1LINEITEM1_it900 != qPARTS1NATION3ORDERS1LINEITEM1_end899; 
        ++qPARTS1NATION3ORDERS1LINEITEM1_it900)
    {
        int64_t x_qPARTS1NATION3ORDERS_O__CUSTKEY = get<2>(
            qPARTS1NATION3ORDERS1LINEITEM1_it900->first);
        qPARTS1NATION3ORDERS1LINEITEM1[make_tuple(
            SUPPKEY,NATIONKEY,x_qPARTS1NATION3ORDERS_O__CUSTKEY)] += 
            -1*qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qPARTS1NATION3ORDERS_O__CUSTKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2_it902 = qPARTS2.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator qPARTS2_end901 
        = qPARTS2.end();
    for (; qPARTS2_it902 != qPARTS2_end901; ++qPARTS2_it902)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2_it902->first);
        int64_t C__NATIONKEY = get<1>(qPARTS2_it902->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2_it902->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS2_it902->first);
        qPARTS2[make_tuple(
            x_qPARTS_P__PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)] += 
            -1*qSUPPLIER3PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)]*qSUPPLIER2[make_tuple(
            NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_it904 = qPARTS2NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1_end903 = qPARTS2NATION1.end();
    for (
        ; qPARTS2NATION1_it904 != qPARTS2NATION1_end903; ++qPARTS2NATION1_it904)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION1_it904->first);
        int64_t C__NATIONKEY = get<2>(qPARTS2NATION1_it904->first);
        int64_t N1__REGIONKEY = get<3>(qPARTS2NATION1_it904->first);
        qPARTS2NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*qSUPPLIER3PARTS1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1NATION1_it906 = qPARTS2NATION1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION1NATION1_end905 = qPARTS2NATION1NATION1.end();
    for (
        ; qPARTS2NATION1NATION1_it906 != qPARTS2NATION1NATION1_end905; 
        ++qPARTS2NATION1NATION1_it906)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(
            qPARTS2NATION1NATION1_it906->first);
        int64_t x_qPARTS2NATION1NATION_N1__NATIONKEY = get<2>(
            qPARTS2NATION1NATION1_it906->first);
        qPARTS2NATION1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY,x_qPARTS2NATION1NATION_N1__NATIONKEY)] += 
            -1*qSUPPLIER3PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,x_qPARTS2NATION1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qPARTS2NATION2_it908 = 
        qPARTS2NATION2.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qPARTS2NATION2_end907 = qPARTS2NATION2.end();
    for (
        ; qPARTS2NATION2_it908 != qPARTS2NATION2_end907; ++qPARTS2NATION2_it908)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION2_it908->first);
        int64_t x_qPARTS2NATION_N1__NATIONKEY = get<1>(
            qPARTS2NATION2_it908->first);
        int64_t N2__REGIONKEY = get<2>(qPARTS2NATION2_it908->first);
        qPARTS2NATION2[make_tuple(
            x_qPARTS_P__PARTKEY,x_qPARTS2NATION_N1__NATIONKEY,N2__REGIONKEY)] += 
            -1*qSUPPLIER3PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,
            x_qPARTS2NATION_N1__NATIONKEY)]*qSUPPLIER2[make_tuple(NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t>,double>::iterator qPARTS2NATION3_it910 = 
        qPARTS2NATION3.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qPARTS2NATION3_end909 = 
        qPARTS2NATION3.end();
    for (
        ; qPARTS2NATION3_it910 != qPARTS2NATION3_end909; ++qPARTS2NATION3_it910)
    {
        int64_t x_qPARTS_P__PARTKEY = get<0>(qPARTS2NATION3_it910->first);
        qPARTS2NATION3[make_tuple(
            x_qPARTS_P__PARTKEY,NATIONKEY)] += -1*qSUPPLIER3PARTS1NATION1[make_tuple(
            x_qPARTS_P__PARTKEY,SUPPKEY,NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION3ORDERS1_it912 = qPARTS2NATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,int64_t,int64_t>,double>::iterator 
        qPARTS2NATION3ORDERS1_end911 = qPARTS2NATION3ORDERS1.end();
    for (
        ; qPARTS2NATION3ORDERS1_it912 != qPARTS2NATION3ORDERS1_end911; 
        ++qPARTS2NATION3ORDERS1_it912)
    {
        int64_t x_qPARTS2NATION3ORDERS_O__ORDERKEY = get<0>(
            qPARTS2NATION3ORDERS1_it912->first);
        int64_t x_qPARTS_P__PARTKEY = get<1>(
            qPARTS2NATION3ORDERS1_it912->first);
        int64_t x_qPARTS2NATION3ORDERS_O__CUSTKEY = get<3>(
            qPARTS2NATION3ORDERS1_it912->first);
        qPARTS2NATION3ORDERS1[make_tuple(
            x_qPARTS2NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,NATIONKEY,
            x_qPARTS2NATION3ORDERS_O__CUSTKEY)] += -1*qORDERS3SUPPLIER1PARTS1[make_tuple(
            x_qPARTS2NATION3ORDERS_O__ORDERKEY,x_qPARTS_P__PARTKEY,
            SUPPKEY)]*qLINEITEM1NATION1ORDERS1[make_tuple(
            x_qPARTS2NATION3ORDERS_O__CUSTKEY,NATIONKEY)];
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
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it914 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end913 = 
        q.end();
    for (; q_it914 != q_end913; ++q_it914)
    {
        int64_t N2__REGIONKEY = get<1>(q_it914->first);
        int64_t N1__REGIONKEY = get<2>(q_it914->first);
        int64_t C__NATIONKEY = get<3>(q_it914->first);
        q[make_tuple(
            MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += -1*qPARTS1[make_tuple(
            PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)]*100;
    }

    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_it916 = 
        q.begin();
    map<tuple<string,int64_t,int64_t,int64_t>,double>::iterator q_end915 = 
        q.end();
    for (; q_it916 != q_end915; ++q_it916)
    {
        int64_t N2__REGIONKEY = get<1>(q_it916->first);
        int64_t N1__REGIONKEY = get<2>(q_it916->first);
        int64_t C__NATIONKEY = get<3>(q_it916->first);
        q[make_tuple(
            MFGR,N2__REGIONKEY,N1__REGIONKEY,C__NATIONKEY)] += -1*qPARTS2[make_tuple(
            PARTKEY,C__NATIONKEY,N2__REGIONKEY,N1__REGIONKEY)]*-1;
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_it918 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER1_end917 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it918 != qCUSTOMER1_end917; ++qCUSTOMER1_it918)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it918->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER1_it918->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,MFGR,N2__REGIONKEY)] += -1*qCUSTOMER1PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1NATION1_it920 
        = qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1NATION1_end919 = qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it920 != qCUSTOMER1NATION1_end919; 
        ++qCUSTOMER1NATION1_it920)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1NATION1_it920->first);
        int64_t x_qCUSTOMER1NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER1NATION1_it920->first);
        qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1NATION_N1__NATIONKEY,MFGR)] += 
            -1*qCUSTOMER1PARTS1NATION1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_it922 = qCUSTOMER1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER1SUPPLIER1_end921 = qCUSTOMER1SUPPLIER1.end();
    for (
        ; qCUSTOMER1SUPPLIER1_it922 != qCUSTOMER1SUPPLIER1_end921; 
        ++qCUSTOMER1SUPPLIER1_it922)
    {
        int64_t x_qCUSTOMER1SUPPLIER_S__SUPPKEY = get<0>(
            qCUSTOMER1SUPPLIER1_it922->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1SUPPLIER1_it922->first);
        qCUSTOMER1SUPPLIER1[make_tuple(
            x_qCUSTOMER1SUPPLIER_S__SUPPKEY,x_qCUSTOMER_C__CUSTKEY,MFGR)] += 
            -1*qCUSTOMER1SUPPLIER1PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER1SUPPLIER_S__SUPPKEY,x_qCUSTOMER_C__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_it924 = 
        qCUSTOMER3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qCUSTOMER3_end923 = 
        qCUSTOMER3.end();
    for (; qCUSTOMER3_it924 != qCUSTOMER3_end923; ++qCUSTOMER3_it924)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER3_it924->first);
        int64_t N2__REGIONKEY = get<2>(qCUSTOMER3_it924->first);
        qCUSTOMER3[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,MFGR,N2__REGIONKEY)] += -1*qCUSTOMER3PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER3NATION1_it926 
        = qCUSTOMER3NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER3NATION1_end925 = qCUSTOMER3NATION1.end();
    for (
        ; qCUSTOMER3NATION1_it926 != qCUSTOMER3NATION1_end925; 
        ++qCUSTOMER3NATION1_it926)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER3NATION1_it926->first);
        int64_t x_qCUSTOMER3NATION_N1__NATIONKEY = get<1>(
            qCUSTOMER3NATION1_it926->first);
        qCUSTOMER3NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER3NATION_N1__NATIONKEY,MFGR)] += 
            -1*qCUSTOMER3PARTS1NATION1[make_tuple(
            PARTKEY,x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER3NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER3SUPPLIER1_it928 = qCUSTOMER3SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator 
        qCUSTOMER3SUPPLIER1_end927 = qCUSTOMER3SUPPLIER1.end();
    for (
        ; qCUSTOMER3SUPPLIER1_it928 != qCUSTOMER3SUPPLIER1_end927; 
        ++qCUSTOMER3SUPPLIER1_it928)
    {
        int64_t x_qCUSTOMER3SUPPLIER_S__SUPPKEY = get<0>(
            qCUSTOMER3SUPPLIER1_it928->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER3SUPPLIER1_it928->first);
        qCUSTOMER3SUPPLIER1[make_tuple(
            x_qCUSTOMER3SUPPLIER_S__SUPPKEY,x_qCUSTOMER_C__CUSTKEY,MFGR)] += 
            -1*qCUSTOMER3SUPPLIER1PARTS1[make_tuple(
            PARTKEY,x_qCUSTOMER3SUPPLIER_S__SUPPKEY,x_qCUSTOMER_C__CUSTKEY)];
    }
    qLINEITEM2[make_tuple(PARTKEY,MFGR)] += -1;
    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION1_it930 = qNATION1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION1_end929 
        = qNATION1.end();
    for (; qNATION1_it930 != qNATION1_end929; ++qNATION1_it930)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1_it930->first);
        int64_t C__NATIONKEY = get<2>(qNATION1_it930->first);
        int64_t N1__REGIONKEY = get<3>(qNATION1_it930->first);
        qNATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*qPARTS1NATION1[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION1NATION1_it932 = 
        qNATION1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION1NATION1_end931 = qNATION1NATION1.end();
    for (
        ; qNATION1NATION1_it932 != qNATION1NATION1_end931; ++qNATION1NATION1_it932)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION1NATION1_it932->first);
        int64_t x_qNATION1NATION_N1__NATIONKEY = get<2>(
            qNATION1NATION1_it932->first);
        qNATION1NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR,x_qNATION1NATION_N1__NATIONKEY)] += 
            -1*qPARTS1NATION1NATION1[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,x_qNATION1NATION_N1__NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_it934 = 
        qNATION2.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION2_end933 = 
        qNATION2.end();
    for (; qNATION2_it934 != qNATION2_end933; ++qNATION2_it934)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION2_it934->first);
        int64_t N2__REGIONKEY = get<2>(qNATION2_it934->first);
        qNATION2[make_tuple(
            MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += -1*qPARTS1NATION2[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION3_it936 = qNATION3.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION3_end935 = qNATION3.end();
    for (; qNATION3_it936 != qNATION3_end935; ++qNATION3_it936)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION3_it936->first);
        qNATION3[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR)] += -1*qPARTS1NATION3[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_it938 = qNATION3ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION3ORDERS1_end937 = qNATION3ORDERS1.end();
    for (
        ; qNATION3ORDERS1_it938 != qNATION3ORDERS1_end937; ++qNATION3ORDERS1_it938)
    {
        int64_t x_qNATION3ORDERS_O__ORDERKEY = get<0>(
            qNATION3ORDERS1_it938->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION3ORDERS1_it938->first);
        int64_t x_qNATION3ORDERS_O__CUSTKEY = get<3>(
            qNATION3ORDERS1_it938->first);
        qNATION3ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,x_qNATION_N1__NATIONKEY,MFGR,
            x_qNATION3ORDERS_O__CUSTKEY)] += -1*qPARTS1NATION3ORDERS1[make_tuple(
            x_qNATION3ORDERS_O__ORDERKEY,PARTKEY,x_qNATION_N1__NATIONKEY,
            x_qNATION3ORDERS_O__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qNATION4_it940 = qNATION4.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator qNATION4_end939 
        = qNATION4.end();
    for (; qNATION4_it940 != qNATION4_end939; ++qNATION4_it940)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4_it940->first);
        int64_t C__NATIONKEY = get<2>(qNATION4_it940->first);
        int64_t N1__REGIONKEY = get<3>(qNATION4_it940->first);
        qNATION4[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*qPARTS2NATION1[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qNATION4NATION1_it942 = 
        qNATION4NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qNATION4NATION1_end941 = qNATION4NATION1.end();
    for (
        ; qNATION4NATION1_it942 != qNATION4NATION1_end941; ++qNATION4NATION1_it942)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION4NATION1_it942->first);
        int64_t x_qNATION4NATION_N1__NATIONKEY = get<2>(
            qNATION4NATION1_it942->first);
        qNATION4NATION1[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR,x_qNATION4NATION_N1__NATIONKEY)] += 
            -1*qPARTS2NATION1NATION1[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,x_qNATION4NATION_N1__NATIONKEY)];
    }

    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_it944 = 
        qNATION5.begin();
    map<tuple<string,int64_t,int64_t>,double>::iterator qNATION5_end943 = 
        qNATION5.end();
    for (; qNATION5_it944 != qNATION5_end943; ++qNATION5_it944)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION5_it944->first);
        int64_t N2__REGIONKEY = get<2>(qNATION5_it944->first);
        qNATION5[make_tuple(
            MFGR,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)] += -1*qPARTS2NATION2[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,string>,double>::iterator qNATION6_it946 = qNATION6.begin();
    map<tuple<int64_t,string>,double>::iterator qNATION6_end945 = qNATION6.end();
    for (; qNATION6_it946 != qNATION6_end945; ++qNATION6_it946)
    {
        int64_t x_qNATION_N1__NATIONKEY = get<0>(qNATION6_it946->first);
        qNATION6[make_tuple(
            x_qNATION_N1__NATIONKEY,MFGR)] += -1*qPARTS2NATION3[make_tuple(
            PARTKEY,x_qNATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION6ORDERS1_it948 = qNATION6ORDERS1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qNATION6ORDERS1_end947 = qNATION6ORDERS1.end();
    for (
        ; qNATION6ORDERS1_it948 != qNATION6ORDERS1_end947; ++qNATION6ORDERS1_it948)
    {
        int64_t x_qNATION6ORDERS_O__ORDERKEY = get<0>(
            qNATION6ORDERS1_it948->first);
        int64_t x_qNATION_N1__NATIONKEY = get<1>(qNATION6ORDERS1_it948->first);
        int64_t x_qNATION6ORDERS_O__CUSTKEY = get<3>(
            qNATION6ORDERS1_it948->first);
        qNATION6ORDERS1[make_tuple(
            x_qNATION6ORDERS_O__ORDERKEY,x_qNATION_N1__NATIONKEY,MFGR,
            x_qNATION6ORDERS_O__CUSTKEY)] += -1*qPARTS2NATION3ORDERS1[make_tuple(
            x_qNATION6ORDERS_O__ORDERKEY,PARTKEY,x_qNATION_N1__NATIONKEY,
            x_qNATION6ORDERS_O__CUSTKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_it950 = 
        qORDERS1.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS1_end949 = 
        qORDERS1.end();
    for (; qORDERS1_it950 != qORDERS1_end949; ++qORDERS1_it950)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it950->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS1_it950->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,MFGR,N2__REGIONKEY)] += -1*qORDERS1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1NATION1_it952 = 
        qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1NATION1_end951 = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it952 != qORDERS1NATION1_end951; ++qORDERS1NATION1_it952)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1NATION1_it952->first);
        int64_t x_qORDERS1NATION_N1__NATIONKEY = get<1>(
            qORDERS1NATION1_it952->first);
        qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1NATION_N1__NATIONKEY,MFGR)] += 
            -1*qORDERS1PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,x_qORDERS1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1SUPPLIER1_it954 
        = qORDERS1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS1SUPPLIER1_end953 = qORDERS1SUPPLIER1.end();
    for (
        ; qORDERS1SUPPLIER1_it954 != qORDERS1SUPPLIER1_end953; 
        ++qORDERS1SUPPLIER1_it954)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1SUPPLIER1_it954->first);
        int64_t x_qORDERS1SUPPLIER_S__SUPPKEY = get<1>(
            qORDERS1SUPPLIER1_it954->first);
        qORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS1SUPPLIER_S__SUPPKEY,MFGR)] += 
            -1*qORDERS1SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,x_qORDERS1SUPPLIER_S__SUPPKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_it956 = 
        qORDERS3.begin();
    map<tuple<int64_t,string,int64_t>,double>::iterator qORDERS3_end955 = 
        qORDERS3.end();
    for (; qORDERS3_it956 != qORDERS3_end955; ++qORDERS3_it956)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3_it956->first);
        int64_t N2__REGIONKEY = get<2>(qORDERS3_it956->first);
        qORDERS3[make_tuple(
            x_qORDERS_O__ORDERKEY,MFGR,N2__REGIONKEY)] += -1*qORDERS3PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,N2__REGIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS3NATION1_it958 = 
        qORDERS3NATION1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS3NATION1_end957 = qORDERS3NATION1.end();
    for (
        ; qORDERS3NATION1_it958 != qORDERS3NATION1_end957; ++qORDERS3NATION1_it958)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3NATION1_it958->first);
        int64_t x_qORDERS3NATION_N1__NATIONKEY = get<1>(
            qORDERS3NATION1_it958->first);
        qORDERS3NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS3NATION_N1__NATIONKEY,MFGR)] += 
            -1*qORDERS3PARTS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,x_qORDERS3NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS3SUPPLIER1_it960 
        = qORDERS3SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qORDERS3SUPPLIER1_end959 = qORDERS3SUPPLIER1.end();
    for (
        ; qORDERS3SUPPLIER1_it960 != qORDERS3SUPPLIER1_end959; 
        ++qORDERS3SUPPLIER1_it960)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS3SUPPLIER1_it960->first);
        int64_t x_qORDERS3SUPPLIER_S__SUPPKEY = get<1>(
            qORDERS3SUPPLIER1_it960->first);
        qORDERS3SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS3SUPPLIER_S__SUPPKEY,MFGR)] += 
            -1*qORDERS3SUPPLIER1PARTS1[make_tuple(
            x_qORDERS_O__ORDERKEY,PARTKEY,x_qORDERS3SUPPLIER_S__SUPPKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER1_it962 = qSUPPLIER1.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER1_end961 = qSUPPLIER1.end();
    for (; qSUPPLIER1_it962 != qSUPPLIER1_end961; ++qSUPPLIER1_it962)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it962->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER1_it962->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER1_it962->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*qSUPPLIER1PARTS1[make_tuple(
            PARTKEY,x_qSUPPLIER_S__SUPPKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER1NATION1_it964 
        = qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER1NATION1_end963 = qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it964 != qSUPPLIER1NATION1_end963; 
        ++qSUPPLIER1NATION1_it964)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1NATION1_it964->first);
        int64_t x_qSUPPLIER1NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER1NATION1_it964->first);
        qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,MFGR,x_qSUPPLIER1NATION_N1__NATIONKEY)] += 
            -1*qSUPPLIER1PARTS1NATION1[make_tuple(
            PARTKEY,x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER1NATION_N1__NATIONKEY)];
    }

    map<tuple<int64_t,string,int64_t,int64_t>,
        double>::iterator qSUPPLIER3_it966 = qSUPPLIER3.begin();
    map<tuple<int64_t,string,int64_t,int64_t>,double>::iterator 
        qSUPPLIER3_end965 = qSUPPLIER3.end();
    for (; qSUPPLIER3_it966 != qSUPPLIER3_end965; ++qSUPPLIER3_it966)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER3_it966->first);
        int64_t C__NATIONKEY = get<2>(qSUPPLIER3_it966->first);
        int64_t N1__REGIONKEY = get<3>(qSUPPLIER3_it966->first);
        qSUPPLIER3[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,MFGR,C__NATIONKEY,N1__REGIONKEY)] += 
            -1*qSUPPLIER3PARTS1[make_tuple(
            PARTKEY,x_qSUPPLIER_S__SUPPKEY,C__NATIONKEY,N1__REGIONKEY)];
    }

    map<tuple<int64_t,string,int64_t>,double>::iterator qSUPPLIER3NATION1_it968 
        = qSUPPLIER3NATION1.begin();
    map<tuple<int64_t,string,int64_t>,
        double>::iterator qSUPPLIER3NATION1_end967 = qSUPPLIER3NATION1.end();
    for (
        ; qSUPPLIER3NATION1_it968 != qSUPPLIER3NATION1_end967; 
        ++qSUPPLIER3NATION1_it968)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER3NATION1_it968->first);
        int64_t x_qSUPPLIER3NATION_N1__NATIONKEY = get<2>(
            qSUPPLIER3NATION1_it968->first);
        qSUPPLIER3NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,MFGR,x_qSUPPLIER3NATION_N1__NATIONKEY)] += 
            -1*qSUPPLIER3PARTS1NATION1[make_tuple(
            PARTKEY,x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER3NATION_N1__NATIONKEY)];
    }

    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_PARTS_sec_span, on_delete_PARTS_usec_span);
}

DBToaster::DemoDatasets::NationStream SSBNation("/home/yanif/datasets/tpch/sf1/singlefile/nation.tbl",&DBToaster::DemoDatasets::parseNationField,4,100,512);

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

DBToaster::DemoDatasets::OrderStream SSBOrder("/home/yanif/datasets/tpch/sf1/singlefile/orders.tbl.a",&DBToaster::DemoDatasets::parseOrderField,9,17000000,512);

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

DBToaster::DemoDatasets::LineitemStream SSBLineitem("/home/yanif/datasets/tpch/sf1/singlefile/lineitem.tbl.a",&DBToaster::DemoDatasets::parseLineitemField,16,65000000,512);

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

DBToaster::DemoDatasets::CustomerStream SSBCustomer("/home/yanif/datasets/tpch/sf1/singlefile/customer.tbl.a",&DBToaster::DemoDatasets::parseCustomerField,8,1600000, 512);

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

DBToaster::DemoDatasets::SupplierStream SSBSupplier("/home/yanif/datasets/tpch/sf1/singlefile/supplier.tbl.a",&DBToaster::DemoDatasets::parseSupplierField,7,110000,512);

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

DBToaster::DemoDatasets::PartStream SSBParts("/home/yanif/datasets/tpch/sf1/singlefile/part.tbl.a",&DBToaster::DemoDatasets::parsePartField,9,2100000,512);

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
