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
map<tuple<int64_t,int64_t>,double> qCUSTOMER1ORDERS1SUPPLIER1;
map<tuple<int64_t,int64_t,string>,int> qLINEITEM1;
map<tuple<int64_t,int64_t,string>,int> qORDERS1LINEITEM1;
map<tuple<int64_t,int64_t,string>,int> qLINEITEM1SUPPLIER1;
map<tuple<int64_t,int64_t,string,int64_t>,double> qCUSTOMER1REGION1;
map<tuple<int64_t,int64_t>,double> qCUSTOMER1ORDERS1NATION1;
map<tuple<int64_t,int64_t,int64_t>,double> qORDERS2NATION1;
map<tuple<int64_t,int64_t,string>,double> qSUPPLIER1;
map<tuple<int64_t,int64_t,string>,double> qSUPPLIER2;
map<tuple<int64_t,int64_t,string>,int> qORDERS1SUPPLIER2;
map<tuple<int64_t,int64_t>,int> qCUSTOMER1ORDERS1NATION1LINEITEM1;
map<int64_t,double> qNATION1;
map<tuple<int64_t,int64_t,string,int64_t>,double> qSUPPLIER1REGION1;
map<tuple<int64_t,int64_t>,double> qCUSTOMER2ORDERS1SUPPLIER1;
map<int64_t,int> qNATION2;
map<int64_t,double> qNATION3;
map<tuple<int64_t,int64_t>,int> qCUSTOMER1LINEITEM1;
map<tuple<int64_t,int64_t,string>,int> qCUSTOMER1LINEITEM2;
map<tuple<int64_t,int64_t,string,int64_t>,int> 
    qCUSTOMER1ORDERS1REGION1LINEITEM1;
map<tuple<int64_t,int64_t,int64_t>,int> qLINEITEM1NATION1;
map<tuple<int64_t,int64_t,int64_t>,double> qORDERS1NATION1;
map<tuple<int64_t,int64_t>,double> qCUSTOMER1SUPPLIER1;
map<tuple<int64_t,int64_t>,int> qLINEITEM1SUPPLIER1NATION1;
map<tuple<int64_t,int64_t,int64_t>,int> qLINEITEM1ORDERS1NATION1;
map<tuple<int64_t,string>,int> qCUSTOMER1SUPPLIER2;
map<tuple<int64_t,string,int64_t>,int> qCUSTOMER1ORDERS1REGION1SUPPLIER2;
map<tuple<int64_t,int64_t,int64_t>,int> qORDERS1LINEITEM1NATION1;
map<tuple<int64_t,int64_t>,double> qCUSTOMER2NATION1;
map<tuple<int64_t,int64_t,string,int64_t>,double> qCUSTOMER1ORDERS1REGION1;
map<tuple<int64_t,int64_t,string,int64_t>,double> qORDERS2REGION1;
map<tuple<int64_t,int64_t,string>,double> qORDERS1;
map<tuple<int64_t,int64_t,string>,double> qCUSTOMER1;
map<tuple<int64_t,int64_t,string>,double> qORDERS2;
map<tuple<int64_t,int64_t>,double> qCUSTOMER2ORDERS1NATION1;
map<tuple<int64_t,int64_t>,double> qSUPPLIER2NATION1;
map<tuple<int64_t,int64_t,string>,double> qCUSTOMER2;
map<tuple<string,int64_t>,double> qREGION1;
map<tuple<string,int64_t>,double> qREGION2;
map<tuple<int64_t,int64_t,string>,int> qLINEITEM1ORDERS1;
map<tuple<int64_t,int64_t>,double> qCUSTOMER1NATION1;
map<tuple<int64_t,int64_t,string,int64_t>,int> qLINEITEM1REGION1;
map<tuple<int64_t,int64_t,string>,double> qCUSTOMER2ORDERS1;
map<tuple<int64_t,int64_t,string,int64_t>,int> qLINEITEM1SUPPLIER1REGION1;
map<tuple<int64_t,int64_t,string,int64_t>,int> qLINEITEM1ORDERS1REGION1;
map<tuple<int64_t,int64_t,string,int64_t>,double> qORDERS1REGION1;
map<string,double> q;
map<tuple<int64_t,int64_t>,double> qSUPPLIER1NATION1;
map<tuple<int64_t,int64_t>,int> qORDERS1LINEITEM1NATION1SUPPLIER1;
map<tuple<int64_t,int64_t,string,int64_t>,int> qORDERS1LINEITEM1REGION1;
map<tuple<int64_t,int64_t,string,int64_t>,double> qCUSTOMER2REGION1;
map<tuple<int64_t,int64_t>,double> qCUSTOMER2SUPPLIER1;
map<tuple<int64_t,int64_t,string,int64_t>,int> 
    qORDERS1LINEITEM1REGION1SUPPLIER1;
map<tuple<int64_t,int64_t,string,int64_t>,double> qCUSTOMER2ORDERS1REGION1;
map<tuple<int64_t,int64_t,string,int64_t>,double> qSUPPLIER2REGION1;
map<tuple<int64_t,int64_t,string>,double> qCUSTOMER1ORDERS1;

double on_insert_REGION_sec_span = 0.0;
double on_insert_REGION_usec_span = 0.0;
double on_insert_CUSTOMER_sec_span = 0.0;
double on_insert_CUSTOMER_usec_span = 0.0;
double on_insert_ORDERS_sec_span = 0.0;
double on_insert_ORDERS_usec_span = 0.0;
double on_insert_LINEITEM_sec_span = 0.0;
double on_insert_LINEITEM_usec_span = 0.0;
double on_insert_SUPPLIER_sec_span = 0.0;
double on_insert_SUPPLIER_usec_span = 0.0;
double on_insert_NATION_sec_span = 0.0;
double on_insert_NATION_usec_span = 0.0;
double on_delete_REGION_sec_span = 0.0;
double on_delete_REGION_usec_span = 0.0;
double on_delete_CUSTOMER_sec_span = 0.0;
double on_delete_CUSTOMER_usec_span = 0.0;
double on_delete_ORDERS_sec_span = 0.0;
double on_delete_ORDERS_usec_span = 0.0;
double on_delete_LINEITEM_sec_span = 0.0;
double on_delete_LINEITEM_usec_span = 0.0;
double on_delete_SUPPLIER_sec_span = 0.0;
double on_delete_SUPPLIER_usec_span = 0.0;
double on_delete_NATION_sec_span = 0.0;
double on_delete_NATION_usec_span = 0.0;



void analyse_mem_usage(ofstream* stats)
{
   cout << "qCUSTOMER1ORDERS1SUPPLIER1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1ORDERS1SUPPLIER1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qLINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,int>::key_type, map<tuple<int64_t,int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,int>::value_type>, map<tuple<int64_t,int64_t,string>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,int>::key_type, map<tuple<int64_t,int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,int>::value_type>, map<tuple<int64_t,int64_t,string>,int>::key_compare>))) << endl;

   cout << "qORDERS1LINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,int>::key_type, map<tuple<int64_t,int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,int>::value_type>, map<tuple<int64_t,int64_t,string>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1LINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,int>::key_type, map<tuple<int64_t,int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,int>::value_type>, map<tuple<int64_t,int64_t,string>,int>::key_compare>))) << endl;

   cout << "qLINEITEM1SUPPLIER1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,int>::key_type, map<tuple<int64_t,int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,int>::value_type>, map<tuple<int64_t,int64_t,string>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1SUPPLIER1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,int>::key_type, map<tuple<int64_t,int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,int>::value_type>, map<tuple<int64_t,int64_t,string>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER1REGION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1REGION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER1ORDERS1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1ORDERS1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS2NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS2NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS2NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS2NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qSUPPLIER1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "qSUPPLIER2 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER2" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "qORDERS1SUPPLIER2 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1SUPPLIER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,int>::key_type, map<tuple<int64_t,int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,int>::value_type>, map<tuple<int64_t,int64_t,string>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1SUPPLIER2" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1SUPPLIER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,int>::key_type, map<tuple<int64_t,int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,int>::value_type>, map<tuple<int64_t,int64_t,string>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER1ORDERS1NATION1LINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1NATION1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1ORDERS1NATION1LINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1NATION1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qNATION1 size: " << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION1.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION1" << "," << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION1.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   cout << "qSUPPLIER1REGION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER1REGION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER2ORDERS1SUPPLIER1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2ORDERS1SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER2ORDERS1SUPPLIER1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2ORDERS1SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qNATION2 size: " << (((sizeof(map<int64_t,int>::key_type)
       + sizeof(map<int64_t,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION2.size())  + (sizeof(struct _Rb_tree<map<int64_t,int>::key_type, map<int64_t,int>::value_type, _Select1st<map<int64_t,int>::value_type>, map<int64_t,int>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION2" << "," << (((sizeof(map<int64_t,int>::key_type)
       + sizeof(map<int64_t,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION2.size())  + (sizeof(struct _Rb_tree<map<int64_t,int>::key_type, map<int64_t,int>::value_type, _Select1st<map<int64_t,int>::value_type>, map<int64_t,int>::key_compare>))) << endl;

   cout << "qNATION3 size: " << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION3.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   (*stats) << "m," << "qNATION3" << "," << (((sizeof(map<int64_t,double>::key_type)
       + sizeof(map<int64_t,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qNATION3.size())  + (sizeof(struct _Rb_tree<map<int64_t,double>::key_type, map<int64_t,double>::value_type, _Select1st<map<int64_t,double>::value_type>, map<int64_t,double>::key_compare>))) << endl;

   cout << "qCUSTOMER1LINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1LINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER1LINEITEM2 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1LINEITEM2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,int>::key_type, map<tuple<int64_t,int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,int>::value_type>, map<tuple<int64_t,int64_t,string>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1LINEITEM2" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1LINEITEM2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,int>::key_type, map<tuple<int64_t,int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,int>::value_type>, map<tuple<int64_t,int64_t,string>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER1ORDERS1REGION1LINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1REGION1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1ORDERS1REGION1LINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1REGION1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,int>::key_compare>))) << endl;

   cout << "qLINEITEM1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qORDERS1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER1SUPPLIER1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1SUPPLIER1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qLINEITEM1SUPPLIER1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1SUPPLIER1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1SUPPLIER1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1SUPPLIER1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qLINEITEM1ORDERS1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1ORDERS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1ORDERS1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1ORDERS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER1SUPPLIER2 size: " << (((sizeof(map<tuple<int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1SUPPLIER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string>,int>::key_type, map<tuple<int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,string>,int>::value_type>, map<tuple<int64_t,string>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1SUPPLIER2" << "," << (((sizeof(map<tuple<int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1SUPPLIER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string>,int>::key_type, map<tuple<int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,string>,int>::value_type>, map<tuple<int64_t,string>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER1ORDERS1REGION1SUPPLIER2 size: " << (((sizeof(map<tuple<int64_t,string,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1REGION1SUPPLIER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,int>::key_type, map<tuple<int64_t,string,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,int>::value_type>, map<tuple<int64_t,string,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1ORDERS1REGION1SUPPLIER2" << "," << (((sizeof(map<tuple<int64_t,string,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,string,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1REGION1SUPPLIER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,string,int64_t>,int>::key_type, map<tuple<int64_t,string,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,string,int64_t>,int>::value_type>, map<tuple<int64_t,string,int64_t>,int>::key_compare>))) << endl;

   cout << "qORDERS1LINEITEM1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1LINEITEM1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1LINEITEM1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1LINEITEM1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER2NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER2NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER1ORDERS1REGION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1ORDERS1REGION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS2REGION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS2REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS2REGION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS2REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "qORDERS2 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS2" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER2ORDERS1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2ORDERS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER2ORDERS1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2ORDERS1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qSUPPLIER2NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER2NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER2NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER2NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER2 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER2" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "qREGION1 size: " << (((sizeof(map<tuple<string,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qREGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t>,double>::key_type, map<tuple<string,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t>,double>::value_type>, map<tuple<string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qREGION1" << "," << (((sizeof(map<tuple<string,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qREGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t>,double>::key_type, map<tuple<string,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t>,double>::value_type>, map<tuple<string,int64_t>,double>::key_compare>))) << endl;

   cout << "qREGION2 size: " << (((sizeof(map<tuple<string,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qREGION2.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t>,double>::key_type, map<tuple<string,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t>,double>::value_type>, map<tuple<string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qREGION2" << "," << (((sizeof(map<tuple<string,int64_t>,double>::key_type)
       + sizeof(map<tuple<string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qREGION2.size())  + (sizeof(struct _Rb_tree<map<tuple<string,int64_t>,double>::key_type, map<tuple<string,int64_t>,double>::value_type, _Select1st<map<tuple<string,int64_t>,double>::value_type>, map<tuple<string,int64_t>,double>::key_compare>))) << endl;

   cout << "qLINEITEM1ORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,int>::key_type, map<tuple<int64_t,int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,int>::value_type>, map<tuple<int64_t,int64_t,string>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1ORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,int>::key_type, map<tuple<int64_t,int64_t,string>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,int>::value_type>, map<tuple<int64_t,int64_t,string>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qLINEITEM1REGION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1REGION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER2ORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER2ORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   cout << "qLINEITEM1SUPPLIER1REGION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1SUPPLIER1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1SUPPLIER1REGION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1SUPPLIER1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,int>::key_compare>))) << endl;

   cout << "qLINEITEM1ORDERS1REGION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1ORDERS1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1ORDERS1REGION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1ORDERS1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,int>::key_compare>))) << endl;

   cout << "qORDERS1REGION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1REGION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "q size: " << (((sizeof(map<string,double>::key_type)
       + sizeof(map<string,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<string,double>::key_type, map<string,double>::value_type, _Select1st<map<string,double>::value_type>, map<string,double>::key_compare>))) << endl;

   (*stats) << "m," << "q" << "," << (((sizeof(map<string,double>::key_type)
       + sizeof(map<string,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<string,double>::key_type, map<string,double>::value_type, _Select1st<map<string,double>::value_type>, map<string,double>::key_compare>))) << endl;

   cout << "qSUPPLIER1NATION1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER1NATION1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER1NATION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS1LINEITEM1NATION1SUPPLIER1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1LINEITEM1NATION1SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1LINEITEM1NATION1SUPPLIER1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1LINEITEM1NATION1SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int>::key_type, map<tuple<int64_t,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t>,int>::key_compare>))) << endl;

   cout << "qORDERS1LINEITEM1REGION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1LINEITEM1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1LINEITEM1REGION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1LINEITEM1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER2REGION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER2REGION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER2SUPPLIER1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER2SUPPLIER1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double>::key_type, map<tuple<int64_t,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t>,double>::key_compare>))) << endl;

   cout << "qORDERS1LINEITEM1REGION1SUPPLIER1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1LINEITEM1REGION1SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,int>::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1LINEITEM1REGION1SUPPLIER1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,int>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1LINEITEM1REGION1SUPPLIER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,int>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,int>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,int>::key_compare>))) << endl;

   cout << "qCUSTOMER2ORDERS1REGION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2ORDERS1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER2ORDERS1REGION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2ORDERS1REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qSUPPLIER2REGION1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER2REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qSUPPLIER2REGION1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string,int64_t>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qSUPPLIER2REGION1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string,int64_t>,double>::key_type, map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string,int64_t>,double>::value_type>, map<tuple<int64_t,int64_t,string,int64_t>,double>::key_compare>))) << endl;

   cout << "qCUSTOMER1ORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1ORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t,string>,double>::key_type)
       + sizeof(map<tuple<int64_t,int64_t,string>,double>::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,string>,double>::key_type, map<tuple<int64_t,int64_t,string>,double>::value_type, _Select1st<map<tuple<int64_t,int64_t,string>,double>::value_type>, map<tuple<int64_t,int64_t,string>,double>::key_compare>))) << endl;

}

void analyse_handler_usage(ofstream* stats)
{
   cout << "on_insert_REGION cost: " << (on_insert_REGION_sec_span + (on_insert_REGION_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_REGION" << "," << (on_insert_REGION_sec_span + (on_insert_REGION_usec_span / 1000000.0)) << endl;
   cout << "on_insert_CUSTOMER cost: " << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_CUSTOMER" << "," << (on_insert_CUSTOMER_sec_span + (on_insert_CUSTOMER_usec_span / 1000000.0)) << endl;
   cout << "on_insert_ORDERS cost: " << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_ORDERS" << "," << (on_insert_ORDERS_sec_span + (on_insert_ORDERS_usec_span / 1000000.0)) << endl;
   cout << "on_insert_LINEITEM cost: " << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_LINEITEM" << "," << (on_insert_LINEITEM_sec_span + (on_insert_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_insert_SUPPLIER cost: " << (on_insert_SUPPLIER_sec_span + (on_insert_SUPPLIER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_SUPPLIER" << "," << (on_insert_SUPPLIER_sec_span + (on_insert_SUPPLIER_usec_span / 1000000.0)) << endl;
   cout << "on_insert_NATION cost: " << (on_insert_NATION_sec_span + (on_insert_NATION_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_insert_NATION" << "," << (on_insert_NATION_sec_span + (on_insert_NATION_usec_span / 1000000.0)) << endl;
   cout << "on_delete_REGION cost: " << (on_delete_REGION_sec_span + (on_delete_REGION_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_REGION" << "," << (on_delete_REGION_sec_span + (on_delete_REGION_usec_span / 1000000.0)) << endl;
   cout << "on_delete_CUSTOMER cost: " << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_CUSTOMER" << "," << (on_delete_CUSTOMER_sec_span + (on_delete_CUSTOMER_usec_span / 1000000.0)) << endl;
   cout << "on_delete_ORDERS cost: " << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_ORDERS" << "," << (on_delete_ORDERS_sec_span + (on_delete_ORDERS_usec_span / 1000000.0)) << endl;
   cout << "on_delete_LINEITEM cost: " << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_LINEITEM" << "," << (on_delete_LINEITEM_sec_span + (on_delete_LINEITEM_usec_span / 1000000.0)) << endl;
   cout << "on_delete_SUPPLIER cost: " << (on_delete_SUPPLIER_sec_span + (on_delete_SUPPLIER_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_SUPPLIER" << "," << (on_delete_SUPPLIER_sec_span + (on_delete_SUPPLIER_usec_span / 1000000.0)) << endl;
   cout << "on_delete_NATION cost: " << (on_delete_NATION_sec_span + (on_delete_NATION_usec_span / 1000000.0)) << endl;
   (*stats) << "h," << "on_delete_NATION" << "," << (on_delete_NATION_sec_span + (on_delete_NATION_usec_span / 1000000.0)) << endl;
}


void on_insert_REGION(
    ofstream* results, ofstream* log, ofstream* stats, int64_t REGIONKEY,string 
    NAME,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
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
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it6 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end5 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it6 != qCUSTOMER1_end5; ++qCUSTOMER1_it6)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it6->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER1_it6->first);
        string N__NAME = get<2>(qCUSTOMER1_it6->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            qCUSTOMER1REGION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qCUSTOMER1LINEITEM2_it8 = 
        qCUSTOMER1LINEITEM2.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qCUSTOMER1LINEITEM2_end7 = 
        qCUSTOMER1LINEITEM2.end();
    for (
        ; qCUSTOMER1LINEITEM2_it8 != qCUSTOMER1LINEITEM2_end7; 
        ++qCUSTOMER1LINEITEM2_it8)
    {
        int64_t x_qCUSTOMER1LINEITEM_L__SUPPKEY = get<0>(
            qCUSTOMER1LINEITEM2_it8->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER1LINEITEM2_it8->first);
        string N__NAME = get<2>(qCUSTOMER1LINEITEM2_it8->first);
        qCUSTOMER1LINEITEM2[make_tuple(
            x_qCUSTOMER1LINEITEM_L__SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            x_qCUSTOMER1LINEITEM_L__SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1ORDERS1_it10 = qCUSTOMER1ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1ORDERS1_end9 = qCUSTOMER1ORDERS1.end();
    for (
        ; qCUSTOMER1ORDERS1_it10 != qCUSTOMER1ORDERS1_end9; ++qCUSTOMER1ORDERS1_it10)
    {
        int64_t x_qCUSTOMER1ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER1ORDERS1_it10->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER1ORDERS1_it10->first);
        string N__NAME = get<2>(qCUSTOMER1ORDERS1_it10->first);
        qCUSTOMER1ORDERS1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            qCUSTOMER1ORDERS1REGION1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,string>,int>::iterator qCUSTOMER1SUPPLIER2_it12 = 
        qCUSTOMER1SUPPLIER2.begin();
    map<tuple<int64_t,string>,int>::iterator qCUSTOMER1SUPPLIER2_end11 = 
        qCUSTOMER1SUPPLIER2.end();
    for (
        ; qCUSTOMER1SUPPLIER2_it12 != qCUSTOMER1SUPPLIER2_end11; 
        ++qCUSTOMER1SUPPLIER2_it12)
    {
        int64_t x_qCUSTOMER1SUPPLIER_S__NATIONKEY = get<0>(
            qCUSTOMER1SUPPLIER2_it12->first);
        string N__NAME = get<1>(qCUSTOMER1SUPPLIER2_it12->first);
        qCUSTOMER1SUPPLIER2[make_tuple(
            x_qCUSTOMER1SUPPLIER_S__NATIONKEY,N__NAME)] += 
            qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            x_qCUSTOMER1SUPPLIER_S__NATIONKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it14 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end13 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it14 != qCUSTOMER2_end13; ++qCUSTOMER2_it14)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it14->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER2_it14->first);
        string N__NAME = get<2>(qCUSTOMER2_it14->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            qCUSTOMER2REGION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER2ORDERS1_it16 = qCUSTOMER2ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2ORDERS1_end15 
        = qCUSTOMER2ORDERS1.end();
    for (
        ; qCUSTOMER2ORDERS1_it16 != qCUSTOMER2ORDERS1_end15; ++qCUSTOMER2ORDERS1_it16)
    {
        int64_t x_qCUSTOMER2ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER2ORDERS1_it16->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER2ORDERS1_it16->first);
        string N__NAME = get<2>(qCUSTOMER2ORDERS1_it16->first);
        qCUSTOMER2ORDERS1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            qCUSTOMER2ORDERS1REGION1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it18 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end17 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it18 != qLINEITEM1_end17; ++qLINEITEM1_it18)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it18->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it18->first);
        string N__NAME = get<2>(qLINEITEM1_it18->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += 
            qLINEITEM1REGION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1ORDERS1_it20 = 
        qLINEITEM1ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1ORDERS1_end19 = 
        qLINEITEM1ORDERS1.end();
    for (
        ; qLINEITEM1ORDERS1_it20 != qLINEITEM1ORDERS1_end19; ++qLINEITEM1ORDERS1_it20)
    {
        int64_t x_qLINEITEM1ORDERS_O__CUSTKEY = get<0>(
            qLINEITEM1ORDERS1_it20->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1ORDERS1_it20->first);
        string N__NAME = get<2>(qLINEITEM1ORDERS1_it20->first);
        qLINEITEM1ORDERS1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += 
            qLINEITEM1ORDERS1REGION1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,x_qLINEITEM_L__SUPPKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1SUPPLIER1_it22 = 
        qLINEITEM1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,
        int>::iterator qLINEITEM1SUPPLIER1_end21 = qLINEITEM1SUPPLIER1.end();
    for (
        ; qLINEITEM1SUPPLIER1_it22 != qLINEITEM1SUPPLIER1_end21; 
        ++qLINEITEM1SUPPLIER1_it22)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1SUPPLIER1_it22->first);
        int64_t x_qLINEITEM1SUPPLIER_S__NATIONKEY = get<1>(
            qLINEITEM1SUPPLIER1_it22->first);
        string N__NAME = get<2>(qLINEITEM1SUPPLIER1_it22->first);
        qLINEITEM1SUPPLIER1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM1SUPPLIER_S__NATIONKEY,N__NAME)] += 
            qLINEITEM1SUPPLIER1REGION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM1SUPPLIER_S__NATIONKEY,N__NAME,REGIONKEY)];
    }
    qNATION2[REGIONKEY] += 1;
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it24 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end23 = 
        qORDERS1.end();
    for (; qORDERS1_it24 != qORDERS1_end23; ++qORDERS1_it24)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it24->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it24->first);
        string N__NAME = get<2>(qORDERS1_it24->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
            qORDERS1REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1LINEITEM1_it26 = 
        qORDERS1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1LINEITEM1_end25 = 
        qORDERS1LINEITEM1.end();
    for (
        ; qORDERS1LINEITEM1_it26 != qORDERS1LINEITEM1_end25; ++qORDERS1LINEITEM1_it26)
    {
        int64_t x_qORDERS1LINEITEM_L__SUPPKEY = get<0>(
            qORDERS1LINEITEM1_it26->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1LINEITEM1_it26->first);
        string N__NAME = get<2>(qORDERS1LINEITEM1_it26->first);
        qORDERS1LINEITEM1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
            qORDERS1LINEITEM1REGION1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,x_qORDERS_O__CUSTKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1SUPPLIER2_it28 = 
        qORDERS1SUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1SUPPLIER2_end27 = 
        qORDERS1SUPPLIER2.end();
    for (
        ; qORDERS1SUPPLIER2_it28 != qORDERS1SUPPLIER2_end27; ++qORDERS1SUPPLIER2_it28)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<0>(qORDERS1SUPPLIER2_it28->first);
        int64_t x_qORDERS1SUPPLIER_S__NATIONKEY = get<1>(
            qORDERS1SUPPLIER2_it28->first);
        string N__NAME = get<2>(qORDERS1SUPPLIER2_it28->first);
        qORDERS1SUPPLIER2[make_tuple(
            x_qORDERS_O__CUSTKEY,x_qORDERS1SUPPLIER_S__NATIONKEY,N__NAME)] += 
            qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,x_qORDERS1SUPPLIER_S__NATIONKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it30 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end29 = 
        qORDERS2.end();
    for (; qORDERS2_it30 != qORDERS2_end29; ++qORDERS2_it30)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it30->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it30->first);
        string N__NAME = get<2>(qORDERS2_it30->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
            qORDERS2REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it32 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end31 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it32 != qSUPPLIER1_end31; ++qSUPPLIER1_it32)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it32->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER1_it32->first);
        string N__NAME = get<2>(qSUPPLIER1_it32->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
            qSUPPLIER1REGION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it34 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end33 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it34 != qSUPPLIER2_end33; ++qSUPPLIER2_it34)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it34->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER2_it34->first);
        string N__NAME = get<2>(qSUPPLIER2_it34->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
            qSUPPLIER2REGION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,REGIONKEY)];
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_REGION_sec_span, on_insert_REGION_usec_span);
}

void on_insert_CUSTOMER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t CUSTKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    MKTSEGMENT,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<string,double>::iterator q_it36 = q.begin();
    map<string,double>::iterator q_end35 = q.end();
    for (; q_it36 != q_end35; ++q_it36)
    {
        string N__NAME = q_it36->first;
        q[N__NAME] += qCUSTOMER1[make_tuple(CUSTKEY,NATIONKEY,N__NAME)];
    }
    map<string,double>::iterator q_it38 = q.begin();
    map<string,double>::iterator q_end37 = q.end();
    for (; q_it38 != q_end37; ++q_it38)
    {
        string N__NAME = q_it38->first;
        q[N__NAME] += qCUSTOMER2[make_tuple(CUSTKEY,NATIONKEY,N__NAME)]*-1;
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
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += 
            qCUSTOMER1LINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)]*qCUSTOMER1LINEITEM2[make_tuple(
            x_qLINEITEM_L__SUPPKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_it42 = 
        qLINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_end41 = 
        qLINEITEM1NATION1.end();
    for (
        ; qLINEITEM1NATION1_it42 != qLINEITEM1NATION1_end41; ++qLINEITEM1NATION1_it42)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1NATION1_it42->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<2>(qLINEITEM1NATION1_it42->first);
        qLINEITEM1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,x_qLINEITEM_L__SUPPKEY)] += 
            qCUSTOMER1LINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)]*qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            x_qLINEITEM_L__SUPPKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1ORDERS1_it44 = 
        qLINEITEM1ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1ORDERS1_end43 = 
        qLINEITEM1ORDERS1.end();
    for (
        ; qLINEITEM1ORDERS1_it44 != qLINEITEM1ORDERS1_end43; ++qLINEITEM1ORDERS1_it44)
    {
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1ORDERS1_it44->first);
        string N__NAME = get<2>(qLINEITEM1ORDERS1_it44->first);
        qLINEITEM1ORDERS1[make_tuple(
            CUSTKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += qCUSTOMER1LINEITEM2[make_tuple(
            x_qLINEITEM_L__SUPPKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1NATION1_it46 = qLINEITEM1ORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1NATION1_end45 = qLINEITEM1ORDERS1NATION1.end();
    for (
        ; qLINEITEM1ORDERS1NATION1_it46 != qLINEITEM1ORDERS1NATION1_end45; 
        ++qLINEITEM1ORDERS1NATION1_it46)
    {
        int64_t x_qLINEITEM_L__SUPPKEY = get<2>(
            qLINEITEM1ORDERS1NATION1_it46->first);
        qLINEITEM1ORDERS1NATION1[make_tuple(
            CUSTKEY,NATIONKEY,x_qLINEITEM_L__SUPPKEY)] += 
            qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(x_qLINEITEM_L__SUPPKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1REGION1_it48 = qLINEITEM1ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1REGION1_end47 = qLINEITEM1ORDERS1REGION1.end();
    for (
        ; qLINEITEM1ORDERS1REGION1_it48 != qLINEITEM1ORDERS1REGION1_end47; 
        ++qLINEITEM1ORDERS1REGION1_it48)
    {
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(
            qLINEITEM1ORDERS1REGION1_it48->first);
        string N__NAME = get<2>(qLINEITEM1ORDERS1REGION1_it48->first);
        int64_t x_qLINEITEM1ORDERS1REGION_R__REGIONKEY = get<3>(
            qLINEITEM1ORDERS1REGION1_it48->first);
        qLINEITEM1ORDERS1REGION1[make_tuple(
            CUSTKEY,x_qLINEITEM_L__SUPPKEY,N__NAME,x_qLINEITEM1ORDERS1REGION_R__REGIONKEY)] 
            += qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            x_qLINEITEM_L__SUPPKEY,NATIONKEY,N__NAME,
            x_qLINEITEM1ORDERS1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1REGION1_it50 = qLINEITEM1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1REGION1_end49 = qLINEITEM1REGION1.end();
    for (
        ; qLINEITEM1REGION1_it50 != qLINEITEM1REGION1_end49; ++qLINEITEM1REGION1_it50)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1REGION1_it50->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1REGION1_it50->first);
        string N__NAME = get<2>(qLINEITEM1REGION1_it50->first);
        int64_t x_qLINEITEM1REGION_R__REGIONKEY = get<3>(
            qLINEITEM1REGION1_it50->first);
        qLINEITEM1REGION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME,
            x_qLINEITEM1REGION_R__REGIONKEY)] += qCUSTOMER1LINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)]*qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            x_qLINEITEM_L__SUPPKEY,NATIONKEY,N__NAME,x_qLINEITEM1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1SUPPLIER1_it52 = 
        qLINEITEM1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,
        int>::iterator qLINEITEM1SUPPLIER1_end51 = qLINEITEM1SUPPLIER1.end();
    for (
        ; qLINEITEM1SUPPLIER1_it52 != qLINEITEM1SUPPLIER1_end51; 
        ++qLINEITEM1SUPPLIER1_it52)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1SUPPLIER1_it52->first);
        string N__NAME = get<2>(qLINEITEM1SUPPLIER1_it52->first);
        qLINEITEM1SUPPLIER1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,N__NAME)] += qCUSTOMER1LINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)]*qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1SUPPLIER1NATION1_it54 = 
        qLINEITEM1SUPPLIER1NATION1.begin();
    map<tuple<int64_t,int64_t>,
        int>::iterator qLINEITEM1SUPPLIER1NATION1_end53 = 
        qLINEITEM1SUPPLIER1NATION1.end();
    for (
        ; qLINEITEM1SUPPLIER1NATION1_it54 != qLINEITEM1SUPPLIER1NATION1_end53; 
        ++qLINEITEM1SUPPLIER1NATION1_it54)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1SUPPLIER1NATION1_it54->first);
        qLINEITEM1SUPPLIER1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY)] += qCUSTOMER1LINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1SUPPLIER1REGION1_it56 = qLINEITEM1SUPPLIER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1SUPPLIER1REGION1_end55 = qLINEITEM1SUPPLIER1REGION1.end();
    for (
        ; qLINEITEM1SUPPLIER1REGION1_it56 != qLINEITEM1SUPPLIER1REGION1_end55; 
        ++qLINEITEM1SUPPLIER1REGION1_it56)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1SUPPLIER1REGION1_it56->first);
        string N__NAME = get<2>(qLINEITEM1SUPPLIER1REGION1_it56->first);
        int64_t x_qLINEITEM1SUPPLIER1REGION_R__REGIONKEY = get<3>(
            qLINEITEM1SUPPLIER1REGION1_it56->first);
        qLINEITEM1SUPPLIER1REGION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,N__NAME,
            x_qLINEITEM1SUPPLIER1REGION_R__REGIONKEY)] += qCUSTOMER1LINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)]*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qLINEITEM1SUPPLIER1REGION_R__REGIONKEY)];
    }
    qNATION1[NATIONKEY] += qCUSTOMER1NATION1[make_tuple(CUSTKEY,NATIONKEY)];
    qNATION3[NATIONKEY] += qCUSTOMER2NATION1[make_tuple(CUSTKEY,NATIONKEY)];
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it58 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end57 = 
        qORDERS1.end();
    for (; qORDERS1_it58 != qORDERS1_end57; ++qORDERS1_it58)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it58->first);
        string N__NAME = get<2>(qORDERS1_it58->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,CUSTKEY,N__NAME)] += qCUSTOMER1ORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1LINEITEM1_it60 = 
        qORDERS1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1LINEITEM1_end59 = 
        qORDERS1LINEITEM1.end();
    for (
        ; qORDERS1LINEITEM1_it60 != qORDERS1LINEITEM1_end59; ++qORDERS1LINEITEM1_it60)
    {
        int64_t x_qORDERS1LINEITEM_L__SUPPKEY = get<0>(
            qORDERS1LINEITEM1_it60->first);
        string N__NAME = get<2>(qORDERS1LINEITEM1_it60->first);
        qORDERS1LINEITEM1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,CUSTKEY,N__NAME)] += 
            qCUSTOMER1LINEITEM2[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qORDERS1LINEITEM1NATION1_it62 = qORDERS1LINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qORDERS1LINEITEM1NATION1_end61 = qORDERS1LINEITEM1NATION1.end();
    for (
        ; qORDERS1LINEITEM1NATION1_it62 != qORDERS1LINEITEM1NATION1_end61; 
        ++qORDERS1LINEITEM1NATION1_it62)
    {
        int64_t x_qORDERS1LINEITEM_L__SUPPKEY = get<0>(
            qORDERS1LINEITEM1NATION1_it62->first);
        qORDERS1LINEITEM1NATION1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,NATIONKEY,CUSTKEY)] += 
            qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,NATIONKEY)];
    }
    qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(CUSTKEY,NATIONKEY)] += 1;
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1_it64 = qORDERS1LINEITEM1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1_end63 = qORDERS1LINEITEM1REGION1.end();
    for (
        ; qORDERS1LINEITEM1REGION1_it64 != qORDERS1LINEITEM1REGION1_end63; 
        ++qORDERS1LINEITEM1REGION1_it64)
    {
        int64_t x_qORDERS1LINEITEM_L__SUPPKEY = get<0>(
            qORDERS1LINEITEM1REGION1_it64->first);
        string N__NAME = get<2>(qORDERS1LINEITEM1REGION1_it64->first);
        int64_t x_qORDERS1LINEITEM1REGION_R__REGIONKEY = get<3>(
            qORDERS1LINEITEM1REGION1_it64->first);
        qORDERS1LINEITEM1REGION1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,CUSTKEY,N__NAME,
            x_qORDERS1LINEITEM1REGION_R__REGIONKEY)] += 
            qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,NATIONKEY,N__NAME,
            x_qORDERS1LINEITEM1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1SUPPLIER1_it66 = 
        qORDERS1LINEITEM1REGION1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1SUPPLIER1_end65 = qORDERS1LINEITEM1REGION1SUPPLIER1.end(
        );
    for (
        ; qORDERS1LINEITEM1REGION1SUPPLIER1_it66 != 
        qORDERS1LINEITEM1REGION1SUPPLIER1_end65; 
        ++qORDERS1LINEITEM1REGION1SUPPLIER1_it66)
    {
        string N__NAME = get<2>(qORDERS1LINEITEM1REGION1SUPPLIER1_it66->first);
        int64_t x_qORDERS1LINEITEM1REGION_R__REGIONKEY = get<3>(
            qORDERS1LINEITEM1REGION1SUPPLIER1_it66->first);
        qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            CUSTKEY,NATIONKEY,N__NAME,x_qORDERS1LINEITEM1REGION_R__REGIONKEY)] += 
            qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qORDERS1LINEITEM1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1NATION1_it68 = 
        qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS1NATION1_end67 = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it68 != qORDERS1NATION1_end67; ++qORDERS1NATION1_it68)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1NATION1_it68->first);
        qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,CUSTKEY)] += 
            qCUSTOMER1ORDERS1NATION1[make_tuple(x_qORDERS_O__ORDERKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS1REGION1_it70 = qORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS1REGION1_end69 = qORDERS1REGION1.end();
    for (
        ; qORDERS1REGION1_it70 != qORDERS1REGION1_end69; ++qORDERS1REGION1_it70)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1REGION1_it70->first);
        string N__NAME = get<2>(qORDERS1REGION1_it70->first);
        int64_t x_qORDERS1REGION_R__REGIONKEY = get<3>(
            qORDERS1REGION1_it70->first);
        qORDERS1REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,CUSTKEY,N__NAME,x_qORDERS1REGION_R__REGIONKEY)] += 
            qCUSTOMER1ORDERS1REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,N__NAME,x_qORDERS1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1SUPPLIER2_it72 = 
        qORDERS1SUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1SUPPLIER2_end71 = 
        qORDERS1SUPPLIER2.end();
    for (
        ; qORDERS1SUPPLIER2_it72 != qORDERS1SUPPLIER2_end71; ++qORDERS1SUPPLIER2_it72)
    {
        string N__NAME = get<2>(qORDERS1SUPPLIER2_it72->first);
        qORDERS1SUPPLIER2[make_tuple(
            CUSTKEY,NATIONKEY,N__NAME)] += qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it74 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end73 = 
        qORDERS2.end();
    for (; qORDERS2_it74 != qORDERS2_end73; ++qORDERS2_it74)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it74->first);
        string N__NAME = get<2>(qORDERS2_it74->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,CUSTKEY,N__NAME)] += qCUSTOMER2ORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS2NATION1_it76 = 
        qORDERS2NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS2NATION1_end75 = qORDERS2NATION1.end();
    for (
        ; qORDERS2NATION1_it76 != qORDERS2NATION1_end75; ++qORDERS2NATION1_it76)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2NATION1_it76->first);
        qORDERS2NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,CUSTKEY)] += 
            qCUSTOMER2ORDERS1NATION1[make_tuple(x_qORDERS_O__ORDERKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS2REGION1_it78 = qORDERS2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS2REGION1_end77 = qORDERS2REGION1.end();
    for (
        ; qORDERS2REGION1_it78 != qORDERS2REGION1_end77; ++qORDERS2REGION1_it78)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2REGION1_it78->first);
        string N__NAME = get<2>(qORDERS2REGION1_it78->first);
        int64_t x_qORDERS2REGION_R__REGIONKEY = get<3>(
            qORDERS2REGION1_it78->first);
        qORDERS2REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,CUSTKEY,N__NAME,x_qORDERS2REGION_R__REGIONKEY)] += 
            qCUSTOMER2ORDERS1REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,N__NAME,x_qORDERS2REGION_R__REGIONKEY)];
    }
    map<tuple<string,int64_t>,double>::iterator qREGION1_it80 = qREGION1.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION1_end79 = qREGION1.end();
    for (; qREGION1_it80 != qREGION1_end79; ++qREGION1_it80)
    {
        string N__NAME = get<0>(qREGION1_it80->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it80->first);
        qREGION1[make_tuple(
            N__NAME,x_qREGION_R__REGIONKEY)] += qCUSTOMER1REGION1[make_tuple(
            CUSTKEY,NATIONKEY,N__NAME,x_qREGION_R__REGIONKEY)];
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it82 = qREGION2.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION2_end81 = qREGION2.end();
    for (; qREGION2_it82 != qREGION2_end81; ++qREGION2_it82)
    {
        string N__NAME = get<0>(qREGION2_it82->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it82->first);
        qREGION2[make_tuple(
            N__NAME,x_qREGION_R__REGIONKEY)] += qCUSTOMER2REGION1[make_tuple(
            CUSTKEY,NATIONKEY,N__NAME,x_qREGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it84 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end83 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it84 != qSUPPLIER1_end83; ++qSUPPLIER1_it84)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it84->first);
        string N__NAME = get<2>(qSUPPLIER1_it84->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY,N__NAME)] += qCUSTOMER1SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY)]*qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER1NATION1_it86 = 
        qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER1NATION1_end85 = 
        qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it86 != qSUPPLIER1NATION1_end85; ++qSUPPLIER1NATION1_it86)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1NATION1_it86->first);
        qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY)] += qCUSTOMER1SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER1REGION1_it88 = qSUPPLIER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER1REGION1_end87 = qSUPPLIER1REGION1.end();
    for (
        ; qSUPPLIER1REGION1_it88 != qSUPPLIER1REGION1_end87; ++qSUPPLIER1REGION1_it88)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1REGION1_it88->first);
        string N__NAME = get<2>(qSUPPLIER1REGION1_it88->first);
        int64_t x_qSUPPLIER1REGION_R__REGIONKEY = get<3>(
            qSUPPLIER1REGION1_it88->first);
        qSUPPLIER1REGION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY,N__NAME,x_qSUPPLIER1REGION_R__REGIONKEY)] += 
            qCUSTOMER1SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY)]*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qSUPPLIER1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it90 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end89 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it90 != qSUPPLIER2_end89; ++qSUPPLIER2_it90)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it90->first);
        string N__NAME = get<2>(qSUPPLIER2_it90->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY,N__NAME)] += qCUSTOMER2SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY)]*qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER2NATION1_it92 = 
        qSUPPLIER2NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER2NATION1_end91 = 
        qSUPPLIER2NATION1.end();
    for (
        ; qSUPPLIER2NATION1_it92 != qSUPPLIER2NATION1_end91; ++qSUPPLIER2NATION1_it92)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2NATION1_it92->first);
        qSUPPLIER2NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY)] += qCUSTOMER2SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER2REGION1_it94 = qSUPPLIER2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER2REGION1_end93 = qSUPPLIER2REGION1.end();
    for (
        ; qSUPPLIER2REGION1_it94 != qSUPPLIER2REGION1_end93; ++qSUPPLIER2REGION1_it94)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2REGION1_it94->first);
        string N__NAME = get<2>(qSUPPLIER2REGION1_it94->first);
        int64_t x_qSUPPLIER2REGION_R__REGIONKEY = get<3>(
            qSUPPLIER2REGION1_it94->first);
        qSUPPLIER2REGION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY,N__NAME,x_qSUPPLIER2REGION_R__REGIONKEY)] += 
            qCUSTOMER2SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY)]*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qSUPPLIER2REGION_R__REGIONKEY)];
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
    map<string,double>::iterator q_it96 = q.begin();
    map<string,double>::iterator q_end95 = q.end();
    for (; q_it96 != q_end95; ++q_it96)
    {
        string N__NAME = q_it96->first;
        q[N__NAME] += qORDERS1[make_tuple(ORDERKEY,CUSTKEY,N__NAME)];
    }
    map<string,double>::iterator q_it98 = q.begin();
    map<string,double>::iterator q_end97 = q.end();
    for (; q_it98 != q_end97; ++q_it98)
    {
        string N__NAME = q_it98->first;
        q[N__NAME] += qORDERS2[make_tuple(ORDERKEY,CUSTKEY,N__NAME)]*-1;
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it100 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end99 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it100 != qCUSTOMER1_end99; ++qCUSTOMER1_it100)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER1_it100->first);
        string N__NAME = get<2>(qCUSTOMER1_it100->first);
        qCUSTOMER1[make_tuple(
            CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += qCUSTOMER1ORDERS1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)];
    }
    qCUSTOMER1LINEITEM1[make_tuple(ORDERKEY,CUSTKEY)] += 1;
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1NATION1_it102 = 
        qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1NATION1_end101 = 
        qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it102 != qCUSTOMER1NATION1_end101; 
        ++qCUSTOMER1NATION1_it102)
    {
        int64_t x_qCUSTOMER1NATION_N__NATIONKEY = get<1>(
            qCUSTOMER1NATION1_it102->first);
        qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,x_qCUSTOMER1NATION_N__NATIONKEY)] += 
            qCUSTOMER1ORDERS1NATION1[make_tuple(ORDERKEY,x_qCUSTOMER1NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1REGION1_it104 = qCUSTOMER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1REGION1_end103 = qCUSTOMER1REGION1.end();
    for (
        ; qCUSTOMER1REGION1_it104 != qCUSTOMER1REGION1_end103; 
        ++qCUSTOMER1REGION1_it104)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER1REGION1_it104->first);
        string N__NAME = get<2>(qCUSTOMER1REGION1_it104->first);
        int64_t x_qCUSTOMER1REGION_R__REGIONKEY = get<3>(
            qCUSTOMER1REGION1_it104->first);
        qCUSTOMER1REGION1[make_tuple(
            CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,x_qCUSTOMER1REGION_R__REGIONKEY)] += 
            qCUSTOMER1ORDERS1REGION1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,x_qCUSTOMER1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1SUPPLIER1_it106 = 
        qCUSTOMER1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1SUPPLIER1_end105 = 
        qCUSTOMER1SUPPLIER1.end();
    for (
        ; qCUSTOMER1SUPPLIER1_it106 != qCUSTOMER1SUPPLIER1_end105; 
        ++qCUSTOMER1SUPPLIER1_it106)
    {
        int64_t x_qCUSTOMER1SUPPLIER_S__SUPPKEY = get<0>(
            qCUSTOMER1SUPPLIER1_it106->first);
        qCUSTOMER1SUPPLIER1[make_tuple(
            x_qCUSTOMER1SUPPLIER_S__SUPPKEY,CUSTKEY)] += 
            qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qCUSTOMER1SUPPLIER_S__SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it108 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end107 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it108 != qCUSTOMER2_end107; ++qCUSTOMER2_it108)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER2_it108->first);
        string N__NAME = get<2>(qCUSTOMER2_it108->first);
        qCUSTOMER2[make_tuple(
            CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += qCUSTOMER2ORDERS1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2NATION1_it110 = 
        qCUSTOMER2NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2NATION1_end109 = 
        qCUSTOMER2NATION1.end();
    for (
        ; qCUSTOMER2NATION1_it110 != qCUSTOMER2NATION1_end109; 
        ++qCUSTOMER2NATION1_it110)
    {
        int64_t x_qCUSTOMER2NATION_N__NATIONKEY = get<1>(
            qCUSTOMER2NATION1_it110->first);
        qCUSTOMER2NATION1[make_tuple(
            CUSTKEY,x_qCUSTOMER2NATION_N__NATIONKEY)] += 
            qCUSTOMER2ORDERS1NATION1[make_tuple(ORDERKEY,x_qCUSTOMER2NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2REGION1_it112 = qCUSTOMER2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2REGION1_end111 = qCUSTOMER2REGION1.end();
    for (
        ; qCUSTOMER2REGION1_it112 != qCUSTOMER2REGION1_end111; 
        ++qCUSTOMER2REGION1_it112)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER2REGION1_it112->first);
        string N__NAME = get<2>(qCUSTOMER2REGION1_it112->first);
        int64_t x_qCUSTOMER2REGION_R__REGIONKEY = get<3>(
            qCUSTOMER2REGION1_it112->first);
        qCUSTOMER2REGION1[make_tuple(
            CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,x_qCUSTOMER2REGION_R__REGIONKEY)] += 
            qCUSTOMER2ORDERS1REGION1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,x_qCUSTOMER2REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2SUPPLIER1_it114 = 
        qCUSTOMER2SUPPLIER1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2SUPPLIER1_end113 = 
        qCUSTOMER2SUPPLIER1.end();
    for (
        ; qCUSTOMER2SUPPLIER1_it114 != qCUSTOMER2SUPPLIER1_end113; 
        ++qCUSTOMER2SUPPLIER1_it114)
    {
        int64_t x_qCUSTOMER2SUPPLIER_S__SUPPKEY = get<0>(
            qCUSTOMER2SUPPLIER1_it114->first);
        qCUSTOMER2SUPPLIER1[make_tuple(
            x_qCUSTOMER2SUPPLIER_S__SUPPKEY,CUSTKEY)] += 
            qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qCUSTOMER2SUPPLIER_S__SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it116 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end115 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it116 != qLINEITEM1_end115; ++qLINEITEM1_it116)
    {
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it116->first);
        string N__NAME = get<2>(qLINEITEM1_it116->first);
        qLINEITEM1[make_tuple(
            ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += qLINEITEM1ORDERS1[make_tuple(
            CUSTKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_it118 = 
        qLINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        int>::iterator qLINEITEM1NATION1_end117 = qLINEITEM1NATION1.end();
    for (
        ; qLINEITEM1NATION1_it118 != qLINEITEM1NATION1_end117; 
        ++qLINEITEM1NATION1_it118)
    {
        int64_t x_qLINEITEM1NATION_N__NATIONKEY = get<1>(
            qLINEITEM1NATION1_it118->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<2>(qLINEITEM1NATION1_it118->first);
        qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qLINEITEM1NATION_N__NATIONKEY,x_qLINEITEM_L__SUPPKEY)] += 
            qLINEITEM1ORDERS1NATION1[make_tuple(
            CUSTKEY,x_qLINEITEM1NATION_N__NATIONKEY,x_qLINEITEM_L__SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1REGION1_it120 = qLINEITEM1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1REGION1_end119 = qLINEITEM1REGION1.end();
    for (
        ; qLINEITEM1REGION1_it120 != qLINEITEM1REGION1_end119; 
        ++qLINEITEM1REGION1_it120)
    {
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1REGION1_it120->first);
        string N__NAME = get<2>(qLINEITEM1REGION1_it120->first);
        int64_t x_qLINEITEM1REGION_R__REGIONKEY = get<3>(
            qLINEITEM1REGION1_it120->first);
        qLINEITEM1REGION1[make_tuple(
            ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME,x_qLINEITEM1REGION_R__REGIONKEY)] += 
            qLINEITEM1ORDERS1REGION1[make_tuple(
            CUSTKEY,x_qLINEITEM_L__SUPPKEY,N__NAME,x_qLINEITEM1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,
        int>::iterator qLINEITEM1SUPPLIER1_it122 = qLINEITEM1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1SUPPLIER1_end121 
        = qLINEITEM1SUPPLIER1.end();
    for (
        ; qLINEITEM1SUPPLIER1_it122 != qLINEITEM1SUPPLIER1_end121; 
        ++qLINEITEM1SUPPLIER1_it122)
    {
        int64_t x_qLINEITEM1SUPPLIER_S__NATIONKEY = get<1>(
            qLINEITEM1SUPPLIER1_it122->first);
        string N__NAME = get<2>(qLINEITEM1SUPPLIER1_it122->first);
        qLINEITEM1SUPPLIER1[make_tuple(
            ORDERKEY,x_qLINEITEM1SUPPLIER_S__NATIONKEY,N__NAME)] += 
            qORDERS1SUPPLIER2[make_tuple(
            CUSTKEY,x_qLINEITEM1SUPPLIER_S__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,
        int>::iterator qLINEITEM1SUPPLIER1NATION1_it124 = 
        qLINEITEM1SUPPLIER1NATION1.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1SUPPLIER1NATION1_end123 
        = qLINEITEM1SUPPLIER1NATION1.end();
    for (
        ; qLINEITEM1SUPPLIER1NATION1_it124 != qLINEITEM1SUPPLIER1NATION1_end123; 
        ++qLINEITEM1SUPPLIER1NATION1_it124)
    {
        int64_t x_qLINEITEM1SUPPLIER1NATION_N__NATIONKEY = get<1>(
            qLINEITEM1SUPPLIER1NATION1_it124->first);
        qLINEITEM1SUPPLIER1NATION1[make_tuple(
            ORDERKEY,x_qLINEITEM1SUPPLIER1NATION_N__NATIONKEY)] += 
            qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(
            CUSTKEY,x_qLINEITEM1SUPPLIER1NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1SUPPLIER1REGION1_it126 = qLINEITEM1SUPPLIER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1SUPPLIER1REGION1_end125 = qLINEITEM1SUPPLIER1REGION1.end();
    for (
        ; qLINEITEM1SUPPLIER1REGION1_it126 != qLINEITEM1SUPPLIER1REGION1_end125; 
        ++qLINEITEM1SUPPLIER1REGION1_it126)
    {
        int64_t x_qLINEITEM1SUPPLIER_S__NATIONKEY = get<1>(
            qLINEITEM1SUPPLIER1REGION1_it126->first);
        string N__NAME = get<2>(qLINEITEM1SUPPLIER1REGION1_it126->first);
        int64_t x_qLINEITEM1SUPPLIER1REGION_R__REGIONKEY = get<3>(
            qLINEITEM1SUPPLIER1REGION1_it126->first);
        qLINEITEM1SUPPLIER1REGION1[make_tuple(
            ORDERKEY,x_qLINEITEM1SUPPLIER_S__NATIONKEY,N__NAME,
            x_qLINEITEM1SUPPLIER1REGION_R__REGIONKEY)] += 
            qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            CUSTKEY,x_qLINEITEM1SUPPLIER_S__NATIONKEY,N__NAME,
            x_qLINEITEM1SUPPLIER1REGION_R__REGIONKEY)];
    }
    map<int64_t,double>::iterator qNATION1_it128 = qNATION1.begin();
    map<int64_t,double>::iterator qNATION1_end127 = qNATION1.end();
    for (; qNATION1_it128 != qNATION1_end127; ++qNATION1_it128)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION1_it128->first;
        qNATION1[x_qNATION_N__NATIONKEY] += qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N__NATIONKEY,CUSTKEY)];
    }
    map<int64_t,double>::iterator qNATION3_it130 = qNATION3.begin();
    map<int64_t,double>::iterator qNATION3_end129 = qNATION3.end();
    for (; qNATION3_it130 != qNATION3_end129; ++qNATION3_it130)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION3_it130->first;
        qNATION3[x_qNATION_N__NATIONKEY] += qORDERS2NATION1[make_tuple(
            ORDERKEY,x_qNATION_N__NATIONKEY,CUSTKEY)];
    }
    map<tuple<string,int64_t>,double>::iterator qREGION1_it132 = qREGION1.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION1_end131 = qREGION1.end(
        );
    for (; qREGION1_it132 != qREGION1_end131; ++qREGION1_it132)
    {
        string N__NAME = get<0>(qREGION1_it132->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it132->first);
        qREGION1[make_tuple(
            N__NAME,x_qREGION_R__REGIONKEY)] += qORDERS1REGION1[make_tuple(
            ORDERKEY,CUSTKEY,N__NAME,x_qREGION_R__REGIONKEY)];
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it134 = qREGION2.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION2_end133 = qREGION2.end(
        );
    for (; qREGION2_it134 != qREGION2_end133; ++qREGION2_it134)
    {
        string N__NAME = get<0>(qREGION2_it134->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it134->first);
        qREGION2[make_tuple(
            N__NAME,x_qREGION_R__REGIONKEY)] += qORDERS2REGION1[make_tuple(
            ORDERKEY,CUSTKEY,N__NAME,x_qREGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it136 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end135 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it136 != qSUPPLIER1_end135; ++qSUPPLIER1_it136)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it136->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER1_it136->first);
        string N__NAME = get<2>(qSUPPLIER1_it136->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
            qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY)]*qORDERS1SUPPLIER2[make_tuple(
            CUSTKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER1NATION1_it138 = 
        qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER1NATION1_end137 = 
        qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it138 != qSUPPLIER1NATION1_end137; 
        ++qSUPPLIER1NATION1_it138)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1NATION1_it138->first);
        int64_t x_qSUPPLIER1NATION_N__NATIONKEY = get<1>(
            qSUPPLIER1NATION1_it138->first);
        qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER1NATION_N__NATIONKEY)] += 
            qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY)]*qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(
            CUSTKEY,x_qSUPPLIER1NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER1REGION1_it140 = qSUPPLIER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER1REGION1_end139 = qSUPPLIER1REGION1.end();
    for (
        ; qSUPPLIER1REGION1_it140 != qSUPPLIER1REGION1_end139; 
        ++qSUPPLIER1REGION1_it140)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1REGION1_it140->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(
            qSUPPLIER1REGION1_it140->first);
        string N__NAME = get<2>(qSUPPLIER1REGION1_it140->first);
        int64_t x_qSUPPLIER1REGION_R__REGIONKEY = get<3>(
            qSUPPLIER1REGION1_it140->first);
        qSUPPLIER1REGION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,
            x_qSUPPLIER1REGION_R__REGIONKEY)] += qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY)]*qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            CUSTKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,x_qSUPPLIER1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it142 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end141 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it142 != qSUPPLIER2_end141; ++qSUPPLIER2_it142)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it142->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER2_it142->first);
        string N__NAME = get<2>(qSUPPLIER2_it142->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
            qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY)]*qORDERS1SUPPLIER2[make_tuple(
            CUSTKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER2NATION1_it144 = 
        qSUPPLIER2NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER2NATION1_end143 = 
        qSUPPLIER2NATION1.end();
    for (
        ; qSUPPLIER2NATION1_it144 != qSUPPLIER2NATION1_end143; 
        ++qSUPPLIER2NATION1_it144)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2NATION1_it144->first);
        int64_t x_qSUPPLIER2NATION_N__NATIONKEY = get<1>(
            qSUPPLIER2NATION1_it144->first);
        qSUPPLIER2NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER2NATION_N__NATIONKEY)] += 
            qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY)]*qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(
            CUSTKEY,x_qSUPPLIER2NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER2REGION1_it146 = qSUPPLIER2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER2REGION1_end145 = qSUPPLIER2REGION1.end();
    for (
        ; qSUPPLIER2REGION1_it146 != qSUPPLIER2REGION1_end145; 
        ++qSUPPLIER2REGION1_it146)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2REGION1_it146->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(
            qSUPPLIER2REGION1_it146->first);
        string N__NAME = get<2>(qSUPPLIER2REGION1_it146->first);
        int64_t x_qSUPPLIER2REGION_R__REGIONKEY = get<3>(
            qSUPPLIER2REGION1_it146->first);
        qSUPPLIER2REGION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,
            x_qSUPPLIER2REGION_R__REGIONKEY)] += qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY)]*qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            CUSTKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,x_qSUPPLIER2REGION_R__REGIONKEY)];
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
    map<string,double>::iterator q_it148 = q.begin();
    map<string,double>::iterator q_end147 = q.end();
    for (; q_it148 != q_end147; ++q_it148)
    {
        string N__NAME = q_it148->first;
        q[N__NAME] += EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(
            ORDERKEY,SUPPKEY,N__NAME)]*-1;
    }
    map<string,double>::iterator q_it150 = q.begin();
    map<string,double>::iterator q_end149 = q.end();
    for (; q_it150 != q_end149; ++q_it150)
    {
        string N__NAME = q_it150->first;
        q[N__NAME] += EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,SUPPKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it152 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end151 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it152 != qCUSTOMER1_end151; ++qCUSTOMER1_it152)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it152->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER1_it152->first);
        string N__NAME = get<2>(qCUSTOMER1_it152->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            EXTENDEDPRICE*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1LINEITEM2[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1NATION1_it154 = 
        qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1NATION1_end153 = 
        qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it154 != qCUSTOMER1NATION1_end153; 
        ++qCUSTOMER1NATION1_it154)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1NATION1_it154->first);
        int64_t x_qCUSTOMER1NATION_N__NATIONKEY = get<1>(
            qCUSTOMER1NATION1_it154->first);
        qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1NATION_N__NATIONKEY)] += 
            EXTENDEDPRICE*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            SUPPKEY,x_qCUSTOMER1NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1ORDERS1_it156 
        = qCUSTOMER1ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1ORDERS1_end155 = qCUSTOMER1ORDERS1.end();
    for (
        ; qCUSTOMER1ORDERS1_it156 != qCUSTOMER1ORDERS1_end155; 
        ++qCUSTOMER1ORDERS1_it156)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER1ORDERS1_it156->first);
        string N__NAME = get<2>(qCUSTOMER1ORDERS1_it156->first);
        qCUSTOMER1ORDERS1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            EXTENDEDPRICE*qCUSTOMER1LINEITEM2[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1ORDERS1NATION1_it158 
        = qCUSTOMER1ORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t>,
        double>::iterator qCUSTOMER1ORDERS1NATION1_end157 = 
        qCUSTOMER1ORDERS1NATION1.end();
    for (
        ; qCUSTOMER1ORDERS1NATION1_it158 != qCUSTOMER1ORDERS1NATION1_end157; 
        ++qCUSTOMER1ORDERS1NATION1_it158)
    {
        int64_t x_qCUSTOMER1ORDERS1NATION_N__NATIONKEY = get<1>(
            qCUSTOMER1ORDERS1NATION1_it158->first);
        qCUSTOMER1ORDERS1NATION1[make_tuple(
            ORDERKEY,x_qCUSTOMER1ORDERS1NATION_N__NATIONKEY)] += 
            EXTENDEDPRICE*qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            SUPPKEY,x_qCUSTOMER1ORDERS1NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1ORDERS1REGION1_it160 = qCUSTOMER1ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1ORDERS1REGION1_end159 = qCUSTOMER1ORDERS1REGION1.end();
    for (
        ; qCUSTOMER1ORDERS1REGION1_it160 != qCUSTOMER1ORDERS1REGION1_end159; 
        ++qCUSTOMER1ORDERS1REGION1_it160)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER1ORDERS1REGION1_it160->first);
        string N__NAME = get<2>(qCUSTOMER1ORDERS1REGION1_it160->first);
        int64_t x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY = get<3>(
            qCUSTOMER1ORDERS1REGION1_it160->first);
        qCUSTOMER1ORDERS1REGION1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,
            x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY)] += 
            EXTENDEDPRICE*qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,
            x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY)];
    }
    qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(ORDERKEY,SUPPKEY)] += EXTENDEDPRICE;
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1REGION1_it162 = qCUSTOMER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1REGION1_end161 = qCUSTOMER1REGION1.end();
    for (
        ; qCUSTOMER1REGION1_it162 != qCUSTOMER1REGION1_end161; 
        ++qCUSTOMER1REGION1_it162)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1REGION1_it162->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER1REGION1_it162->first);
        string N__NAME = get<2>(qCUSTOMER1REGION1_it162->first);
        int64_t x_qCUSTOMER1REGION_R__REGIONKEY = get<3>(
            qCUSTOMER1REGION1_it162->first);
        qCUSTOMER1REGION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,
            x_qCUSTOMER1REGION_R__REGIONKEY)] += 
            EXTENDEDPRICE*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,x_qCUSTOMER1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1SUPPLIER1_it164 = 
        qCUSTOMER1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1SUPPLIER1_end163 = 
        qCUSTOMER1SUPPLIER1.end();
    for (
        ; qCUSTOMER1SUPPLIER1_it164 != qCUSTOMER1SUPPLIER1_end163; 
        ++qCUSTOMER1SUPPLIER1_it164)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1SUPPLIER1_it164->first);
        qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY)] += 
            EXTENDEDPRICE*qCUSTOMER1LINEITEM1[make_tuple(ORDERKEY,x_qCUSTOMER_C__CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it166 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end165 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it166 != qCUSTOMER2_end165; ++qCUSTOMER2_it166)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it166->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER2_it166->first);
        string N__NAME = get<2>(qCUSTOMER2_it166->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            EXTENDEDPRICE*DISCOUNT*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1LINEITEM2[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2NATION1_it168 = 
        qCUSTOMER2NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2NATION1_end167 = 
        qCUSTOMER2NATION1.end();
    for (
        ; qCUSTOMER2NATION1_it168 != qCUSTOMER2NATION1_end167; 
        ++qCUSTOMER2NATION1_it168)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2NATION1_it168->first);
        int64_t x_qCUSTOMER2NATION_N__NATIONKEY = get<1>(
            qCUSTOMER2NATION1_it168->first);
        qCUSTOMER2NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER2NATION_N__NATIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            SUPPKEY,x_qCUSTOMER2NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2ORDERS1_it170 
        = qCUSTOMER2ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER2ORDERS1_end169 = qCUSTOMER2ORDERS1.end();
    for (
        ; qCUSTOMER2ORDERS1_it170 != qCUSTOMER2ORDERS1_end169; 
        ++qCUSTOMER2ORDERS1_it170)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER2ORDERS1_it170->first);
        string N__NAME = get<2>(qCUSTOMER2ORDERS1_it170->first);
        qCUSTOMER2ORDERS1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            EXTENDEDPRICE*DISCOUNT*qCUSTOMER1LINEITEM2[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2ORDERS1NATION1_it172 
        = qCUSTOMER2ORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t>,
        double>::iterator qCUSTOMER2ORDERS1NATION1_end171 = 
        qCUSTOMER2ORDERS1NATION1.end();
    for (
        ; qCUSTOMER2ORDERS1NATION1_it172 != qCUSTOMER2ORDERS1NATION1_end171; 
        ++qCUSTOMER2ORDERS1NATION1_it172)
    {
        int64_t x_qCUSTOMER2ORDERS1NATION_N__NATIONKEY = get<1>(
            qCUSTOMER2ORDERS1NATION1_it172->first);
        qCUSTOMER2ORDERS1NATION1[make_tuple(
            ORDERKEY,x_qCUSTOMER2ORDERS1NATION_N__NATIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            SUPPKEY,x_qCUSTOMER2ORDERS1NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2ORDERS1REGION1_it174 = qCUSTOMER2ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2ORDERS1REGION1_end173 = qCUSTOMER2ORDERS1REGION1.end();
    for (
        ; qCUSTOMER2ORDERS1REGION1_it174 != qCUSTOMER2ORDERS1REGION1_end173; 
        ++qCUSTOMER2ORDERS1REGION1_it174)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER2ORDERS1REGION1_it174->first);
        string N__NAME = get<2>(qCUSTOMER2ORDERS1REGION1_it174->first);
        int64_t x_qCUSTOMER2ORDERS1REGION_R__REGIONKEY = get<3>(
            qCUSTOMER2ORDERS1REGION1_it174->first);
        qCUSTOMER2ORDERS1REGION1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,
            x_qCUSTOMER2ORDERS1REGION_R__REGIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,
            x_qCUSTOMER2ORDERS1REGION_R__REGIONKEY)];
    }
    qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
        ORDERKEY,SUPPKEY)] += EXTENDEDPRICE*DISCOUNT;
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2REGION1_it176 = qCUSTOMER2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2REGION1_end175 = qCUSTOMER2REGION1.end();
    for (
        ; qCUSTOMER2REGION1_it176 != qCUSTOMER2REGION1_end175; 
        ++qCUSTOMER2REGION1_it176)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2REGION1_it176->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER2REGION1_it176->first);
        string N__NAME = get<2>(qCUSTOMER2REGION1_it176->first);
        int64_t x_qCUSTOMER2REGION_R__REGIONKEY = get<3>(
            qCUSTOMER2REGION1_it176->first);
        qCUSTOMER2REGION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,
            x_qCUSTOMER2REGION_R__REGIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,x_qCUSTOMER2REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2SUPPLIER1_it178 = 
        qCUSTOMER2SUPPLIER1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2SUPPLIER1_end177 = 
        qCUSTOMER2SUPPLIER1.end();
    for (
        ; qCUSTOMER2SUPPLIER1_it178 != qCUSTOMER2SUPPLIER1_end177; 
        ++qCUSTOMER2SUPPLIER1_it178)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER2SUPPLIER1_it178->first);
        qCUSTOMER2SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)];
    }
    map<int64_t,double>::iterator qNATION1_it180 = qNATION1.begin();
    map<int64_t,double>::iterator qNATION1_end179 = qNATION1.end();
    for (; qNATION1_it180 != qNATION1_end179; ++qNATION1_it180)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION1_it180->first;
        qNATION1[x_qNATION_N__NATIONKEY] += 
            EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N__NATIONKEY,SUPPKEY)];
    }
    map<int64_t,double>::iterator qNATION3_it182 = qNATION3.begin();
    map<int64_t,double>::iterator qNATION3_end181 = qNATION3.end();
    for (; qNATION3_it182 != qNATION3_end181; ++qNATION3_it182)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION3_it182->first;
        qNATION3[x_qNATION_N__NATIONKEY] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N__NATIONKEY,SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it184 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end183 = 
        qORDERS1.end();
    for (; qORDERS1_it184 != qORDERS1_end183; ++qORDERS1_it184)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it184->first);
        string N__NAME = get<2>(qORDERS1_it184->first);
        qORDERS1[make_tuple(
            ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
            EXTENDEDPRICE*qORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qORDERS_O__CUSTKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS1NATION1_it186 = qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1NATION1_end185 
        = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it186 != qORDERS1NATION1_end185; ++qORDERS1NATION1_it186)
    {
        int64_t x_qORDERS1NATION_N__NATIONKEY = get<1>(
            qORDERS1NATION1_it186->first);
        int64_t x_qORDERS_O__CUSTKEY = get<2>(qORDERS1NATION1_it186->first);
        qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qORDERS1NATION_N__NATIONKEY,x_qORDERS_O__CUSTKEY)] += 
            EXTENDEDPRICE*qORDERS1LINEITEM1NATION1[make_tuple(
            SUPPKEY,x_qORDERS1NATION_N__NATIONKEY,x_qORDERS_O__CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS1REGION1_it188 = qORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS1REGION1_end187 = qORDERS1REGION1.end();
    for (
        ; qORDERS1REGION1_it188 != qORDERS1REGION1_end187; ++qORDERS1REGION1_it188)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1REGION1_it188->first);
        string N__NAME = get<2>(qORDERS1REGION1_it188->first);
        int64_t x_qORDERS1REGION_R__REGIONKEY = get<3>(
            qORDERS1REGION1_it188->first);
        qORDERS1REGION1[make_tuple(
            ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME,x_qORDERS1REGION_R__REGIONKEY)] += 
            EXTENDEDPRICE*qORDERS1LINEITEM1REGION1[make_tuple(
            SUPPKEY,x_qORDERS_O__CUSTKEY,N__NAME,x_qORDERS1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it190 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end189 = 
        qORDERS2.end();
    for (; qORDERS2_it190 != qORDERS2_end189; ++qORDERS2_it190)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it190->first);
        string N__NAME = get<2>(qORDERS2_it190->first);
        qORDERS2[make_tuple(
            ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
            EXTENDEDPRICE*DISCOUNT*qORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qORDERS_O__CUSTKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS2NATION1_it192 = qORDERS2NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS2NATION1_end191 
        = qORDERS2NATION1.end();
    for (
        ; qORDERS2NATION1_it192 != qORDERS2NATION1_end191; ++qORDERS2NATION1_it192)
    {
        int64_t x_qORDERS2NATION_N__NATIONKEY = get<1>(
            qORDERS2NATION1_it192->first);
        int64_t x_qORDERS_O__CUSTKEY = get<2>(qORDERS2NATION1_it192->first);
        qORDERS2NATION1[make_tuple(
            ORDERKEY,x_qORDERS2NATION_N__NATIONKEY,x_qORDERS_O__CUSTKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qORDERS1LINEITEM1NATION1[make_tuple(
            SUPPKEY,x_qORDERS2NATION_N__NATIONKEY,x_qORDERS_O__CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS2REGION1_it194 = qORDERS2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS2REGION1_end193 = qORDERS2REGION1.end();
    for (
        ; qORDERS2REGION1_it194 != qORDERS2REGION1_end193; ++qORDERS2REGION1_it194)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2REGION1_it194->first);
        string N__NAME = get<2>(qORDERS2REGION1_it194->first);
        int64_t x_qORDERS2REGION_R__REGIONKEY = get<3>(
            qORDERS2REGION1_it194->first);
        qORDERS2REGION1[make_tuple(
            ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME,x_qORDERS2REGION_R__REGIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qORDERS1LINEITEM1REGION1[make_tuple(
            SUPPKEY,x_qORDERS_O__CUSTKEY,N__NAME,x_qORDERS2REGION_R__REGIONKEY)];
    }
    map<tuple<string,int64_t>,double>::iterator qREGION1_it196 = qREGION1.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION1_end195 = qREGION1.end(
        );
    for (; qREGION1_it196 != qREGION1_end195; ++qREGION1_it196)
    {
        string N__NAME = get<0>(qREGION1_it196->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it196->first);
        qREGION1[make_tuple(
            N__NAME,x_qREGION_R__REGIONKEY)] += EXTENDEDPRICE*qLINEITEM1REGION1[make_tuple(
            ORDERKEY,SUPPKEY,N__NAME,x_qREGION_R__REGIONKEY)];
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it198 = qREGION2.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION2_end197 = qREGION2.end(
        );
    for (; qREGION2_it198 != qREGION2_end197; ++qREGION2_it198)
    {
        string N__NAME = get<0>(qREGION2_it198->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it198->first);
        qREGION2[make_tuple(
            N__NAME,x_qREGION_R__REGIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1REGION1[make_tuple(
            ORDERKEY,SUPPKEY,N__NAME,x_qREGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it200 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end199 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it200 != qSUPPLIER1_end199; ++qSUPPLIER1_it200)
    {
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER1_it200->first);
        string N__NAME = get<2>(qSUPPLIER1_it200->first);
        qSUPPLIER1[make_tuple(
            SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
            EXTENDEDPRICE*qLINEITEM1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER1NATION1_it202 = 
        qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER1NATION1_end201 = 
        qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it202 != qSUPPLIER1NATION1_end201; 
        ++qSUPPLIER1NATION1_it202)
    {
        int64_t x_qSUPPLIER1NATION_N__NATIONKEY = get<1>(
            qSUPPLIER1NATION1_it202->first);
        qSUPPLIER1NATION1[make_tuple(
            SUPPKEY,x_qSUPPLIER1NATION_N__NATIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1SUPPLIER1NATION1[make_tuple(
            ORDERKEY,x_qSUPPLIER1NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER1REGION1_it204 = qSUPPLIER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER1REGION1_end203 = qSUPPLIER1REGION1.end();
    for (
        ; qSUPPLIER1REGION1_it204 != qSUPPLIER1REGION1_end203; 
        ++qSUPPLIER1REGION1_it204)
    {
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(
            qSUPPLIER1REGION1_it204->first);
        string N__NAME = get<2>(qSUPPLIER1REGION1_it204->first);
        int64_t x_qSUPPLIER1REGION_R__REGIONKEY = get<3>(
            qSUPPLIER1REGION1_it204->first);
        qSUPPLIER1REGION1[make_tuple(
            SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,x_qSUPPLIER1REGION_R__REGIONKEY)] += 
            EXTENDEDPRICE*qLINEITEM1SUPPLIER1REGION1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,x_qSUPPLIER1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it206 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end205 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it206 != qSUPPLIER2_end205; ++qSUPPLIER2_it206)
    {
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER2_it206->first);
        string N__NAME = get<2>(qSUPPLIER2_it206->first);
        qSUPPLIER2[make_tuple(
            SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER2NATION1_it208 = 
        qSUPPLIER2NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER2NATION1_end207 = 
        qSUPPLIER2NATION1.end();
    for (
        ; qSUPPLIER2NATION1_it208 != qSUPPLIER2NATION1_end207; 
        ++qSUPPLIER2NATION1_it208)
    {
        int64_t x_qSUPPLIER2NATION_N__NATIONKEY = get<1>(
            qSUPPLIER2NATION1_it208->first);
        qSUPPLIER2NATION1[make_tuple(
            SUPPKEY,x_qSUPPLIER2NATION_N__NATIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1SUPPLIER1NATION1[make_tuple(
            ORDERKEY,x_qSUPPLIER2NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER2REGION1_it210 = qSUPPLIER2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER2REGION1_end209 = qSUPPLIER2REGION1.end();
    for (
        ; qSUPPLIER2REGION1_it210 != qSUPPLIER2REGION1_end209; 
        ++qSUPPLIER2REGION1_it210)
    {
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(
            qSUPPLIER2REGION1_it210->first);
        string N__NAME = get<2>(qSUPPLIER2REGION1_it210->first);
        int64_t x_qSUPPLIER2REGION_R__REGIONKEY = get<3>(
            qSUPPLIER2REGION1_it210->first);
        qSUPPLIER2REGION1[make_tuple(
            SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,x_qSUPPLIER2REGION_R__REGIONKEY)] += 
            EXTENDEDPRICE*DISCOUNT*qLINEITEM1SUPPLIER1REGION1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,x_qSUPPLIER2REGION_R__REGIONKEY)];
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_LINEITEM_sec_span, on_insert_LINEITEM_usec_span);
}

void on_insert_SUPPLIER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t SUPPKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<string,double>::iterator q_it212 = q.begin();
    map<string,double>::iterator q_end211 = q.end();
    for (; q_it212 != q_end211; ++q_it212)
    {
        string N__NAME = q_it212->first;
        q[N__NAME] += qSUPPLIER1[make_tuple(SUPPKEY,NATIONKEY,N__NAME)];
    }
    map<string,double>::iterator q_it214 = q.begin();
    map<string,double>::iterator q_end213 = q.end();
    for (; q_it214 != q_end213; ++q_it214)
    {
        string N__NAME = q_it214->first;
        q[N__NAME] += qSUPPLIER2[make_tuple(SUPPKEY,NATIONKEY,N__NAME)]*-1;
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it216 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end215 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it216 != qCUSTOMER1_end215; ++qCUSTOMER1_it216)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it216->first);
        string N__NAME = get<2>(qCUSTOMER1_it216->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,N__NAME)] += qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,string>,
        int>::iterator qCUSTOMER1LINEITEM2_it218 = qCUSTOMER1LINEITEM2.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qCUSTOMER1LINEITEM2_end217 
        = qCUSTOMER1LINEITEM2.end();
    for (
        ; qCUSTOMER1LINEITEM2_it218 != qCUSTOMER1LINEITEM2_end217; 
        ++qCUSTOMER1LINEITEM2_it218)
    {
        string N__NAME = get<2>(qCUSTOMER1LINEITEM2_it218->first);
        qCUSTOMER1LINEITEM2[make_tuple(
            SUPPKEY,NATIONKEY,N__NAME)] += qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1NATION1_it220 = 
        qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1NATION1_end219 = 
        qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it220 != qCUSTOMER1NATION1_end219; 
        ++qCUSTOMER1NATION1_it220)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1NATION1_it220->first);
        qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY)] += qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1ORDERS1_it222 
        = qCUSTOMER1ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1ORDERS1_end221 = qCUSTOMER1ORDERS1.end();
    for (
        ; qCUSTOMER1ORDERS1_it222 != qCUSTOMER1ORDERS1_end221; 
        ++qCUSTOMER1ORDERS1_it222)
    {
        int64_t x_qCUSTOMER1ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER1ORDERS1_it222->first);
        string N__NAME = get<2>(qCUSTOMER1ORDERS1_it222->first);
        qCUSTOMER1ORDERS1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,NATIONKEY,N__NAME)] += 
            qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,SUPPKEY)]*qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1ORDERS1NATION1_it224 
        = qCUSTOMER1ORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t>,
        double>::iterator qCUSTOMER1ORDERS1NATION1_end223 = 
        qCUSTOMER1ORDERS1NATION1.end();
    for (
        ; qCUSTOMER1ORDERS1NATION1_it224 != qCUSTOMER1ORDERS1NATION1_end223; 
        ++qCUSTOMER1ORDERS1NATION1_it224)
    {
        int64_t x_qCUSTOMER1ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER1ORDERS1NATION1_it224->first);
        qCUSTOMER1ORDERS1NATION1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,NATIONKEY)] += 
            qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(x_qCUSTOMER1ORDERS_O__ORDERKEY,SUPPKEY)];
    }
    qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(SUPPKEY,NATIONKEY)] += 1;
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1ORDERS1REGION1_it226 = qCUSTOMER1ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1ORDERS1REGION1_end225 = qCUSTOMER1ORDERS1REGION1.end();
    for (
        ; qCUSTOMER1ORDERS1REGION1_it226 != qCUSTOMER1ORDERS1REGION1_end225; 
        ++qCUSTOMER1ORDERS1REGION1_it226)
    {
        int64_t x_qCUSTOMER1ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER1ORDERS1REGION1_it226->first);
        string N__NAME = get<2>(qCUSTOMER1ORDERS1REGION1_it226->first);
        int64_t x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY = get<3>(
            qCUSTOMER1ORDERS1REGION1_it226->first);
        qCUSTOMER1ORDERS1REGION1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,NATIONKEY,N__NAME,
            x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY)] += 
            qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,
            SUPPKEY)]*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qCUSTOMER1ORDERS1REGION1LINEITEM1_it228 = 
        qCUSTOMER1ORDERS1REGION1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qCUSTOMER1ORDERS1REGION1LINEITEM1_end227 = 
        qCUSTOMER1ORDERS1REGION1LINEITEM1.end();
    for (
        ; qCUSTOMER1ORDERS1REGION1LINEITEM1_it228 != 
        qCUSTOMER1ORDERS1REGION1LINEITEM1_end227; 
        ++qCUSTOMER1ORDERS1REGION1LINEITEM1_it228)
    {
        string N__NAME = get<2>(qCUSTOMER1ORDERS1REGION1LINEITEM1_it228->first);
        int64_t x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY = get<3>(
            qCUSTOMER1ORDERS1REGION1LINEITEM1_it228->first);
        qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            SUPPKEY,NATIONKEY,N__NAME,x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY)] += 
            qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1REGION1_it230 = qCUSTOMER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1REGION1_end229 = qCUSTOMER1REGION1.end();
    for (
        ; qCUSTOMER1REGION1_it230 != qCUSTOMER1REGION1_end229; 
        ++qCUSTOMER1REGION1_it230)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1REGION1_it230->first);
        string N__NAME = get<2>(qCUSTOMER1REGION1_it230->first);
        int64_t x_qCUSTOMER1REGION_R__REGIONKEY = get<3>(
            qCUSTOMER1REGION1_it230->first);
        qCUSTOMER1REGION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,N__NAME,x_qCUSTOMER1REGION_R__REGIONKEY)] += 
            qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qCUSTOMER1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it232 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end231 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it232 != qCUSTOMER2_end231; ++qCUSTOMER2_it232)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it232->first);
        string N__NAME = get<2>(qCUSTOMER2_it232->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,N__NAME)] += qCUSTOMER2SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2NATION1_it234 = 
        qCUSTOMER2NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2NATION1_end233 = 
        qCUSTOMER2NATION1.end();
    for (
        ; qCUSTOMER2NATION1_it234 != qCUSTOMER2NATION1_end233; 
        ++qCUSTOMER2NATION1_it234)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2NATION1_it234->first);
        qCUSTOMER2NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY)] += qCUSTOMER2SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2ORDERS1_it236 
        = qCUSTOMER2ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER2ORDERS1_end235 = qCUSTOMER2ORDERS1.end();
    for (
        ; qCUSTOMER2ORDERS1_it236 != qCUSTOMER2ORDERS1_end235; 
        ++qCUSTOMER2ORDERS1_it236)
    {
        int64_t x_qCUSTOMER2ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER2ORDERS1_it236->first);
        string N__NAME = get<2>(qCUSTOMER2ORDERS1_it236->first);
        qCUSTOMER2ORDERS1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,NATIONKEY,N__NAME)] += 
            qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,SUPPKEY)]*qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2ORDERS1NATION1_it238 
        = qCUSTOMER2ORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t>,
        double>::iterator qCUSTOMER2ORDERS1NATION1_end237 = 
        qCUSTOMER2ORDERS1NATION1.end();
    for (
        ; qCUSTOMER2ORDERS1NATION1_it238 != qCUSTOMER2ORDERS1NATION1_end237; 
        ++qCUSTOMER2ORDERS1NATION1_it238)
    {
        int64_t x_qCUSTOMER2ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER2ORDERS1NATION1_it238->first);
        qCUSTOMER2ORDERS1NATION1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,NATIONKEY)] += 
            qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(x_qCUSTOMER2ORDERS_O__ORDERKEY,SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2ORDERS1REGION1_it240 = qCUSTOMER2ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2ORDERS1REGION1_end239 = qCUSTOMER2ORDERS1REGION1.end();
    for (
        ; qCUSTOMER2ORDERS1REGION1_it240 != qCUSTOMER2ORDERS1REGION1_end239; 
        ++qCUSTOMER2ORDERS1REGION1_it240)
    {
        int64_t x_qCUSTOMER2ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER2ORDERS1REGION1_it240->first);
        string N__NAME = get<2>(qCUSTOMER2ORDERS1REGION1_it240->first);
        int64_t x_qCUSTOMER2ORDERS1REGION_R__REGIONKEY = get<3>(
            qCUSTOMER2ORDERS1REGION1_it240->first);
        qCUSTOMER2ORDERS1REGION1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,NATIONKEY,N__NAME,
            x_qCUSTOMER2ORDERS1REGION_R__REGIONKEY)] += 
            qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,
            SUPPKEY)]*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qCUSTOMER2ORDERS1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2REGION1_it242 = qCUSTOMER2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2REGION1_end241 = qCUSTOMER2REGION1.end();
    for (
        ; qCUSTOMER2REGION1_it242 != qCUSTOMER2REGION1_end241; 
        ++qCUSTOMER2REGION1_it242)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2REGION1_it242->first);
        string N__NAME = get<2>(qCUSTOMER2REGION1_it242->first);
        int64_t x_qCUSTOMER2REGION_R__REGIONKEY = get<3>(
            qCUSTOMER2REGION1_it242->first);
        qCUSTOMER2REGION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,N__NAME,x_qCUSTOMER2REGION_R__REGIONKEY)] += 
            qCUSTOMER2SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qCUSTOMER2REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it244 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end243 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it244 != qLINEITEM1_end243; ++qLINEITEM1_it244)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it244->first);
        string N__NAME = get<2>(qLINEITEM1_it244->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,SUPPKEY,N__NAME)] += qLINEITEM1SUPPLIER1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_it246 = 
        qLINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        int>::iterator qLINEITEM1NATION1_end245 = qLINEITEM1NATION1.end();
    for (
        ; qLINEITEM1NATION1_it246 != qLINEITEM1NATION1_end245; 
        ++qLINEITEM1NATION1_it246)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1NATION1_it246->first);
        qLINEITEM1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,SUPPKEY)] += 
            qLINEITEM1SUPPLIER1NATION1[make_tuple(x_qLINEITEM_L__ORDERKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1ORDERS1_it248 = 
        qLINEITEM1ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1ORDERS1_end247 = 
        qLINEITEM1ORDERS1.end();
    for (
        ; qLINEITEM1ORDERS1_it248 != qLINEITEM1ORDERS1_end247; 
        ++qLINEITEM1ORDERS1_it248)
    {
        int64_t x_qLINEITEM1ORDERS_O__CUSTKEY = get<0>(
            qLINEITEM1ORDERS1_it248->first);
        string N__NAME = get<2>(qLINEITEM1ORDERS1_it248->first);
        qLINEITEM1ORDERS1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,SUPPKEY,N__NAME)] += qORDERS1SUPPLIER2[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1NATION1_it250 = qLINEITEM1ORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1NATION1_end249 = qLINEITEM1ORDERS1NATION1.end();
    for (
        ; qLINEITEM1ORDERS1NATION1_it250 != qLINEITEM1ORDERS1NATION1_end249; 
        ++qLINEITEM1ORDERS1NATION1_it250)
    {
        int64_t x_qLINEITEM1ORDERS_O__CUSTKEY = get<0>(
            qLINEITEM1ORDERS1NATION1_it250->first);
        qLINEITEM1ORDERS1NATION1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,NATIONKEY,SUPPKEY)] += 
            qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1REGION1_it252 = qLINEITEM1ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1REGION1_end251 = qLINEITEM1ORDERS1REGION1.end();
    for (
        ; qLINEITEM1ORDERS1REGION1_it252 != qLINEITEM1ORDERS1REGION1_end251; 
        ++qLINEITEM1ORDERS1REGION1_it252)
    {
        int64_t x_qLINEITEM1ORDERS_O__CUSTKEY = get<0>(
            qLINEITEM1ORDERS1REGION1_it252->first);
        string N__NAME = get<2>(qLINEITEM1ORDERS1REGION1_it252->first);
        int64_t x_qLINEITEM1ORDERS1REGION_R__REGIONKEY = get<3>(
            qLINEITEM1ORDERS1REGION1_it252->first);
        qLINEITEM1ORDERS1REGION1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,SUPPKEY,N__NAME,
            x_qLINEITEM1ORDERS1REGION_R__REGIONKEY)] += 
            qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,NATIONKEY,N__NAME,
            x_qLINEITEM1ORDERS1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1REGION1_it254 = qLINEITEM1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1REGION1_end253 = qLINEITEM1REGION1.end();
    for (
        ; qLINEITEM1REGION1_it254 != qLINEITEM1REGION1_end253; 
        ++qLINEITEM1REGION1_it254)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1REGION1_it254->first);
        string N__NAME = get<2>(qLINEITEM1REGION1_it254->first);
        int64_t x_qLINEITEM1REGION_R__REGIONKEY = get<3>(
            qLINEITEM1REGION1_it254->first);
        qLINEITEM1REGION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,SUPPKEY,N__NAME,x_qLINEITEM1REGION_R__REGIONKEY)] += 
            qLINEITEM1SUPPLIER1REGION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,N__NAME,x_qLINEITEM1REGION_R__REGIONKEY)];
    }
    qNATION1[NATIONKEY] += qSUPPLIER1NATION1[make_tuple(SUPPKEY,NATIONKEY)];
    qNATION3[NATIONKEY] += qSUPPLIER2NATION1[make_tuple(SUPPKEY,NATIONKEY)];
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it256 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end255 = 
        qORDERS1.end();
    for (; qORDERS1_it256 != qORDERS1_end255; ++qORDERS1_it256)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it256->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it256->first);
        string N__NAME = get<2>(qORDERS1_it256->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
            qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY)]*qORDERS1SUPPLIER2[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1LINEITEM1_it258 = 
        qORDERS1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1LINEITEM1_end257 = 
        qORDERS1LINEITEM1.end();
    for (
        ; qORDERS1LINEITEM1_it258 != qORDERS1LINEITEM1_end257; 
        ++qORDERS1LINEITEM1_it258)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1LINEITEM1_it258->first);
        string N__NAME = get<2>(qORDERS1LINEITEM1_it258->first);
        qORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += qORDERS1SUPPLIER2[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qORDERS1LINEITEM1NATION1_it260 = qORDERS1LINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qORDERS1LINEITEM1NATION1_end259 = qORDERS1LINEITEM1NATION1.end();
    for (
        ; qORDERS1LINEITEM1NATION1_it260 != qORDERS1LINEITEM1NATION1_end259; 
        ++qORDERS1LINEITEM1NATION1_it260)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<2>(
            qORDERS1LINEITEM1NATION1_it260->first);
        qORDERS1LINEITEM1NATION1[make_tuple(
            SUPPKEY,NATIONKEY,x_qORDERS_O__CUSTKEY)] += 
            qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(x_qORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1_it262 = qORDERS1LINEITEM1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1_end261 = qORDERS1LINEITEM1REGION1.end();
    for (
        ; qORDERS1LINEITEM1REGION1_it262 != qORDERS1LINEITEM1REGION1_end261; 
        ++qORDERS1LINEITEM1REGION1_it262)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<1>(
            qORDERS1LINEITEM1REGION1_it262->first);
        string N__NAME = get<2>(qORDERS1LINEITEM1REGION1_it262->first);
        int64_t x_qORDERS1LINEITEM1REGION_R__REGIONKEY = get<3>(
            qORDERS1LINEITEM1REGION1_it262->first);
        qORDERS1LINEITEM1REGION1[make_tuple(
            SUPPKEY,x_qORDERS_O__CUSTKEY,N__NAME,
            x_qORDERS1LINEITEM1REGION_R__REGIONKEY)] += 
            qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,N__NAME,x_qORDERS1LINEITEM1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS1NATION1_it264 = qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1NATION1_end263 
        = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it264 != qORDERS1NATION1_end263; ++qORDERS1NATION1_it264)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1NATION1_it264->first);
        int64_t x_qORDERS_O__CUSTKEY = get<2>(qORDERS1NATION1_it264->first);
        qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,x_qORDERS_O__CUSTKEY)] += 
            qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY)]*qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS1REGION1_it266 = qORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS1REGION1_end265 = qORDERS1REGION1.end();
    for (
        ; qORDERS1REGION1_it266 != qORDERS1REGION1_end265; ++qORDERS1REGION1_it266)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1REGION1_it266->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1REGION1_it266->first);
        string N__NAME = get<2>(qORDERS1REGION1_it266->first);
        int64_t x_qORDERS1REGION_R__REGIONKEY = get<3>(
            qORDERS1REGION1_it266->first);
        qORDERS1REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME,
            x_qORDERS1REGION_R__REGIONKEY)] += qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY)]*qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,N__NAME,x_qORDERS1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it268 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end267 = 
        qORDERS2.end();
    for (; qORDERS2_it268 != qORDERS2_end267; ++qORDERS2_it268)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it268->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it268->first);
        string N__NAME = get<2>(qORDERS2_it268->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
            qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY)]*qORDERS1SUPPLIER2[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS2NATION1_it270 = qORDERS2NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS2NATION1_end269 
        = qORDERS2NATION1.end();
    for (
        ; qORDERS2NATION1_it270 != qORDERS2NATION1_end269; ++qORDERS2NATION1_it270)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2NATION1_it270->first);
        int64_t x_qORDERS_O__CUSTKEY = get<2>(qORDERS2NATION1_it270->first);
        qORDERS2NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,x_qORDERS_O__CUSTKEY)] += 
            qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY)]*qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS2REGION1_it272 = qORDERS2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS2REGION1_end271 = qORDERS2REGION1.end();
    for (
        ; qORDERS2REGION1_it272 != qORDERS2REGION1_end271; ++qORDERS2REGION1_it272)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2REGION1_it272->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2REGION1_it272->first);
        string N__NAME = get<2>(qORDERS2REGION1_it272->first);
        int64_t x_qORDERS2REGION_R__REGIONKEY = get<3>(
            qORDERS2REGION1_it272->first);
        qORDERS2REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME,
            x_qORDERS2REGION_R__REGIONKEY)] += qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY)]*qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,N__NAME,x_qORDERS2REGION_R__REGIONKEY)];
    }
    map<tuple<string,int64_t>,double>::iterator qREGION1_it274 = qREGION1.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION1_end273 = qREGION1.end(
        );
    for (; qREGION1_it274 != qREGION1_end273; ++qREGION1_it274)
    {
        string N__NAME = get<0>(qREGION1_it274->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it274->first);
        qREGION1[make_tuple(
            N__NAME,x_qREGION_R__REGIONKEY)] += qSUPPLIER1REGION1[make_tuple(
            SUPPKEY,NATIONKEY,N__NAME,x_qREGION_R__REGIONKEY)];
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it276 = qREGION2.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION2_end275 = qREGION2.end(
        );
    for (; qREGION2_it276 != qREGION2_end275; ++qREGION2_it276)
    {
        string N__NAME = get<0>(qREGION2_it276->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it276->first);
        qREGION2[make_tuple(
            N__NAME,x_qREGION_R__REGIONKEY)] += qSUPPLIER2REGION1[make_tuple(
            SUPPKEY,NATIONKEY,N__NAME,x_qREGION_R__REGIONKEY)];
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_insert_SUPPLIER_sec_span, on_insert_SUPPLIER_usec_span);
}

void on_insert_NATION(
    ofstream* results, ofstream* log, ofstream* stats, int64_t NATIONKEY,string 
    NAME,int64_t REGIONKEY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    q[NAME] += qNATION1[NATIONKEY]*qNATION2[REGIONKEY];
    q[NAME] += qNATION3[NATIONKEY]*qNATION2[REGIONKEY]*-1;
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it278 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end277 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it278 != qCUSTOMER1_end277; ++qCUSTOMER1_it278)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it278->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,NAME)] += qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string>,
        int>::iterator qCUSTOMER1LINEITEM2_it280 = qCUSTOMER1LINEITEM2.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qCUSTOMER1LINEITEM2_end279 
        = qCUSTOMER1LINEITEM2.end();
    for (
        ; qCUSTOMER1LINEITEM2_it280 != qCUSTOMER1LINEITEM2_end279; 
        ++qCUSTOMER1LINEITEM2_it280)
    {
        int64_t x_qCUSTOMER1LINEITEM_L__SUPPKEY = get<0>(
            qCUSTOMER1LINEITEM2_it280->first);
        qCUSTOMER1LINEITEM2[make_tuple(
            x_qCUSTOMER1LINEITEM_L__SUPPKEY,NATIONKEY,NAME)] += 
            qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            x_qCUSTOMER1LINEITEM_L__SUPPKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1ORDERS1_it282 
        = qCUSTOMER1ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1ORDERS1_end281 = qCUSTOMER1ORDERS1.end();
    for (
        ; qCUSTOMER1ORDERS1_it282 != qCUSTOMER1ORDERS1_end281; 
        ++qCUSTOMER1ORDERS1_it282)
    {
        int64_t x_qCUSTOMER1ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER1ORDERS1_it282->first);
        qCUSTOMER1ORDERS1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,NATIONKEY,NAME)] += 
            qCUSTOMER1ORDERS1NATION1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1ORDERS1REGION1_it284 = qCUSTOMER1ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1ORDERS1REGION1_end283 = qCUSTOMER1ORDERS1REGION1.end();
    for (
        ; qCUSTOMER1ORDERS1REGION1_it284 != qCUSTOMER1ORDERS1REGION1_end283; 
        ++qCUSTOMER1ORDERS1REGION1_it284)
    {
        int64_t x_qCUSTOMER1ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER1ORDERS1REGION1_it284->first);
        qCUSTOMER1ORDERS1REGION1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,NATIONKEY,NAME,REGIONKEY)] += 
            qCUSTOMER1ORDERS1NATION1[make_tuple(x_qCUSTOMER1ORDERS_O__ORDERKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qCUSTOMER1ORDERS1REGION1LINEITEM1_it286 = 
        qCUSTOMER1ORDERS1REGION1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qCUSTOMER1ORDERS1REGION1LINEITEM1_end285 = 
        qCUSTOMER1ORDERS1REGION1LINEITEM1.end();
    for (
        ; qCUSTOMER1ORDERS1REGION1LINEITEM1_it286 != 
        qCUSTOMER1ORDERS1REGION1LINEITEM1_end285; 
        ++qCUSTOMER1ORDERS1REGION1LINEITEM1_it286)
    {
        int64_t x_qCUSTOMER1ORDERS1REGION1LINEITEM_L__SUPPKEY = get<0>(
            qCUSTOMER1ORDERS1REGION1LINEITEM1_it286->first);
        qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            x_qCUSTOMER1ORDERS1REGION1LINEITEM_L__SUPPKEY,NATIONKEY,NAME,REGIONKEY)] += 
            qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            x_qCUSTOMER1ORDERS1REGION1LINEITEM_L__SUPPKEY,NATIONKEY)];
    }
    qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
        NATIONKEY,NAME,REGIONKEY)] += 1;
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1REGION1_it288 = qCUSTOMER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1REGION1_end287 = qCUSTOMER1REGION1.end();
    for (
        ; qCUSTOMER1REGION1_it288 != qCUSTOMER1REGION1_end287; 
        ++qCUSTOMER1REGION1_it288)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1REGION1_it288->first);
        qCUSTOMER1REGION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,NAME,REGIONKEY)] += 
            qCUSTOMER1NATION1[make_tuple(x_qCUSTOMER_C__CUSTKEY,NATIONKEY)];
    }
    qCUSTOMER1SUPPLIER2[make_tuple(NATIONKEY,NAME)] += qNATION2[REGIONKEY];
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it290 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end289 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it290 != qCUSTOMER2_end289; ++qCUSTOMER2_it290)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it290->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,NAME)] += qCUSTOMER2NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2ORDERS1_it292 
        = qCUSTOMER2ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER2ORDERS1_end291 = qCUSTOMER2ORDERS1.end();
    for (
        ; qCUSTOMER2ORDERS1_it292 != qCUSTOMER2ORDERS1_end291; 
        ++qCUSTOMER2ORDERS1_it292)
    {
        int64_t x_qCUSTOMER2ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER2ORDERS1_it292->first);
        qCUSTOMER2ORDERS1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,NATIONKEY,NAME)] += 
            qCUSTOMER2ORDERS1NATION1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2ORDERS1REGION1_it294 = qCUSTOMER2ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2ORDERS1REGION1_end293 = qCUSTOMER2ORDERS1REGION1.end();
    for (
        ; qCUSTOMER2ORDERS1REGION1_it294 != qCUSTOMER2ORDERS1REGION1_end293; 
        ++qCUSTOMER2ORDERS1REGION1_it294)
    {
        int64_t x_qCUSTOMER2ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER2ORDERS1REGION1_it294->first);
        qCUSTOMER2ORDERS1REGION1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,NATIONKEY,NAME,REGIONKEY)] += 
            qCUSTOMER2ORDERS1NATION1[make_tuple(x_qCUSTOMER2ORDERS_O__ORDERKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2REGION1_it296 = qCUSTOMER2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2REGION1_end295 = qCUSTOMER2REGION1.end();
    for (
        ; qCUSTOMER2REGION1_it296 != qCUSTOMER2REGION1_end295; 
        ++qCUSTOMER2REGION1_it296)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2REGION1_it296->first);
        qCUSTOMER2REGION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,NAME,REGIONKEY)] += 
            qCUSTOMER2NATION1[make_tuple(x_qCUSTOMER_C__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it298 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end297 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it298 != qLINEITEM1_end297; ++qLINEITEM1_it298)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it298->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it298->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,NAME)] += 
            qLINEITEM1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,x_qLINEITEM_L__SUPPKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1ORDERS1_it300 = 
        qLINEITEM1ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1ORDERS1_end299 = 
        qLINEITEM1ORDERS1.end();
    for (
        ; qLINEITEM1ORDERS1_it300 != qLINEITEM1ORDERS1_end299; 
        ++qLINEITEM1ORDERS1_it300)
    {
        int64_t x_qLINEITEM1ORDERS_O__CUSTKEY = get<0>(
            qLINEITEM1ORDERS1_it300->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1ORDERS1_it300->first);
        qLINEITEM1ORDERS1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,x_qLINEITEM_L__SUPPKEY,NAME)] += 
            qLINEITEM1ORDERS1NATION1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,NATIONKEY,
            x_qLINEITEM_L__SUPPKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1REGION1_it302 = qLINEITEM1ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1REGION1_end301 = qLINEITEM1ORDERS1REGION1.end();
    for (
        ; qLINEITEM1ORDERS1REGION1_it302 != qLINEITEM1ORDERS1REGION1_end301; 
        ++qLINEITEM1ORDERS1REGION1_it302)
    {
        int64_t x_qLINEITEM1ORDERS_O__CUSTKEY = get<0>(
            qLINEITEM1ORDERS1REGION1_it302->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(
            qLINEITEM1ORDERS1REGION1_it302->first);
        qLINEITEM1ORDERS1REGION1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,x_qLINEITEM_L__SUPPKEY,NAME,REGIONKEY)] += 
            qLINEITEM1ORDERS1NATION1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,NATIONKEY,x_qLINEITEM_L__SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1REGION1_it304 = qLINEITEM1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1REGION1_end303 = qLINEITEM1REGION1.end();
    for (
        ; qLINEITEM1REGION1_it304 != qLINEITEM1REGION1_end303; 
        ++qLINEITEM1REGION1_it304)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1REGION1_it304->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1REGION1_it304->first);
        qLINEITEM1REGION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,NAME,REGIONKEY)] += 
            qLINEITEM1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,x_qLINEITEM_L__SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,string>,
        int>::iterator qLINEITEM1SUPPLIER1_it306 = qLINEITEM1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1SUPPLIER1_end305 
        = qLINEITEM1SUPPLIER1.end();
    for (
        ; qLINEITEM1SUPPLIER1_it306 != qLINEITEM1SUPPLIER1_end305; 
        ++qLINEITEM1SUPPLIER1_it306)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1SUPPLIER1_it306->first);
        qLINEITEM1SUPPLIER1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,NAME)] += 
            qLINEITEM1SUPPLIER1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1SUPPLIER1REGION1_it308 = qLINEITEM1SUPPLIER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1SUPPLIER1REGION1_end307 = qLINEITEM1SUPPLIER1REGION1.end();
    for (
        ; qLINEITEM1SUPPLIER1REGION1_it308 != qLINEITEM1SUPPLIER1REGION1_end307; 
        ++qLINEITEM1SUPPLIER1REGION1_it308)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1SUPPLIER1REGION1_it308->first);
        qLINEITEM1SUPPLIER1REGION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,NAME,REGIONKEY)] += 
            qLINEITEM1SUPPLIER1NATION1[make_tuple(x_qLINEITEM_L__ORDERKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it310 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end309 = 
        qORDERS1.end();
    for (; qORDERS1_it310 != qORDERS1_end309; ++qORDERS1_it310)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it310->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it310->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,NAME)] += qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,x_qORDERS_O__CUSTKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1LINEITEM1_it312 = 
        qORDERS1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1LINEITEM1_end311 = 
        qORDERS1LINEITEM1.end();
    for (
        ; qORDERS1LINEITEM1_it312 != qORDERS1LINEITEM1_end311; 
        ++qORDERS1LINEITEM1_it312)
    {
        int64_t x_qORDERS1LINEITEM_L__SUPPKEY = get<0>(
            qORDERS1LINEITEM1_it312->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1LINEITEM1_it312->first);
        qORDERS1LINEITEM1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,x_qORDERS_O__CUSTKEY,NAME)] += 
            qORDERS1LINEITEM1NATION1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,NATIONKEY,
            x_qORDERS_O__CUSTKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1_it314 = qORDERS1LINEITEM1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1_end313 = qORDERS1LINEITEM1REGION1.end();
    for (
        ; qORDERS1LINEITEM1REGION1_it314 != qORDERS1LINEITEM1REGION1_end313; 
        ++qORDERS1LINEITEM1REGION1_it314)
    {
        int64_t x_qORDERS1LINEITEM_L__SUPPKEY = get<0>(
            qORDERS1LINEITEM1REGION1_it314->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(
            qORDERS1LINEITEM1REGION1_it314->first);
        qORDERS1LINEITEM1REGION1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,x_qORDERS_O__CUSTKEY,NAME,REGIONKEY)] += 
            qORDERS1LINEITEM1NATION1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,NATIONKEY,x_qORDERS_O__CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1SUPPLIER1_it316 = 
        qORDERS1LINEITEM1REGION1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1SUPPLIER1_end315 = 
        qORDERS1LINEITEM1REGION1SUPPLIER1.end();
    for (
        ; qORDERS1LINEITEM1REGION1SUPPLIER1_it316 != 
        qORDERS1LINEITEM1REGION1SUPPLIER1_end315; 
        ++qORDERS1LINEITEM1REGION1SUPPLIER1_it316)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<0>(
            qORDERS1LINEITEM1REGION1SUPPLIER1_it316->first);
        qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,NAME,REGIONKEY)] += 
            qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(x_qORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS1REGION1_it318 = qORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS1REGION1_end317 = qORDERS1REGION1.end();
    for (
        ; qORDERS1REGION1_it318 != qORDERS1REGION1_end317; ++qORDERS1REGION1_it318)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1REGION1_it318->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1REGION1_it318->first);
        qORDERS1REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,NAME,REGIONKEY)] += 
            qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,x_qORDERS_O__CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1SUPPLIER2_it320 = 
        qORDERS1SUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1SUPPLIER2_end319 = 
        qORDERS1SUPPLIER2.end();
    for (
        ; qORDERS1SUPPLIER2_it320 != qORDERS1SUPPLIER2_end319; 
        ++qORDERS1SUPPLIER2_it320)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<0>(qORDERS1SUPPLIER2_it320->first);
        qORDERS1SUPPLIER2[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,NAME)] += 
            qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it322 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end321 = 
        qORDERS2.end();
    for (; qORDERS2_it322 != qORDERS2_end321; ++qORDERS2_it322)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it322->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it322->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,NAME)] += qORDERS2NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,x_qORDERS_O__CUSTKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS2REGION1_it324 = qORDERS2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS2REGION1_end323 = qORDERS2REGION1.end();
    for (
        ; qORDERS2REGION1_it324 != qORDERS2REGION1_end323; ++qORDERS2REGION1_it324)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2REGION1_it324->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2REGION1_it324->first);
        qORDERS2REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,NAME,REGIONKEY)] += 
            qORDERS2NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,x_qORDERS_O__CUSTKEY)];
    }
    qREGION1[make_tuple(NAME,REGIONKEY)] += qNATION1[NATIONKEY];
    qREGION2[make_tuple(NAME,REGIONKEY)] += qNATION3[NATIONKEY];
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it326 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end325 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it326 != qSUPPLIER1_end325; ++qSUPPLIER1_it326)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it326->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY,NAME)] += qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER1REGION1_it328 = qSUPPLIER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER1REGION1_end327 = qSUPPLIER1REGION1.end();
    for (
        ; qSUPPLIER1REGION1_it328 != qSUPPLIER1REGION1_end327; 
        ++qSUPPLIER1REGION1_it328)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1REGION1_it328->first);
        qSUPPLIER1REGION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY,NAME,REGIONKEY)] += 
            qSUPPLIER1NATION1[make_tuple(x_qSUPPLIER_S__SUPPKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it330 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end329 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it330 != qSUPPLIER2_end329; ++qSUPPLIER2_it330)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it330->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY,NAME)] += qSUPPLIER2NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER2REGION1_it332 = qSUPPLIER2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER2REGION1_end331 = qSUPPLIER2REGION1.end();
    for (
        ; qSUPPLIER2REGION1_it332 != qSUPPLIER2REGION1_end331; 
        ++qSUPPLIER2REGION1_it332)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2REGION1_it332->first);
        qSUPPLIER2REGION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY,NAME,REGIONKEY)] += 
            qSUPPLIER2NATION1[make_tuple(x_qSUPPLIER_S__SUPPKEY,NATIONKEY)];
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
    map<string,double>::iterator q_it334 = q.begin();
    map<string,double>::iterator q_end333 = q.end();
    for (; q_it334 != q_end333; ++q_it334)
    {
        string N__NAME = q_it334->first;
        q[N__NAME] += -1*qREGION1[make_tuple(N__NAME,REGIONKEY)];
    }
    map<string,double>::iterator q_it336 = q.begin();
    map<string,double>::iterator q_end335 = q.end();
    for (; q_it336 != q_end335; ++q_it336)
    {
        string N__NAME = q_it336->first;
        q[N__NAME] += -1*qREGION2[make_tuple(N__NAME,REGIONKEY)]*-1;
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it338 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end337 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it338 != qCUSTOMER1_end337; ++qCUSTOMER1_it338)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it338->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER1_it338->first);
        string N__NAME = get<2>(qCUSTOMER1_it338->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            -1*qCUSTOMER1REGION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,
        int>::iterator qCUSTOMER1LINEITEM2_it340 = qCUSTOMER1LINEITEM2.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qCUSTOMER1LINEITEM2_end339 
        = qCUSTOMER1LINEITEM2.end();
    for (
        ; qCUSTOMER1LINEITEM2_it340 != qCUSTOMER1LINEITEM2_end339; 
        ++qCUSTOMER1LINEITEM2_it340)
    {
        int64_t x_qCUSTOMER1LINEITEM_L__SUPPKEY = get<0>(
            qCUSTOMER1LINEITEM2_it340->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER1LINEITEM2_it340->first);
        string N__NAME = get<2>(qCUSTOMER1LINEITEM2_it340->first);
        qCUSTOMER1LINEITEM2[make_tuple(
            x_qCUSTOMER1LINEITEM_L__SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            -1*qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            x_qCUSTOMER1LINEITEM_L__SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1ORDERS1_it342 
        = qCUSTOMER1ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1ORDERS1_end341 = qCUSTOMER1ORDERS1.end();
    for (
        ; qCUSTOMER1ORDERS1_it342 != qCUSTOMER1ORDERS1_end341; 
        ++qCUSTOMER1ORDERS1_it342)
    {
        int64_t x_qCUSTOMER1ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER1ORDERS1_it342->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER1ORDERS1_it342->first);
        string N__NAME = get<2>(qCUSTOMER1ORDERS1_it342->first);
        qCUSTOMER1ORDERS1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            -1*qCUSTOMER1ORDERS1REGION1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,string>,int>::iterator qCUSTOMER1SUPPLIER2_it344 = 
        qCUSTOMER1SUPPLIER2.begin();
    map<tuple<int64_t,string>,int>::iterator qCUSTOMER1SUPPLIER2_end343 = 
        qCUSTOMER1SUPPLIER2.end();
    for (
        ; qCUSTOMER1SUPPLIER2_it344 != qCUSTOMER1SUPPLIER2_end343; 
        ++qCUSTOMER1SUPPLIER2_it344)
    {
        int64_t x_qCUSTOMER1SUPPLIER_S__NATIONKEY = get<0>(
            qCUSTOMER1SUPPLIER2_it344->first);
        string N__NAME = get<1>(qCUSTOMER1SUPPLIER2_it344->first);
        qCUSTOMER1SUPPLIER2[make_tuple(
            x_qCUSTOMER1SUPPLIER_S__NATIONKEY,N__NAME)] += 
            -1*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            x_qCUSTOMER1SUPPLIER_S__NATIONKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it346 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end345 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it346 != qCUSTOMER2_end345; ++qCUSTOMER2_it346)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it346->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER2_it346->first);
        string N__NAME = get<2>(qCUSTOMER2_it346->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            -1*qCUSTOMER2REGION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2ORDERS1_it348 
        = qCUSTOMER2ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER2ORDERS1_end347 = qCUSTOMER2ORDERS1.end();
    for (
        ; qCUSTOMER2ORDERS1_it348 != qCUSTOMER2ORDERS1_end347; 
        ++qCUSTOMER2ORDERS1_it348)
    {
        int64_t x_qCUSTOMER2ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER2ORDERS1_it348->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER2ORDERS1_it348->first);
        string N__NAME = get<2>(qCUSTOMER2ORDERS1_it348->first);
        qCUSTOMER2ORDERS1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            -1*qCUSTOMER2ORDERS1REGION1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it350 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end349 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it350 != qLINEITEM1_end349; ++qLINEITEM1_it350)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it350->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it350->first);
        string N__NAME = get<2>(qLINEITEM1_it350->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += 
            -1*qLINEITEM1REGION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1ORDERS1_it352 = 
        qLINEITEM1ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1ORDERS1_end351 = 
        qLINEITEM1ORDERS1.end();
    for (
        ; qLINEITEM1ORDERS1_it352 != qLINEITEM1ORDERS1_end351; 
        ++qLINEITEM1ORDERS1_it352)
    {
        int64_t x_qLINEITEM1ORDERS_O__CUSTKEY = get<0>(
            qLINEITEM1ORDERS1_it352->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1ORDERS1_it352->first);
        string N__NAME = get<2>(qLINEITEM1ORDERS1_it352->first);
        qLINEITEM1ORDERS1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += 
            -1*qLINEITEM1ORDERS1REGION1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,x_qLINEITEM_L__SUPPKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,
        int>::iterator qLINEITEM1SUPPLIER1_it354 = qLINEITEM1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1SUPPLIER1_end353 
        = qLINEITEM1SUPPLIER1.end();
    for (
        ; qLINEITEM1SUPPLIER1_it354 != qLINEITEM1SUPPLIER1_end353; 
        ++qLINEITEM1SUPPLIER1_it354)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1SUPPLIER1_it354->first);
        int64_t x_qLINEITEM1SUPPLIER_S__NATIONKEY = get<1>(
            qLINEITEM1SUPPLIER1_it354->first);
        string N__NAME = get<2>(qLINEITEM1SUPPLIER1_it354->first);
        qLINEITEM1SUPPLIER1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM1SUPPLIER_S__NATIONKEY,N__NAME)] += 
            -1*qLINEITEM1SUPPLIER1REGION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM1SUPPLIER_S__NATIONKEY,N__NAME,REGIONKEY)];
    }
    qNATION2[REGIONKEY] += -1;
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it356 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end355 = 
        qORDERS1.end();
    for (; qORDERS1_it356 != qORDERS1_end355; ++qORDERS1_it356)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it356->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it356->first);
        string N__NAME = get<2>(qORDERS1_it356->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
            -1*qORDERS1REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1LINEITEM1_it358 = 
        qORDERS1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1LINEITEM1_end357 = 
        qORDERS1LINEITEM1.end();
    for (
        ; qORDERS1LINEITEM1_it358 != qORDERS1LINEITEM1_end357; 
        ++qORDERS1LINEITEM1_it358)
    {
        int64_t x_qORDERS1LINEITEM_L__SUPPKEY = get<0>(
            qORDERS1LINEITEM1_it358->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1LINEITEM1_it358->first);
        string N__NAME = get<2>(qORDERS1LINEITEM1_it358->first);
        qORDERS1LINEITEM1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
            -1*qORDERS1LINEITEM1REGION1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,x_qORDERS_O__CUSTKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1SUPPLIER2_it360 = 
        qORDERS1SUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1SUPPLIER2_end359 = 
        qORDERS1SUPPLIER2.end();
    for (
        ; qORDERS1SUPPLIER2_it360 != qORDERS1SUPPLIER2_end359; 
        ++qORDERS1SUPPLIER2_it360)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<0>(qORDERS1SUPPLIER2_it360->first);
        int64_t x_qORDERS1SUPPLIER_S__NATIONKEY = get<1>(
            qORDERS1SUPPLIER2_it360->first);
        string N__NAME = get<2>(qORDERS1SUPPLIER2_it360->first);
        qORDERS1SUPPLIER2[make_tuple(
            x_qORDERS_O__CUSTKEY,x_qORDERS1SUPPLIER_S__NATIONKEY,N__NAME)] += 
            -1*qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,x_qORDERS1SUPPLIER_S__NATIONKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it362 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end361 = 
        qORDERS2.end();
    for (; qORDERS2_it362 != qORDERS2_end361; ++qORDERS2_it362)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it362->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it362->first);
        string N__NAME = get<2>(qORDERS2_it362->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
            -1*qORDERS2REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it364 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end363 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it364 != qSUPPLIER1_end363; ++qSUPPLIER1_it364)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it364->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER1_it364->first);
        string N__NAME = get<2>(qSUPPLIER1_it364->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
            -1*qSUPPLIER1REGION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it366 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end365 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it366 != qSUPPLIER2_end365; ++qSUPPLIER2_it366)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it366->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER2_it366->first);
        string N__NAME = get<2>(qSUPPLIER2_it366->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
            -1*qSUPPLIER2REGION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,REGIONKEY)];
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_REGION_sec_span, on_delete_REGION_usec_span);
}

void on_delete_CUSTOMER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t CUSTKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    MKTSEGMENT,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<string,double>::iterator q_it368 = q.begin();
    map<string,double>::iterator q_end367 = q.end();
    for (; q_it368 != q_end367; ++q_it368)
    {
        string N__NAME = q_it368->first;
        q[N__NAME] += -1*qCUSTOMER1[make_tuple(CUSTKEY,NATIONKEY,N__NAME)];
    }
    map<string,double>::iterator q_it370 = q.begin();
    map<string,double>::iterator q_end369 = q.end();
    for (; q_it370 != q_end369; ++q_it370)
    {
        string N__NAME = q_it370->first;
        q[N__NAME] += -1*qCUSTOMER2[make_tuple(CUSTKEY,NATIONKEY,N__NAME)]*-1;
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it372 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end371 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it372 != qLINEITEM1_end371; ++qLINEITEM1_it372)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it372->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it372->first);
        string N__NAME = get<2>(qLINEITEM1_it372->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += 
            -1*qCUSTOMER1LINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)]*qCUSTOMER1LINEITEM2[make_tuple(
            x_qLINEITEM_L__SUPPKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_it374 = 
        qLINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        int>::iterator qLINEITEM1NATION1_end373 = qLINEITEM1NATION1.end();
    for (
        ; qLINEITEM1NATION1_it374 != qLINEITEM1NATION1_end373; 
        ++qLINEITEM1NATION1_it374)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1NATION1_it374->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<2>(qLINEITEM1NATION1_it374->first);
        qLINEITEM1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,x_qLINEITEM_L__SUPPKEY)] += 
            -1*qCUSTOMER1LINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)]*qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            x_qLINEITEM_L__SUPPKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1ORDERS1_it376 = 
        qLINEITEM1ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1ORDERS1_end375 = 
        qLINEITEM1ORDERS1.end();
    for (
        ; qLINEITEM1ORDERS1_it376 != qLINEITEM1ORDERS1_end375; 
        ++qLINEITEM1ORDERS1_it376)
    {
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1ORDERS1_it376->first);
        string N__NAME = get<2>(qLINEITEM1ORDERS1_it376->first);
        qLINEITEM1ORDERS1[make_tuple(
            CUSTKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += -1*qCUSTOMER1LINEITEM2[make_tuple(
            x_qLINEITEM_L__SUPPKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1NATION1_it378 = qLINEITEM1ORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1NATION1_end377 = qLINEITEM1ORDERS1NATION1.end();
    for (
        ; qLINEITEM1ORDERS1NATION1_it378 != qLINEITEM1ORDERS1NATION1_end377; 
        ++qLINEITEM1ORDERS1NATION1_it378)
    {
        int64_t x_qLINEITEM_L__SUPPKEY = get<2>(
            qLINEITEM1ORDERS1NATION1_it378->first);
        qLINEITEM1ORDERS1NATION1[make_tuple(
            CUSTKEY,NATIONKEY,x_qLINEITEM_L__SUPPKEY)] += 
            -1*qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            x_qLINEITEM_L__SUPPKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1REGION1_it380 = qLINEITEM1ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1REGION1_end379 = qLINEITEM1ORDERS1REGION1.end();
    for (
        ; qLINEITEM1ORDERS1REGION1_it380 != qLINEITEM1ORDERS1REGION1_end379; 
        ++qLINEITEM1ORDERS1REGION1_it380)
    {
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(
            qLINEITEM1ORDERS1REGION1_it380->first);
        string N__NAME = get<2>(qLINEITEM1ORDERS1REGION1_it380->first);
        int64_t x_qLINEITEM1ORDERS1REGION_R__REGIONKEY = get<3>(
            qLINEITEM1ORDERS1REGION1_it380->first);
        qLINEITEM1ORDERS1REGION1[make_tuple(
            CUSTKEY,x_qLINEITEM_L__SUPPKEY,N__NAME,x_qLINEITEM1ORDERS1REGION_R__REGIONKEY)] 
            += -1*qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            x_qLINEITEM_L__SUPPKEY,NATIONKEY,N__NAME,
            x_qLINEITEM1ORDERS1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1REGION1_it382 = qLINEITEM1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1REGION1_end381 = qLINEITEM1REGION1.end();
    for (
        ; qLINEITEM1REGION1_it382 != qLINEITEM1REGION1_end381; 
        ++qLINEITEM1REGION1_it382)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1REGION1_it382->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1REGION1_it382->first);
        string N__NAME = get<2>(qLINEITEM1REGION1_it382->first);
        int64_t x_qLINEITEM1REGION_R__REGIONKEY = get<3>(
            qLINEITEM1REGION1_it382->first);
        qLINEITEM1REGION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME,
            x_qLINEITEM1REGION_R__REGIONKEY)] += -1*qCUSTOMER1LINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)]*qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            x_qLINEITEM_L__SUPPKEY,NATIONKEY,N__NAME,x_qLINEITEM1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,
        int>::iterator qLINEITEM1SUPPLIER1_it384 = qLINEITEM1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1SUPPLIER1_end383 
        = qLINEITEM1SUPPLIER1.end();
    for (
        ; qLINEITEM1SUPPLIER1_it384 != qLINEITEM1SUPPLIER1_end383; 
        ++qLINEITEM1SUPPLIER1_it384)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1SUPPLIER1_it384->first);
        string N__NAME = get<2>(qLINEITEM1SUPPLIER1_it384->first);
        qLINEITEM1SUPPLIER1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,N__NAME)] += 
            -1*qCUSTOMER1LINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)]*qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,
        int>::iterator qLINEITEM1SUPPLIER1NATION1_it386 = 
        qLINEITEM1SUPPLIER1NATION1.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1SUPPLIER1NATION1_end385 
        = qLINEITEM1SUPPLIER1NATION1.end();
    for (
        ; qLINEITEM1SUPPLIER1NATION1_it386 != qLINEITEM1SUPPLIER1NATION1_end385; 
        ++qLINEITEM1SUPPLIER1NATION1_it386)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1SUPPLIER1NATION1_it386->first);
        qLINEITEM1SUPPLIER1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY)] += -1*qCUSTOMER1LINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1SUPPLIER1REGION1_it388 = qLINEITEM1SUPPLIER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1SUPPLIER1REGION1_end387 = qLINEITEM1SUPPLIER1REGION1.end();
    for (
        ; qLINEITEM1SUPPLIER1REGION1_it388 != qLINEITEM1SUPPLIER1REGION1_end387; 
        ++qLINEITEM1SUPPLIER1REGION1_it388)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1SUPPLIER1REGION1_it388->first);
        string N__NAME = get<2>(qLINEITEM1SUPPLIER1REGION1_it388->first);
        int64_t x_qLINEITEM1SUPPLIER1REGION_R__REGIONKEY = get<3>(
            qLINEITEM1SUPPLIER1REGION1_it388->first);
        qLINEITEM1SUPPLIER1REGION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,N__NAME,
            x_qLINEITEM1SUPPLIER1REGION_R__REGIONKEY)] += -1*qCUSTOMER1LINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,CUSTKEY)]*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qLINEITEM1SUPPLIER1REGION_R__REGIONKEY)];
    }
    qNATION1[NATIONKEY] += -1*qCUSTOMER1NATION1[make_tuple(CUSTKEY,NATIONKEY)];
    qNATION3[NATIONKEY] += -1*qCUSTOMER2NATION1[make_tuple(CUSTKEY,NATIONKEY)];
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it390 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end389 = 
        qORDERS1.end();
    for (; qORDERS1_it390 != qORDERS1_end389; ++qORDERS1_it390)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it390->first);
        string N__NAME = get<2>(qORDERS1_it390->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,CUSTKEY,N__NAME)] += -1*qCUSTOMER1ORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1LINEITEM1_it392 = 
        qORDERS1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1LINEITEM1_end391 = 
        qORDERS1LINEITEM1.end();
    for (
        ; qORDERS1LINEITEM1_it392 != qORDERS1LINEITEM1_end391; 
        ++qORDERS1LINEITEM1_it392)
    {
        int64_t x_qORDERS1LINEITEM_L__SUPPKEY = get<0>(
            qORDERS1LINEITEM1_it392->first);
        string N__NAME = get<2>(qORDERS1LINEITEM1_it392->first);
        qORDERS1LINEITEM1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,CUSTKEY,N__NAME)] += 
            -1*qCUSTOMER1LINEITEM2[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qORDERS1LINEITEM1NATION1_it394 = qORDERS1LINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qORDERS1LINEITEM1NATION1_end393 = qORDERS1LINEITEM1NATION1.end();
    for (
        ; qORDERS1LINEITEM1NATION1_it394 != qORDERS1LINEITEM1NATION1_end393; 
        ++qORDERS1LINEITEM1NATION1_it394)
    {
        int64_t x_qORDERS1LINEITEM_L__SUPPKEY = get<0>(
            qORDERS1LINEITEM1NATION1_it394->first);
        qORDERS1LINEITEM1NATION1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,NATIONKEY,CUSTKEY)] += 
            -1*qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,NATIONKEY)];
    }
    qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(CUSTKEY,NATIONKEY)] += -1;
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1_it396 = qORDERS1LINEITEM1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1_end395 = qORDERS1LINEITEM1REGION1.end();
    for (
        ; qORDERS1LINEITEM1REGION1_it396 != qORDERS1LINEITEM1REGION1_end395; 
        ++qORDERS1LINEITEM1REGION1_it396)
    {
        int64_t x_qORDERS1LINEITEM_L__SUPPKEY = get<0>(
            qORDERS1LINEITEM1REGION1_it396->first);
        string N__NAME = get<2>(qORDERS1LINEITEM1REGION1_it396->first);
        int64_t x_qORDERS1LINEITEM1REGION_R__REGIONKEY = get<3>(
            qORDERS1LINEITEM1REGION1_it396->first);
        qORDERS1LINEITEM1REGION1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,CUSTKEY,N__NAME,
            x_qORDERS1LINEITEM1REGION_R__REGIONKEY)] += 
            -1*qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,NATIONKEY,N__NAME,
            x_qORDERS1LINEITEM1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1SUPPLIER1_it398 = 
        qORDERS1LINEITEM1REGION1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1SUPPLIER1_end397 = 
        qORDERS1LINEITEM1REGION1SUPPLIER1.end();
    for (
        ; qORDERS1LINEITEM1REGION1SUPPLIER1_it398 != 
        qORDERS1LINEITEM1REGION1SUPPLIER1_end397; 
        ++qORDERS1LINEITEM1REGION1SUPPLIER1_it398)
    {
        string N__NAME = get<2>(qORDERS1LINEITEM1REGION1SUPPLIER1_it398->first);
        int64_t x_qORDERS1LINEITEM1REGION_R__REGIONKEY = get<3>(
            qORDERS1LINEITEM1REGION1SUPPLIER1_it398->first);
        qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            CUSTKEY,NATIONKEY,N__NAME,x_qORDERS1LINEITEM1REGION_R__REGIONKEY)] += 
            -1*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qORDERS1LINEITEM1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS1NATION1_it400 = qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1NATION1_end399 
        = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it400 != qORDERS1NATION1_end399; ++qORDERS1NATION1_it400)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1NATION1_it400->first);
        qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,CUSTKEY)] += 
            -1*qCUSTOMER1ORDERS1NATION1[make_tuple(x_qORDERS_O__ORDERKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS1REGION1_it402 = qORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS1REGION1_end401 = qORDERS1REGION1.end();
    for (
        ; qORDERS1REGION1_it402 != qORDERS1REGION1_end401; ++qORDERS1REGION1_it402)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1REGION1_it402->first);
        string N__NAME = get<2>(qORDERS1REGION1_it402->first);
        int64_t x_qORDERS1REGION_R__REGIONKEY = get<3>(
            qORDERS1REGION1_it402->first);
        qORDERS1REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,CUSTKEY,N__NAME,x_qORDERS1REGION_R__REGIONKEY)] += 
            -1*qCUSTOMER1ORDERS1REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,N__NAME,x_qORDERS1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1SUPPLIER2_it404 = 
        qORDERS1SUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1SUPPLIER2_end403 = 
        qORDERS1SUPPLIER2.end();
    for (
        ; qORDERS1SUPPLIER2_it404 != qORDERS1SUPPLIER2_end403; 
        ++qORDERS1SUPPLIER2_it404)
    {
        string N__NAME = get<2>(qORDERS1SUPPLIER2_it404->first);
        qORDERS1SUPPLIER2[make_tuple(
            CUSTKEY,NATIONKEY,N__NAME)] += -1*qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it406 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end405 = 
        qORDERS2.end();
    for (; qORDERS2_it406 != qORDERS2_end405; ++qORDERS2_it406)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it406->first);
        string N__NAME = get<2>(qORDERS2_it406->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,CUSTKEY,N__NAME)] += -1*qCUSTOMER2ORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS2NATION1_it408 = qORDERS2NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS2NATION1_end407 
        = qORDERS2NATION1.end();
    for (
        ; qORDERS2NATION1_it408 != qORDERS2NATION1_end407; ++qORDERS2NATION1_it408)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2NATION1_it408->first);
        qORDERS2NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,CUSTKEY)] += 
            -1*qCUSTOMER2ORDERS1NATION1[make_tuple(x_qORDERS_O__ORDERKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS2REGION1_it410 = qORDERS2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS2REGION1_end409 = qORDERS2REGION1.end();
    for (
        ; qORDERS2REGION1_it410 != qORDERS2REGION1_end409; ++qORDERS2REGION1_it410)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2REGION1_it410->first);
        string N__NAME = get<2>(qORDERS2REGION1_it410->first);
        int64_t x_qORDERS2REGION_R__REGIONKEY = get<3>(
            qORDERS2REGION1_it410->first);
        qORDERS2REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,CUSTKEY,N__NAME,x_qORDERS2REGION_R__REGIONKEY)] += 
            -1*qCUSTOMER2ORDERS1REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,N__NAME,x_qORDERS2REGION_R__REGIONKEY)];
    }
    map<tuple<string,int64_t>,double>::iterator qREGION1_it412 = qREGION1.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION1_end411 = qREGION1.end(
        );
    for (; qREGION1_it412 != qREGION1_end411; ++qREGION1_it412)
    {
        string N__NAME = get<0>(qREGION1_it412->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it412->first);
        qREGION1[make_tuple(
            N__NAME,x_qREGION_R__REGIONKEY)] += -1*qCUSTOMER1REGION1[make_tuple(
            CUSTKEY,NATIONKEY,N__NAME,x_qREGION_R__REGIONKEY)];
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it414 = qREGION2.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION2_end413 = qREGION2.end(
        );
    for (; qREGION2_it414 != qREGION2_end413; ++qREGION2_it414)
    {
        string N__NAME = get<0>(qREGION2_it414->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it414->first);
        qREGION2[make_tuple(
            N__NAME,x_qREGION_R__REGIONKEY)] += -1*qCUSTOMER2REGION1[make_tuple(
            CUSTKEY,NATIONKEY,N__NAME,x_qREGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it416 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end415 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it416 != qSUPPLIER1_end415; ++qSUPPLIER1_it416)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it416->first);
        string N__NAME = get<2>(qSUPPLIER1_it416->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY,N__NAME)] += -1*qCUSTOMER1SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY)]*qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER1NATION1_it418 = 
        qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER1NATION1_end417 = 
        qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it418 != qSUPPLIER1NATION1_end417; 
        ++qSUPPLIER1NATION1_it418)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1NATION1_it418->first);
        qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY)] += -1*qCUSTOMER1SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER1REGION1_it420 = qSUPPLIER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER1REGION1_end419 = qSUPPLIER1REGION1.end();
    for (
        ; qSUPPLIER1REGION1_it420 != qSUPPLIER1REGION1_end419; 
        ++qSUPPLIER1REGION1_it420)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1REGION1_it420->first);
        string N__NAME = get<2>(qSUPPLIER1REGION1_it420->first);
        int64_t x_qSUPPLIER1REGION_R__REGIONKEY = get<3>(
            qSUPPLIER1REGION1_it420->first);
        qSUPPLIER1REGION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY,N__NAME,x_qSUPPLIER1REGION_R__REGIONKEY)] += 
            -1*qCUSTOMER1SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY)]*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qSUPPLIER1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it422 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end421 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it422 != qSUPPLIER2_end421; ++qSUPPLIER2_it422)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it422->first);
        string N__NAME = get<2>(qSUPPLIER2_it422->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY,N__NAME)] += -1*qCUSTOMER2SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY)]*qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER2NATION1_it424 = 
        qSUPPLIER2NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER2NATION1_end423 = 
        qSUPPLIER2NATION1.end();
    for (
        ; qSUPPLIER2NATION1_it424 != qSUPPLIER2NATION1_end423; 
        ++qSUPPLIER2NATION1_it424)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2NATION1_it424->first);
        qSUPPLIER2NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY)] += -1*qCUSTOMER2SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER2REGION1_it426 = qSUPPLIER2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER2REGION1_end425 = qSUPPLIER2REGION1.end();
    for (
        ; qSUPPLIER2REGION1_it426 != qSUPPLIER2REGION1_end425; 
        ++qSUPPLIER2REGION1_it426)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2REGION1_it426->first);
        string N__NAME = get<2>(qSUPPLIER2REGION1_it426->first);
        int64_t x_qSUPPLIER2REGION_R__REGIONKEY = get<3>(
            qSUPPLIER2REGION1_it426->first);
        qSUPPLIER2REGION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY,N__NAME,x_qSUPPLIER2REGION_R__REGIONKEY)] += 
            -1*qCUSTOMER2SUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,CUSTKEY)]*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qSUPPLIER2REGION_R__REGIONKEY)];
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
    map<string,double>::iterator q_it428 = q.begin();
    map<string,double>::iterator q_end427 = q.end();
    for (; q_it428 != q_end427; ++q_it428)
    {
        string N__NAME = q_it428->first;
        q[N__NAME] += -1*qORDERS1[make_tuple(ORDERKEY,CUSTKEY,N__NAME)];
    }
    map<string,double>::iterator q_it430 = q.begin();
    map<string,double>::iterator q_end429 = q.end();
    for (; q_it430 != q_end429; ++q_it430)
    {
        string N__NAME = q_it430->first;
        q[N__NAME] += -1*qORDERS2[make_tuple(ORDERKEY,CUSTKEY,N__NAME)]*-1;
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it432 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end431 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it432 != qCUSTOMER1_end431; ++qCUSTOMER1_it432)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER1_it432->first);
        string N__NAME = get<2>(qCUSTOMER1_it432->first);
        qCUSTOMER1[make_tuple(
            CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += -1*qCUSTOMER1ORDERS1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)];
    }
    qCUSTOMER1LINEITEM1[make_tuple(ORDERKEY,CUSTKEY)] += -1;
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1NATION1_it434 = 
        qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1NATION1_end433 = 
        qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it434 != qCUSTOMER1NATION1_end433; 
        ++qCUSTOMER1NATION1_it434)
    {
        int64_t x_qCUSTOMER1NATION_N__NATIONKEY = get<1>(
            qCUSTOMER1NATION1_it434->first);
        qCUSTOMER1NATION1[make_tuple(
            CUSTKEY,x_qCUSTOMER1NATION_N__NATIONKEY)] += 
            -1*qCUSTOMER1ORDERS1NATION1[make_tuple(
            ORDERKEY,x_qCUSTOMER1NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1REGION1_it436 = qCUSTOMER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1REGION1_end435 = qCUSTOMER1REGION1.end();
    for (
        ; qCUSTOMER1REGION1_it436 != qCUSTOMER1REGION1_end435; 
        ++qCUSTOMER1REGION1_it436)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER1REGION1_it436->first);
        string N__NAME = get<2>(qCUSTOMER1REGION1_it436->first);
        int64_t x_qCUSTOMER1REGION_R__REGIONKEY = get<3>(
            qCUSTOMER1REGION1_it436->first);
        qCUSTOMER1REGION1[make_tuple(
            CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,x_qCUSTOMER1REGION_R__REGIONKEY)] += 
            -1*qCUSTOMER1ORDERS1REGION1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,x_qCUSTOMER1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1SUPPLIER1_it438 = 
        qCUSTOMER1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1SUPPLIER1_end437 = 
        qCUSTOMER1SUPPLIER1.end();
    for (
        ; qCUSTOMER1SUPPLIER1_it438 != qCUSTOMER1SUPPLIER1_end437; 
        ++qCUSTOMER1SUPPLIER1_it438)
    {
        int64_t x_qCUSTOMER1SUPPLIER_S__SUPPKEY = get<0>(
            qCUSTOMER1SUPPLIER1_it438->first);
        qCUSTOMER1SUPPLIER1[make_tuple(
            x_qCUSTOMER1SUPPLIER_S__SUPPKEY,CUSTKEY)] += 
            -1*qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qCUSTOMER1SUPPLIER_S__SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it440 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end439 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it440 != qCUSTOMER2_end439; ++qCUSTOMER2_it440)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER2_it440->first);
        string N__NAME = get<2>(qCUSTOMER2_it440->first);
        qCUSTOMER2[make_tuple(
            CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += -1*qCUSTOMER2ORDERS1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2NATION1_it442 = 
        qCUSTOMER2NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2NATION1_end441 = 
        qCUSTOMER2NATION1.end();
    for (
        ; qCUSTOMER2NATION1_it442 != qCUSTOMER2NATION1_end441; 
        ++qCUSTOMER2NATION1_it442)
    {
        int64_t x_qCUSTOMER2NATION_N__NATIONKEY = get<1>(
            qCUSTOMER2NATION1_it442->first);
        qCUSTOMER2NATION1[make_tuple(
            CUSTKEY,x_qCUSTOMER2NATION_N__NATIONKEY)] += 
            -1*qCUSTOMER2ORDERS1NATION1[make_tuple(
            ORDERKEY,x_qCUSTOMER2NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2REGION1_it444 = qCUSTOMER2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2REGION1_end443 = qCUSTOMER2REGION1.end();
    for (
        ; qCUSTOMER2REGION1_it444 != qCUSTOMER2REGION1_end443; 
        ++qCUSTOMER2REGION1_it444)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER2REGION1_it444->first);
        string N__NAME = get<2>(qCUSTOMER2REGION1_it444->first);
        int64_t x_qCUSTOMER2REGION_R__REGIONKEY = get<3>(
            qCUSTOMER2REGION1_it444->first);
        qCUSTOMER2REGION1[make_tuple(
            CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,x_qCUSTOMER2REGION_R__REGIONKEY)] += 
            -1*qCUSTOMER2ORDERS1REGION1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,x_qCUSTOMER2REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2SUPPLIER1_it446 = 
        qCUSTOMER2SUPPLIER1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2SUPPLIER1_end445 = 
        qCUSTOMER2SUPPLIER1.end();
    for (
        ; qCUSTOMER2SUPPLIER1_it446 != qCUSTOMER2SUPPLIER1_end445; 
        ++qCUSTOMER2SUPPLIER1_it446)
    {
        int64_t x_qCUSTOMER2SUPPLIER_S__SUPPKEY = get<0>(
            qCUSTOMER2SUPPLIER1_it446->first);
        qCUSTOMER2SUPPLIER1[make_tuple(
            x_qCUSTOMER2SUPPLIER_S__SUPPKEY,CUSTKEY)] += 
            -1*qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qCUSTOMER2SUPPLIER_S__SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it448 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end447 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it448 != qLINEITEM1_end447; ++qLINEITEM1_it448)
    {
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it448->first);
        string N__NAME = get<2>(qLINEITEM1_it448->first);
        qLINEITEM1[make_tuple(
            ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)] += -1*qLINEITEM1ORDERS1[make_tuple(
            CUSTKEY,x_qLINEITEM_L__SUPPKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_it450 = 
        qLINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        int>::iterator qLINEITEM1NATION1_end449 = qLINEITEM1NATION1.end();
    for (
        ; qLINEITEM1NATION1_it450 != qLINEITEM1NATION1_end449; 
        ++qLINEITEM1NATION1_it450)
    {
        int64_t x_qLINEITEM1NATION_N__NATIONKEY = get<1>(
            qLINEITEM1NATION1_it450->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<2>(qLINEITEM1NATION1_it450->first);
        qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qLINEITEM1NATION_N__NATIONKEY,x_qLINEITEM_L__SUPPKEY)] += 
            -1*qLINEITEM1ORDERS1NATION1[make_tuple(
            CUSTKEY,x_qLINEITEM1NATION_N__NATIONKEY,x_qLINEITEM_L__SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1REGION1_it452 = qLINEITEM1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1REGION1_end451 = qLINEITEM1REGION1.end();
    for (
        ; qLINEITEM1REGION1_it452 != qLINEITEM1REGION1_end451; 
        ++qLINEITEM1REGION1_it452)
    {
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1REGION1_it452->first);
        string N__NAME = get<2>(qLINEITEM1REGION1_it452->first);
        int64_t x_qLINEITEM1REGION_R__REGIONKEY = get<3>(
            qLINEITEM1REGION1_it452->first);
        qLINEITEM1REGION1[make_tuple(
            ORDERKEY,x_qLINEITEM_L__SUPPKEY,N__NAME,x_qLINEITEM1REGION_R__REGIONKEY)] += 
            -1*qLINEITEM1ORDERS1REGION1[make_tuple(
            CUSTKEY,x_qLINEITEM_L__SUPPKEY,N__NAME,x_qLINEITEM1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,
        int>::iterator qLINEITEM1SUPPLIER1_it454 = qLINEITEM1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1SUPPLIER1_end453 
        = qLINEITEM1SUPPLIER1.end();
    for (
        ; qLINEITEM1SUPPLIER1_it454 != qLINEITEM1SUPPLIER1_end453; 
        ++qLINEITEM1SUPPLIER1_it454)
    {
        int64_t x_qLINEITEM1SUPPLIER_S__NATIONKEY = get<1>(
            qLINEITEM1SUPPLIER1_it454->first);
        string N__NAME = get<2>(qLINEITEM1SUPPLIER1_it454->first);
        qLINEITEM1SUPPLIER1[make_tuple(
            ORDERKEY,x_qLINEITEM1SUPPLIER_S__NATIONKEY,N__NAME)] += 
            -1*qORDERS1SUPPLIER2[make_tuple(
            CUSTKEY,x_qLINEITEM1SUPPLIER_S__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,
        int>::iterator qLINEITEM1SUPPLIER1NATION1_it456 = 
        qLINEITEM1SUPPLIER1NATION1.begin();
    map<tuple<int64_t,int64_t>,int>::iterator qLINEITEM1SUPPLIER1NATION1_end455 
        = qLINEITEM1SUPPLIER1NATION1.end();
    for (
        ; qLINEITEM1SUPPLIER1NATION1_it456 != qLINEITEM1SUPPLIER1NATION1_end455; 
        ++qLINEITEM1SUPPLIER1NATION1_it456)
    {
        int64_t x_qLINEITEM1SUPPLIER1NATION_N__NATIONKEY = get<1>(
            qLINEITEM1SUPPLIER1NATION1_it456->first);
        qLINEITEM1SUPPLIER1NATION1[make_tuple(
            ORDERKEY,x_qLINEITEM1SUPPLIER1NATION_N__NATIONKEY)] += 
            -1*qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(
            CUSTKEY,x_qLINEITEM1SUPPLIER1NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1SUPPLIER1REGION1_it458 = qLINEITEM1SUPPLIER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1SUPPLIER1REGION1_end457 = qLINEITEM1SUPPLIER1REGION1.end();
    for (
        ; qLINEITEM1SUPPLIER1REGION1_it458 != qLINEITEM1SUPPLIER1REGION1_end457; 
        ++qLINEITEM1SUPPLIER1REGION1_it458)
    {
        int64_t x_qLINEITEM1SUPPLIER_S__NATIONKEY = get<1>(
            qLINEITEM1SUPPLIER1REGION1_it458->first);
        string N__NAME = get<2>(qLINEITEM1SUPPLIER1REGION1_it458->first);
        int64_t x_qLINEITEM1SUPPLIER1REGION_R__REGIONKEY = get<3>(
            qLINEITEM1SUPPLIER1REGION1_it458->first);
        qLINEITEM1SUPPLIER1REGION1[make_tuple(
            ORDERKEY,x_qLINEITEM1SUPPLIER_S__NATIONKEY,N__NAME,
            x_qLINEITEM1SUPPLIER1REGION_R__REGIONKEY)] += 
            -1*qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            CUSTKEY,x_qLINEITEM1SUPPLIER_S__NATIONKEY,N__NAME,
            x_qLINEITEM1SUPPLIER1REGION_R__REGIONKEY)];
    }
    map<int64_t,double>::iterator qNATION1_it460 = qNATION1.begin();
    map<int64_t,double>::iterator qNATION1_end459 = qNATION1.end();
    for (; qNATION1_it460 != qNATION1_end459; ++qNATION1_it460)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION1_it460->first;
        qNATION1[x_qNATION_N__NATIONKEY] += -1*qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N__NATIONKEY,CUSTKEY)];
    }
    map<int64_t,double>::iterator qNATION3_it462 = qNATION3.begin();
    map<int64_t,double>::iterator qNATION3_end461 = qNATION3.end();
    for (; qNATION3_it462 != qNATION3_end461; ++qNATION3_it462)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION3_it462->first;
        qNATION3[x_qNATION_N__NATIONKEY] += -1*qORDERS2NATION1[make_tuple(
            ORDERKEY,x_qNATION_N__NATIONKEY,CUSTKEY)];
    }
    map<tuple<string,int64_t>,double>::iterator qREGION1_it464 = qREGION1.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION1_end463 = qREGION1.end(
        );
    for (; qREGION1_it464 != qREGION1_end463; ++qREGION1_it464)
    {
        string N__NAME = get<0>(qREGION1_it464->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it464->first);
        qREGION1[make_tuple(
            N__NAME,x_qREGION_R__REGIONKEY)] += -1*qORDERS1REGION1[make_tuple(
            ORDERKEY,CUSTKEY,N__NAME,x_qREGION_R__REGIONKEY)];
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it466 = qREGION2.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION2_end465 = qREGION2.end(
        );
    for (; qREGION2_it466 != qREGION2_end465; ++qREGION2_it466)
    {
        string N__NAME = get<0>(qREGION2_it466->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it466->first);
        qREGION2[make_tuple(
            N__NAME,x_qREGION_R__REGIONKEY)] += -1*qORDERS2REGION1[make_tuple(
            ORDERKEY,CUSTKEY,N__NAME,x_qREGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it468 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end467 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it468 != qSUPPLIER1_end467; ++qSUPPLIER1_it468)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it468->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER1_it468->first);
        string N__NAME = get<2>(qSUPPLIER1_it468->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
            -1*qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY)]*qORDERS1SUPPLIER2[make_tuple(
            CUSTKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER1NATION1_it470 = 
        qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER1NATION1_end469 = 
        qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it470 != qSUPPLIER1NATION1_end469; 
        ++qSUPPLIER1NATION1_it470)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1NATION1_it470->first);
        int64_t x_qSUPPLIER1NATION_N__NATIONKEY = get<1>(
            qSUPPLIER1NATION1_it470->first);
        qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER1NATION_N__NATIONKEY)] += 
            -1*qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY)]*qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(
            CUSTKEY,x_qSUPPLIER1NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER1REGION1_it472 = qSUPPLIER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER1REGION1_end471 = qSUPPLIER1REGION1.end();
    for (
        ; qSUPPLIER1REGION1_it472 != qSUPPLIER1REGION1_end471; 
        ++qSUPPLIER1REGION1_it472)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1REGION1_it472->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(
            qSUPPLIER1REGION1_it472->first);
        string N__NAME = get<2>(qSUPPLIER1REGION1_it472->first);
        int64_t x_qSUPPLIER1REGION_R__REGIONKEY = get<3>(
            qSUPPLIER1REGION1_it472->first);
        qSUPPLIER1REGION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,
            x_qSUPPLIER1REGION_R__REGIONKEY)] += -1*qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY)]*qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            CUSTKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,x_qSUPPLIER1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it474 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end473 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it474 != qSUPPLIER2_end473; ++qSUPPLIER2_it474)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it474->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER2_it474->first);
        string N__NAME = get<2>(qSUPPLIER2_it474->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
            -1*qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY)]*qORDERS1SUPPLIER2[make_tuple(
            CUSTKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER2NATION1_it476 = 
        qSUPPLIER2NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER2NATION1_end475 = 
        qSUPPLIER2NATION1.end();
    for (
        ; qSUPPLIER2NATION1_it476 != qSUPPLIER2NATION1_end475; 
        ++qSUPPLIER2NATION1_it476)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2NATION1_it476->first);
        int64_t x_qSUPPLIER2NATION_N__NATIONKEY = get<1>(
            qSUPPLIER2NATION1_it476->first);
        qSUPPLIER2NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER2NATION_N__NATIONKEY)] += 
            -1*qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY)]*qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(
            CUSTKEY,x_qSUPPLIER2NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER2REGION1_it478 = qSUPPLIER2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER2REGION1_end477 = qSUPPLIER2REGION1.end();
    for (
        ; qSUPPLIER2REGION1_it478 != qSUPPLIER2REGION1_end477; 
        ++qSUPPLIER2REGION1_it478)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2REGION1_it478->first);
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(
            qSUPPLIER2REGION1_it478->first);
        string N__NAME = get<2>(qSUPPLIER2REGION1_it478->first);
        int64_t x_qSUPPLIER2REGION_R__REGIONKEY = get<3>(
            qSUPPLIER2REGION1_it478->first);
        qSUPPLIER2REGION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,
            x_qSUPPLIER2REGION_R__REGIONKEY)] += -1*qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__SUPPKEY)]*qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            CUSTKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,x_qSUPPLIER2REGION_R__REGIONKEY)];
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
    map<string,double>::iterator q_it480 = q.begin();
    map<string,double>::iterator q_end479 = q.end();
    for (; q_it480 != q_end479; ++q_it480)
    {
        string N__NAME = q_it480->first;
        q[N__NAME] += -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1[make_tuple(
            ORDERKEY,SUPPKEY,N__NAME)]*-1;
    }
    map<string,double>::iterator q_it482 = q.begin();
    map<string,double>::iterator q_end481 = q.end();
    for (; q_it482 != q_end481; ++q_it482)
    {
        string N__NAME = q_it482->first;
        q[N__NAME] += -1*EXTENDEDPRICE*qLINEITEM1[make_tuple(
            ORDERKEY,SUPPKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it484 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end483 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it484 != qCUSTOMER1_end483; ++qCUSTOMER1_it484)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it484->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER1_it484->first);
        string N__NAME = get<2>(qCUSTOMER1_it484->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            -1*EXTENDEDPRICE*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1LINEITEM2[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1NATION1_it486 = 
        qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1NATION1_end485 = 
        qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it486 != qCUSTOMER1NATION1_end485; 
        ++qCUSTOMER1NATION1_it486)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1NATION1_it486->first);
        int64_t x_qCUSTOMER1NATION_N__NATIONKEY = get<1>(
            qCUSTOMER1NATION1_it486->first);
        qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER1NATION_N__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            SUPPKEY,x_qCUSTOMER1NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1ORDERS1_it488 
        = qCUSTOMER1ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1ORDERS1_end487 = qCUSTOMER1ORDERS1.end();
    for (
        ; qCUSTOMER1ORDERS1_it488 != qCUSTOMER1ORDERS1_end487; 
        ++qCUSTOMER1ORDERS1_it488)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER1ORDERS1_it488->first);
        string N__NAME = get<2>(qCUSTOMER1ORDERS1_it488->first);
        qCUSTOMER1ORDERS1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            -1*EXTENDEDPRICE*qCUSTOMER1LINEITEM2[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1ORDERS1NATION1_it490 
        = qCUSTOMER1ORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t>,
        double>::iterator qCUSTOMER1ORDERS1NATION1_end489 = 
        qCUSTOMER1ORDERS1NATION1.end();
    for (
        ; qCUSTOMER1ORDERS1NATION1_it490 != qCUSTOMER1ORDERS1NATION1_end489; 
        ++qCUSTOMER1ORDERS1NATION1_it490)
    {
        int64_t x_qCUSTOMER1ORDERS1NATION_N__NATIONKEY = get<1>(
            qCUSTOMER1ORDERS1NATION1_it490->first);
        qCUSTOMER1ORDERS1NATION1[make_tuple(
            ORDERKEY,x_qCUSTOMER1ORDERS1NATION_N__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            SUPPKEY,x_qCUSTOMER1ORDERS1NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1ORDERS1REGION1_it492 = qCUSTOMER1ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1ORDERS1REGION1_end491 = qCUSTOMER1ORDERS1REGION1.end();
    for (
        ; qCUSTOMER1ORDERS1REGION1_it492 != qCUSTOMER1ORDERS1REGION1_end491; 
        ++qCUSTOMER1ORDERS1REGION1_it492)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER1ORDERS1REGION1_it492->first);
        string N__NAME = get<2>(qCUSTOMER1ORDERS1REGION1_it492->first);
        int64_t x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY = get<3>(
            qCUSTOMER1ORDERS1REGION1_it492->first);
        qCUSTOMER1ORDERS1REGION1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,
            x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,
            x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY)];
    }
    qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
        ORDERKEY,SUPPKEY)] += -1*EXTENDEDPRICE;
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1REGION1_it494 = qCUSTOMER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1REGION1_end493 = qCUSTOMER1REGION1.end();
    for (
        ; qCUSTOMER1REGION1_it494 != qCUSTOMER1REGION1_end493; 
        ++qCUSTOMER1REGION1_it494)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1REGION1_it494->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER1REGION1_it494->first);
        string N__NAME = get<2>(qCUSTOMER1REGION1_it494->first);
        int64_t x_qCUSTOMER1REGION_R__REGIONKEY = get<3>(
            qCUSTOMER1REGION1_it494->first);
        qCUSTOMER1REGION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,
            x_qCUSTOMER1REGION_R__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,x_qCUSTOMER1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1SUPPLIER1_it496 = 
        qCUSTOMER1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1SUPPLIER1_end495 = 
        qCUSTOMER1SUPPLIER1.end();
    for (
        ; qCUSTOMER1SUPPLIER1_it496 != qCUSTOMER1SUPPLIER1_end495; 
        ++qCUSTOMER1SUPPLIER1_it496)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER1SUPPLIER1_it496->first);
        qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY)] += 
            -1*EXTENDEDPRICE*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it498 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end497 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it498 != qCUSTOMER2_end497; ++qCUSTOMER2_it498)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it498->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(qCUSTOMER2_it498->first);
        string N__NAME = get<2>(qCUSTOMER2_it498->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1LINEITEM2[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2NATION1_it500 = 
        qCUSTOMER2NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2NATION1_end499 = 
        qCUSTOMER2NATION1.end();
    for (
        ; qCUSTOMER2NATION1_it500 != qCUSTOMER2NATION1_end499; 
        ++qCUSTOMER2NATION1_it500)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2NATION1_it500->first);
        int64_t x_qCUSTOMER2NATION_N__NATIONKEY = get<1>(
            qCUSTOMER2NATION1_it500->first);
        qCUSTOMER2NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER2NATION_N__NATIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            SUPPKEY,x_qCUSTOMER2NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2ORDERS1_it502 
        = qCUSTOMER2ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER2ORDERS1_end501 = qCUSTOMER2ORDERS1.end();
    for (
        ; qCUSTOMER2ORDERS1_it502 != qCUSTOMER2ORDERS1_end501; 
        ++qCUSTOMER2ORDERS1_it502)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER2ORDERS1_it502->first);
        string N__NAME = get<2>(qCUSTOMER2ORDERS1_it502->first);
        qCUSTOMER2ORDERS1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qCUSTOMER1LINEITEM2[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2ORDERS1NATION1_it504 
        = qCUSTOMER2ORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t>,
        double>::iterator qCUSTOMER2ORDERS1NATION1_end503 = 
        qCUSTOMER2ORDERS1NATION1.end();
    for (
        ; qCUSTOMER2ORDERS1NATION1_it504 != qCUSTOMER2ORDERS1NATION1_end503; 
        ++qCUSTOMER2ORDERS1NATION1_it504)
    {
        int64_t x_qCUSTOMER2ORDERS1NATION_N__NATIONKEY = get<1>(
            qCUSTOMER2ORDERS1NATION1_it504->first);
        qCUSTOMER2ORDERS1NATION1[make_tuple(
            ORDERKEY,x_qCUSTOMER2ORDERS1NATION_N__NATIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            SUPPKEY,x_qCUSTOMER2ORDERS1NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2ORDERS1REGION1_it506 = qCUSTOMER2ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2ORDERS1REGION1_end505 = qCUSTOMER2ORDERS1REGION1.end();
    for (
        ; qCUSTOMER2ORDERS1REGION1_it506 != qCUSTOMER2ORDERS1REGION1_end505; 
        ++qCUSTOMER2ORDERS1REGION1_it506)
    {
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER2ORDERS1REGION1_it506->first);
        string N__NAME = get<2>(qCUSTOMER2ORDERS1REGION1_it506->first);
        int64_t x_qCUSTOMER2ORDERS1REGION_R__REGIONKEY = get<3>(
            qCUSTOMER2ORDERS1REGION1_it506->first);
        qCUSTOMER2ORDERS1REGION1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,
            x_qCUSTOMER2ORDERS1REGION_R__REGIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,
            x_qCUSTOMER2ORDERS1REGION_R__REGIONKEY)];
    }
    qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
        ORDERKEY,SUPPKEY)] += -1*EXTENDEDPRICE*DISCOUNT;
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2REGION1_it508 = qCUSTOMER2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2REGION1_end507 = qCUSTOMER2REGION1.end();
    for (
        ; qCUSTOMER2REGION1_it508 != qCUSTOMER2REGION1_end507; 
        ++qCUSTOMER2REGION1_it508)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2REGION1_it508->first);
        int64_t x_qCUSTOMER_C__NATIONKEY = get<1>(
            qCUSTOMER2REGION1_it508->first);
        string N__NAME = get<2>(qCUSTOMER2REGION1_it508->first);
        int64_t x_qCUSTOMER2REGION_R__REGIONKEY = get<3>(
            qCUSTOMER2REGION1_it508->first);
        qCUSTOMER2REGION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,
            x_qCUSTOMER2REGION_R__REGIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__NATIONKEY,N__NAME,x_qCUSTOMER2REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2SUPPLIER1_it510 = 
        qCUSTOMER2SUPPLIER1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2SUPPLIER1_end509 = 
        qCUSTOMER2SUPPLIER1.end();
    for (
        ; qCUSTOMER2SUPPLIER1_it510 != qCUSTOMER2SUPPLIER1_end509; 
        ++qCUSTOMER2SUPPLIER1_it510)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(
            qCUSTOMER2SUPPLIER1_it510->first);
        qCUSTOMER2SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qCUSTOMER1LINEITEM1[make_tuple(
            ORDERKEY,x_qCUSTOMER_C__CUSTKEY)];
    }
    map<int64_t,double>::iterator qNATION1_it512 = qNATION1.begin();
    map<int64_t,double>::iterator qNATION1_end511 = qNATION1.end();
    for (; qNATION1_it512 != qNATION1_end511; ++qNATION1_it512)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION1_it512->first;
        qNATION1[x_qNATION_N__NATIONKEY] += 
            -1*EXTENDEDPRICE*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N__NATIONKEY,SUPPKEY)];
    }
    map<int64_t,double>::iterator qNATION3_it514 = qNATION3.begin();
    map<int64_t,double>::iterator qNATION3_end513 = qNATION3.end();
    for (; qNATION3_it514 != qNATION3_end513; ++qNATION3_it514)
    {
        int64_t x_qNATION_N__NATIONKEY = qNATION3_it514->first;
        qNATION3[x_qNATION_N__NATIONKEY] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1NATION1[make_tuple(
            ORDERKEY,x_qNATION_N__NATIONKEY,SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it516 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end515 = 
        qORDERS1.end();
    for (; qORDERS1_it516 != qORDERS1_end515; ++qORDERS1_it516)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it516->first);
        string N__NAME = get<2>(qORDERS1_it516->first);
        qORDERS1[make_tuple(
            ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
            -1*EXTENDEDPRICE*qORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qORDERS_O__CUSTKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS1NATION1_it518 = qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1NATION1_end517 
        = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it518 != qORDERS1NATION1_end517; ++qORDERS1NATION1_it518)
    {
        int64_t x_qORDERS1NATION_N__NATIONKEY = get<1>(
            qORDERS1NATION1_it518->first);
        int64_t x_qORDERS_O__CUSTKEY = get<2>(qORDERS1NATION1_it518->first);
        qORDERS1NATION1[make_tuple(
            ORDERKEY,x_qORDERS1NATION_N__NATIONKEY,x_qORDERS_O__CUSTKEY)] += 
            -1*EXTENDEDPRICE*qORDERS1LINEITEM1NATION1[make_tuple(
            SUPPKEY,x_qORDERS1NATION_N__NATIONKEY,x_qORDERS_O__CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS1REGION1_it520 = qORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS1REGION1_end519 = qORDERS1REGION1.end();
    for (
        ; qORDERS1REGION1_it520 != qORDERS1REGION1_end519; ++qORDERS1REGION1_it520)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1REGION1_it520->first);
        string N__NAME = get<2>(qORDERS1REGION1_it520->first);
        int64_t x_qORDERS1REGION_R__REGIONKEY = get<3>(
            qORDERS1REGION1_it520->first);
        qORDERS1REGION1[make_tuple(
            ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME,x_qORDERS1REGION_R__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qORDERS1LINEITEM1REGION1[make_tuple(
            SUPPKEY,x_qORDERS_O__CUSTKEY,N__NAME,x_qORDERS1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it522 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end521 = 
        qORDERS2.end();
    for (; qORDERS2_it522 != qORDERS2_end521; ++qORDERS2_it522)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it522->first);
        string N__NAME = get<2>(qORDERS2_it522->first);
        qORDERS2[make_tuple(
            ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qORDERS_O__CUSTKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS2NATION1_it524 = qORDERS2NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS2NATION1_end523 
        = qORDERS2NATION1.end();
    for (
        ; qORDERS2NATION1_it524 != qORDERS2NATION1_end523; ++qORDERS2NATION1_it524)
    {
        int64_t x_qORDERS2NATION_N__NATIONKEY = get<1>(
            qORDERS2NATION1_it524->first);
        int64_t x_qORDERS_O__CUSTKEY = get<2>(qORDERS2NATION1_it524->first);
        qORDERS2NATION1[make_tuple(
            ORDERKEY,x_qORDERS2NATION_N__NATIONKEY,x_qORDERS_O__CUSTKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qORDERS1LINEITEM1NATION1[make_tuple(
            SUPPKEY,x_qORDERS2NATION_N__NATIONKEY,x_qORDERS_O__CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS2REGION1_it526 = qORDERS2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS2REGION1_end525 = qORDERS2REGION1.end();
    for (
        ; qORDERS2REGION1_it526 != qORDERS2REGION1_end525; ++qORDERS2REGION1_it526)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2REGION1_it526->first);
        string N__NAME = get<2>(qORDERS2REGION1_it526->first);
        int64_t x_qORDERS2REGION_R__REGIONKEY = get<3>(
            qORDERS2REGION1_it526->first);
        qORDERS2REGION1[make_tuple(
            ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME,x_qORDERS2REGION_R__REGIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qORDERS1LINEITEM1REGION1[make_tuple(
            SUPPKEY,x_qORDERS_O__CUSTKEY,N__NAME,x_qORDERS2REGION_R__REGIONKEY)];
    }
    map<tuple<string,int64_t>,double>::iterator qREGION1_it528 = qREGION1.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION1_end527 = qREGION1.end(
        );
    for (; qREGION1_it528 != qREGION1_end527; ++qREGION1_it528)
    {
        string N__NAME = get<0>(qREGION1_it528->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it528->first);
        qREGION1[make_tuple(
            N__NAME,x_qREGION_R__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1REGION1[make_tuple(
            ORDERKEY,SUPPKEY,N__NAME,x_qREGION_R__REGIONKEY)];
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it530 = qREGION2.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION2_end529 = qREGION2.end(
        );
    for (; qREGION2_it530 != qREGION2_end529; ++qREGION2_it530)
    {
        string N__NAME = get<0>(qREGION2_it530->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it530->first);
        qREGION2[make_tuple(
            N__NAME,x_qREGION_R__REGIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1REGION1[make_tuple(
            ORDERKEY,SUPPKEY,N__NAME,x_qREGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it532 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end531 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it532 != qSUPPLIER1_end531; ++qSUPPLIER1_it532)
    {
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER1_it532->first);
        string N__NAME = get<2>(qSUPPLIER1_it532->first);
        qSUPPLIER1[make_tuple(
            SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
            -1*EXTENDEDPRICE*qLINEITEM1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER1NATION1_it534 = 
        qSUPPLIER1NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER1NATION1_end533 = 
        qSUPPLIER1NATION1.end();
    for (
        ; qSUPPLIER1NATION1_it534 != qSUPPLIER1NATION1_end533; 
        ++qSUPPLIER1NATION1_it534)
    {
        int64_t x_qSUPPLIER1NATION_N__NATIONKEY = get<1>(
            qSUPPLIER1NATION1_it534->first);
        qSUPPLIER1NATION1[make_tuple(
            SUPPKEY,x_qSUPPLIER1NATION_N__NATIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1SUPPLIER1NATION1[make_tuple(
            ORDERKEY,x_qSUPPLIER1NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER1REGION1_it536 = qSUPPLIER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER1REGION1_end535 = qSUPPLIER1REGION1.end();
    for (
        ; qSUPPLIER1REGION1_it536 != qSUPPLIER1REGION1_end535; 
        ++qSUPPLIER1REGION1_it536)
    {
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(
            qSUPPLIER1REGION1_it536->first);
        string N__NAME = get<2>(qSUPPLIER1REGION1_it536->first);
        int64_t x_qSUPPLIER1REGION_R__REGIONKEY = get<3>(
            qSUPPLIER1REGION1_it536->first);
        qSUPPLIER1REGION1[make_tuple(
            SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,x_qSUPPLIER1REGION_R__REGIONKEY)] += 
            -1*EXTENDEDPRICE*qLINEITEM1SUPPLIER1REGION1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,x_qSUPPLIER1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it538 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end537 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it538 != qSUPPLIER2_end537; ++qSUPPLIER2_it538)
    {
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(qSUPPLIER2_it538->first);
        string N__NAME = get<2>(qSUPPLIER2_it538->first);
        qSUPPLIER2[make_tuple(
            SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1SUPPLIER1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER2NATION1_it540 = 
        qSUPPLIER2NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qSUPPLIER2NATION1_end539 = 
        qSUPPLIER2NATION1.end();
    for (
        ; qSUPPLIER2NATION1_it540 != qSUPPLIER2NATION1_end539; 
        ++qSUPPLIER2NATION1_it540)
    {
        int64_t x_qSUPPLIER2NATION_N__NATIONKEY = get<1>(
            qSUPPLIER2NATION1_it540->first);
        qSUPPLIER2NATION1[make_tuple(
            SUPPKEY,x_qSUPPLIER2NATION_N__NATIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1SUPPLIER1NATION1[make_tuple(
            ORDERKEY,x_qSUPPLIER2NATION_N__NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER2REGION1_it542 = qSUPPLIER2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER2REGION1_end541 = qSUPPLIER2REGION1.end();
    for (
        ; qSUPPLIER2REGION1_it542 != qSUPPLIER2REGION1_end541; 
        ++qSUPPLIER2REGION1_it542)
    {
        int64_t x_qSUPPLIER_S__NATIONKEY = get<1>(
            qSUPPLIER2REGION1_it542->first);
        string N__NAME = get<2>(qSUPPLIER2REGION1_it542->first);
        int64_t x_qSUPPLIER2REGION_R__REGIONKEY = get<3>(
            qSUPPLIER2REGION1_it542->first);
        qSUPPLIER2REGION1[make_tuple(
            SUPPKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,x_qSUPPLIER2REGION_R__REGIONKEY)] += 
            -1*EXTENDEDPRICE*DISCOUNT*qLINEITEM1SUPPLIER1REGION1[make_tuple(
            ORDERKEY,x_qSUPPLIER_S__NATIONKEY,N__NAME,x_qSUPPLIER2REGION_R__REGIONKEY)];
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_LINEITEM_sec_span, on_delete_LINEITEM_usec_span);
}

void on_delete_SUPPLIER(
    ofstream* results, ofstream* log, ofstream* stats, int64_t SUPPKEY,string 
    NAME,string ADDRESS,int64_t NATIONKEY,string PHONE,double ACCTBAL,string 
    COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    map<string,double>::iterator q_it544 = q.begin();
    map<string,double>::iterator q_end543 = q.end();
    for (; q_it544 != q_end543; ++q_it544)
    {
        string N__NAME = q_it544->first;
        q[N__NAME] += -1*qSUPPLIER1[make_tuple(SUPPKEY,NATIONKEY,N__NAME)];
    }
    map<string,double>::iterator q_it546 = q.begin();
    map<string,double>::iterator q_end545 = q.end();
    for (; q_it546 != q_end545; ++q_it546)
    {
        string N__NAME = q_it546->first;
        q[N__NAME] += -1*qSUPPLIER2[make_tuple(SUPPKEY,NATIONKEY,N__NAME)]*-1;
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it548 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end547 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it548 != qCUSTOMER1_end547; ++qCUSTOMER1_it548)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it548->first);
        string N__NAME = get<2>(qCUSTOMER1_it548->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,N__NAME)] += -1*qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,string>,
        int>::iterator qCUSTOMER1LINEITEM2_it550 = qCUSTOMER1LINEITEM2.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qCUSTOMER1LINEITEM2_end549 
        = qCUSTOMER1LINEITEM2.end();
    for (
        ; qCUSTOMER1LINEITEM2_it550 != qCUSTOMER1LINEITEM2_end549; 
        ++qCUSTOMER1LINEITEM2_it550)
    {
        string N__NAME = get<2>(qCUSTOMER1LINEITEM2_it550->first);
        qCUSTOMER1LINEITEM2[make_tuple(
            SUPPKEY,NATIONKEY,N__NAME)] += -1*qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1NATION1_it552 = 
        qCUSTOMER1NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1NATION1_end551 = 
        qCUSTOMER1NATION1.end();
    for (
        ; qCUSTOMER1NATION1_it552 != qCUSTOMER1NATION1_end551; 
        ++qCUSTOMER1NATION1_it552)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1NATION1_it552->first);
        qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY)] += -1*qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1ORDERS1_it554 
        = qCUSTOMER1ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1ORDERS1_end553 = qCUSTOMER1ORDERS1.end();
    for (
        ; qCUSTOMER1ORDERS1_it554 != qCUSTOMER1ORDERS1_end553; 
        ++qCUSTOMER1ORDERS1_it554)
    {
        int64_t x_qCUSTOMER1ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER1ORDERS1_it554->first);
        string N__NAME = get<2>(qCUSTOMER1ORDERS1_it554->first);
        qCUSTOMER1ORDERS1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,NATIONKEY,N__NAME)] += 
            -1*qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,SUPPKEY)]*qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER1ORDERS1NATION1_it556 
        = qCUSTOMER1ORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t>,
        double>::iterator qCUSTOMER1ORDERS1NATION1_end555 = 
        qCUSTOMER1ORDERS1NATION1.end();
    for (
        ; qCUSTOMER1ORDERS1NATION1_it556 != qCUSTOMER1ORDERS1NATION1_end555; 
        ++qCUSTOMER1ORDERS1NATION1_it556)
    {
        int64_t x_qCUSTOMER1ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER1ORDERS1NATION1_it556->first);
        qCUSTOMER1ORDERS1NATION1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,NATIONKEY)] += 
            -1*qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,SUPPKEY)];
    }
    qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(SUPPKEY,NATIONKEY)] += -1;
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1ORDERS1REGION1_it558 = qCUSTOMER1ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1ORDERS1REGION1_end557 = qCUSTOMER1ORDERS1REGION1.end();
    for (
        ; qCUSTOMER1ORDERS1REGION1_it558 != qCUSTOMER1ORDERS1REGION1_end557; 
        ++qCUSTOMER1ORDERS1REGION1_it558)
    {
        int64_t x_qCUSTOMER1ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER1ORDERS1REGION1_it558->first);
        string N__NAME = get<2>(qCUSTOMER1ORDERS1REGION1_it558->first);
        int64_t x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY = get<3>(
            qCUSTOMER1ORDERS1REGION1_it558->first);
        qCUSTOMER1ORDERS1REGION1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,NATIONKEY,N__NAME,
            x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY)] += 
            -1*qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,
            SUPPKEY)]*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qCUSTOMER1ORDERS1REGION1LINEITEM1_it560 = 
        qCUSTOMER1ORDERS1REGION1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qCUSTOMER1ORDERS1REGION1LINEITEM1_end559 = 
        qCUSTOMER1ORDERS1REGION1LINEITEM1.end();
    for (
        ; qCUSTOMER1ORDERS1REGION1LINEITEM1_it560 != 
        qCUSTOMER1ORDERS1REGION1LINEITEM1_end559; 
        ++qCUSTOMER1ORDERS1REGION1LINEITEM1_it560)
    {
        string N__NAME = get<2>(qCUSTOMER1ORDERS1REGION1LINEITEM1_it560->first);
        int64_t x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY = get<3>(
            qCUSTOMER1ORDERS1REGION1LINEITEM1_it560->first);
        qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            SUPPKEY,NATIONKEY,N__NAME,x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY)] += 
            -1*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qCUSTOMER1ORDERS1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1REGION1_it562 = qCUSTOMER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1REGION1_end561 = qCUSTOMER1REGION1.end();
    for (
        ; qCUSTOMER1REGION1_it562 != qCUSTOMER1REGION1_end561; 
        ++qCUSTOMER1REGION1_it562)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1REGION1_it562->first);
        string N__NAME = get<2>(qCUSTOMER1REGION1_it562->first);
        int64_t x_qCUSTOMER1REGION_R__REGIONKEY = get<3>(
            qCUSTOMER1REGION1_it562->first);
        qCUSTOMER1REGION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,N__NAME,x_qCUSTOMER1REGION_R__REGIONKEY)] += 
            -1*qCUSTOMER1SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qCUSTOMER1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it564 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end563 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it564 != qCUSTOMER2_end563; ++qCUSTOMER2_it564)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it564->first);
        string N__NAME = get<2>(qCUSTOMER2_it564->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,N__NAME)] += -1*qCUSTOMER2SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2NATION1_it566 = 
        qCUSTOMER2NATION1.begin();
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2NATION1_end565 = 
        qCUSTOMER2NATION1.end();
    for (
        ; qCUSTOMER2NATION1_it566 != qCUSTOMER2NATION1_end565; 
        ++qCUSTOMER2NATION1_it566)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2NATION1_it566->first);
        qCUSTOMER2NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY)] += -1*qCUSTOMER2SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2ORDERS1_it568 
        = qCUSTOMER2ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER2ORDERS1_end567 = qCUSTOMER2ORDERS1.end();
    for (
        ; qCUSTOMER2ORDERS1_it568 != qCUSTOMER2ORDERS1_end567; 
        ++qCUSTOMER2ORDERS1_it568)
    {
        int64_t x_qCUSTOMER2ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER2ORDERS1_it568->first);
        string N__NAME = get<2>(qCUSTOMER2ORDERS1_it568->first);
        qCUSTOMER2ORDERS1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,NATIONKEY,N__NAME)] += 
            -1*qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,SUPPKEY)]*qCUSTOMER1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t>,double>::iterator qCUSTOMER2ORDERS1NATION1_it570 
        = qCUSTOMER2ORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t>,
        double>::iterator qCUSTOMER2ORDERS1NATION1_end569 = 
        qCUSTOMER2ORDERS1NATION1.end();
    for (
        ; qCUSTOMER2ORDERS1NATION1_it570 != qCUSTOMER2ORDERS1NATION1_end569; 
        ++qCUSTOMER2ORDERS1NATION1_it570)
    {
        int64_t x_qCUSTOMER2ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER2ORDERS1NATION1_it570->first);
        qCUSTOMER2ORDERS1NATION1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,NATIONKEY)] += 
            -1*qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2ORDERS1REGION1_it572 = qCUSTOMER2ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2ORDERS1REGION1_end571 = qCUSTOMER2ORDERS1REGION1.end();
    for (
        ; qCUSTOMER2ORDERS1REGION1_it572 != qCUSTOMER2ORDERS1REGION1_end571; 
        ++qCUSTOMER2ORDERS1REGION1_it572)
    {
        int64_t x_qCUSTOMER2ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER2ORDERS1REGION1_it572->first);
        string N__NAME = get<2>(qCUSTOMER2ORDERS1REGION1_it572->first);
        int64_t x_qCUSTOMER2ORDERS1REGION_R__REGIONKEY = get<3>(
            qCUSTOMER2ORDERS1REGION1_it572->first);
        qCUSTOMER2ORDERS1REGION1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,NATIONKEY,N__NAME,
            x_qCUSTOMER2ORDERS1REGION_R__REGIONKEY)] += 
            -1*qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,
            SUPPKEY)]*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qCUSTOMER2ORDERS1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2REGION1_it574 = qCUSTOMER2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2REGION1_end573 = qCUSTOMER2REGION1.end();
    for (
        ; qCUSTOMER2REGION1_it574 != qCUSTOMER2REGION1_end573; 
        ++qCUSTOMER2REGION1_it574)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2REGION1_it574->first);
        string N__NAME = get<2>(qCUSTOMER2REGION1_it574->first);
        int64_t x_qCUSTOMER2REGION_R__REGIONKEY = get<3>(
            qCUSTOMER2REGION1_it574->first);
        qCUSTOMER2REGION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,N__NAME,x_qCUSTOMER2REGION_R__REGIONKEY)] += 
            -1*qCUSTOMER2SUPPLIER1[make_tuple(
            SUPPKEY,x_qCUSTOMER_C__CUSTKEY)]*qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
            NATIONKEY,N__NAME,x_qCUSTOMER2REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it576 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end575 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it576 != qLINEITEM1_end575; ++qLINEITEM1_it576)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it576->first);
        string N__NAME = get<2>(qLINEITEM1_it576->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,SUPPKEY,N__NAME)] += -1*qLINEITEM1SUPPLIER1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator qLINEITEM1NATION1_it578 = 
        qLINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,
        int>::iterator qLINEITEM1NATION1_end577 = qLINEITEM1NATION1.end();
    for (
        ; qLINEITEM1NATION1_it578 != qLINEITEM1NATION1_end577; 
        ++qLINEITEM1NATION1_it578)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1NATION1_it578->first);
        qLINEITEM1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,SUPPKEY)] += 
            -1*qLINEITEM1SUPPLIER1NATION1[make_tuple(x_qLINEITEM_L__ORDERKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1ORDERS1_it580 = 
        qLINEITEM1ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1ORDERS1_end579 = 
        qLINEITEM1ORDERS1.end();
    for (
        ; qLINEITEM1ORDERS1_it580 != qLINEITEM1ORDERS1_end579; 
        ++qLINEITEM1ORDERS1_it580)
    {
        int64_t x_qLINEITEM1ORDERS_O__CUSTKEY = get<0>(
            qLINEITEM1ORDERS1_it580->first);
        string N__NAME = get<2>(qLINEITEM1ORDERS1_it580->first);
        qLINEITEM1ORDERS1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,SUPPKEY,N__NAME)] += 
            -1*qORDERS1SUPPLIER2[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1NATION1_it582 = qLINEITEM1ORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1NATION1_end581 = qLINEITEM1ORDERS1NATION1.end();
    for (
        ; qLINEITEM1ORDERS1NATION1_it582 != qLINEITEM1ORDERS1NATION1_end581; 
        ++qLINEITEM1ORDERS1NATION1_it582)
    {
        int64_t x_qLINEITEM1ORDERS_O__CUSTKEY = get<0>(
            qLINEITEM1ORDERS1NATION1_it582->first);
        qLINEITEM1ORDERS1NATION1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,NATIONKEY,SUPPKEY)] += 
            -1*qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1REGION1_it584 = qLINEITEM1ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1REGION1_end583 = qLINEITEM1ORDERS1REGION1.end();
    for (
        ; qLINEITEM1ORDERS1REGION1_it584 != qLINEITEM1ORDERS1REGION1_end583; 
        ++qLINEITEM1ORDERS1REGION1_it584)
    {
        int64_t x_qLINEITEM1ORDERS_O__CUSTKEY = get<0>(
            qLINEITEM1ORDERS1REGION1_it584->first);
        string N__NAME = get<2>(qLINEITEM1ORDERS1REGION1_it584->first);
        int64_t x_qLINEITEM1ORDERS1REGION_R__REGIONKEY = get<3>(
            qLINEITEM1ORDERS1REGION1_it584->first);
        qLINEITEM1ORDERS1REGION1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,SUPPKEY,N__NAME,
            x_qLINEITEM1ORDERS1REGION_R__REGIONKEY)] += 
            -1*qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,NATIONKEY,N__NAME,
            x_qLINEITEM1ORDERS1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1REGION1_it586 = qLINEITEM1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1REGION1_end585 = qLINEITEM1REGION1.end();
    for (
        ; qLINEITEM1REGION1_it586 != qLINEITEM1REGION1_end585; 
        ++qLINEITEM1REGION1_it586)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1REGION1_it586->first);
        string N__NAME = get<2>(qLINEITEM1REGION1_it586->first);
        int64_t x_qLINEITEM1REGION_R__REGIONKEY = get<3>(
            qLINEITEM1REGION1_it586->first);
        qLINEITEM1REGION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,SUPPKEY,N__NAME,x_qLINEITEM1REGION_R__REGIONKEY)] += 
            -1*qLINEITEM1SUPPLIER1REGION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,N__NAME,x_qLINEITEM1REGION_R__REGIONKEY)];
    }
    qNATION1[NATIONKEY] += -1*qSUPPLIER1NATION1[make_tuple(SUPPKEY,NATIONKEY)];
    qNATION3[NATIONKEY] += -1*qSUPPLIER2NATION1[make_tuple(SUPPKEY,NATIONKEY)];
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it588 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end587 = 
        qORDERS1.end();
    for (; qORDERS1_it588 != qORDERS1_end587; ++qORDERS1_it588)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it588->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it588->first);
        string N__NAME = get<2>(qORDERS1_it588->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
            -1*qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY)]*qORDERS1SUPPLIER2[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1LINEITEM1_it590 = 
        qORDERS1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1LINEITEM1_end589 = 
        qORDERS1LINEITEM1.end();
    for (
        ; qORDERS1LINEITEM1_it590 != qORDERS1LINEITEM1_end589; 
        ++qORDERS1LINEITEM1_it590)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1LINEITEM1_it590->first);
        string N__NAME = get<2>(qORDERS1LINEITEM1_it590->first);
        qORDERS1LINEITEM1[make_tuple(
            SUPPKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += -1*qORDERS1SUPPLIER2[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qORDERS1LINEITEM1NATION1_it592 = qORDERS1LINEITEM1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,int>::iterator 
        qORDERS1LINEITEM1NATION1_end591 = qORDERS1LINEITEM1NATION1.end();
    for (
        ; qORDERS1LINEITEM1NATION1_it592 != qORDERS1LINEITEM1NATION1_end591; 
        ++qORDERS1LINEITEM1NATION1_it592)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<2>(
            qORDERS1LINEITEM1NATION1_it592->first);
        qORDERS1LINEITEM1NATION1[make_tuple(
            SUPPKEY,NATIONKEY,x_qORDERS_O__CUSTKEY)] += 
            -1*qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1_it594 = qORDERS1LINEITEM1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1_end593 = qORDERS1LINEITEM1REGION1.end();
    for (
        ; qORDERS1LINEITEM1REGION1_it594 != qORDERS1LINEITEM1REGION1_end593; 
        ++qORDERS1LINEITEM1REGION1_it594)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<1>(
            qORDERS1LINEITEM1REGION1_it594->first);
        string N__NAME = get<2>(qORDERS1LINEITEM1REGION1_it594->first);
        int64_t x_qORDERS1LINEITEM1REGION_R__REGIONKEY = get<3>(
            qORDERS1LINEITEM1REGION1_it594->first);
        qORDERS1LINEITEM1REGION1[make_tuple(
            SUPPKEY,x_qORDERS_O__CUSTKEY,N__NAME,
            x_qORDERS1LINEITEM1REGION_R__REGIONKEY)] += 
            -1*qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,N__NAME,x_qORDERS1LINEITEM1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS1NATION1_it596 = qORDERS1NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS1NATION1_end595 
        = qORDERS1NATION1.end();
    for (
        ; qORDERS1NATION1_it596 != qORDERS1NATION1_end595; ++qORDERS1NATION1_it596)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1NATION1_it596->first);
        int64_t x_qORDERS_O__CUSTKEY = get<2>(qORDERS1NATION1_it596->first);
        qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,x_qORDERS_O__CUSTKEY)] += 
            -1*qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY)]*qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS1REGION1_it598 = qORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS1REGION1_end597 = qORDERS1REGION1.end();
    for (
        ; qORDERS1REGION1_it598 != qORDERS1REGION1_end597; ++qORDERS1REGION1_it598)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1REGION1_it598->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1REGION1_it598->first);
        string N__NAME = get<2>(qORDERS1REGION1_it598->first);
        int64_t x_qORDERS1REGION_R__REGIONKEY = get<3>(
            qORDERS1REGION1_it598->first);
        qORDERS1REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME,
            x_qORDERS1REGION_R__REGIONKEY)] += -1*qCUSTOMER1ORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY)]*qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,N__NAME,x_qORDERS1REGION_R__REGIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it600 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end599 = 
        qORDERS2.end();
    for (; qORDERS2_it600 != qORDERS2_end599; ++qORDERS2_it600)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it600->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it600->first);
        string N__NAME = get<2>(qORDERS2_it600->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME)] += 
            -1*qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY)]*qORDERS1SUPPLIER2[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,N__NAME)];
    }
    map<tuple<int64_t,int64_t,int64_t>,
        double>::iterator qORDERS2NATION1_it602 = qORDERS2NATION1.begin();
    map<tuple<int64_t,int64_t,int64_t>,double>::iterator qORDERS2NATION1_end601 
        = qORDERS2NATION1.end();
    for (
        ; qORDERS2NATION1_it602 != qORDERS2NATION1_end601; ++qORDERS2NATION1_it602)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2NATION1_it602->first);
        int64_t x_qORDERS_O__CUSTKEY = get<2>(qORDERS2NATION1_it602->first);
        qORDERS2NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,x_qORDERS_O__CUSTKEY)] += 
            -1*qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY)]*qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS2REGION1_it604 = qORDERS2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS2REGION1_end603 = qORDERS2REGION1.end();
    for (
        ; qORDERS2REGION1_it604 != qORDERS2REGION1_end603; ++qORDERS2REGION1_it604)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2REGION1_it604->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2REGION1_it604->first);
        string N__NAME = get<2>(qORDERS2REGION1_it604->first);
        int64_t x_qORDERS2REGION_R__REGIONKEY = get<3>(
            qORDERS2REGION1_it604->first);
        qORDERS2REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,N__NAME,
            x_qORDERS2REGION_R__REGIONKEY)] += -1*qCUSTOMER2ORDERS1SUPPLIER1[make_tuple(
            x_qORDERS_O__ORDERKEY,SUPPKEY)]*qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,N__NAME,x_qORDERS2REGION_R__REGIONKEY)];
    }
    map<tuple<string,int64_t>,double>::iterator qREGION1_it606 = qREGION1.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION1_end605 = qREGION1.end(
        );
    for (; qREGION1_it606 != qREGION1_end605; ++qREGION1_it606)
    {
        string N__NAME = get<0>(qREGION1_it606->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION1_it606->first);
        qREGION1[make_tuple(
            N__NAME,x_qREGION_R__REGIONKEY)] += -1*qSUPPLIER1REGION1[make_tuple(
            SUPPKEY,NATIONKEY,N__NAME,x_qREGION_R__REGIONKEY)];
    }
    map<tuple<string,int64_t>,double>::iterator qREGION2_it608 = qREGION2.begin(
        );
    map<tuple<string,int64_t>,double>::iterator qREGION2_end607 = qREGION2.end(
        );
    for (; qREGION2_it608 != qREGION2_end607; ++qREGION2_it608)
    {
        string N__NAME = get<0>(qREGION2_it608->first);
        int64_t x_qREGION_R__REGIONKEY = get<1>(qREGION2_it608->first);
        qREGION2[make_tuple(
            N__NAME,x_qREGION_R__REGIONKEY)] += -1*qSUPPLIER2REGION1[make_tuple(
            SUPPKEY,NATIONKEY,N__NAME,x_qREGION_R__REGIONKEY)];
    }
    gettimeofday(&hend, NULL);
    DBToaster::Profiler::accumulate_time_span(
        hstart, hend, on_delete_SUPPLIER_sec_span, on_delete_SUPPLIER_usec_span);
}

void on_delete_NATION(
    ofstream* results, ofstream* log, ofstream* stats, int64_t NATIONKEY,string 
    NAME,int64_t REGIONKEY,string COMMENT)
{
    struct timeval hstart, hend;
    gettimeofday(&hstart, NULL);
    q[NAME] += -1*qNATION1[NATIONKEY]*qNATION2[REGIONKEY];
    q[NAME] += -1*qNATION3[NATIONKEY]*qNATION2[REGIONKEY]*-1;
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_it610 = 
        qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1_end609 = 
        qCUSTOMER1.end();
    for (; qCUSTOMER1_it610 != qCUSTOMER1_end609; ++qCUSTOMER1_it610)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1_it610->first);
        qCUSTOMER1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,NAME)] += -1*qCUSTOMER1NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string>,
        int>::iterator qCUSTOMER1LINEITEM2_it612 = qCUSTOMER1LINEITEM2.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qCUSTOMER1LINEITEM2_end611 
        = qCUSTOMER1LINEITEM2.end();
    for (
        ; qCUSTOMER1LINEITEM2_it612 != qCUSTOMER1LINEITEM2_end611; 
        ++qCUSTOMER1LINEITEM2_it612)
    {
        int64_t x_qCUSTOMER1LINEITEM_L__SUPPKEY = get<0>(
            qCUSTOMER1LINEITEM2_it612->first);
        qCUSTOMER1LINEITEM2[make_tuple(
            x_qCUSTOMER1LINEITEM_L__SUPPKEY,NATIONKEY,NAME)] += 
            -1*qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            x_qCUSTOMER1LINEITEM_L__SUPPKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER1ORDERS1_it614 
        = qCUSTOMER1ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER1ORDERS1_end613 = qCUSTOMER1ORDERS1.end();
    for (
        ; qCUSTOMER1ORDERS1_it614 != qCUSTOMER1ORDERS1_end613; 
        ++qCUSTOMER1ORDERS1_it614)
    {
        int64_t x_qCUSTOMER1ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER1ORDERS1_it614->first);
        qCUSTOMER1ORDERS1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,NATIONKEY,NAME)] += 
            -1*qCUSTOMER1ORDERS1NATION1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1ORDERS1REGION1_it616 = qCUSTOMER1ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1ORDERS1REGION1_end615 = qCUSTOMER1ORDERS1REGION1.end();
    for (
        ; qCUSTOMER1ORDERS1REGION1_it616 != qCUSTOMER1ORDERS1REGION1_end615; 
        ++qCUSTOMER1ORDERS1REGION1_it616)
    {
        int64_t x_qCUSTOMER1ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER1ORDERS1REGION1_it616->first);
        qCUSTOMER1ORDERS1REGION1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,NATIONKEY,NAME,REGIONKEY)] += 
            -1*qCUSTOMER1ORDERS1NATION1[make_tuple(
            x_qCUSTOMER1ORDERS_O__ORDERKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qCUSTOMER1ORDERS1REGION1LINEITEM1_it618 = 
        qCUSTOMER1ORDERS1REGION1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qCUSTOMER1ORDERS1REGION1LINEITEM1_end617 = 
        qCUSTOMER1ORDERS1REGION1LINEITEM1.end();
    for (
        ; qCUSTOMER1ORDERS1REGION1LINEITEM1_it618 != 
        qCUSTOMER1ORDERS1REGION1LINEITEM1_end617; 
        ++qCUSTOMER1ORDERS1REGION1LINEITEM1_it618)
    {
        int64_t x_qCUSTOMER1ORDERS1REGION1LINEITEM_L__SUPPKEY = get<0>(
            qCUSTOMER1ORDERS1REGION1LINEITEM1_it618->first);
        qCUSTOMER1ORDERS1REGION1LINEITEM1[make_tuple(
            x_qCUSTOMER1ORDERS1REGION1LINEITEM_L__SUPPKEY,NATIONKEY,NAME,REGIONKEY)] += 
            -1*qCUSTOMER1ORDERS1NATION1LINEITEM1[make_tuple(
            x_qCUSTOMER1ORDERS1REGION1LINEITEM_L__SUPPKEY,NATIONKEY)];
    }
    qCUSTOMER1ORDERS1REGION1SUPPLIER2[make_tuple(
        NATIONKEY,NAME,REGIONKEY)] += -1;
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1REGION1_it620 = qCUSTOMER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER1REGION1_end619 = qCUSTOMER1REGION1.end();
    for (
        ; qCUSTOMER1REGION1_it620 != qCUSTOMER1REGION1_end619; 
        ++qCUSTOMER1REGION1_it620)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER1REGION1_it620->first);
        qCUSTOMER1REGION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,NAME,REGIONKEY)] += 
            -1*qCUSTOMER1NATION1[make_tuple(x_qCUSTOMER_C__CUSTKEY,NATIONKEY)];
    }
    qCUSTOMER1SUPPLIER2[make_tuple(NATIONKEY,NAME)] += -1*qNATION2[REGIONKEY];
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_it622 = 
        qCUSTOMER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2_end621 = 
        qCUSTOMER2.end();
    for (; qCUSTOMER2_it622 != qCUSTOMER2_end621; ++qCUSTOMER2_it622)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2_it622->first);
        qCUSTOMER2[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,NAME)] += -1*qCUSTOMER2NATION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qCUSTOMER2ORDERS1_it624 
        = qCUSTOMER2ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,
        double>::iterator qCUSTOMER2ORDERS1_end623 = qCUSTOMER2ORDERS1.end();
    for (
        ; qCUSTOMER2ORDERS1_it624 != qCUSTOMER2ORDERS1_end623; 
        ++qCUSTOMER2ORDERS1_it624)
    {
        int64_t x_qCUSTOMER2ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER2ORDERS1_it624->first);
        qCUSTOMER2ORDERS1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,NATIONKEY,NAME)] += 
            -1*qCUSTOMER2ORDERS1NATION1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2ORDERS1REGION1_it626 = qCUSTOMER2ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2ORDERS1REGION1_end625 = qCUSTOMER2ORDERS1REGION1.end();
    for (
        ; qCUSTOMER2ORDERS1REGION1_it626 != qCUSTOMER2ORDERS1REGION1_end625; 
        ++qCUSTOMER2ORDERS1REGION1_it626)
    {
        int64_t x_qCUSTOMER2ORDERS_O__ORDERKEY = get<0>(
            qCUSTOMER2ORDERS1REGION1_it626->first);
        qCUSTOMER2ORDERS1REGION1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,NATIONKEY,NAME,REGIONKEY)] += 
            -1*qCUSTOMER2ORDERS1NATION1[make_tuple(
            x_qCUSTOMER2ORDERS_O__ORDERKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2REGION1_it628 = qCUSTOMER2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qCUSTOMER2REGION1_end627 = qCUSTOMER2REGION1.end();
    for (
        ; qCUSTOMER2REGION1_it628 != qCUSTOMER2REGION1_end627; 
        ++qCUSTOMER2REGION1_it628)
    {
        int64_t x_qCUSTOMER_C__CUSTKEY = get<0>(qCUSTOMER2REGION1_it628->first);
        qCUSTOMER2REGION1[make_tuple(
            x_qCUSTOMER_C__CUSTKEY,NATIONKEY,NAME,REGIONKEY)] += 
            -1*qCUSTOMER2NATION1[make_tuple(x_qCUSTOMER_C__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_it630 = 
        qLINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1_end629 = 
        qLINEITEM1.end();
    for (; qLINEITEM1_it630 != qLINEITEM1_end629; ++qLINEITEM1_it630)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(qLINEITEM1_it630->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1_it630->first);
        qLINEITEM1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,NAME)] += 
            -1*qLINEITEM1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,x_qLINEITEM_L__SUPPKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1ORDERS1_it632 = 
        qLINEITEM1ORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1ORDERS1_end631 = 
        qLINEITEM1ORDERS1.end();
    for (
        ; qLINEITEM1ORDERS1_it632 != qLINEITEM1ORDERS1_end631; 
        ++qLINEITEM1ORDERS1_it632)
    {
        int64_t x_qLINEITEM1ORDERS_O__CUSTKEY = get<0>(
            qLINEITEM1ORDERS1_it632->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1ORDERS1_it632->first);
        qLINEITEM1ORDERS1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,x_qLINEITEM_L__SUPPKEY,NAME)] += 
            -1*qLINEITEM1ORDERS1NATION1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,NATIONKEY,
            x_qLINEITEM_L__SUPPKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1REGION1_it634 = qLINEITEM1ORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1ORDERS1REGION1_end633 = qLINEITEM1ORDERS1REGION1.end();
    for (
        ; qLINEITEM1ORDERS1REGION1_it634 != qLINEITEM1ORDERS1REGION1_end633; 
        ++qLINEITEM1ORDERS1REGION1_it634)
    {
        int64_t x_qLINEITEM1ORDERS_O__CUSTKEY = get<0>(
            qLINEITEM1ORDERS1REGION1_it634->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(
            qLINEITEM1ORDERS1REGION1_it634->first);
        qLINEITEM1ORDERS1REGION1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,x_qLINEITEM_L__SUPPKEY,NAME,REGIONKEY)] += 
            -1*qLINEITEM1ORDERS1NATION1[make_tuple(
            x_qLINEITEM1ORDERS_O__CUSTKEY,NATIONKEY,x_qLINEITEM_L__SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1REGION1_it636 = qLINEITEM1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1REGION1_end635 = qLINEITEM1REGION1.end();
    for (
        ; qLINEITEM1REGION1_it636 != qLINEITEM1REGION1_end635; 
        ++qLINEITEM1REGION1_it636)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1REGION1_it636->first);
        int64_t x_qLINEITEM_L__SUPPKEY = get<1>(qLINEITEM1REGION1_it636->first);
        qLINEITEM1REGION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,x_qLINEITEM_L__SUPPKEY,NAME,REGIONKEY)] += 
            -1*qLINEITEM1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,x_qLINEITEM_L__SUPPKEY)];
    }
    map<tuple<int64_t,int64_t,string>,
        int>::iterator qLINEITEM1SUPPLIER1_it638 = qLINEITEM1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qLINEITEM1SUPPLIER1_end637 
        = qLINEITEM1SUPPLIER1.end();
    for (
        ; qLINEITEM1SUPPLIER1_it638 != qLINEITEM1SUPPLIER1_end637; 
        ++qLINEITEM1SUPPLIER1_it638)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1SUPPLIER1_it638->first);
        qLINEITEM1SUPPLIER1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,NAME)] += 
            -1*qLINEITEM1SUPPLIER1NATION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1SUPPLIER1REGION1_it640 = qLINEITEM1SUPPLIER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qLINEITEM1SUPPLIER1REGION1_end639 = qLINEITEM1SUPPLIER1REGION1.end();
    for (
        ; qLINEITEM1SUPPLIER1REGION1_it640 != qLINEITEM1SUPPLIER1REGION1_end639; 
        ++qLINEITEM1SUPPLIER1REGION1_it640)
    {
        int64_t x_qLINEITEM_L__ORDERKEY = get<0>(
            qLINEITEM1SUPPLIER1REGION1_it640->first);
        qLINEITEM1SUPPLIER1REGION1[make_tuple(
            x_qLINEITEM_L__ORDERKEY,NATIONKEY,NAME,REGIONKEY)] += 
            -1*qLINEITEM1SUPPLIER1NATION1[make_tuple(x_qLINEITEM_L__ORDERKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_it642 = 
        qORDERS1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS1_end641 = 
        qORDERS1.end();
    for (; qORDERS1_it642 != qORDERS1_end641; ++qORDERS1_it642)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1_it642->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1_it642->first);
        qORDERS1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,NAME)] += 
            -1*qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,x_qORDERS_O__CUSTKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1LINEITEM1_it644 = 
        qORDERS1LINEITEM1.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1LINEITEM1_end643 = 
        qORDERS1LINEITEM1.end();
    for (
        ; qORDERS1LINEITEM1_it644 != qORDERS1LINEITEM1_end643; 
        ++qORDERS1LINEITEM1_it644)
    {
        int64_t x_qORDERS1LINEITEM_L__SUPPKEY = get<0>(
            qORDERS1LINEITEM1_it644->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1LINEITEM1_it644->first);
        qORDERS1LINEITEM1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,x_qORDERS_O__CUSTKEY,NAME)] += 
            -1*qORDERS1LINEITEM1NATION1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,NATIONKEY,
            x_qORDERS_O__CUSTKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1_it646 = qORDERS1LINEITEM1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1_end645 = qORDERS1LINEITEM1REGION1.end();
    for (
        ; qORDERS1LINEITEM1REGION1_it646 != qORDERS1LINEITEM1REGION1_end645; 
        ++qORDERS1LINEITEM1REGION1_it646)
    {
        int64_t x_qORDERS1LINEITEM_L__SUPPKEY = get<0>(
            qORDERS1LINEITEM1REGION1_it646->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(
            qORDERS1LINEITEM1REGION1_it646->first);
        qORDERS1LINEITEM1REGION1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,x_qORDERS_O__CUSTKEY,NAME,REGIONKEY)] += 
            -1*qORDERS1LINEITEM1NATION1[make_tuple(
            x_qORDERS1LINEITEM_L__SUPPKEY,NATIONKEY,x_qORDERS_O__CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1SUPPLIER1_it648 = 
        qORDERS1LINEITEM1REGION1SUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,int>::iterator 
        qORDERS1LINEITEM1REGION1SUPPLIER1_end647 = 
        qORDERS1LINEITEM1REGION1SUPPLIER1.end();
    for (
        ; qORDERS1LINEITEM1REGION1SUPPLIER1_it648 != 
        qORDERS1LINEITEM1REGION1SUPPLIER1_end647; 
        ++qORDERS1LINEITEM1REGION1SUPPLIER1_it648)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<0>(
            qORDERS1LINEITEM1REGION1SUPPLIER1_it648->first);
        qORDERS1LINEITEM1REGION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,NAME,REGIONKEY)] += 
            -1*qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS1REGION1_it650 = qORDERS1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS1REGION1_end649 = qORDERS1REGION1.end();
    for (
        ; qORDERS1REGION1_it650 != qORDERS1REGION1_end649; ++qORDERS1REGION1_it650)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS1REGION1_it650->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS1REGION1_it650->first);
        qORDERS1REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,NAME,REGIONKEY)] += 
            -1*qORDERS1NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,x_qORDERS_O__CUSTKEY)];
    }
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1SUPPLIER2_it652 = 
        qORDERS1SUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,int>::iterator qORDERS1SUPPLIER2_end651 = 
        qORDERS1SUPPLIER2.end();
    for (
        ; qORDERS1SUPPLIER2_it652 != qORDERS1SUPPLIER2_end651; 
        ++qORDERS1SUPPLIER2_it652)
    {
        int64_t x_qORDERS_O__CUSTKEY = get<0>(qORDERS1SUPPLIER2_it652->first);
        qORDERS1SUPPLIER2[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY,NAME)] += 
            -1*qORDERS1LINEITEM1NATION1SUPPLIER1[make_tuple(
            x_qORDERS_O__CUSTKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_it654 = 
        qORDERS2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qORDERS2_end653 = 
        qORDERS2.end();
    for (; qORDERS2_it654 != qORDERS2_end653; ++qORDERS2_it654)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2_it654->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2_it654->first);
        qORDERS2[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,NAME)] += 
            -1*qORDERS2NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,x_qORDERS_O__CUSTKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS2REGION1_it656 = qORDERS2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qORDERS2REGION1_end655 = qORDERS2REGION1.end();
    for (
        ; qORDERS2REGION1_it656 != qORDERS2REGION1_end655; ++qORDERS2REGION1_it656)
    {
        int64_t x_qORDERS_O__ORDERKEY = get<0>(qORDERS2REGION1_it656->first);
        int64_t x_qORDERS_O__CUSTKEY = get<1>(qORDERS2REGION1_it656->first);
        qORDERS2REGION1[make_tuple(
            x_qORDERS_O__ORDERKEY,x_qORDERS_O__CUSTKEY,NAME,REGIONKEY)] += 
            -1*qORDERS2NATION1[make_tuple(
            x_qORDERS_O__ORDERKEY,NATIONKEY,x_qORDERS_O__CUSTKEY)];
    }
    qREGION1[make_tuple(NAME,REGIONKEY)] += -1*qNATION1[NATIONKEY];
    qREGION2[make_tuple(NAME,REGIONKEY)] += -1*qNATION3[NATIONKEY];
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_it658 = 
        qSUPPLIER1.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER1_end657 = 
        qSUPPLIER1.end();
    for (; qSUPPLIER1_it658 != qSUPPLIER1_end657; ++qSUPPLIER1_it658)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1_it658->first);
        qSUPPLIER1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY,NAME)] += -1*qSUPPLIER1NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER1REGION1_it660 = qSUPPLIER1REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER1REGION1_end659 = qSUPPLIER1REGION1.end();
    for (
        ; qSUPPLIER1REGION1_it660 != qSUPPLIER1REGION1_end659; 
        ++qSUPPLIER1REGION1_it660)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER1REGION1_it660->first);
        qSUPPLIER1REGION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY,NAME,REGIONKEY)] += 
            -1*qSUPPLIER1NATION1[make_tuple(x_qSUPPLIER_S__SUPPKEY,NATIONKEY)];
    }
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_it662 = 
        qSUPPLIER2.begin();
    map<tuple<int64_t,int64_t,string>,double>::iterator qSUPPLIER2_end661 = 
        qSUPPLIER2.end();
    for (; qSUPPLIER2_it662 != qSUPPLIER2_end661; ++qSUPPLIER2_it662)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2_it662->first);
        qSUPPLIER2[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY,NAME)] += -1*qSUPPLIER2NATION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY)]*qNATION2[REGIONKEY];
    }
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER2REGION1_it664 = qSUPPLIER2REGION1.begin();
    map<tuple<int64_t,int64_t,string,int64_t>,double>::iterator 
        qSUPPLIER2REGION1_end663 = qSUPPLIER2REGION1.end();
    for (
        ; qSUPPLIER2REGION1_it664 != qSUPPLIER2REGION1_end663; 
        ++qSUPPLIER2REGION1_it664)
    {
        int64_t x_qSUPPLIER_S__SUPPKEY = get<0>(qSUPPLIER2REGION1_it664->first);
        qSUPPLIER2REGION1[make_tuple(
            x_qSUPPLIER_S__SUPPKEY,NATIONKEY,NAME,REGIONKEY)] += 
            -1*qSUPPLIER2NATION1[make_tuple(x_qSUPPLIER_S__SUPPKEY,NATIONKEY)];
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

DBToaster::DemoDatasets::LineitemStream SSBLineitem("/home/yanif/datasets/tpch/sf1/singlefile/lineitem.tbl.a",&DBToaster::DemoDatasets::parseLineitemField,16,65000000,512);

boost::shared_ptr<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor> SSBLineitem_adaptor(new DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor());
static int streamSSBLineitemId = 3;

struct on_insert_LINEITEM_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result>(data); 
        on_insert_LINEITEM(results, log, stats, input.orderkey,input.partkey,input.suppkey,input.linenumber,input.quantity,input.extendedprice,input.discount,input.tax);
    }
};

on_insert_LINEITEM_fun_obj fo_on_insert_LINEITEM_3;

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

struct on_delete_CUSTOMER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::CustomerTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::CustomerTupleAdaptor::Result>(data); 
        on_delete_CUSTOMER(results, log, stats, input.custkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.mktsegment,input.comment);
    }
};

on_delete_CUSTOMER_fun_obj fo_on_delete_CUSTOMER_7;

struct on_delete_ORDERS_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::OrderTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::OrderTupleAdaptor::Result>(data); 
        on_delete_ORDERS(results, log, stats, input.orderkey,input.custkey,input.orderstatus,input.totalprice,input.orderdate,input.orderpriority,input.clerk,input.shippriority,input.comment);
    }
};

on_delete_ORDERS_fun_obj fo_on_delete_ORDERS_8;

struct on_delete_LINEITEM_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor::Result>(data); 
        on_delete_LINEITEM(results, log, stats, input.orderkey,input.partkey,input.suppkey,input.linenumber,input.quantity,input.extendedprice,input.discount,input.tax);
    }
};

on_delete_LINEITEM_fun_obj fo_on_delete_LINEITEM_9;

struct on_delete_SUPPLIER_fun_obj { 
    void operator()(boost::any data, ofstream* results, ofstream* log, ofstream* stats) { 
        DBToaster::DemoDatasets::SupplierTupleAdaptor::Result input = 
            boost::any_cast<DBToaster::DemoDatasets::SupplierTupleAdaptor::Result>(data); 
        on_delete_SUPPLIER(results, log, stats, input.suppkey,input.name,input.address,input.nationkey,input.phone,input.acctbal,input.comment);
    }
};

on_delete_SUPPLIER_fun_obj fo_on_delete_SUPPLIER_10;

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
    sources.addStream<DBToaster::DemoDatasets::customer>(&SSBCustomer, boost::ref(*SSBCustomer_adaptor), streamSSBCustomerId);
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_CUSTOMER_1));
    sources.addStream<DBToaster::DemoDatasets::order>(&SSBOrder, boost::ref(*SSBOrder_adaptor), streamSSBOrderId);
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_ORDERS_2));
    sources.addStream<DBToaster::DemoDatasets::lineitem>(&SSBLineitem, boost::ref(*SSBLineitem_adaptor), streamSSBLineitemId);
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_LINEITEM_3));
    sources.addStream<DBToaster::DemoDatasets::supplier>(&SSBSupplier, boost::ref(*SSBSupplier_adaptor), streamSSBSupplierId);
    router.addHandler(streamSSBSupplierId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_SUPPLIER_4));
    sources.addStream<DBToaster::DemoDatasets::nation>(&SSBNation, boost::ref(*SSBNation_adaptor), streamSSBNationId);
    router.addHandler(streamSSBNationId,DBToaster::StandaloneEngine::insertTuple,boost::ref(fo_on_insert_NATION_5));
    router.addHandler(streamSSBRegionId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_REGION_6));
    router.addHandler(streamSSBCustomerId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_CUSTOMER_7));
    router.addHandler(streamSSBOrderId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_ORDERS_8));
    router.addHandler(streamSSBLineitemId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_LINEITEM_9));
    router.addHandler(streamSSBSupplierId,DBToaster::StandaloneEngine::deleteTuple,boost::ref(fo_on_delete_SUPPLIER_10));
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
