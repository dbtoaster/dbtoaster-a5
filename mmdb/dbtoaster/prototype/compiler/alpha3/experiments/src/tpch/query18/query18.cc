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

map<int64_t,double,std::less<int64_t 
    >,boost::pool_allocator<pair<int64_t,double> > > q;

map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
    >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > > qCUSTOMER1;

map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
    >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > > 
    qCUSTOMER1ORDERS1;

// Note this is a redundant map -- equivalent to qLINEITEM1CUSTOMER1 below,
// but the duplicate is not detected due to differences in implicit/explicit
// representation of constraints as either bound vars in relation schemas,
// or additional constraint formula in the calculus representation.
// See log for details.
map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> 
    >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > > 
    qCUSTOMER1LINEITEM1;

map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> 
    >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > > qCUSTOMER2;

map<int64_t,double,std::less<int64_t 
    >,boost::pool_allocator<pair<int64_t,double> > > qCUSTOMER3;

map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
    >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > > qCUSTOMER4;

map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> 
    >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > > 
    qCUSTOMER4LINEITEM1;

map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
    >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > > 
    qCUSTOMER4ORDERS1;

map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
    >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > > qORDERS1;

map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > > 
    qORDERS4;


multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>, 
    std::less<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >, 
    boost::pool_allocator<tuple<int64_t,int64_t,int64_t,int,double,double,double,
    double> > > LINEITEM;

map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> 
    >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > > qLINEITEM1;

// See note above regarding duplication of this map.
map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> 
    >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > > 
    qLINEITEM1CUSTOMER1;


// Bigsum var domains.
set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> > 
    bigsum_L1__ORDERKEY_dom;

set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> > 
    bigsum_L2__ORDERKEY_dom;

// Accumulation temporaries
int cstr615;
int cstr103;
int cstr564566572;
double q324;
int cstr564568601;
double q452;
int cstr448;
int cstr402;
int cstr169173199;
int cstr4041;
int cstr427428;
int cstr4042;
int cstr303306317;
int cstr427429;
int cstr351354365;
int cstr182129;
int cstr97;
int cstr4043;
int cstr402403;
int cstr402404;
int cstr402405;
int cstr404351;
int cstr103104;
double q376;
int cstr103105;
int cstr326;
int cstr125126;
int cstr103106;
int cstr454;
int cstr125127;
int cstr58;
int cstr818492;
int cstr402405416;
int cstr125128;
int cstr12;
int cstr454455;
int cstr427430;
int cstr125129;
int cstr13;
int cstr454456;
int cstr14;
int cstr454457;
int cstr454458;
int cstr6465;
int cstr509513546;
int cstr6466;
double q507;
int cstr372;
int cstr351353359;
int cstr6467;
int cstr18;
int cstr509511517;
int cstr427430441;
int cstr216217;
int cstr169170;
int cstr103106114;
int cstr216218;
int cstr169171;
int cstr509512533;
int cstr379;
int cstr216219;
int cstr169172;
int cstr1819;
double q426;
int cstr64;
int cstr169173;
int cstr303305311;
double q61;
int cstr351352;
int cstr351353;
int cstr454457478;
int cstr351354;
int cstr121;
int cstr454458491;
int cstr379380;
int cstr169;
double q215;
int cstr564565;
int cstr379381;
int cstr564566;
int cstr509;
int cstr379382;
int cstr564567;
int cstr421;
int cstr125;
int cstr564568;
int cstr454456462;
int cstr326329340;
int cstr216220;
int cstr1820;
int cstr402404410;
int cstr1821;
double q301;
int cstr210;
int cstr427;
int cstr259;
int cstr326327;
int cstr326328;
int cstr558;
int cstr326329;
double q563;
int cstr216;
int cstr345;
int cstr509510;
double q350;
int cstr509511;
int cstr216220246;
int cstr509512;
int cstr326328334;
int cstr509513;
int cstr303;
int cstr34;
int cstr125129155;
int cstr379382393;
int cstr1;
int cstr564;
int cstr427429435;
double q39;
int cstr81;
int cstr351;
double q102;
double q400;
int cstr40;
int cstr1412;
int cstr379381387;
int cstr303304;
int cstr8182;
int cstr646775;
int cstr303305;
int cstr8183;
int cstr303306;
int cstr8184;
int cstr564567588;

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
   cout << "qCUSTOMER1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type, _Select1st<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type>, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type, _Select1st<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type>, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_compare>))) << endl;

   cout << "qCUSTOMER2 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::key_type, map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::value_type, _Select1st<map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::value_type>, map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER2" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER2.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::key_type, map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::value_type, _Select1st<map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::value_type>, map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::key_compare>))) << endl;

   cout << "qCUSTOMER3 size: " << (((sizeof(map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_type)
       + sizeof(map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3.size())  + (sizeof(struct _Rb_tree<map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_type, map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::value_type, _Select1st<map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::value_type>, map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER3" << "," << (((sizeof(map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_type)
       + sizeof(map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER3.size())  + (sizeof(struct _Rb_tree<map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_type, map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::value_type, _Select1st<map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::value_type>, map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_compare>))) << endl;

   cout << "qCUSTOMER4 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER4.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type, _Select1st<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type>, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER4" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER4.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type, _Select1st<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type>, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_compare>))) << endl;

   cout << "bigsum_L1__ORDERKEY_dom size: " << (((sizeof(set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_L1__ORDERKEY_dom.size())  + (sizeof(struct _Rb_tree<set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::key_type, set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type, _Identity<set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type>, set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::key_compare>))) << endl;

   (*stats) << "m," << "bigsum_L1__ORDERKEY_dom" << "," << (((sizeof(set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_L1__ORDERKEY_dom.size())  + (sizeof(struct _Rb_tree<set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::key_type, set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type, _Identity<set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type>, set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::key_compare>))) << endl;

   cout << "qCUSTOMER4LINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER4LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_type, map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::value_type>, map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER4LINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER4LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_type, map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::value_type>, map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_compare>))) << endl;

   cout << "bigsum_L2__ORDERKEY_dom size: " << (((sizeof(set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_L2__ORDERKEY_dom.size())  + (sizeof(struct _Rb_tree<set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::key_type, set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type, _Identity<set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type>, set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::key_compare>))) << endl;

   (*stats) << "m," << "bigsum_L2__ORDERKEY_dom" << "," << (((sizeof(set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * bigsum_L2__ORDERKEY_dom.size())  + (sizeof(struct _Rb_tree<set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::key_type, set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type, _Identity<set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::value_type>, set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::key_compare>))) << endl;

   cout << "qCUSTOMER4ORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER4ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type, _Select1st<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type>, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER4ORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER4ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type, _Select1st<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type>, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_compare>))) << endl;

   cout << "q size: " << (((sizeof(map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_type)
       + sizeof(map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_type, map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::value_type, _Select1st<map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::value_type>, map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_compare>))) << endl;

   (*stats) << "m," << "q" << "," << (((sizeof(map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_type)
       + sizeof(map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * q.size())  + (sizeof(struct _Rb_tree<map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_type, map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::value_type, _Select1st<map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::value_type>, map<int64_t,double,std::less<int64_t >,boost::pool_allocator<pair<int64_t,double> > >::key_compare>))) << endl;

   cout << "qORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type, _Select1st<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type>, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type, _Select1st<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type>, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_compare>))) << endl;

   cout << "qORDERS4 size: " << (((sizeof(map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_type)
       + sizeof(map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS4.size())  + (sizeof(struct _Rb_tree<map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_type, map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::value_type, _Select1st<map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::value_type>, map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_compare>))) << endl;

   (*stats) << "m," << "qORDERS4" << "," << (((sizeof(map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_type)
       + sizeof(map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qORDERS4.size())  + (sizeof(struct _Rb_tree<map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_type, map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::value_type, _Select1st<map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::value_type>, map<int64_t,int,std::less<int64_t >,boost::pool_allocator<pair<int64_t,int> > >::key_compare>))) << endl;

   cout << "qCUSTOMER1ORDERS1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type, _Select1st<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type>, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1ORDERS1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1ORDERS1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_type, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type, _Select1st<map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::value_type>, map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::key_compare>))) << endl;

   cout << "qLINEITEM1CUSTOMER1 size: " << (((sizeof(map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1CUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::key_type, map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::value_type, _Select1st<map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::value_type>, map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1CUSTOMER1" << "," << (((sizeof(map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1CUSTOMER1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::key_type, map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::value_type, _Select1st<map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::value_type>, map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::key_compare>))) << endl;

   cout << "qLINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_type, map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::value_type>, map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_compare>))) << endl;

   (*stats) << "m," << "qLINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qLINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_type, map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::value_type>, map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_compare>))) << endl;

   cout << "LINEITEM size: " << (((sizeof(multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>, std::less<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >, boost::pool_allocator<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> > >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * LINEITEM.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>, std::less<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >, boost::pool_allocator<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> > >::key_type, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>, std::less<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >, boost::pool_allocator<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> > >::value_type, _Identity<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>, std::less<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >, boost::pool_allocator<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> > >::value_type>, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>, std::less<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >, boost::pool_allocator<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> > >::key_compare>))) << endl;

   (*stats) << "m," << "LINEITEM" << "," << (((sizeof(multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>, std::less<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >, boost::pool_allocator<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> > >::value_type)
       + sizeof(struct _Rb_tree_node_base))
       * LINEITEM.size())  + (sizeof(struct _Rb_tree<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>, std::less<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >, boost::pool_allocator<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> > >::key_type, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>, std::less<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >, boost::pool_allocator<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> > >::value_type, _Identity<multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>, std::less<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >, boost::pool_allocator<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> > >::value_type>, multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>, std::less<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >, boost::pool_allocator<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> > >::key_compare>))) << endl;

   cout << "qCUSTOMER1LINEITEM1 size: " << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_type, map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::value_type>, map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_compare>))) << endl;

   (*stats) << "m," << "qCUSTOMER1LINEITEM1" << "," << (((sizeof(map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_type)
       + sizeof(map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::mapped_type)
       + sizeof(struct _Rb_tree_node_base))
       * qCUSTOMER1LINEITEM1.size())  + (sizeof(struct _Rb_tree<map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_type, map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::value_type, _Select1st<map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::value_type>, map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::key_compare>))) << endl;

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
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>,
            std::less<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >, 
            boost::pool_allocator<tuple<int64_t,int64_t,int64_t,int,double,double,double,
            double> > >::iterator LINEITEM_it2 = LINEITEM.begin();
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>,
            std::less<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >, 
            boost::pool_allocator<tuple<int64_t,int64_t,int64_t,int,double,double,double,
            double> > >::iterator LINEITEM_end1 = LINEITEM.end();
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
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>,
            std::less<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >, 
            boost::pool_allocator<tuple<int64_t,int64_t,int64_t,int,double,double,double,
            double> > >::iterator LINEITEM_it4 = LINEITEM.begin();
        multiset<tuple<int64_t,int64_t,int64_t,int,double,double,double,double>,
            std::less<tuple<int64_t,int64_t,int64_t,int,double,double,double,double> >, 
            boost::pool_allocator<tuple<int64_t,int64_t,int64_t,int,double,double,double,
            double> > >::iterator LINEITEM_end3 = LINEITEM.end();
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

    map<int64_t,double,std::less<int64_t>,
        boost::pool_allocator<pair<int64_t,double> > >::iterator q_it28 = q.begin();
    map<int64_t,double,std::less<int64_t>,
        boost::pool_allocator<pair<int64_t,double> > >::iterator q_end27 = q.end();
    for (; q_it28 != q_end27; ++q_it28)
    {
        int64_t C__CUSTKEY = q_it28->first;
        q215 = 0.0;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it26 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end25 = bigsum_L1__ORDERKEY_dom.end();
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
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it6 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end5 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it6 != bigsum_L2__ORDERKEY_dom_end5; 
                ++bigsum_L2__ORDERKEY_dom_it6)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it6;
                cstr216 += ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY]?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0);
            }

            // new.2 (dt.1): sum_{bigsum_l2__orderkey} if new then dt else 0
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it10 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L1__ORDERKEY == ORDERKEY &&
                 (bigsum_L2__ORDERKEY_dom_it10 != bigsum_L2__ORDERKEY_dom.end()) )
            {
                cstr216218 += (100 < qCUSTOMER3[ORDERKEY]+QUANTITY? 1 : 0);
            }


            // new.3 (dt.2): sum_{bigsum_l2__orderkey} if new and not old then t else 0
            cstr216 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it14 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it14 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr216 += (
                    (100 < qCUSTOMER3[ORDERKEY]+QUANTITY) && ( qCUSTOMER3[ORDERKEY] <= 100 )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0 );
            }

            // new.4 (dt.3): sum_{bigsum_l2__orderkey} if not new and old then -t else 0
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it18 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it18 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr216 += -1*(
                    ( (qCUSTOMER3[ORDERKEY]+QUANTITY <= 100 ) && ( 100 < qCUSTOMER3[ORDERKEY] ) )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0 );
            }

            // old: sum_{bigsum_l2__orderkey} t
            cstr259 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it22 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L2__ORDERKEY_dom_end21 = bigsum_L2__ORDERKEY_dom.end();
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

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it48 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end47 = q.end();
    for (; q_it48 != q_end47; ++q_it48)
    {
        int64_t C__CUSTKEY = q_it48->first;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it46 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end45 = bigsum_L1__ORDERKEY_dom.end();
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
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it30 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end29 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it30 != bigsum_L2__ORDERKEY_dom_end29; 
                ++bigsum_L2__ORDERKEY_dom_it30)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it30;
                cstr125 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0 );
            }

            // new.2 (dt.1): sum_{bigsum_l2__orderkey} if new then dt else 0
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it34 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);
            if ( bigsum_L1__ORDERKEY == ORDERKEY &&
                 ( bigsum_L2__ORDERKEY_dom_it34 != bigsum_L2__ORDERKEY_dom.end() ) )
            {
                cstr125 += (100 < qCUSTOMER3[ORDERKEY]+QUANTITY? 1 : 0);
            }


            // new.3 (dt.2): sum_{bigsum_l2__orderkey} if new and not old then t else 0
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it38 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it38 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr125 += ( ( (100 < qCUSTOMER3[ORDERKEY]+QUANTITY)
                               && ( qCUSTOMER3[ORDERKEY] <= 100 ) )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0);
            }

            // new.4 (dt.3): sum_{bigsum_l2__orderkey} if not new and old then -t else 0
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it42 = bigsum_L2__ORDERKEY_dom.begin();

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

    map<int64_t,double,std::less<int64_t>,
        boost::pool_allocator<pair<int64_t,double> > >::iterator q_it72 = q.begin();
    map<int64_t,double,std::less<int64_t>,
        boost::pool_allocator<pair<int64_t,double> > >::iterator q_end71 = q.end();
    for (; q_it72 != q_end71; ++q_it72)
    {
        int64_t C__CUSTKEY = q_it72->first;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it70 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end69 = bigsum_L1__ORDERKEY_dom.end();
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
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it50 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end49 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it50 != bigsum_L2__ORDERKEY_dom_end49; 
                ++bigsum_L2__ORDERKEY_dom_it50)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it50;
                cstr169 += ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY]?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0 );
            }


            // new.2 (dt.1): sum_{bigsum_l2__orderkey} if new then dt else 0
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it54 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L1__ORDERKEY == ORDERKEY &&
                 (bigsum_L2__ORDERKEY_dom_it54 != bigsum_L2__ORDERKEY_dom.end()) )
            {
                cstr169 += ((100 < qCUSTOMER3[ORDERKEY]+QUANTITY)? 1 : 0 );
            }

            // new.3 (dt.2): sum_{bigsum_l2__orderkey} if new and not old then t else 0
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it58 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if (  bigsum_L2__ORDERKEY_dom_it58 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr169 += (
                    (100 < qCUSTOMER3[ORDERKEY]+QUANTITY) && (qCUSTOMER3[ORDERKEY] <= 100)?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0 );

            }

            // new.4 (dt.3): sum_{bigsum_l2__orderkey} if not new and old then -t else 0
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it62 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it62 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr169 += -1*( (
                    (qCUSTOMER3[ORDERKEY]+QUANTITY <= 100) && (100 < qCUSTOMER3[ORDERKEY]) )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0 );
            }

            cstr210 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it66 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L2__ORDERKEY_dom_end65 = bigsum_L2__ORDERKEY_dom.end();
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

    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER1_it74 = qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER1_end73 = qCUSTOMER1.end();
    for (; qCUSTOMER1_it74 != qCUSTOMER1_end73; ++qCUSTOMER1_it74)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER1_it74->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1_it74->first);
        qCUSTOMER1[make_tuple(
            bigsum_L1__ORDERKEY,x_qCUSTOMER_C__CUSTKEY)] += 
            QUANTITY*qCUSTOMER1LINEITEM1[make_tuple(
            bigsum_L1__ORDERKEY,x_qCUSTOMER_C__CUSTKEY,ORDERKEY)];
    }

    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER1ORDERS1_it76 = qCUSTOMER1ORDERS1.find(make_tuple(ORDERKEY,ORDERKEY));

    if ( qCUSTOMER1ORDERS1_it76 != qCUSTOMER1ORDERS1.end() )
    {
        qCUSTOMER1ORDERS1[make_tuple(ORDERKEY,ORDERKEY)] += QUANTITY;
    }

    map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::iterator 
        qCUSTOMER2_it78 = qCUSTOMER2.find(make_tuple(ORDERKEY,ORDERKEY));

    if ( qCUSTOMER2_it78 != qCUSTOMER2.end() ) {
        qCUSTOMER2[make_tuple(ORDERKEY,ORDERKEY)] += 1;        
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator qCUSTOMER3_it80 = 
        qCUSTOMER3.find(ORDERKEY);

    if ( qCUSTOMER3_it80 != qCUSTOMER3.end() )
    {
        qCUSTOMER3[ORDERKEY] += QUANTITY;
    }

    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER4_it82 = qCUSTOMER4.begin();
    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER4_end81 = qCUSTOMER4.end();
    for (; qCUSTOMER4_it82 != qCUSTOMER4_end81; ++qCUSTOMER4_it82)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER4_it82->first);
        int64_t C__CUSTKEY = get<1>(qCUSTOMER4_it82->first);
        qCUSTOMER4[make_tuple(
            bigsum_L1__ORDERKEY,C__CUSTKEY)] += QUANTITY*qCUSTOMER4LINEITEM1[make_tuple(
            bigsum_L1__ORDERKEY,C__CUSTKEY,ORDERKEY)];
    }

    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER4ORDERS1_it84 = qCUSTOMER4ORDERS1.find(make_tuple(ORDERKEY,ORDERKEY));

    if ( qCUSTOMER4ORDERS1_it84 != qCUSTOMER4ORDERS1.end() )
    {
        qCUSTOMER4ORDERS1[make_tuple(ORDERKEY,ORDERKEY)] += QUANTITY;
    }

    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qORDERS1_it86 = qORDERS1.find(make_tuple(ORDERKEY, ORDERKEY));

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

    set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::iterator 
        bigsum_L1__ORDERKEY_dom_it100 = bigsum_L1__ORDERKEY_dom.begin();
    set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::iterator 
        bigsum_L1__ORDERKEY_dom_end99 = bigsum_L1__ORDERKEY_dom.end();
    for (
        ; bigsum_L1__ORDERKEY_dom_it100 != bigsum_L1__ORDERKEY_dom_end99; 
        ++bigsum_L1__ORDERKEY_dom_it100)
    {
        int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it100;

        // new.1 (old): sum_{bigsum_l2__orderkey} t
        // Note: delta t = 0
        cstr1 = 0;

        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L2__ORDERKEY_dom_it88 = bigsum_L2__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L2__ORDERKEY_dom_end87 = bigsum_L2__ORDERKEY_dom.end();
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

    /*
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it120 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end119 = q.end();
    for (; q_it120 != q_end119; ++q_it120)
    {
        int64_t CUSTKEY = q_it120->first;

        // Note q39 is always zero, since delta r = 0 for the constraint-only aggregate.
        // We are not pruning away these conditions in the calculus... should be fixed!
        q39 = 0.0;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it118 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end117 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it118 != bigsum_L1__ORDERKEY_dom_end117; 
            ++bigsum_L1__ORDERKEY_dom_it118)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it118;

            cstr40 = 0;

            // new.1 (old): sum_{bigsum_l2__orderkey} t
            // Note: delta t = 0
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it102 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end101 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it102 != bigsum_L2__ORDERKEY_dom_end101; 
                ++bigsum_L2__ORDERKEY_dom_it102)
            {
                int64_t bigsum_L2__ORDERKEY = 
                    *bigsum_L2__ORDERKEY_dom_it102;
                cstr40 += ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY]?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0 );
            }

            cstr58 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it114 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end113 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it114 != bigsum_L2__ORDERKEY_dom_end113; 
                ++bigsum_L2__ORDERKEY_dom_it114)
            {
                int64_t bigsum_L2__ORDERKEY = 
                    *bigsum_L2__ORDERKEY_dom_it114;
                cstr58 += ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY]?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0);
            }

            // if not new and old then -f else 0
            if ( ( cstr40 < 1 ) && ( 1 <= cstr58 ) )
            {
                q39 += qCUSTOMER4[make_tuple(bigsum_L1__ORDERKEY,CUSTKEY)];
            }
        }
        q[CUSTKEY] += -1*q39;
    }
    */

    /*
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it140 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end139 = q.end();
    for (; q_it140 != q_end139; ++q_it140)
    {
        int64_t CUSTKEY = q_it140->first;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it138 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end137 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it138 != bigsum_L1__ORDERKEY_dom_end137; 
            ++bigsum_L1__ORDERKEY_dom_it138)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it138;
            cstr18 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it122 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end121 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it122 != bigsum_L2__ORDERKEY_dom_end121; 
                ++bigsum_L2__ORDERKEY_dom_it122)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it122;
                cstr18 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                            ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
            }

            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it126 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end125 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it126 != bigsum_L2__ORDERKEY_dom_end125; 
                ++bigsum_L2__ORDERKEY_dom_it126)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it126;
                cstr18 += ( ( (
                                  100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) && (
                                      qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) )?
                ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
            }

            cstr1821 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it130 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end129 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it130 != bigsum_L2__ORDERKEY_dom_end129; 
                ++bigsum_L2__ORDERKEY_dom_it130)
            {
                int64_t bigsum_L2__ORDERKEY = 
                    *bigsum_L2__ORDERKEY_dom_it130;
                cstr18 += -1*( ( (qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 )
                                 && (100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) )?
                    ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
            }

            cstr34 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it134 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end133 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it134 != bigsum_L2__ORDERKEY_dom_end133; 
                ++bigsum_L2__ORDERKEY_dom_it134)
            {
                int64_t bigsum_L2__ORDERKEY = 
                    *bigsum_L2__ORDERKEY_dom_it134;
                cstr34 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                    ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
            }

            // if new and not old then f else 0
            if ( ( 1 <= cstr18 ) && ( cstr34 < 1 ) )
            {
                q[CUSTKEY] += qCUSTOMER4[make_tuple(bigsum_L1__ORDERKEY,CUSTKEY)];
            }
        }
    }
    */

    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER4_it142 = qCUSTOMER4.begin();
    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER4_end141 = qCUSTOMER4.end();
    for (; qCUSTOMER4_it142 != qCUSTOMER4_end141; ++qCUSTOMER4_it142)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER4_it142->first);
        qCUSTOMER4[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY)] += qCUSTOMER1[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY)];
    }
    
    map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,
        boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::iterator 
        qCUSTOMER4LINEITEM1_it144 = qCUSTOMER4LINEITEM1.begin();
    
    map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,
        boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::iterator 
        qCUSTOMER4LINEITEM1_end143 = qCUSTOMER4LINEITEM1.end();
    for (
        ; qCUSTOMER4LINEITEM1_it144 != qCUSTOMER4LINEITEM1_end143; 
        ++qCUSTOMER4LINEITEM1_it144)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER4LINEITEM1_it144->first);
        qCUSTOMER4LINEITEM1[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY,bigsum_L1__ORDERKEY)] += 
            qLINEITEM1CUSTOMER1[make_tuple(bigsum_L1__ORDERKEY,CUSTKEY)];
    }
    
    map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,
        boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::iterator 
        qLINEITEM1_it146 = qLINEITEM1.begin();
    
    map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,
        boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::iterator 
        qLINEITEM1_end145 = qLINEITEM1.end();
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
    set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::iterator 
        bigsum_L1__ORDERKEY_dom_it160 = bigsum_L1__ORDERKEY_dom.begin();
    set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::iterator 
        bigsum_L1__ORDERKEY_dom_end159 = bigsum_L1__ORDERKEY_dom.end();
    for (
        ; bigsum_L1__ORDERKEY_dom_it160 != bigsum_L1__ORDERKEY_dom_end159; 
        ++bigsum_L1__ORDERKEY_dom_it160)
    {
        int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it160;
        cstr64 = 0;

            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it148 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end147 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it148 != bigsum_L2__ORDERKEY_dom_end147; 
                ++bigsum_L2__ORDERKEY_dom_it148)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it148;
                cstr64 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                     ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
            }

            // Two nested conditions for bigsum_l2__orderkey, where delta r = 0
            /*
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it152 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end151 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it152 != bigsum_L2__ORDERKEY_dom_end151; 
                ++bigsum_L2__ORDERKEY_dom_it152)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it152;
                cstr64 += ( ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) && (
                     qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) )?
                     ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
            }

            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it156 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end155 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it156 != bigsum_L2__ORDERKEY_dom_end155; 
                ++bigsum_L2__ORDERKEY_dom_it156)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it156;
                cstr64 += -1*( ( ( qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) && 
                    ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) )?
                     ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
            }
            */

        if ( 1 <= cstr64 )
        {
            q61 += qORDERS1[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)];
        }
    }
    q[CUSTKEY] += qORDERS4[CUSTKEY]*q61;


    // The following two constraint-only aggregate conditions can be commented out
    // due to delta r = 0
    /*
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it180 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end179 = q.end();
    for (; q_it180 != q_end179; ++q_it180)
    {
        int64_t C__CUSTKEY = q_it180->first;
        q102 = 0.0;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it178 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end177 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it178 != bigsum_L1__ORDERKEY_dom_end177; 
            ++bigsum_L1__ORDERKEY_dom_it178)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it178;
            cstr103 = 0;
            cstr103104 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it164 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end163 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it164 != bigsum_L1__ORDERKEY_dom_end163; 
                ++bigsum_L1__ORDERKEY_dom_it164)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it164;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it162 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end161 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it162 != bigsum_L2__ORDERKEY_dom_end161; 
                    ++bigsum_L2__ORDERKEY_dom_it162)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it162;
                    cstr103104 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }

            cstr103105 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it168 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end167 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it168 != bigsum_L1__ORDERKEY_dom_end167; 
                ++bigsum_L1__ORDERKEY_dom_it168)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it168;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it166 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end165 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it166 != bigsum_L2__ORDERKEY_dom_end165; 
                    ++bigsum_L2__ORDERKEY_dom_it166)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it166;
                    cstr103105 += ( ( (
                         100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) && (
                         qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }

            cstr103106 = 0;
            cstr103106114 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it172 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end171 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it172 != bigsum_L1__ORDERKEY_dom_end171; 
                ++bigsum_L1__ORDERKEY_dom_it172)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it172;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it170 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end169 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it170 != bigsum_L2__ORDERKEY_dom_end169; 
                    ++bigsum_L2__ORDERKEY_dom_it170)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it170;
                    cstr103106114 += ( ( (
                         qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) && (
                         100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }
            cstr103106 += -1*cstr103106114;
            cstr103 += cstr103104+cstr103105+cstr103106;

            cstr121 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it176 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end175 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it176 != bigsum_L1__ORDERKEY_dom_end175; 
                ++bigsum_L1__ORDERKEY_dom_it176)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it176;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it174 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end173 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it174 != bigsum_L2__ORDERKEY_dom_end173; 
                    ++bigsum_L2__ORDERKEY_dom_it174)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it174;
                    cstr121 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }

            if ( ( cstr103 < 1 ) && ( 1 <= cstr121 ) )
            {
                q102 += qCUSTOMER4[make_tuple(bigsum_L1__ORDERKEY,C__CUSTKEY)];
            }
        }
        q[C__CUSTKEY] += -1*q102;
    }
    */

    /*
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it200 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end199 = q.end();
    for (; q_it200 != q_end199; ++q_it200)
    {
        int64_t C__CUSTKEY = q_it200->first;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it198 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end197 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it198 != bigsum_L1__ORDERKEY_dom_end197; 
            ++bigsum_L1__ORDERKEY_dom_it198)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it198;
            cstr81 = 0;
            cstr8182 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it184 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end183 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it184 != bigsum_L1__ORDERKEY_dom_end183; 
                ++bigsum_L1__ORDERKEY_dom_it184)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it184;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it182 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end181 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it182 != bigsum_L2__ORDERKEY_dom_end181; 
                    ++bigsum_L2__ORDERKEY_dom_it182)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it182;
                    cstr8182 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }

            cstr8183 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it188 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end187 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it188 != bigsum_L1__ORDERKEY_dom_end187; 
                ++bigsum_L1__ORDERKEY_dom_it188)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it188;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it186 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end185 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it186 != bigsum_L2__ORDERKEY_dom_end185; 
                    ++bigsum_L2__ORDERKEY_dom_it186)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it186;
                    cstr8183 += ( ( (
                         100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) && (
                         qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }

            cstr8184 = 0;
            cstr818492 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it192 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end191 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it192 != bigsum_L1__ORDERKEY_dom_end191; 
                ++bigsum_L1__ORDERKEY_dom_it192)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it192;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it190 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end189 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it190 != bigsum_L2__ORDERKEY_dom_end189; 
                    ++bigsum_L2__ORDERKEY_dom_it190)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it190;
                    cstr818492 += ( ( (
                         qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) && (
                         100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }
            cstr8184 += -1*cstr818492;
            cstr81 += cstr8182+cstr8183+cstr8184;

            cstr97 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it196 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end195 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it196 != bigsum_L1__ORDERKEY_dom_end195; 
                ++bigsum_L1__ORDERKEY_dom_it196)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it196;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it194 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end193 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it194 != bigsum_L2__ORDERKEY_dom_end193; 
                    ++bigsum_L2__ORDERKEY_dom_it194)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it194;
                    cstr97 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }

            if ( ( 1 <= cstr81 ) && ( cstr97 < 1 ) )
            {
                q[C__CUSTKEY] += qCUSTOMER4[make_tuple(
                    bigsum_L1__ORDERKEY,C__CUSTKEY)];
            }
        }
    }
    */

    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER1_it202 = qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER1_end201 = qCUSTOMER1.end();
    for (; qCUSTOMER1_it202 != qCUSTOMER1_end201; ++qCUSTOMER1_it202)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER1_it202->first);
        qCUSTOMER1[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY)] += qCUSTOMER1ORDERS1[make_tuple(
            bigsum_L1__ORDERKEY,ORDERKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,
        boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::iterator 
        qCUSTOMER1LINEITEM1_it204 = qCUSTOMER1LINEITEM1.find(make_tuple(ORDERKEY,CUSTKEY,ORDERKEY));

    if ( qCUSTOMER1LINEITEM1_it204 != qCUSTOMER1LINEITEM1.end() )
    {
        qCUSTOMER1LINEITEM1[make_tuple(ORDERKEY,CUSTKEY,ORDERKEY)] += 1;
    }

    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,
        boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER4_it206 = qCUSTOMER4.begin();
    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> >,
        boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER4_end205 = qCUSTOMER4.end();
    for (; qCUSTOMER4_it206 != qCUSTOMER4_end205; ++qCUSTOMER4_it206)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER4_it206->first);
        qCUSTOMER4[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY)] += qCUSTOMER4ORDERS1[make_tuple(
            bigsum_L1__ORDERKEY,ORDERKEY)]*qORDERS4[CUSTKEY];
    }

    map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> >,
        boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::iterator 
        qCUSTOMER4LINEITEM1_it208 = qCUSTOMER4LINEITEM1.find(make_tuple(ORDERKEY,CUSTKEY,ORDERKEY));

    if ( qCUSTOMER4LINEITEM1_it208 != qCUSTOMER4LINEITEM1.end() )
    {
        qCUSTOMER4LINEITEM1[make_tuple(ORDERKEY,CUSTKEY,ORDERKEY)] += qORDERS4[CUSTKEY];
    }
    
    qLINEITEM1[make_tuple(ORDERKEY,CUSTKEY,ORDERKEY)] += qORDERS4[CUSTKEY];

    map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> >,
        boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::iterator 
        qLINEITEM1CUSTOMER1_it210 = qLINEITEM1CUSTOMER1.find(make_tuple(ORDERKEY,CUSTKEY));

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

    map<int64_t,double,std::less<int64_t>,
        boost::pool_allocator<pair<int64_t,double> > >::iterator q_it230 = q.begin();
    map<int64_t,double,std::less<int64_t>,
        boost::pool_allocator<pair<int64_t,double> > >::iterator q_end229 = q.end();
    for (; q_it230 != q_end229; ++q_it230)
    {
        int64_t C__CUSTKEY = q_it230->first;
        q452 = 0.0;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it228 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end227 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it228 != bigsum_L1__ORDERKEY_dom_end227; 
            ++bigsum_L1__ORDERKEY_dom_it228)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it228;
            cstr454 = 0;

            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it212 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end211 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it212 != bigsum_L2__ORDERKEY_dom_end211; 
                ++bigsum_L2__ORDERKEY_dom_it212)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it212;
                cstr454 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0 );
            }


            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it216 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( (bigsum_L1__ORDERKEY == ORDERKEY &&
                  bigsum_L2__ORDERKEY_dom_it216 != bigsum_L2__ORDERKEY_dom.end()) )
            {
                cstr454 += -1*( (100 < qCUSTOMER3[ORDERKEY]-QUANTITY)? 1 : 0 );
            }

            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it220 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it220 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr454 +=
                    ( ( (100 < qCUSTOMER3[ORDERKEY]-QUANTITY)
                        && ( qCUSTOMER3[ORDERKEY] <= 100 ) )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0);
            }

            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it224 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

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

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it254 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end253 = q.end();
    for (; q_it254 != q_end253; ++q_it254)
    {
        int64_t C__CUSTKEY = q_it254->first;
        q507 = 0.0;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it252 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end251 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it252 != bigsum_L1__ORDERKEY_dom_end251; 
            ++bigsum_L1__ORDERKEY_dom_it252)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it252;

            cstr509 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it232 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end231 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it232 != bigsum_L2__ORDERKEY_dom_end231; 
                ++bigsum_L2__ORDERKEY_dom_it232)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it232;
                cstr509 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0);
            }

            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it236 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( (bigsum_L1__ORDERKEY == ORDERKEY) &&
                 (bigsum_L2__ORDERKEY_dom_it236 != bigsum_L2__ORDERKEY_dom.end()) )
            {
                cstr509 += (100 < qCUSTOMER3[ORDERKEY]-QUANTITY )? -1 : 0;
            }


            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it240 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it240 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr509 +=
                    ( (100 < qCUSTOMER3[ORDERKEY]-QUANTITY) && (qCUSTOMER3[ORDERKEY] <= 100)?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0 );
            }

            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it244 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it244 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr509 += -1*
                    ( ( (qCUSTOMER3[ORDERKEY]-QUANTITY <= 100 )
                        && ( 100 < qCUSTOMER3[ORDERKEY] ) )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0 );
            }

            cstr558 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it248 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end247 = bigsum_L2__ORDERKEY_dom.end();
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

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it278 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end277 = q.end();
    for (; q_it278 != q_end277; ++q_it278)
    {
        int64_t C__CUSTKEY = q_it278->first;
        q563 = 0.0;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it276 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end275 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it276 != bigsum_L1__ORDERKEY_dom_end275; 
            ++bigsum_L1__ORDERKEY_dom_it276)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it276;
            cstr564 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it256 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end255 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it256 != bigsum_L2__ORDERKEY_dom_end255; 
                ++bigsum_L2__ORDERKEY_dom_it256)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it256;
                cstr564 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0 );
            }

            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it260 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( (bigsum_L1__ORDERKEY == ORDERKEY) &&
                 (bigsum_L2__ORDERKEY_dom_it260 != bigsum_L2__ORDERKEY_dom.end()) )
            {
                cstr564566572 += (100 < qCUSTOMER3[ORDERKEY]-QUANTITY? -1 : 0);
            }


            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it264 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it264 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr564567588 +=
                    ( ( (100 < qCUSTOMER3[ORDERKEY]-QUANTITY) && ( qCUSTOMER3[ORDERKEY] <= 100 ) )?
                    qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0 );
            }

            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it268 = bigsum_L2__ORDERKEY_dom.find(ORDERKEY);

            if ( bigsum_L2__ORDERKEY_dom_it268 != bigsum_L2__ORDERKEY_dom.end() )
            {
                cstr564568601 += -1*
                    ( ( (qCUSTOMER3[ORDERKEY]-QUANTITY <= 100 ) && ( 100 < qCUSTOMER3[ORDERKEY] ) )?
                      qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)] : 0);

            }

            cstr615 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it272 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end271 = bigsum_L2__ORDERKEY_dom.end();
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

    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER1_it280 = qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER1_end279 = qCUSTOMER1.end();
    for (; qCUSTOMER1_it280 != qCUSTOMER1_end279; ++qCUSTOMER1_it280)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER1_it280->first);
        int64_t x_qCUSTOMER_C__CUSTKEY = get<1>(qCUSTOMER1_it280->first);
        qCUSTOMER1[make_tuple(
            bigsum_L1__ORDERKEY,x_qCUSTOMER_C__CUSTKEY)] += 
            -1*QUANTITY*qCUSTOMER1LINEITEM1[make_tuple(
            bigsum_L1__ORDERKEY,x_qCUSTOMER_C__CUSTKEY,ORDERKEY)];
    }

    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER1ORDERS1_it282 = qCUSTOMER1ORDERS1.find(make_tuple(ORDERKEY,ORDERKEY));

    if ( qCUSTOMER1ORDERS1_it282 != qCUSTOMER1ORDERS1.end() )
    {
        qCUSTOMER1ORDERS1[make_tuple(ORDERKEY,ORDERKEY)] += -QUANTITY;
    }

    map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::iterator 
        qCUSTOMER2_it284 = qCUSTOMER2.find(make_tuple(ORDERKEY,ORDERKEY));

    if ( qCUSTOMER2_it284 != qCUSTOMER2.end() ) {
        qCUSTOMER2[make_tuple(ORDERKEY,ORDERKEY)] += -1;
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator qCUSTOMER3_it286 = 
        qCUSTOMER3.find(ORDERKEY);

    if ( qCUSTOMER3_it286 != qCUSTOMER3.end() )
    {
        qCUSTOMER3[ORDERKEY] += -QUANTITY;
    }

    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER4_it288 = qCUSTOMER4.begin();
    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER4_end287 = qCUSTOMER4.end();
    for (; qCUSTOMER4_it288 != qCUSTOMER4_end287; ++qCUSTOMER4_it288)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER4_it288->first);
        int64_t C__CUSTKEY = get<1>(qCUSTOMER4_it288->first);
        qCUSTOMER4[make_tuple(
            bigsum_L1__ORDERKEY,C__CUSTKEY)] += -1*QUANTITY*qCUSTOMER4LINEITEM1[make_tuple(
            bigsum_L1__ORDERKEY,C__CUSTKEY,ORDERKEY)];
    }

    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER4ORDERS1_it290 = qCUSTOMER4ORDERS1.find(make_tuple(ORDERKEY, ORDERKEY));

    if ( qCUSTOMER4ORDERS1_it290 != qCUSTOMER4ORDERS1.end() )
    {
        qCUSTOMER4ORDERS1[make_tuple(ORDERKEY,ORDERKEY)] += -QUANTITY;
    }

    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
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
    set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::iterator 
        bigsum_L1__ORDERKEY_dom_it306 = bigsum_L1__ORDERKEY_dom.begin();
    set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::iterator 
        bigsum_L1__ORDERKEY_dom_end305 = bigsum_L1__ORDERKEY_dom.end();
    for (
        ; bigsum_L1__ORDERKEY_dom_it306 != bigsum_L1__ORDERKEY_dom_end305; 
        ++bigsum_L1__ORDERKEY_dom_it306)
    {
        int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it306;
        cstr303 = 0;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L2__ORDERKEY_dom_it294 = bigsum_L2__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L2__ORDERKEY_dom_end293 = bigsum_L2__ORDERKEY_dom.end();
        for (
            ; bigsum_L2__ORDERKEY_dom_it294 != bigsum_L2__ORDERKEY_dom_end293; 
            ++bigsum_L2__ORDERKEY_dom_it294)
        {
            int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it294;
            cstr303 += ( (100 < qCUSTOMER3[bigsum_L2__ORDERKEY])?
                qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0);
        }

        /*
        cstr303305 = 0;
        cstr303305311 = 0;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it300 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end299 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it300 != bigsum_L1__ORDERKEY_dom_end299; 
            ++bigsum_L1__ORDERKEY_dom_it300)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it300;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it298 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end297 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it298 != bigsum_L2__ORDERKEY_dom_end297; 
                ++bigsum_L2__ORDERKEY_dom_it298)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it298;
                cstr303305311 += ( ( (
                     100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) && (
                     qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) )?
                     ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
            }
        }
        cstr303305 += -1*-1*cstr303305311;

        cstr303306 = 0;
        cstr303306317 = 0;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it304 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end303 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it304 != bigsum_L1__ORDERKEY_dom_end303; 
            ++bigsum_L1__ORDERKEY_dom_it304)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it304;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it302 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end301 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it302 != bigsum_L2__ORDERKEY_dom_end301; 
                ++bigsum_L2__ORDERKEY_dom_it302)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it302;
                cstr303306317 += ( ( (
                     qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) && (
                     100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) )?
                     ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
            }
        }
        cstr303306 += -1*cstr303306317;
        cstr303 += cstr303304+cstr303305+cstr303306;
        */

        // if new then delta f else 0
        if ( 1 <= cstr303 )
        {
            q301 += qCUSTOMER1[make_tuple(bigsum_L1__ORDERKEY,CUSTKEY)];
        }
    }
    q[CUSTKEY] += -1*q301;

    /*
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it326 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end325 = q.end();
    for (; q_it326 != q_end325; ++q_it326)
    {
        int64_t CUSTKEY = q_it326->first;
        q324 = 0.0;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it324 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end323 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it324 != bigsum_L1__ORDERKEY_dom_end323; 
            ++bigsum_L1__ORDERKEY_dom_it324)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it324;
            cstr326 = 0;
            cstr326327 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it310 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end309 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it310 != bigsum_L1__ORDERKEY_dom_end309; 
                ++bigsum_L1__ORDERKEY_dom_it310)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it310;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it308 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end307 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it308 != bigsum_L2__ORDERKEY_dom_end307; 
                    ++bigsum_L2__ORDERKEY_dom_it308)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it308;
                    cstr326327 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }

            cstr326328 = 0;
            cstr326328334 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it314 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end313 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it314 != bigsum_L1__ORDERKEY_dom_end313; 
                ++bigsum_L1__ORDERKEY_dom_it314)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it314;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it312 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end311 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it312 != bigsum_L2__ORDERKEY_dom_end311; 
                    ++bigsum_L2__ORDERKEY_dom_it312)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it312;
                    cstr326328334 += ( ( (
                         100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) && (
                         qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }
            cstr326328 += -1*-1*cstr326328334;

            cstr326329 = 0;
            cstr326329340 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it318 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end317 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it318 != bigsum_L1__ORDERKEY_dom_end317; 
                ++bigsum_L1__ORDERKEY_dom_it318)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it318;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it316 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end315 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it316 != bigsum_L2__ORDERKEY_dom_end315; 
                    ++bigsum_L2__ORDERKEY_dom_it316)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it316;
                    cstr326329340 += ( ( (
                         qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) && (
                         100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }
            cstr326329 += -1*cstr326329340;
            cstr326 += cstr326327+cstr326328+cstr326329;

            cstr345 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it322 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end321 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it322 != bigsum_L1__ORDERKEY_dom_end321; 
                ++bigsum_L1__ORDERKEY_dom_it322)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it322;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it320 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end319 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it320 != bigsum_L2__ORDERKEY_dom_end319; 
                    ++bigsum_L2__ORDERKEY_dom_it320)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it320;
                    cstr345 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }

            // if new and not old then -f else 0
            if ( ( 1 <= cstr326 ) && ( cstr345 < 1 ) )
            {
                q324 += qCUSTOMER4[make_tuple(bigsum_L1__ORDERKEY,CUSTKEY)];
            }
        }
        q[CUSTKEY] += -1*-1*q324;
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it346 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end345 = q.end();
    for (; q_it346 != q_end345; ++q_it346)
    {
        int64_t CUSTKEY = q_it346->first;
        q350 = 0.0;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it344 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end343 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it344 != bigsum_L1__ORDERKEY_dom_end343; 
            ++bigsum_L1__ORDERKEY_dom_it344)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it344;
            cstr351 = 0;
            cstr351352 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it330 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end329 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it330 != bigsum_L1__ORDERKEY_dom_end329; 
                ++bigsum_L1__ORDERKEY_dom_it330)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it330;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it328 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end327 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it328 != bigsum_L2__ORDERKEY_dom_end327; 
                    ++bigsum_L2__ORDERKEY_dom_it328)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it328;
                    cstr351352 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }

            cstr351353 = 0;
            cstr351353359 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it334 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end333 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it334 != bigsum_L1__ORDERKEY_dom_end333; 
                ++bigsum_L1__ORDERKEY_dom_it334)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it334;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it332 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end331 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it332 != bigsum_L2__ORDERKEY_dom_end331; 
                    ++bigsum_L2__ORDERKEY_dom_it332)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it332;
                    cstr351353359 += ( ( (
                         100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) && (
                         qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }
            cstr351353 += -1*-1*cstr351353359;

            cstr351354 = 0;
            cstr351354365 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it338 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end337 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it338 != bigsum_L1__ORDERKEY_dom_end337; 
                ++bigsum_L1__ORDERKEY_dom_it338)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it338;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it336 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end335 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it336 != bigsum_L2__ORDERKEY_dom_end335; 
                    ++bigsum_L2__ORDERKEY_dom_it336)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it336;
                    cstr351354365 += ( ( (
                         qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) && (
                         100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }
            cstr351354 += -1*cstr351354365;
            cstr351 += cstr351352+cstr351353+cstr351354;

            cstr372 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it342 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end341 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it342 != bigsum_L1__ORDERKEY_dom_end341; 
                ++bigsum_L1__ORDERKEY_dom_it342)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it342;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it340 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end339 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it340 != bigsum_L2__ORDERKEY_dom_end339; 
                    ++bigsum_L2__ORDERKEY_dom_it340)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it340;
                    cstr372 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }

            // if not new and old then f else 0
            if ( ( cstr351 < 1 ) && ( 1 <= cstr372 ) )
            {
                q350 += qCUSTOMER4[make_tuple(bigsum_L1__ORDERKEY,CUSTKEY)];
            }
        }
        q[CUSTKEY] += -1*q350;
    }
    */

    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER4_it348 = qCUSTOMER4.begin();
    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER4_end347 = qCUSTOMER4.end();
    for (; qCUSTOMER4_it348 != qCUSTOMER4_end347; ++qCUSTOMER4_it348)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER4_it348->first);
        qCUSTOMER4[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY)] += -1*qCUSTOMER1[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY)];
    }
    
    map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::iterator 
        qCUSTOMER4LINEITEM1_it350 = qCUSTOMER4LINEITEM1.begin();
    
    map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::iterator 
        qCUSTOMER4LINEITEM1_end349 = qCUSTOMER4LINEITEM1.end();
    for (
        ; qCUSTOMER4LINEITEM1_it350 != qCUSTOMER4LINEITEM1_end349; 
        ++qCUSTOMER4LINEITEM1_it350)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER4LINEITEM1_it350->first);
        qCUSTOMER4LINEITEM1[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY,bigsum_L1__ORDERKEY)] += 
            -1*qLINEITEM1CUSTOMER1[make_tuple(bigsum_L1__ORDERKEY,CUSTKEY)];
    }
    
    map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::iterator 
        qLINEITEM1_it352 = qLINEITEM1.begin();
    
    map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::iterator 
        qLINEITEM1_end351 = qLINEITEM1.end();
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
    set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::iterator 
        bigsum_L1__ORDERKEY_dom_it366 = bigsum_L1__ORDERKEY_dom.begin();
    set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> >::iterator 
        bigsum_L1__ORDERKEY_dom_end365 = bigsum_L1__ORDERKEY_dom.end();
    for (
        ; bigsum_L1__ORDERKEY_dom_it366 != bigsum_L1__ORDERKEY_dom_end365; 
        ++bigsum_L1__ORDERKEY_dom_it366)
    {
        int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it366;
        cstr379 = 0;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L2__ORDERKEY_dom_it354 = bigsum_L2__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L2__ORDERKEY_dom_end353 = bigsum_L2__ORDERKEY_dom.end();
        for (
            ; bigsum_L2__ORDERKEY_dom_it354 != bigsum_L2__ORDERKEY_dom_end353; 
            ++bigsum_L2__ORDERKEY_dom_it354)
        {
            int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it354;
            cstr379 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] : 0);
        }

        /*
        cstr379381 = 0;
        cstr379381387 = 0;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it360 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end359 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it360 != bigsum_L1__ORDERKEY_dom_end359; 
            ++bigsum_L1__ORDERKEY_dom_it360)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it360;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it358 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end357 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it358 != bigsum_L2__ORDERKEY_dom_end357; 
                ++bigsum_L2__ORDERKEY_dom_it358)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it358;
                cstr379381387 += ( ( (
                     100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) && (
                     qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) )?
                     ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
            }
        }
        cstr379381 += -1*-1*cstr379381387;

        cstr379382 = 0;
        cstr379382393 = 0;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it364 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end363 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it364 != bigsum_L1__ORDERKEY_dom_end363; 
            ++bigsum_L1__ORDERKEY_dom_it364)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it364;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_it362 = bigsum_L2__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L2__ORDERKEY_dom_end361 = bigsum_L2__ORDERKEY_dom.end();
            for (
                ; bigsum_L2__ORDERKEY_dom_it362 != bigsum_L2__ORDERKEY_dom_end361; 
                ++bigsum_L2__ORDERKEY_dom_it362)
            {
                int64_t bigsum_L2__ORDERKEY = *bigsum_L2__ORDERKEY_dom_it362;
                cstr379382393 += ( ( (
                     qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) && (
                     100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) )?
                     ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
            }
        }
        cstr379382 += -1*cstr379382393;
        cstr379 += cstr379380+cstr379381+cstr379382;
        */

        // if new_r then delta- f else 0
        if ( 1 <= cstr379 )
        {
            q376 += qORDERS1[make_tuple(bigsum_L1__ORDERKEY,ORDERKEY)];
        }
    }
    q[CUSTKEY] += -1*qORDERS4[CUSTKEY]*q376;

   /*
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it386 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end385 = q.end();
    for (; q_it386 != q_end385; ++q_it386)
    {
        int64_t C__CUSTKEY = q_it386->first;
        q400 = 0.0;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it384 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end383 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it384 != bigsum_L1__ORDERKEY_dom_end383; 
            ++bigsum_L1__ORDERKEY_dom_it384)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it384;
            cstr402 = 0;
            cstr402403 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it370 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end369 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it370 != bigsum_L1__ORDERKEY_dom_end369; 
                ++bigsum_L1__ORDERKEY_dom_it370)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it370;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it368 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end367 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it368 != bigsum_L2__ORDERKEY_dom_end367; 
                    ++bigsum_L2__ORDERKEY_dom_it368)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it368;
                    cstr402403 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }

            cstr402404 = 0;
            cstr402404410 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it374 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end373 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it374 != bigsum_L1__ORDERKEY_dom_end373; 
                ++bigsum_L1__ORDERKEY_dom_it374)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it374;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it372 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end371 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it372 != bigsum_L2__ORDERKEY_dom_end371; 
                    ++bigsum_L2__ORDERKEY_dom_it372)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it372;
                    cstr402404410 += ( ( (
                         100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) && (
                         qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }
            cstr402404 += -1*-1*cstr402404410;

            cstr402405 = 0;
            cstr402405416 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it378 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end377 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it378 != bigsum_L1__ORDERKEY_dom_end377; 
                ++bigsum_L1__ORDERKEY_dom_it378)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it378;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it376 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end375 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it376 != bigsum_L2__ORDERKEY_dom_end375; 
                    ++bigsum_L2__ORDERKEY_dom_it376)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it376;
                    cstr402405416 += ( ( (
                         qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) && (
                         100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }
            cstr402405 += -1*cstr402405416;
            cstr402 += cstr402403+cstr402404+cstr402405;

            cstr421 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it382 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end381 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it382 != bigsum_L1__ORDERKEY_dom_end381; 
                ++bigsum_L1__ORDERKEY_dom_it382)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it382;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it380 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end379 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it380 != bigsum_L2__ORDERKEY_dom_end379; 
                    ++bigsum_L2__ORDERKEY_dom_it380)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it380;
                    cstr421 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }

            if ( ( 1 <= cstr402 ) && ( cstr421 < 1 ) )
            {
                q400 += qCUSTOMER4[make_tuple(bigsum_L1__ORDERKEY,C__CUSTKEY)];
            }
        }
        q[C__CUSTKEY] += -1*-1*q400;
    }

    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_it406 = q.begin();
    map<int64_t,double,std::less<int64_t 
        >,boost::pool_allocator<pair<int64_t,double> > >::iterator q_end405 = q.end();
    for (; q_it406 != q_end405; ++q_it406)
    {
        int64_t C__CUSTKEY = q_it406->first;
        q426 = 0.0;
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_it404 = bigsum_L1__ORDERKEY_dom.begin();
        set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
            >::iterator bigsum_L1__ORDERKEY_dom_end403 = bigsum_L1__ORDERKEY_dom.end();
        for (
            ; bigsum_L1__ORDERKEY_dom_it404 != bigsum_L1__ORDERKEY_dom_end403; 
            ++bigsum_L1__ORDERKEY_dom_it404)
        {
            int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it404;
            cstr427 = 0;
            cstr427428 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it390 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end389 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it390 != bigsum_L1__ORDERKEY_dom_end389; 
                ++bigsum_L1__ORDERKEY_dom_it390)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it390;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it388 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end387 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it388 != bigsum_L2__ORDERKEY_dom_end387; 
                    ++bigsum_L2__ORDERKEY_dom_it388)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it388;
                    cstr427428 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }

            cstr427429 = 0;
            cstr427429435 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it394 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end393 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it394 != bigsum_L1__ORDERKEY_dom_end393; 
                ++bigsum_L1__ORDERKEY_dom_it394)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it394;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it392 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end391 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it392 != bigsum_L2__ORDERKEY_dom_end391; 
                    ++bigsum_L2__ORDERKEY_dom_it392)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it392;
                    cstr427429435 += ( ( (
                         100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) && (
                         qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }
            cstr427429 += -1*-1*cstr427429435;

            cstr427430 = 0;
            cstr427430441 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it398 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end397 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it398 != bigsum_L1__ORDERKEY_dom_end397; 
                ++bigsum_L1__ORDERKEY_dom_it398)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it398;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it396 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end395 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it396 != bigsum_L2__ORDERKEY_dom_end395; 
                    ++bigsum_L2__ORDERKEY_dom_it396)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it396;
                    cstr427430441 += ( ( (
                         qCUSTOMER3[bigsum_L2__ORDERKEY] <= 100 ) && (
                         100 < qCUSTOMER3[bigsum_L2__ORDERKEY] ) )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }
            cstr427430 += -1*cstr427430441;
            cstr427 += cstr427428+cstr427429+cstr427430;

            cstr448 = 0;
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_it402 = bigsum_L1__ORDERKEY_dom.begin();
            set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                >::iterator bigsum_L1__ORDERKEY_dom_end401 = bigsum_L1__ORDERKEY_dom.end();
            for (
                ; bigsum_L1__ORDERKEY_dom_it402 != bigsum_L1__ORDERKEY_dom_end401; 
                ++bigsum_L1__ORDERKEY_dom_it402)
            {
                int64_t bigsum_L1__ORDERKEY = *bigsum_L1__ORDERKEY_dom_it402;
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_it400 = bigsum_L2__ORDERKEY_dom.begin();
                set<int64_t, std::less<int64_t>, boost::pool_allocator<int64_t> 
                    >::iterator bigsum_L2__ORDERKEY_dom_end399 = bigsum_L2__ORDERKEY_dom.end();
                for (
                    ; bigsum_L2__ORDERKEY_dom_it400 != bigsum_L2__ORDERKEY_dom_end399; 
                    ++bigsum_L2__ORDERKEY_dom_it400)
                {
                    int64_t bigsum_L2__ORDERKEY = 
                        *bigsum_L2__ORDERKEY_dom_it400;
                    cstr448 += ( ( 100 < qCUSTOMER3[bigsum_L2__ORDERKEY] )?
                         ( qCUSTOMER2[make_tuple(bigsum_L1__ORDERKEY,bigsum_L2__ORDERKEY)] ) : ( 0 ) );
                }
            }

            if ( ( cstr427 < 1 ) && ( 1 <= cstr448 ) )
            {
                q426 += qCUSTOMER4[make_tuple(bigsum_L1__ORDERKEY,C__CUSTKEY)];
            }
        }
        q[C__CUSTKEY] += -1*q426;
    }
   */

    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER1_it408 = qCUSTOMER1.begin();
    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER1_end407 = qCUSTOMER1.end();
    for (; qCUSTOMER1_it408 != qCUSTOMER1_end407; ++qCUSTOMER1_it408)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER1_it408->first);
        qCUSTOMER1[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY)] += -1*qCUSTOMER1ORDERS1[make_tuple(
            bigsum_L1__ORDERKEY,ORDERKEY)];
    }

    map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::iterator 
        qCUSTOMER1LINEITEM1_it410 = qCUSTOMER1LINEITEM1.find(make_tuple(ORDERKEY,CUSTKEY,ORDERKEY));

    if ( qCUSTOMER1LINEITEM1_it410 != qCUSTOMER1LINEITEM1.end() )
    {
        qCUSTOMER1LINEITEM1[make_tuple(ORDERKEY,CUSTKEY,ORDERKEY)] += -1;
    }

    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER4_it412 = qCUSTOMER4.begin();
    map<tuple<int64_t,int64_t>,double,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,double> > >::iterator 
        qCUSTOMER4_end411 = qCUSTOMER4.end();
    for (; qCUSTOMER4_it412 != qCUSTOMER4_end411; ++qCUSTOMER4_it412)
    {
        int64_t bigsum_L1__ORDERKEY = get<0>(qCUSTOMER4_it412->first);
        qCUSTOMER4[make_tuple(
            bigsum_L1__ORDERKEY,CUSTKEY)] += -1*qCUSTOMER4ORDERS1[make_tuple(
            bigsum_L1__ORDERKEY,ORDERKEY)]*qORDERS4[CUSTKEY];
    }

    map<tuple<int64_t,int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t,int64_t>,int> > >::iterator 
        qCUSTOMER4LINEITEM1_it414 = qCUSTOMER4LINEITEM1.find(make_tuple(ORDERKEY,CUSTKEY,ORDERKEY));

    if ( qCUSTOMER4LINEITEM1_it414 != qCUSTOMER4LINEITEM1.end() )
    {
        qCUSTOMER4LINEITEM1[make_tuple(ORDERKEY,CUSTKEY,ORDERKEY)] += -qORDERS4[CUSTKEY];
    }

    qLINEITEM1[make_tuple(ORDERKEY,CUSTKEY,ORDERKEY)] += -1*qORDERS4[CUSTKEY];

    map<tuple<int64_t,int64_t>,int,std::less<tuple<int64_t,int64_t> 
        >,boost::pool_allocator<pair<tuple<int64_t,int64_t>,int> > >::iterator 
        qLINEITEM1CUSTOMER1_it416 = qLINEITEM1CUSTOMER1.find(make_tuple(ORDERKEY,CUSTKEY));

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
