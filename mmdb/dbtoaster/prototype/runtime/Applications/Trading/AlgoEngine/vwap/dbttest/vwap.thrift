include "datasets.thrift"

namespace cpp DBToaster.Debugger
namespace java DBToaster.Debugger

typedef i32 DBToasterStreamId
enum DmlType { insertTuple = 0, deleteTuple = 1 }

struct ThriftVwapTuple {
    1: DmlType type,
    2: DBToasterStreamId id,
    3: datasets.VwapTuple data
}

struct B_elem {
    1: i32 P1,
    2: i32 V1,
}

service Debugger {
    void step_VwapBids(1:ThriftVwapTuple input),
    void stepn_VwapBids(1:i32 n),
    i32 get_var2(),
    map<i32,i32> get_map0(),
    set<i32> get_dom0(),
    list<B_elem> get_B(),
    map<i32,i32> get_map1(),
    set<i32> get_dom1(),
    map<i32,i32> get_map2(),
    map<i32,i32> get_map3(),
}