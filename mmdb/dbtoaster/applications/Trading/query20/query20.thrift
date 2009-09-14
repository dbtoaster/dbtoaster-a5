include "profiler.thrift"



namespace cpp DBToaster.Viewer.query20
namespace java DBToaster.Viewer.query20


typedef i32 DBToasterStreamId
enum DmlType { insertTuple = 0, deleteTuple = 1 }

struct var15_tuple {
1: i32 var0,
2: i32 var1,
}
struct asks_elem {
    1: i32 t11,
    2: i32 id11,
    3: i32 broker_id11,
    4: i32 p11,
    5: i32 v11,
}



service AccessMethod extends profiler.Profiler{
    i32 get_var0(),
    i32 get_var1(),
    map<i32,double> get_map0(),
    map<i32,i32> get_dom0(),
    list<asks_elem> get_asks(),
    map<i32,i32> get_map1(),
    map<i32,i32> get_map2(),
    var15_tuple get_var15()
}