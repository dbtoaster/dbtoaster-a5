open Util;;
open M3;;
open StmtToMapAccessList;;

let stmt0 =
  (("q5", [], ["a"; "c"], (mk_c 0.0, ())), 
    ((mk_prod (mk_ma ("q4", [], ["a"], (mk_c 0.0, ()))) 
              (mk_ma ("q3", [], ["c"], (mk_c 0.0, ())))), 
    ()),
  ()) in
  let left_ma, _ = StmtToMapAccessList.maps_of_stmt stmt0 in
  Debug.log_unit_test "Left Hand side q5" StmtToMapAccessList.string_of_map_access 
    (left_ma)
      (
        ("q5", [], ["a"; "c"], (mk_c 0.0, ()))
      )
;;
