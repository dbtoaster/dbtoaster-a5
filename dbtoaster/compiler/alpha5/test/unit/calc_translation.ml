
open Util
open Calculus
open Sql.Types

let parse_file (f:string): Sql.file_t =
  let lexbuff = Lexing.from_channel (if f <> "-" then (open_in f) else stdin) in
     Sql.reset_table_defs ();
     Sqlparser.dbtoasterSqlList Sqllexer.tokenize lexbuff
;;
let translate_test (f:string) (expected:unit Calculus.calc_t list): unit =
   let (tables,queries) = parse_file f in
   Debug.log_unit_test 
      ("Compiling Calc from SQL file '"^f^"'")
      (string_of_list0 "\n" Calculus.string_of_calc)
      (List.map snd (List.flatten (
         List.map (fun q -> Calculus.calc_of_sql tables q) queries
      )))
      expected
;;

let ra = ("R_A",IntegerT) in
let rb = ("R_B",IntegerT) in
let sb = ("S_B",IntegerT) in
let sc = ("S_C",IntegerT) in
let tc = ("T_C",IntegerT) in
let td = ("T_D",IntegerT) in
   translate_test "test/sql/rst.sql" [
      Leaf(AggSum([],
         (Prod[
            Leaf(Relation("R", [ra;rb]));
            Leaf(Relation("S", [sb;sc]));
            Leaf(Relation("T", [tc;td]));
            Leaf(Cmp(Var(rb),Eq,Var(sb)));
            Leaf(Cmp(Var(sc),Eq,Var(tc)));
            Leaf(Value(Var(ra)));
            Leaf(Value(Var(td)))
         ])
      ))
   ]
;;
let ra = ("R_A",IntegerT) in
let rb = ("R_B",IntegerT) in
let sc = ("S_C",IntegerT) in
let sd = ("S_D",IntegerT) in
   translate_test "test/sql/rs_inequality.sql" [
      Leaf(AggSum([],
         (Prod[
            Leaf(Relation("R", [ra;rb]));
            Leaf(Relation("S", [sc;sd]));
            Leaf(Cmp(Var(rb),Lt,Var(sc)));
            Leaf(Value(Var(ra)));
            Leaf(Value(Var(sc)))
         ])
      ))
   ]
;;
let p i = (("B"^(string_of_int i)^"_PRICE"), DoubleT) in
let v i = (("B"^(string_of_int i)^"_VOLUME"), IntegerT) in
   translate_test "test/sql/finance/vwap.sql" [
      Leaf(AggSum([],
         (Prod[
            Leaf(Relation("BIDS", [p 1; v 1]));
            Leaf(Definition(("TMP_0", DoubleT), (Prod[
               Leaf(Value(Const(Double(0.25))));
               Leaf(AggSum([],
                  (Prod[
                     Leaf(Relation("BIDS", [p 3; v 3]));
                     Leaf(Value(Var(v 3)))
                  ])
               ))
            ])));
            Leaf(Definition(("TMP_1", IntegerT), (Leaf(AggSum([], (Prod[
               Leaf(Relation("BIDS", [p 2; v 2]));
               Leaf(Cmp((Var(p 2)), Gt, (Var(p 1))));
               Leaf(Value(Var(v 2)))
            ]))))));
            Leaf(Cmp((Var("TMP_0", DoubleT)), Gt, (Var("TMP_1", IntegerT))));
            Leaf(Value(Var(p 1)));
            Leaf(Value(Var(v 1)))
         ])
      ))
   ]