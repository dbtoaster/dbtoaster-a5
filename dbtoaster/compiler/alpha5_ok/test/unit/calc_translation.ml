
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
      AggSum([],
         (Prod[
            Relation("R", [ra;rb]);
            Relation("S", [sb;sc]);
            Relation("T", [tc;td]);
            Cmp(Var(rb),Eq,Var(sb));
            Cmp(Var(sc),Eq,Var(tc));
            Value(Var(ra));
            Value(Var(td))
         ])
      )
   ]
;;
let ra = ("R_A",IntegerT) in
let rb = ("R_B",IntegerT) in
let sc = ("S_C",IntegerT) in
let sd = ("S_D",IntegerT) in
   translate_test "test/sql/rs_inequality.sql" [
      AggSum([],
         (Prod[
            Relation("R", [ra;rb]);
            Relation("S", [sc;sd]);
            Cmp(Var(rb),Lt,Var(sc));
            Value(Var(ra));
            Value(Var(sc))
         ])
      )
   ]
;;
let p i = (("B"^(string_of_int i)^"_PRICE"), DoubleT) in
let v i = (("B"^(string_of_int i)^"_VOLUME"), IntegerT) in
   translate_test "test/sql/finance/vwap.sql" [
      AggSum([],
         (Prod[
            Relation("BIDS", [p 1; v 1]);
            Definition(("TMP_0", DoubleT), (Prod[
               Value(Const(Double(0.25)));
               AggSum([],
                  (Prod[
                     Relation("BIDS", [p 3; v 3]);
                     Value(Var(v 3))
                  ])
               )
            ]));
            Definition(("TMP_1", IntegerT), (AggSum([], (Prod[
               Relation("BIDS", [p 2; v 2]);
               Cmp((Var(p 2)), Gt, (Var(p 1)));
               Value(Var(v 2))
            ]))));
            Cmp((Var("TMP_0", DoubleT)), Gt, (Var("TMP_1", IntegerT)));
            Value(Var(p 1));
            Value(Var(v 1))
         ])
      )
   ]