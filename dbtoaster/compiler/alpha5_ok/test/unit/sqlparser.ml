
open Util
open Sql
open Sql.Types


let parse_file (f:string): Sql.file_t =
  let lexbuff = Lexing.from_channel (if f <> "-" then (open_in f) else stdin) in
     Sql.reset_table_defs ();
     Sqlparser.dbtoasterSqlList Sqllexer.tokenize lexbuff
;;
let parse_test (f:string) (expected:Sql.select_t list): unit =
   Debug.log_unit_test 
      ("Parsing SQL file '"^f^"'")
      (string_of_list0 "\n" Sql.string_of_select)
      (snd (parse_file f))
      expected
;;

let mk_target expr =
   (string_of_expr expr, expr)

;;
parse_test "test/sql/rst.sql" [
   [( "SUM((A)*(D))",
      (Aggregate(SumAgg, (
         Arithmetic((Var((Some("R")), "A", IntegerT)),
                    Prod,
                    (Var((Some("T")), "D", IntegerT))))))
   )],
   [("R", (Table("R"))); 
    ("S", (Table("S")));
    ("T", (Table("T")))],
   (And(Comparison((Var((Some("R"), "B", IntegerT))),
                   Eq,
                   (Var((Some("S"), "B", IntegerT)))),
        Comparison((Var((Some("S"), "C", IntegerT))),
                   Eq,
                   (Var((Some("T"), "C", IntegerT))))
   )),
   []
]
;;
parse_test "test/sql/rs_inequality.sql" [
   [( "SUM((A)*(C))",
      (Aggregate(SumAgg, (
         Arithmetic((Var((Some("R")), "A", IntegerT)),
                    Prod,
                    (Var((Some("S")), "C", IntegerT))))))
   )],
   [("R", (Table("R"))); 
    ("S", (Table("S")))],
   (Comparison((Var((Some("R"), "B", IntegerT))),
                Lt,
                (Var((Some("S"), "C", IntegerT))))),
   []
]
;;
(*Parsing.set_trace true;;*)
let lhs_subq = (
   [(  "SUM(B3.VOLUME)",
      (Aggregate(SumAgg, (
         Var((Some("B3")), "VOLUME", IntegerT)
      )))
   )],
   [("B3", (Table("BIDS")))],
   (ConstB(true)),
   [])
in
let rhs_subq = (
   [(  "SUM(B2.VOLUME)",
      (Aggregate(SumAgg, (
         Var((Some("B2")), "VOLUME", IntegerT)
      )))
   )],
   [("B2", (Table("BIDS")))],
   (Comparison((Var((Some("B2"), "PRICE", DoubleT))),
                Gt,
                (Var((Some("B1"), "PRICE", DoubleT))))),
   [])
in
parse_test "test/sql/finance/vwap.sql" [
   [( "SUM((B1.PRICE)*(B1.VOLUME))",
      (Aggregate(SumAgg, (
         Arithmetic((Var((Some("B1")), "PRICE", DoubleT)),
                    Prod,
                    (Var((Some("B1")), "VOLUME", IntegerT))))))
   )],
   [("B1", (Table("BIDS")))],
   (Comparison(
      (Arithmetic((Const(Double(0.25))), Prod, (NestedQ(lhs_subq)))),
      Gt, (NestedQ(rhs_subq))
   )),
   []
]


