open UnitTest
open Types
;;

let test msg expr exp_type =
   log_test ("Sql Expression Types ( "^msg^" )") 
            (Types.string_of_type)
            (Sql.expr_type (parse_sql_expr expr) [
               "R", [Some("R"), "A", TFloat;
                     Some("R"), "B", TInt], Schema.StreamRel,
                    (Schema.NoSource, ("",[]));
               "S", [Some("S"), "B", TInt;
                     Some("S"), "C", TString], Schema.StreamRel,
                    (Schema.NoSource, ("",[]));
            ] [
               "R", Sql.Table("R");
               "Q", Sql.Table("R");
               "S", Sql.Table("S");
            ])
            exp_type
in
   test "Const Int"             "1"       TInt;
   test "Const Float"           "1.0"     TFloat;
   test "Const String"          "'1'"     TString;
   test "Float Var"             "R.A"     TFloat;
   test "Int Var"               "R.B"     TInt;
   test "String Var (b)"        "S.C"     TString;
   test "String Var (ub)"       "C"       TString;
   test "Addition (i+i)"        "1+1"     TInt;
   test "Addition (f+i)"        "1.0+1"   TFloat;
   test "Addition (i+f)"        "1+1.0"   TFloat;
   test "Addition (f+f)"        "1.0+1.0" TFloat;
   test "Multiplication (i*i)"  "1*1"     TInt;
   test "Multiplication (f*i)"  "1.0*1"   TFloat;
   test "Multiplication (i*f)"  "1*1.0"   TFloat;
   test "Multiplication (f*f)"  "1.0*1.0" TFloat;
   test "Subtraction (i-i)"     "1-1"     TInt;
   test "Subtraction (f-i)"     "1.0-1"   TFloat;
   test "Subtraction (i-f)"     "1-1.0"   TFloat;
   test "Subtraction (f-f)"     "1.0-1.0" TFloat;
   test "Division (i/i)"        "1/1"     TFloat;
   test "Division (f/i)"        "1.0/1"   TFloat;
   test "Division (i/f)"        "1/1.0"   TFloat;
   test "Division (f/f)"        "1.0/1.0" TFloat;
   ()