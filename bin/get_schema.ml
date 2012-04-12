#!/bin/bash

./bin/dbtoaster_unit << EOF
open UnitTest
open Calculus
;;
let get_schema expr = 
   let ivars,ovars = 
      Calculus.schema_of_expr (parse_calc expr)
   in
      print_string (ListExtras.ocaml_of_list fst ivars);
      print_endline (ListExtras.ocaml_of_list fst ovars);
      print_string "\n"
;;

while true do 
   try 
      get_schema (input_line stdin)
   with End_of_file ->
      exit 0
done;;
EOF