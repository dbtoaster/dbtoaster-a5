open UnitTest
open Calculus
;;
let get_schema expr = 
   let ivars,ovars = 
      Calculus.schema_of_expr (parse_calc expr)
   in
      print_string (ListExtras.ocaml_of_list fst ivars);
      print_endline (ListExtras.ocaml_of_list fst ovars);
      print_string "\n\n"
;;

while true do 
   try 
      print_string ">> ";
      flush stdout;
      get_schema (input_line stdin)
   with End_of_file ->
      print_string "\n\n";
      exit 0
done;;
