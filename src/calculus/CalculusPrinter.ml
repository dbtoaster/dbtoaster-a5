open Types
open Arithmetic
open Calculus
open Format
;;

let line_indent = ref 2;

type formatter_t = {
   bopen  : int -> unit;
   bclose : unit -> unit;
   cut    : unit -> unit;
   space  : unit -> unit;
   break  : int -> int -> unit;
   string : string -> unit;
   flush  : unit -> unit;
}

let wrap_formatter (f:formatter) = {
   bopen  = pp_open_box     f;
   bclose = pp_close_box    f;
   cut    = pp_print_cut    f;
   space  = pp_print_space  f;
   break  = pp_print_break  f;
   string = pp_print_string f;
   flush  = pp_print_flush  f;
};;

let fmt:formatter_t ref = ref (wrap_formatter str_formatter);;

let rec dump (stringifier: 'a -> string) (thing:'a): unit =
   !fmt.string (stringifier thing)
;;

let rec format_list ?(default = "") (format_element:'a -> unit) (sep:string) 
                    (l:'a list): unit =
   if l = [] then !fmt.string default
   else (
      format_element (List.hd l);
      List.iter (fun element ->
         !fmt.string sep; !fmt.break 1 !line_indent; format_element element
      ) (List.tl l)
   )
;;

let rec format_value (v:value_t) =
   !fmt.bopen 0;
   begin match v with
      | ValueRing.Sum(sl)  -> 
         !fmt.string "(";
         format_list format_value " +" ~default:"0" sl;
         !fmt.string ")"
         
      | ValueRing.Prod(pl) -> 
         !fmt.string "(";
         format_list format_value " *" ~default:"1" pl;
         !fmt.string ")"
         
      | ValueRing.Neg(element) -> 
         !fmt.string "(";
         format_list format_value " *" [(mk_int (-1)); element];
         !fmt.string ")"
         
      | ValueRing.Val(AConst(_)) | ValueRing.Val(AVar(_)) -> 
         !fmt.string (string_of_value v)
         
      | ValueRing.Val(AFn(fname, fargs, _)) ->
         !fmt.string fname;
         !fmt.string "(";
         !fmt.bopen 0;
         format_list format_value "," fargs;
         !fmt.bclose ();
         !fmt.string ")";
         
   end;
   !fmt.bclose ();
;;

let rec format_expr (expr:expr_t) = 
   !fmt.bopen 0;
   begin match expr with
      | CalcRing.Sum(sl)  -> 
         !fmt.string "(";
         format_list format_expr " +" ~default:"0" sl;
         !fmt.string ")"
         
      | CalcRing.Prod(pl) -> 
         !fmt.string "(";
         format_list format_expr " *" ~default:"1" pl;
         !fmt.string ")"
         
      | CalcRing.Neg(element) -> 
         !fmt.string "(";
         format_list format_expr " *"
                     [  (CalcRing.mk_val (Value(mk_int (-1))));
                        element ];
         !fmt.string ")"

      | CalcRing.Val(Value(v)) ->
         !fmt.string "[";
         format_value v;
         !fmt.string "]"

      | CalcRing.Val(AggSum(gb_vars, subexp)) ->
         !fmt.string "AggSum([";
         !fmt.bopen 0;
         format_list (dump string_of_var) "," gb_vars;
         !fmt.bclose ();
         !fmt.string "]";
         !fmt.string ", ";
         !fmt.break 0 !line_indent;
         format_expr subexp;
         !fmt.string ")"
      
      | CalcRing.Val(Rel(reln, relv)) ->
         !fmt.string reln;
         !fmt.string "(";
         !fmt.bopen 0;
         format_list (dump string_of_var) "," relv;
         !fmt.bclose ();
         !fmt.string ")"
         
      | CalcRing.Val(External(extn, ivars, ovars, extt, extivc)) ->
         !fmt.string extn;
         if(extt <> TFloat) then (
            !fmt.string "(";
            dump string_of_type extt;
            !fmt.string ")"
         );
         !fmt.cut ();
         !fmt.string "[";
         !fmt.bopen 0;
         format_list (dump string_of_var) "," ivars;
         !fmt.bclose ();
         !fmt.string "]";
         !fmt.cut ();
         !fmt.string "[";
         !fmt.bopen 0;
         format_list (dump string_of_var) "," ovars;
         !fmt.bclose ();
         !fmt.string "]"
      
      | CalcRing.Val(Cmp(cmpop, lhs, rhs)) ->
         !fmt.string "[";
         format_value lhs;
         !fmt.string " ";
         !fmt.string (string_of_cmp cmpop);
         !fmt.space ();
         format_value rhs;
         !fmt.string "]";
         
      | CalcRing.Val(Lift(v, subexp)) ->
         !fmt.string "(";
         !fmt.string (string_of_var v);
         !fmt.string " ^=";
         !fmt.break 1 !line_indent;
         format_expr subexp;
         !fmt.string ")";
      
   end;
   !fmt.bclose();;


(******************************* API **************************)

let string_of_value v =
   format_value v;
   flush_str_formatter ()
;;

let string_of_expr e =
   format_expr e;
   flush_str_formatter ()
