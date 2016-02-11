(**
   A module for pretty-printing Calculus expressions and Arithmetic values.
   
   {b WARNING}: [CalculusPrinter] is not threadsafe.  Only one thread may be
   using it at any given time.
*)

open Type
open Arithmetic
open Calculus
open Format
open Constants
;;


(**/**)
(** The number of spaces used to indent the second and following lines of a 
    term *)
let line_indent = ref 2;

(** Shorthand record for invoking relevant functions from [Format] *)
type formatter_t = {
   bopen  : int -> unit;
   bclose : unit -> unit;
   cut    : unit -> unit;
   space  : unit -> unit;
   break  : int -> int -> unit;
   string : string -> unit;
   flush  : unit -> unit;
}

(** Generate a [formatter_t] shorthand record from a formatter. *)
let wrap_formatter (f:formatter) = {
   bopen  = pp_open_box     f;
   bclose = pp_close_box    f;
   cut    = pp_print_cut    f;
   space  = pp_print_space  f;
   break  = pp_print_break  f;
   string = pp_print_string f;
   flush  = pp_print_flush  f;
};;

(** The globally used formatter *)
let fmt:formatter_t ref = ref (wrap_formatter str_formatter);;

(** Shorthand function for transforming stringifiers into methods that output to
    a formatter. *)
let rec dump (stringifier: 'a -> string) (thing:'a): unit =
   !fmt.string (stringifier thing)
;;

(**
   Dump a list of elements to the formatter.
   @param default          (optional) The string to dump if [l] is empty 
                           (default: "")
   @param format_element   The function for formatting elements of the list
   @param sep              The string to separate elements of the list by
   @param l                The list to dump
*)
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

(**
   Dump a value expression to the formatter
   @param v   The value to dump
*)
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
         format_list format_value " *" [(Arithmetic.mk_int (-1)); element];
         !fmt.string ")"
         
      | ValueRing.Val(AConst(_)) | ValueRing.Val(AVar(_)) -> 
         !fmt.string (string_of_value v)
         
      | ValueRing.Val(AFn(fname, fargs, ftype)) ->
         !fmt.string "[";
         !fmt.string fname;
         !fmt.string ":";
         !fmt.string (string_of_type ftype);
         !fmt.string "](";
         !fmt.bopen 0;
         format_list format_value "," fargs;
         !fmt.bclose ();
         !fmt.string ")";
         
   end;
   !fmt.bclose ();
;;

(**
   Dump a Calculus expression to the formatter
   @param expr   The Calculus expression to dump
*)
let rec format_expr ?(show_type = false) (expr:expr_t) = 
   let rcr = format_expr ~show_type:show_type in
   !fmt.bopen 0;
   begin match expr with
      | CalcRing.Sum(sl)  -> 
         !fmt.string "(";
         format_list rcr " +" ~default:"0" sl;
         !fmt.string ")"
         
      | CalcRing.Prod(pl) -> 
         !fmt.string "(";
         format_list rcr " *" ~default:"1" pl;
         !fmt.string ")"
         
      | CalcRing.Neg(element) -> 
         !fmt.string "(NEG * ";
         rcr element;
         !fmt.string ")"

      | CalcRing.Val(Value(ValueRing.Val(AVar(_) | AConst(_)) as v)) ->
         format_value v;
      
      | CalcRing.Val(Value(v)) ->
         !fmt.string "{";
         format_value v;
         !fmt.string "}"

      | CalcRing.Val(AggSum(gb_vars, subexp)) ->
         !fmt.string "AggSum([";
         !fmt.bopen 0;
         format_list (dump string_of_var) "," gb_vars;
         !fmt.bclose ();
         !fmt.string "]";
         !fmt.string ", ";
         !fmt.break 0 !line_indent;
         rcr subexp;
         !fmt.string ")"
      
      | CalcRing.Val(Rel(reln, relv)) ->
         !fmt.string reln;
         !fmt.string "(";
         !fmt.bopen 0;
         format_list (dump string_of_var) "," relv;
         !fmt.bclose ();
         !fmt.string ")"

      | CalcRing.Val(DeltaRel(reln, relv)) ->
         !fmt.string "(DELTA ";
         !fmt.string reln;
         !fmt.string ")(";
         !fmt.bopen 0;
         format_list (dump string_of_var) "," relv;
         !fmt.bclose ();
         !fmt.string ")"
               
      | CalcRing.Val(DomainDelta(subexp)) ->
         !fmt.string "DOMAIN(";
         !fmt.break 1 !line_indent;
         rcr subexp;
         !fmt.string ")";

      | CalcRing.Val(External(extn, ivars, ovars, extt, extivc)) ->
         !fmt.string extn;
         !fmt.string "(";
         dump string_of_type extt;
         !fmt.string ")";
         !fmt.cut ();
         !fmt.string "[";
         !fmt.bopen 0;
         format_list (dump (string_of_var ~verbose:show_type)) "," ivars;
         !fmt.bclose ();
         !fmt.string "]";
         !fmt.cut ();
         !fmt.string "[";
         !fmt.bopen 0;
         format_list (dump (string_of_var ~verbose:show_type)) "," ovars;
         !fmt.bclose ();
         !fmt.string "]";
         begin match extivc with
            | None -> ()
            | Some(ivcexpr) -> 
               !fmt.string ":(";
               rcr ivcexpr;
               !fmt.string ")"
         end
      
      | CalcRing.Val(Cmp(cmpop, lhs, rhs)) ->
         !fmt.string "{";
         format_value lhs;
         !fmt.string " ";
         !fmt.string (string_of_cmp cmpop);
         !fmt.space ();
         format_value rhs;
         !fmt.string "}";

      | CalcRing.Val(CmpOrList(v, consts)) ->
         !fmt.string "{";
         format_value v;
         !fmt.string " IN [";
         format_list (dump string_of_value) "," 
            (List.map (Arithmetic.mk_const) consts);
         !fmt.string "]}";
         
      | CalcRing.Val(Lift(v, subexp)) ->
         !fmt.string "(";
         !fmt.string (string_of_var v);
         !fmt.string " ^=";
         !fmt.break 1 !line_indent;
         rcr subexp;
         !fmt.string ")";

(***** BEGIN EXISTS HACK *****)
      | CalcRing.Val(Exists(subexp)) ->
         !fmt.string "EXISTS(";
         !fmt.break 1 !line_indent;
         rcr subexp;
         !fmt.string ")";
(***** END EXISTS HACK *****)
      
   end;
   !fmt.bclose();;

(**/**)

(******************************* API **************************)

(**
   Generate the pretty-printed (Calculusparser-compatible) representation of a 
   value expression.
   @param v    A value expression
   @return     The pretty-printed string representation of [v]
*)
let string_of_value v =
   format_value v;
   flush_str_formatter ()
;;

(**
   Generate the pretty-printed (Calculusparser-compatible) representation of a 
   Calculus expression.
   @param e    A Calculus expression
   @return     The pretty-printed string representation of [e]
*)
let string_of_expr ?(show_type = Debug.active "PRINT-VERBOSE") e =
  format_expr ~show_type:show_type e;
   flush_str_formatter ()
