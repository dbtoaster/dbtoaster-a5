open Util
open M3
open K3.SR
open Format

type op_t = Add | Mult | Eq | Neq | Lt | Leq | If0 | Assign | UMinus

type 'ext_type type_t = Host of K3.SR.type_t | Target of 'ext_type
type 'ext_type decl_t = id_t * 'ext_type type_t
type 'ext_type var_t  = id_t * 'ext_type type_t

type 'ext_fn fn_id_t =
  | TupleElement of int
  | Projection of int list
  | Singleton
  | Combine
  | Member
  | Lookup
  | Slice of int list
  | Concat
  | ConcatElement
  | MapUpdate
  | MapValueUpdate
  | Ext of 'ext_fn
  
(* Parameterized expression types for imperative expressions. The parameter
 * may be arbitrary metadata maintained at AST nodes *)
type ('a, 'ext_type, 'ext_fn) expr_t =
    Const   of 'a * const_t
  | Var     of 'a * 'ext_type var_t
  | Tuple   of 'a * ('a, 'ext_type, 'ext_fn) expr_t list
  | Op      of 'a * op_t * ('a, 'ext_type, 'ext_fn) expr_t
  | BinOp   of 'a * op_t * ('a, 'ext_type, 'ext_fn) expr_t
                         * ('a, 'ext_type, 'ext_fn) expr_t
  | Fn      of 'a * 'ext_fn fn_id_t * ('a, 'ext_type, 'ext_fn) expr_t list

 
(* Imperative statements. These all have type unit, thus no typing 
 * information beyond that for expressions is needed. *)
(* We use forced declaration of the loop element to distinguish collection
 * function application of vars of type tuple, and tuple application.
 * Vars that are tuples may be used anywhere within the loop body, thus we
 * ensure their declaration. Tuple application may be inlined as desired
 * by the target language implementation. *)
type ('a, 'ext_type, 'ext_fn) imp_t =
    Expr       of  'a *  ('a, 'ext_type, 'ext_fn) expr_t

  | Block      of  'a * (('a, 'ext_type, 'ext_fn) imp_t) list

  | Decl       of  'a *  ('ext_type decl_t) *
                        (('a, 'ext_type, 'ext_fn) expr_t) option
  
  | For        of  'a *  ('ext_type decl_t * bool) * (* bool => forced decl *)
                         ('a, 'ext_type, 'ext_fn) expr_t *
                         ('a, 'ext_type, 'ext_fn) imp_t
  
  | IfThenElse of  'a *  ('a, 'ext_type, 'ext_fn) expr_t * 
                         ('a, 'ext_type, 'ext_fn) imp_t *
                         ('a, 'ext_type, 'ext_fn) imp_t

type ('ext_type, 'ext_fn) typed_expr_t = ('ext_type type_t, 'ext_type, 'ext_fn) expr_t
type ('ext_type, 'ext_fn) typed_imp_t = ('ext_type type_t, 'ext_type, 'ext_fn) imp_t

(* Typing helpers *)

let host_type t =
  match t with | Host ht -> ht | _ -> failwith "invalid host type"

(* Helper method to extract a type from an imperative expression *)
let metadata_of_expr_t (e : ('a, 'ext_type, 'ext_fn) expr_t) : 'a = match e with
  | Const (m,_) -> m 
  | Var (m,_) -> m
  | Tuple (m,_) -> m
  | Op (m,_,_) -> m
  | BinOp (m,_,_,_) -> m
  | Fn (m,_,_) -> m    

let metadata_of_imp_t (i : ('a, 'ext_type, 'ext_fn) imp_t) : 'a = match i with
  | Expr (m,_) -> m
  | Block (m,_) -> m
  | Decl (m,_,_) -> m
  | For (m,_,_,_) -> m
  | IfThenElse (m,_,_,_) -> m

let type_of_expr_t (e: ('ext_type, 'ext_fn) typed_expr_t) = metadata_of_expr_t e
let type_of_imp_t (i: ('ext_type, 'ext_fn) typed_imp_t) = metadata_of_imp_t i

type ('a, 'ext_type, 'ext_fn) expr_trace = 
   (('a, 'ext_type, 'ext_fn) expr_t -> string -> unit)

type ('a, 'ext_type, 'ext_fn) imp_trace  = 
   (('a, 'ext_type, 'ext_fn) imp_t -> string -> unit)

type ('a, 'ext_type, 'ext_fn) all_trace =
   ('a, 'ext_type, 'ext_fn) expr_trace * ('a, 'ext_type, 'ext_fn) imp_trace
   
(* Fold function *)
let rec fold_expr ?(trace:(('a, 'ext_type, 'ext_fn) all_trace option) = None)
                  (f:   'b -> 'c list -> ('a, 'ext_type, 'ext_fn) expr_t -> 'c)
                  (pre: 'b ->            ('a, 'ext_type, 'ext_fn) expr_t -> 'b)
                  (acc: 'b) (init: 'c) (e : ('a, 'ext_type, 'ext_fn) expr_t) : 'c
=
  try
     let nacc = pre acc e in
     let app_e = f nacc in
     let rcr = fold_expr ~trace:trace f pre nacc init in
     begin match e with
       | Const _ -> app_e [init] e
       | Var _ -> app_e [init] e
       | Tuple (_,el) -> app_e (List.map rcr el) e
       | Op (_,_,ce) -> app_e [rcr ce] e
       | BinOp (_,_,le,re) -> app_e (List.map rcr [le; re]) e
       | Fn (_,_,args) -> app_e (List.map rcr args) e  
     end
   with Failure(msg) -> (
      (match trace with None -> () | Some(f,_) -> f e msg);
      failwith msg
   )
   
and fold_imp ?(trace:(('a, 'ext_type, 'ext_fn) all_trace option) = None)
             (f_imp :   'b -> 'c list -> ('a, 'ext_type, 'ext_fn) imp_t  -> 'c)
             (f_expr:   'b -> 'c list -> ('a, 'ext_type, 'ext_fn) expr_t -> 'c)
             (pre:      'b ->            ('a, 'ext_type, 'ext_fn) imp_t  -> 'b)
             (pre_expr: 'b ->            ('a, 'ext_type, 'ext_fn) expr_t -> 'b)
             (acc: 'b) (init: 'c) (i : ('a, 'ext_type, 'ext_fn) imp_t) : 'c
=
  try
     let nacc = pre acc i in
     let app_f = f_imp nacc in
     let rcr_e = fold_expr ~trace:trace f_expr pre_expr nacc init in
     let rcr = fold_imp ~trace:trace f_imp f_expr pre pre_expr nacc init in
     begin match i with
       | Expr (_,e) -> app_f [rcr_e e] i
       | Block (_,l) -> app_f (List.map rcr l) i 
       | Decl (_,(id,ty),Some(init)) -> app_f [rcr_e init] i
       | Decl (_,(id,ty),None) -> app_f [] i
       | For (_,((id,ty),f),s,b) ->
         let rs = rcr_e s in app_f [rs; rcr b] i
       | IfThenElse(_,p,t,e) ->
         let rp = rcr_e p in app_f ([rp]@(List.map rcr [t;e])) i
     end
   with Failure(msg) -> (
      (match trace with None -> () | Some(_,f) -> f i msg);
      failwith msg
   )

(* AST stringification *)
let string_of_op op = match op with
  | Add -> "Add"
  | Mult -> "Mult"
  | Eq -> "Eq"
  | Neq -> "Neq"
  | Lt -> "Lt"
  | Leq -> "Leq"
  | If0 -> "If0"
  | Assign -> "Assign"
  | UMinus -> "UMinus" 

let string_of_type ext_type_f t = match t with
  | Host x -> K3.SR.string_of_type x
  | Target(y) -> ext_type_f t

let string_of_fn ext_fn_f fn_id =
  let of_list ?(delim = ",") l = String.concat delim l in
  let list_of_int_list l =
    "["^(of_list ~delim:";" (List.map string_of_int l))^"]" in
  match fn_id with
  | TupleElement i -> "TupleElement("^(string_of_int i)^")"
  | Projection idx -> "Projection("^(list_of_int_list idx)^"])"
  | Singleton -> "Singleton"
  | Combine -> "Combine"
  | Member -> "Member"
  | Lookup -> "Lookup"
  | Slice (idx) -> "Slice("^(list_of_int_list idx)^")"
  | Concat -> "Concat"
  | ConcatElement -> "ConcatElement"
  | MapUpdate -> "MapUpdate"
  | MapValueUpdate -> "MapValueUpdate"
  | Ext fn -> "Ext("^(ext_fn_f fn)^")"

let string_of_decl ext_type_f (id,ty) = id^","^(string_of_type ext_type_f ty)

let string_of_imp string_of_meta string_of_ext_type string_of_ext_fn i =
  let ob () = pp_open_hovbox str_formatter 2 in
  let cb () = pp_close_box str_formatter () in
  let pc () = pp_print_cut str_formatter () in
  let ps s = pp_print_string str_formatter s in
  let psp = pp_print_space str_formatter in 
  let pm m = let x = string_of_meta m in if x <> "" then (ps x; ps ",") in
  let pop_pre tag meta = pc(); ob(); ps tag; ps "("; pm meta; pc() in
  let pop_post (sep,lref,_) =
    if !lref = 0 then ps "]"; ps ")";
    if sep <> "" then (ps sep; psp()); cb(); pc()
  in
  let sdecl = string_of_decl string_of_ext_type in
  let sfn = string_of_fn string_of_ext_fn in   
  let aux i : int option =
    let xrf = ref (-1) in  (* a don't care sublist counter, i.e. no sublist *)
    let post_imp_f td_acc _ _ = pop_post td_acc; None in
    let post_expr_f td_acc _ _ = pop_post td_acc; None in
    let pre_imp_f (_,lref,nref) i =
      let post_sep =
        decr lref; decr nref;
        if !lref > 0 then ";" else if !nref > 0 then "," else "" in
      let c_lref, c_nref = match i with
        | Expr (m,e) -> pop_pre "Expr" m; xrf, xrf
        | Block (m,l) -> pop_pre "Block" m; ps "["; (ref (List.length l)), xrf

        | Decl(m,(id,ty2),init) -> pop_pre "Decl" m;
          ps ("("^(sdecl (id,ty2))^")"^(if init = None then ",None" else ","));
          xrf, xrf
 
        | For(m,((id,ty2),f),s,b) -> pop_pre "For" m;
          ps ("("^(sdecl (id,ty2))^","^(string_of_bool f)^"),"); xrf, (ref 2)

        | IfThenElse(m,p,t,e) -> pop_pre "IfThenElse" m; xrf, (ref 3)
      in (post_sep, c_lref, c_nref)
    in
    let pre_expr_f (_,lref,nref) e =
      let post_sep =
        decr lref; decr nref; 
        if !lref > 0 then ";" else if !nref > 0 then "," else "" in 
      let c_lref, c_nref = match e with
      | Const (m,c)   -> pop_pre "Const" m;
        ps ("CFloat("^(M3Common.string_of_const c)^")"); xrf, xrf
      | Var (m,(v,t)) -> pop_pre "Var" m; ps (sdecl (v,t)); xrf, xrf
      | Tuple(m,el)   -> pop_pre "Tuple" m; xrf, (ref (List.length el))
      | Op(m,op,ce)   -> pop_pre "Op" m; ps ((string_of_op op)^","); xrf, xrf 
      
      | BinOp(m,op,le,re) ->
        pop_pre "BinOp" m; ps ((string_of_op op)^","); xrf, (ref 2) 
      
      | Fn(m,fn_id,args) ->
        pop_pre "Fn" m; ps ((sfn fn_id)^","); ps "[";
        (ref (List.length args)), xrf

      in (post_sep, c_lref, c_nref)
    in
      fold_imp post_imp_f post_expr_f pre_imp_f pre_expr_f
        ("", ref (-1), ref (-1)) None i
  in pp_set_margin str_formatter 80;
     (*pp_set_max_indent str_formatter 40;*)
     flush_str_formatter (ignore (aux i))

let (fold_expr_traced,fold_imp_traced) = 
   let trace_expr e msg = () in
   let trace_imp i msg = Debug.print "IMPERATIVE-TRACEBACK" (fun () ->
      "\n------- Traceback -------\n"^
      (string_of_imp (fun _ -> "") (fun _ -> "") (fun _ -> "") i)
   ) in (
      (fun a b c -> fold_expr ~trace:(Some(trace_expr, trace_imp)) a b c),
      (fun a b c -> fold_imp  ~trace:(Some(trace_expr, trace_imp)) a b c)
   )

let string_of_imp_noext (i : ('a, 'ext_type, 'ext_fn) imp_t) : string =
  string_of_imp
    (fun _ -> "") (fun _ -> failwith "invalid host type") (fun _ -> "") i

let string_of_typed_imp ext_type_f ext_fn_f i =
  string_of_imp (string_of_type ext_type_f) ext_type_f ext_fn_f i


module type ProgramType =
sig
  type source_code_t = SourceCode.source_code_t

  type ('a, 'ext_type, 'ext_fn) trigger_t =
    M3.pm_t * M3.rel_id_t * M3.var_t list * ('a, 'ext_type, 'ext_fn) imp_t

  type ('a, 'ext_type, 'ext_fn) src_prog_t = 
    | Trigger of ('a, 'ext_type, 'ext_fn) trigger_t
    | Main of ('a, 'ext_type, 'ext_fn) trigger_t list * source_code_t
end

module Program : ProgramType =
struct
  type source_code_t = SourceCode.source_code_t 

  type ('a, 'ext_type, 'ext_fn) trigger_t =
    M3.pm_t * M3.rel_id_t * M3.var_t list * ('a, 'ext_type, 'ext_fn) imp_t

  type ('a, 'ext_type, 'ext_fn) src_prog_t = 
    | Trigger of ('a, 'ext_type, 'ext_fn) trigger_t
    | Main of ('a, 'ext_type, 'ext_fn) trigger_t list * source_code_t
end
