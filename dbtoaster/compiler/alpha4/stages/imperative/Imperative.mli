open M3
open K3.SR

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
  | Concat        (* Collection concatentation *)
  | ConcatElement (* Singleton collection concatenation *) 
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
type ('ext_type, 'ext_fn) typed_imp_t  = ('ext_type type_t, 'ext_type, 'ext_fn) imp_t

(* Typing helpers *)
val host_type : 'ext_type type_t -> K3.SR.type_t

(* Helper method to extract a type from an imperative expression *)
val metadata_of_expr_t : ('a, 'ext_type, 'ext_fn) expr_t -> 'a    
val metadata_of_imp_t : ('a, 'ext_type, 'ext_fn) imp_t -> 'a

val type_of_expr_t : ('ext_type, 'ext_fn) typed_expr_t -> 'ext_type type_t
val type_of_imp_t : ('ext_type, 'ext_fn) typed_imp_t -> 'ext_type type_t

val fold_expr : ('b -> 'c list -> ('a, 'ext_type, 'ext_fn) expr_t -> 'c) ->
                ('b ->            ('a, 'ext_type, 'ext_fn) expr_t -> 'b) ->
                 'b -> 'c -> ('a, 'ext_type, 'ext_fn) expr_t -> 'c

val fold_imp : ('b -> 'c list -> ('a, 'ext_type, 'ext_fn) imp_t  -> 'c) ->
               ('b -> 'c list -> ('a, 'ext_type, 'ext_fn) expr_t -> 'c) ->
               ('b ->            ('a, 'ext_type, 'ext_fn) imp_t  -> 'b) ->
               ('b ->            ('a, 'ext_type, 'ext_fn) expr_t -> 'b) ->
                'b -> 'c -> ('a, 'ext_type, 'ext_fn) imp_t -> 'c

(* AST stringification *)
val string_of_type : ('ext_type type_t -> string) -> 'ext_type type_t -> string

val string_of_typed_imp :
  ('ext_type type_t -> string) -> ('ext_fn -> string)
  -> ('ext_type, 'ext_fn) typed_imp_t -> string

module type ProgramType =
sig
  type source_code_t = SourceCode.source_code_t 

  type ('a, 'ext_type, 'ext_fn) trigger_t =
    M3.pm_t * M3.rel_id_t * M3.var_t list * ('a, 'ext_type, 'ext_fn) imp_t

  type ('a, 'ext_type, 'ext_fn) src_prog_t = 
    | Trigger of ('a, 'ext_type, 'ext_fn) trigger_t
    | Main of ('a, 'ext_type, 'ext_fn) trigger_t list * source_code_t
end

module Program : ProgramType