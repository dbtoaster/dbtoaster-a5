(*
   CREATE TABLE name(col1 type1[, ...]) 
   FROM type 
   
   SELECT   gb_term1[, ...], SUM(agg1)[, ...]
   FROM     rel
   WHERE    condition
   GROUP BY 
   
*)

module Input : sig

   type frame_t = 
    | Fixed       of int
    | Delimited   of string
      
   type adaptor_t = string * (string * string) list
   
   type bytestream_options_t = frame_t * adaptor_t
   
   type source_t = 
    | Manual 
    | File   of string (*path*)               * bytestream_options_t
    | Socket of Unix.inet_addr * int (*port*) * bytestream_options_t
   
end

module Types : sig
   type type_t = IntegerT | DoubleT | StringT | AnyT
   
   type const_t = 
    | Integer of int 
    | Double  of float
    | String  of string

   type cmp_t = Lt | Gt | Lte | Gte | Eq | Neq
end

exception SqlException of string
exception Variable_binding_err of string * int

type type_t = Types.type_t

type const_t = Types.const_t

type var_t = string option * string * type_t

type schema_t = var_t list

type table_t = string * schema_t * Input.source_t

type arith_t = Sum | Prod | Sub | Div

type cmp_t = Types.cmp_t

type agg_t = SumAgg

type expr_t = 
 | Const      of const_t
 | Var        of var_t
 | Arithmetic of expr_t * arith_t * expr_t
 | Negation   of expr_t
 | NestedQ    of select_t
 | Aggregate  of agg_t * expr_t

and target_t = string * expr_t

and cond_t   = 
 | Comparison of expr_t * cmp_t * expr_t
 | And        of cond_t * cond_t
 | Or         of cond_t * cond_t
 | Not        of cond_t
 | Exists     of select_t
 | ConstB     of bool

and source_t = 
 | Table of (*rel name*) string
 | SubQ  of select_t

and labeled_source_t = string * source_t

and select_t =
   (* SELECT   *) target_t list *
   (* FROM     *) labeled_source_t list *
   (* WHERE    *) cond_t *
   (* GROUP BY *) var_t list

type t = 
 | Create_Table of table_t
 | Select       of select_t

type file_t = table_t list * select_t list

(* Construction Helpers *)
val mk_file:     t -> file_t
val add_to_file: t -> file_t -> file_t
val merge_files: file_t -> file_t -> file_t
val empty_file:  file_t

val mk_and: cond_t -> cond_t -> cond_t
val mk_or:  cond_t -> cond_t -> cond_t

(* Printing *)
val string_of_const: const_t -> string
val string_of_var: var_t -> string
val string_of_type: type_t -> string
val string_of_arith_op: arith_t -> string
val string_of_cmp_op: cmp_t -> string
val string_of_agg: agg_t -> string
val string_of_expr: expr_t -> string
val string_of_cond: cond_t -> string
val string_of_select: select_t -> string

(* Misc Utility *)
val find_table: string -> table_t list -> table_t
val expr_type: expr_t -> table_t list -> labeled_source_t list -> type_t
val select_schema: table_t list -> select_t -> schema_t
val source_for_var_name: string -> table_t list -> labeled_source_t list ->
                         labeled_source_t
val source_for_var: var_t -> table_t list -> labeled_source_t list ->
                    labeled_source_t
val bind_select_vars: ?parent_sources:labeled_source_t list -> select_t -> 
                       table_t list -> select_t
val bind_cond_vars: cond_t -> table_t list -> labeled_source_t list -> cond_t
val bind_expr_vars: expr_t -> table_t list -> labeled_source_t list -> expr_t
val is_agg_expr: expr_t -> bool
val is_agg_query: select_t -> bool

(* parsing tools *)
val global_table_defs: ((string * table_t) list ref)
val reset_table_defs: unit -> unit

