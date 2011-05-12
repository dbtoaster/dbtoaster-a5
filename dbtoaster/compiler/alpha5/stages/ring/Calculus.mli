type type_t  = Sql.type_t
type const_t = Sql.const_t
type var_t = Common.Types.var_t
type value_t = 
    | Var   of var_t
    | Const of const_t
type cmp_t = Sql.cmp_t

type ('init_expr) external_t = 
  string *               (* name *)
  (var_t * bool) list *  (* variable * is_output_var *)
  type_t *               (* external's content type *)
  'init_expr             (* customizable external metadata *)

type ('init_expr) calc_t =
    | Sum        of 'init_expr calc_t list         (* c1+c2+... *)
    | Prod       of 'init_expr calc_t list         (* c1*c2*... *)
    | Neg        of 'init_expr calc_t              (* -c *)
    | Cmp        of value_t * cmp_t * value_t      (* v1 [cmp] v2 *)
    | AggSum     of var_t list * 'init_expr calc_t (* AggSum([v1,v2,...],c) *)
    | Value      of value_t                        (* Var(v) | Const(#) *)
    | Relation   of string * var_t list            (* R(v1,v2,...) *)
    | External   of 'init_expr external_t
    | Definition of var_t * 'init_expr calc_t      (* v <- c *)

exception TypecheckError of string * unit calc_t * unit calc_t option

(*** Constructors ***)
val calc_zero: 'i calc_t
val calc_one:  'i calc_t

val sum_list: 'i calc_t -> 'i calc_t list
val prod_list: 'i calc_t -> 'i calc_t list

val mk_neg: 'i calc_t -> 'i calc_t
val mk_sum: 'i calc_t list -> 'i calc_t
val mk_prod: 'i calc_t list -> 'i calc_t

(*** Basic Operations ***)

val calc_of_sql: ?tmp_var_id:(int ref) ->
                 Sql.table_t list ->
                 Sql.select_t -> 
                 ((string * unit calc_t) list)

val fold_calc : 
   ('ret list -> 'ret) ->                 (* Sum *)
   ('ret list -> 'ret) ->                 (* Prod *)
   ('ret -> 'ret) ->                      (* Neg *)
   (value_t * cmp_t * value_t -> 'ret) -> (* Cmp *)
   (var_t list * 'ret -> 'ret) ->         (* AggSum *)
   (value_t -> 'ret) ->                   (* Value *)
   (string * var_t list -> 'ret) ->       (* Relation *)
   ('a external_t -> 'ret) ->             (* External *)
   (var_t * 'ret -> 'ret) ->              (* Definition *)
   'a calc_t ->
   'ret

val rewrite_leaves: 
   (value_t * cmp_t * value_t -> 'b calc_t) -> (* Cmp *)
   (var_t list * 'b calc_t -> 'b calc_t) ->    (* AggSum *)
   (value_t -> 'b calc_t) ->                   (* Value *)
   (string * var_t list -> 'b calc_t) ->       (* Relation *)
   ('a external_t -> 'b calc_t) ->             (* External *)
   (var_t * 'b calc_t -> 'b calc_t) ->         (* Definition *)
   'a calc_t ->
   'b calc_t

val convert_calc: ('a external_t -> 'b external_t) -> 'a calc_t -> 'b calc_t

val replace_vars: (var_t * var_t) list -> 'a calc_t -> 'a calc_t


(*** Basic Queries ***)

(* Returns true if the specified variable is bound in the expression *)
val is_bound      : 'a calc_t -> var_t -> bool
val is_not_bound  : 'a calc_t -> var_t -> bool

(* Returns all variables, bound and unbound involved in the expression *)
val get_schema    : 'a calc_t -> (var_t * bool) list

(* Returns true if (A * B) can be rewritten as (B * A) 
   Include an optional external schema (the output variables of C) to test
   whether (C * (A * B)) can be rewritten as (C * (B * A))
   
   For example, in general S(A) * A <> A * S(A) because A is bound by S(A).
   However, R(A) * S(A) * A = R(A) * A * S(A) because R(A) binds A even earlier 
   in the expression. *)
val commutes_with : ?external_sch:var_t list -> 'a calc_t -> 'b calc_t -> bool

(* Returns Some() and a schema mapping that will make the two expressions 
   equivalent, or none if no such mapping can be found.  Note that false
   negatives may be returned *)
val get_mapping   : 'a calc_t -> 'a calc_t -> (var_t * var_t) list option

(* Returns a list of all Relations appearing in the expression *)
val get_relations : 'a calc_t -> string list

(* Returns a list of all Externals (and related metadata) appearing in the 
   expression *)
val get_externals : 'a calc_t -> ('a external_t) list

val calc_type : ?strict:bool -> ('a calc_t) -> type_t

(*** Printing ***)
val string_of_type  : type_t -> string
val string_of_const : const_t -> string
val string_of_var   : var_t -> string
val string_of_value : value_t -> string
val string_of_cmp   : cmp_t -> string
val string_of_calc  : 'a calc_t -> string
val string_of_enhanced_calc : ('a -> string) -> 'a calc_t -> string