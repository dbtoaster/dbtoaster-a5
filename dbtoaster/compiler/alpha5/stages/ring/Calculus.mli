type type_t  = Sql.type_t
type const_t = Sql.const_t
type var_t = string * type_t
type value_t = 
    | Var   of var_t
    | Const of const_t
type cmp_t = Sql.cmp_t

type ('init_expr) calc_t =
    | Sum        of 'init_expr calc_t list         (* c1+c2+... *)
    | Prod       of 'init_expr calc_t list         (* c1*c2*... *)
    | Neg        of 'init_expr calc_t              (* -c *)
    | Leaf       of ('init_expr) leaf_t
    
and ('init_expr) leaf_t =
    | Cmp        of value_t * cmp_t * value_t      (* v1 [cmp] v2 *)
    | AggSum     of var_t list * 'init_expr calc_t (* AggSum([v1,v2,...],c) *)
    | Value      of value_t                        (* Var(v) | Const(#) *)
    | Relation   of string * var_t list            (* R(v1,v2,...) *)
    | External   of string * var_t list * type_t * (* {M1(v1[i],v2[o],...) *)
                    bool list * 'init_expr         (*    := 'a} (true = in) *)
    | Definition of var_t * 'init_expr calc_t      (* v <- c *)

exception TypecheckError of string * unit calc_t * unit calc_t option

(*** Basic Operations ***)

val calc_of_sql: ?tmp_var_id:(int ref) ->
                 Sql.table_t list ->
                 Sql.select_t -> 
                 ((string * unit calc_t) list)

val fold_calc : 
   ('ret list -> 'ret) -> (* Sum join op *)
   ('ret list -> 'ret) -> (* Prod join op *)
   ('ret -> 'ret) ->      (* Neg *)
   ('a leaf_t -> 'ret) ->
   'a calc_t ->
   'ret

val rewrite_leaves: ('a leaf_t -> 'b calc_t) -> ('a calc_t) -> ('b calc_t)

val replace_vars: (var_t * var_t) list -> 'a calc_t -> 'a calc_t


(*** Basic Queries ***)

(* Returns true if the specified variable is bound in the expression *)
val is_bound      : 'a calc_t -> var_t -> bool
val is_not_bound  : 'a calc_t -> var_t -> bool

(* Returns all variables, bound and unbound involved in the expression *)
val get_schema    : 'a calc_t -> var_t list


(* Returns Some() and a schema mapping that will make the two expressions 
   equivalent, or none if no such mapping can be found.  Note that false
   negatives may be returned *)
val get_mapping   : 'a calc_t -> 'a calc_t -> (var_t * var_t) list option

(* Returns a list of all Relations appearing in the expression *)
val get_relations : 'a calc_t -> string list

(* Returns a list of all Externals (and related metadata) appearing in the 
   expression *)
val get_externals : 'a calc_t -> (string * 'a) list

val calc_type : ?strict:bool -> ('a calc_t) -> type_t

(*** Printing ***)
val string_of_type  : type_t -> string
val string_of_const : const_t -> string
val string_of_var   : var_t -> string
val string_of_value : value_t -> string
val string_of_cmp   : cmp_t -> string
val string_of_leaf  : ?string_of_meta:(('a -> string) option) -> 
                      'a leaf_t -> string
val string_of_calc  : 'a calc_t -> string
val string_of_enhanced_calc : ('a -> string) -> 'a calc_t -> string