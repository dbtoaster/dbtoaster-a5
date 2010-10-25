open M3
open M3Common

(* Signatures *)
module type SRSig =
sig
    type id_t = M3.var_id_t
    type coll_id_t = M3.map_id_t
    
    type prebind   = M3.Prepared.pprebind_t
    type inbind    = M3.Prepared.pinbind_t
    type m3schema  = M3.var_t list
    type extension = M3.var_t list
    type pattern   = int list

    (*
    type fn_id_t = string
    type iap_meta  = pattern * m3schema * m3schema * extension
    type ext_fn_id = IndexAndProject of iap_meta | Symbol of fn_id_t
    *)

    type type_t =
          Unit | Float | Int
        | TTuple     of type_t list     (* unnamed records *)
        | Collection of type_t          (* collections *)
        | Fn         of type_t * type_t (* arg * body *)

    type schema = (id_t * type_t) list

    type expr_t =
   
       (* Terminals *)
         Const         of M3.const_t
       | Var           of id_t        * type_t
    
       (* Tuples, i.e. unnamed records *)
       | Tuple         of expr_t list
       | Project       of expr_t      * int list
    
       (* Collection construction *)
       | Singleton     of expr_t
       | Combine       of expr_t      * expr_t 
    
       (* Arithmetic and comparison operators, conditionals *) 
       | Add           of expr_t      * expr_t
       | Mult          of expr_t      * expr_t
       | Eq            of expr_t      * expr_t
       | Neq           of expr_t      * expr_t
       | Lt            of expr_t      * expr_t
       | Leq           of expr_t      * expr_t
       | IfThenElse0   of expr_t      * expr_t

       (* Control flow: conditionals, sequences, side-effecting
        * iteration over collections *)
       | IfThenElse    of expr_t      * expr_t   * expr_t
       | Block         of expr_t list
       | Iterate       of expr_t      * expr_t
    
       (* Functions *)
       | Lambda        of id_t        * type_t   * expr_t
       | AssocLambda   of id_t        * type_t   * id_t   * type_t * expr_t
       | Apply         of expr_t      * expr_t
    
       (* Structural recursion operators *)
       | Map              of expr_t      * expr_t 
       | Flatten          of expr_t 
       | Aggregate        of expr_t      * expr_t   * expr_t
       | GroupByAggregate of expr_t      * expr_t   * expr_t * expr_t

       (* Tuple collection operators *)
       | Member      of expr_t      * expr_t list  
       | Lookup      of expr_t      * expr_t list
       | Slice       of expr_t      * schema      * (id_t * expr_t) list

       (* Persistent collections *)
       | SingletonPC   of coll_id_t   * type_t
       | OutPC         of coll_id_t   * schema   * type_t
       | InPC          of coll_id_t   * schema   * type_t    * expr_t
       | PC            of coll_id_t   * schema   * schema    * type_t * expr_t

       | PCUpdate      of expr_t      * expr_t list * expr_t
       | PCValueUpdate of expr_t      * expr_t list * expr_t list * expr_t 
    
       (*| External      of ext_fn_id*)

    (* Construction from M3 *)
    val calc_to_singleton_expr : M3.Prepared.calc_t -> expr_t
    
    val op_to_expr : (M3.var_t list -> expr_t -> expr_t -> expr_t) ->
        M3.Prepared.calc_t -> M3.Prepared.calc_t -> M3.Prepared.calc_t -> expr_t
    
    val calc_to_expr : M3.Prepared.calc_t -> expr_t

    (* K3 methods *)
    val type_as_string : type_t -> string
    val typecheck_expr : expr_t -> type_t
    
    (* Helpers *)
    val collection_of_list : expr_t list -> expr_t
    val collection_of_float_list : float list -> expr_t
    
    (* Incremental section *)
    type statement = expr_t * expr_t
    type trigger = M3.pm_t * M3.rel_id_t * M3.var_t list * statement list
    type program = M3.map_type_t list * trigger list

    val m3rhs_to_expr : M3.var_t list -> M3.Prepared.aggecalc_t -> expr_t

    val collection_stmt : M3.var_t list -> M3.Prepared.stmt_t -> statement
    
    val collection_trig : M3.Prepared.trig_t -> trigger
    
    val collection_prog : M3.Prepared.prog_t -> program
    
end

module SR : SRSig