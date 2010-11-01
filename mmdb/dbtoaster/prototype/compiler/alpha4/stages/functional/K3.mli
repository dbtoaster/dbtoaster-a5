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
    type ext_fn_id = Symbol of fn_id_t
    *)

    type type_t =
          TUnit | TFloat | TInt
        | TTuple     of type_t list          (* unnamed records *)
        | Collection of type_t               (* collections *)
        | Fn         of type_t list * type_t (* args * body *)

    type schema = (id_t * type_t) list

    type arg_t = AVar of id_t * type_t | ATuple of (id_t * type_t) list

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
       | Lambda        of arg_t       * expr_t
       | AssocLambda   of arg_t       * arg_t    * expr_t
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
       | InPC          of coll_id_t   * schema   * type_t
       | PC            of coll_id_t   * schema   * schema    * type_t

       | PCUpdate      of expr_t      * expr_t list * expr_t
       | PCValueUpdate of expr_t      * expr_t list * expr_t list * expr_t 
    
       (*| External      of ext_fn_id*)

    (* K3 methods *)
    val fold_expr :
        ('a -> expr_t -> 'a) -> ('a list -> 'a) -> 'a -> expr_t -> 'a

    val string_of_type : type_t -> string
    val string_of_expr : expr_t -> string
    
    (* Helpers *)
    val collection_of_list : expr_t list -> expr_t
    val collection_of_float_list : float list -> expr_t

    (* Incremental section *)
    type statement = expr_t * expr_t
    type trigger = M3.pm_t * M3.rel_id_t * M3.var_t list * statement list
    type program =
        M3.map_type_t list * M3Common.Patterns.pattern_map * trigger list
end

module SR : SRSig
