open Types
open Patterns

module type CG =
sig
    type code_t
    type op_t
    type db_t

    (* Data sources *)
    type source_impl_t
      
    (* Operators *)
    val add_op         : op_t
    val mult_op        : op_t
    val eq_op          : op_t
    val neq_op         : op_t
    val lt_op          : op_t
    val leq_op         : op_t   
    val ifthenelse0_op : op_t

    (* Terminals *)
    val const : ?expr:K3.expr_t option -> const_t -> code_t
    val var   : ?expr:K3.expr_t option -> K3.id_t -> K3.type_t -> code_t

    (* Tuples *)
    val tuple   : ?expr:K3.expr_t option -> code_t list -> code_t
    val project : ?expr:K3.expr_t option -> code_t -> int list -> code_t

    (* Native collections *)
    val singleton : ?expr:K3.expr_t option -> code_t -> K3.type_t -> code_t
    val combine   : ?expr:K3.expr_t option -> code_t -> code_t -> code_t

    (* Arithmetic, comparision operators *)
    (* op type, lhs, rhs -> op *)
    val op : ?expr:K3.expr_t option -> op_t -> code_t -> code_t -> code_t

    (* Control flow *)
    (* predicate, then clause, else clause -> condition *) 
    val ifthenelse : ?expr:K3.expr_t option -> code_t -> code_t -> code_t -> code_t

    (* statements -> block *)    
    val block : ?expr:K3.expr_t option -> code_t list -> code_t
    
    (* comment string -> nested_expr  *)    
    val comment : ?expr:K3.expr_t option -> string -> code_t -> code_t

    (* iter fn, collection -> iterate *)
    val iterate : ?expr:K3.expr_t option -> code_t -> code_t -> code_t
 
    (* Functions *)
    (* arg, body -> fn *)
    val lambda : ?expr:K3.expr_t option -> K3.arg_t -> code_t -> code_t
    
    (* arg1, type, arg2, type, body -> assoc fn *)
    val assoc_lambda : ?expr:K3.expr_t option ->
        K3.arg_t -> K3.arg_t -> code_t -> code_t

		(* arg, function id, arg, return type -> external fn *)
    val external_lambda : ?expr:K3.expr_t option ->
        K3.id_t -> K3.arg_t -> K3.type_t -> code_t
				
    (* fn, arg -> evaluated fn *)
    val apply : ?expr:K3.expr_t option -> code_t -> code_t -> code_t
    
    (* Structural recursion operations *)
    (* map fn, collection -> map *)
    val map : ?expr:K3.expr_t option ->
        code_t -> K3.type_t -> code_t -> code_t
    
    (* agg fn, initial agg, collection -> agg *)
    val aggregate : ?expr:K3.expr_t option -> code_t -> code_t -> code_t -> code_t
    
    (* agg fn, initial agg, grouping fn, collection -> agg *)
    val group_by_aggregate: ?expr:K3.expr_t option -> code_t -> code_t -> code_t -> code_t -> code_t
    
    (* nested collection -> flatten *)
    val flatten : ?expr:K3.expr_t option -> code_t -> code_t

    (* Tuple collection operators *)
    (* TODO: write documentation on datatypes + implementation requirements
     * for these methods *)
    (* map, key -> bool/float *)
    val exists : ?expr:K3.expr_t option -> code_t -> code_t list -> code_t

    (* map, key -> map value *)
    val lookup : ?expr:K3.expr_t option -> code_t -> code_t list -> code_t
    
    (* map, partial key, pattern -> slice *)
    (* TODO: see notes on datatype conversions/secondary indexes *)
    val slice  : ?expr:K3.expr_t option -> code_t -> code_t list -> int list -> code_t

    (* Persistent collections *)
    
    (* K3 API *)
    (*
    val get_value : K3.coll_id_t -> code_t
    
    (* Database udpate methods *)
    val update_value : K3.coll_id_t -> code_t -> code_t
    val update_map : K3.coll_id_t -> code_t list -> code_t -> code_t
    val update_io_map :
        K3.coll_id_t -> code_t list -> code_t list -> code_t -> code_t
    *)

    (* M3 API *)

    (* Database retrieval methods *)
    val get_value   : 
      ?expr:K3.expr_t option -> K3.type_t -> K3.coll_id_t -> code_t

    val get_in_map  : 
      ?expr:K3.expr_t option -> K3.schema -> K3.type_t ->
      K3.coll_id_t -> code_t
    
    val get_out_map : 
      ?expr:K3.expr_t option -> K3.schema -> K3.type_t ->
      K3.coll_id_t -> code_t
    
    val get_map     : 
      ?expr:K3.expr_t option -> (K3.schema * K3.schema) -> 
      K3.type_t -> K3.coll_id_t -> code_t
    
    
    (* Database udpate methods *)
    (* persistent collection id, value -> update *)
    val update_value         : ?expr:K3.expr_t option ->
        K3.coll_id_t -> code_t -> code_t
    
    (* persistent collection id, in key, value -> update *)
    val update_in_map_value  : ?expr:K3.expr_t option ->
        K3.coll_id_t -> code_t list -> code_t -> code_t
    
    (* persistent collection id, out key, value -> update *)
    val update_out_map_value : ?expr:K3.expr_t option -> 
        K3.coll_id_t -> code_t list -> code_t -> code_t
    
    (* persistent collection id, in key, out key, value -> update *)
    val update_map_value     : ?expr:K3.expr_t option -> 
        K3.coll_id_t -> code_t list -> code_t list -> code_t -> code_t

    (* persistent collection id, updated collection -> update *)
    val update_in_map  : ?expr:K3.expr_t option ->  
        K3.coll_id_t -> code_t -> code_t
    
    val update_out_map : ?expr:K3.expr_t option -> 
        K3.coll_id_t -> code_t -> code_t

    (* persistent collection id, key, updated collection -> update *)
    val update_map : ?expr:K3.expr_t option ->  
        K3.coll_id_t -> code_t list -> code_t -> code_t

    (* TODO: add update to n-th level in, as given by the key list
     * this should be typechecked to ensure updated collection has
     * same type as exsiting (n+1)-th level
     * persistent collection id, key list, updated collection -> update
    val update_nested_map : ?expr:K3.expr_t option ->
        K3.coll_id -> code_t list list -> code_t -> code_t
    *)

    (* Database remove methods *)    
    (* persistent collection id, in key -> remove *)
    val remove_in_map_element  : ?expr:K3.expr_t option ->
        K3.coll_id_t -> code_t list -> code_t
    
    (* persistent collection id, out key -> remove *)
    val remove_out_map_element : ?expr:K3.expr_t option -> 
        K3.coll_id_t -> code_t list -> code_t
    
    (* persistent collection id, in key, out key -> remove *)
    val remove_map_element     : ?expr:K3.expr_t option -> 
        K3.coll_id_t -> code_t list -> code_t list -> code_t
        
    (* unit operation which has no effect *)
    val unit_operation : code_t 

    (* fn id -> code
     * -- code generator should be able to hooks to implementations of
     *    external functions, e.g. invoke function call *)
    (*
    val ext_fn :  K3.fn_id_t -> code_t
    *)

    (* Toplevel: sources and main *)
    (* event, rel, trigger args, statement code block -> trigger code *)
    val trigger :
        Schema.event_t -> code_t list -> code_t

    (* source type, framing type, (relation * adaptor type) list 
     * -> source impl type,
     *    source and adaptor declaration code, (optional)
     *    source and adaptor initialization code (optional) *)
    val source: Schema.source_t -> (Schema.adaptor_t * Schema.rel_t) list ->
                source_impl_t * code_t option * code_t option

    (* map schema, patterns, table source decls and inits, stream source decls 
       and inits, triggers, toplevel queries
       -> top level code *)
    val main : K3.map_t list -> pattern_map ->
               (source_impl_t * code_t option * code_t option) list ->
               (source_impl_t * code_t option * code_t option) list ->
               code_t list -> (string * K3.expr_t * code_t) list -> code_t

    val output : code_t -> out_channel -> unit

    val to_string : code_t -> string
    
    val debug_string : code_t -> string
end

module type CGI =
sig
    include CG

    type value_t

    val eval : DBChecker.DBAccess.db_session_t option -> code_t -> string list -> 
               const_t list -> db_t -> value_t
end