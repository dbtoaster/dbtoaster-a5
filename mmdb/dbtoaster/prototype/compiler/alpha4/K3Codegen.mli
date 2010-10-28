open M3Common.Patterns

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
    val const : M3.const_t -> code_t
    val var   : M3.var_t -> code_t

    (* Tuples *)
    val tuple   : code_t list -> code_t
    val project : code_t -> int list -> code_t

    (* Native collections *)
    val singleton : code_t -> code_t
    val combine   : code_t -> code_t -> code_t

    (* Arithmetic, comparision operators *)
    (* op type, lhs, rhs -> op *)
    val op : op_t -> code_t -> code_t -> code_t

    (* Control flow *)
    (* predicate, then clause, else clause -> condition *) 
    val ifthenelse : code_t -> code_t -> code_t -> code_t

    (* statements -> block *)    
    val block : code_t list -> code_t

    (* iter fn, collection -> iterate *)
    val iterate : code_t -> code_t -> code_t
 
    (* Functions *)
    (* arg, schema app, body -> fn *)
    val lambda : K3.SR.id_t -> bool -> code_t -> code_t
    
    (* arg1, type, arg2, type, body -> assoc fn *)
    val assoc_lambda : K3.SR.id_t -> K3.SR.id_t -> code_t -> code_t

    (* fn, arg -> evaluated fn *)
    val apply : code_t -> code_t -> code_t
    
    (* Structural recursion operations *)
    (* map fn, collection -> map *)
    val map : code_t -> code_t -> code_t
    
    (* agg fn, initial agg, collection -> agg *)
    val aggregate : code_t -> code_t -> code_t -> code_t
    
    (* agg fn, initial agg, grouping fn, collection -> agg *)
    val group_by_aggregate: code_t -> code_t -> code_t -> code_t -> code_t
    
    (* nested collection -> flatten *)
    val flatten : code_t -> code_t

    (* Tuple collection operators *)
    (* TODO: write documentation on datatypes + implementation requirements
     * for these methods *)
    (* map, key -> bool/float *)
    val exists : code_t -> code_t list -> code_t

    (* map, key -> map value *)
    val lookup : code_t -> code_t list -> code_t
    
    (* map, partial key, pattern -> slice *)
    (* TODO: see notes on datatype conversions/secondary indexes *)
    val slice  : code_t -> code_t list -> int list -> code_t

    (* Persistent collections *)
    
    (* K3 API *)
    (*
    val get_value : K3.SR.coll_id_t -> code_t
    
    (* Database udpate methods *)
    val update_value : K3.SR.coll_id_t -> code_t -> code_t

    val update_map : K3.SR.coll_id_t -> code_t list -> code_t -> code_t

    val update_io_map :
        K3.SR.coll_id_t -> code_t list -> code_t list -> code_t -> code_t
    *)

    (* M3 API *)

    (* Database retrieval methods *)
    val get_value   : K3.SR.coll_id_t -> code_t
    val get_in_map  : K3.SR.coll_id_t -> code_t
    val get_out_map : K3.SR.coll_id_t -> code_t
    val get_map     : K3.SR.coll_id_t -> code_t
    
    
    (* Database udpate methods *)
    (* persistent collection id, value -> update *)
    val update_value         : K3.SR.coll_id_t -> code_t -> code_t
    
    (* persistent collection id, in key, value -> update *)
    val update_in_map_value  : K3.SR.coll_id_t -> code_t list -> code_t -> code_t
    
    (* persistent collection id, out key, value -> update *)
    val update_out_map_value : K3.SR.coll_id_t -> code_t list -> code_t -> code_t
    
    (* persistent collection id, in key, out key, value -> update *)
    val update_map_value     :
        K3.SR.coll_id_t -> code_t list -> code_t list -> code_t -> code_t

    (* persistent collection id, update collection -> update *)
    val update_in_map  : K3.SR.coll_id_t -> code_t -> code_t
    val update_out_map : K3.SR.coll_id_t -> code_t -> code_t

    (* persistent collection id, key, update collection -> update *)
    val update_map : K3.SR.coll_id_t -> code_t list -> code_t -> code_t


    (* fn id -> code
     * -- code generator should be able to hooks to implementations of
     *    external functions, e.g. invoke function call *)
    (*
    val ext_fn :  K3.SR.fn_id_t -> code_t

    (* outv, incr, init, init_singleton -> update, sing init *)
    val singleton_update :
        M3.var_t list -> code_t -> code_t -> bool -> code_t * code_t
    
    (* outv, incr, init, init_singleton -> update, sing init *)
    val slice_update :
        M3.var_t list -> code_t -> code_t -> bool -> code_t * code_t
    
    (* mapn, inv, outv, patv, pat, update, sing init -> statement code *)
    val singleton_statement : K3.SR.coll_id_t ->
        M3.var_t list -> M3.var_t list -> M3.var_t list -> int list ->
            code_t -> code_t -> code_t

    (* mapn, inv, outv, update, sing init -> statement code *)
    val statement : K3.SR.coll_id_t ->
        M3.var_t list -> M3.var_t list -> M3.var_t list -> int list ->
            code_t -> code_t -> code_t
    *)

    (* Toplevel: sources and main *)
    (* event, rel, trigger args, statement code block -> trigger code *)
    val trigger : M3.pm_t -> M3.rel_id_t -> M3.var_t list -> code_t list -> code_t

    (* source type, framing type, (relation * adaptor type) list 
     * -> source impl type,
     *    source and adaptor declaration code, (optional)
     *    source and adaptor initialization code (optional) *)
    val source: M3.source_t -> M3.framing_t -> (string * M3.adaptor_t) list ->
                source_impl_t * code_t option * code_t option

    (* schema, patterns, source decls and inits, triggers, toplevel queries
       -> top level code *)
    val main : M3.map_type_t list -> pattern_map ->
               (source_impl_t * code_t option * code_t option) list ->
               code_t list -> string list -> code_t

    val output : code_t -> out_channel -> unit

    (* TODO: interepreter methods *)
end