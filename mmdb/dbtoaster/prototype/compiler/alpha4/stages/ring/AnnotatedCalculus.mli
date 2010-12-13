open Calculus

module PipedCalculus : sig

   type piped_term_t
   
   (* Constructors *)
   val make_term:
      term_t ->     (* Map term : see Calculus.map_term *)
      term_t ->     (* Map definition : the term defining the map *)
      var_t list -> (* Input variables *)
      piped_term_t
   
   val convert_bigsum:
      term_t ->         (* Map term : see Calculus.map_term *)
      term_t ->         (* Map definition : the term defining the map *)
      var_t list ->     (* Input variables *)
      term_mapping_t -> (* Bigsum theta *)
      var_t list ->     (* Bigsum variables *)
      piped_term_t
   
   (* Accessors *)
   val get_definition: piped_term_t -> term_t
   val get_map_term:   piped_term_t -> term_t
   val get_vars:       piped_term_t -> var_t list
   val get_ivars:      piped_term_t -> var_t list
   val get_ovars:      piped_term_t -> var_t list
   val get_map_name:   piped_term_t -> string
   
   (* Compilation Operators *)
   val bigsum_rewrite: 
      ?(rewrite_mode:bs_rewrite_mode_t) -> (* Rewrite Mode *)
      piped_term_t                      -> (* Term to be rewritten *)
      piped_term_t *                       (* RETURN: rewritten term *)
      piped_term_t list                    (* RETURN: list of todos *)
   
   val simplified_delta_terms: 
      bool ->             (* operation == delete *)
      string ->           (* Relation name *)
      var_t list ->       (* Relation variables (== tuple) *)
      piped_term_t ->     (* Term to be deltaed *)
      (piped_term_t list) (* Delta terms *)
   
   val extract_submaps:
      
   
   val extract_base_relations:
   
   
   (* Accessors *)
   val fold: (* Each operator has a var_t list, the set of input variables at 
                that point in the expression *)
      (var_t list -> 'a -> 'a)       -> (* Negation of term *)
      (var_t list -> 'a list -> 'a)  -> (* Product of terms *)
      (var_t list -> 'a list -> 'a)  -> (* Sum of terms *)
      (var_t list -> 'a -> 'a)       -> (* Sum of values (aggsum, bigsum) *)
      (var_t list -> const_t -> 'a)  -> (* Datum constant *)
      (var_t list -> var_t -> 'a)    -> (* Variable *)
      (var_t list -> piped_term_t * var_t list * var_t list -> 'a)
                                     -> (* External map *)
      (var_t list -> string * var_t list -> 'a)
                                     -> (* Relation *)
      (var_t list -> comp_t -> 'a -> 'a -> 'a)
                                     -> (* Comparison *)
      piped_term_t -> 
      'a      

end

module DeltaCalulus : sig
   
   type delta_term_t
      
   
end