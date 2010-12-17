open Calculus

type piped_term_t

type delta_t = ((bool * string) * (var_t list * piped_term_t list))

(* Constructors *)
val make_p_term:
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
val get_deltas:     piped_term_t -> delta_t list
val get_delta:      piped_term_t -> (bool*string) -> delta_t
val get_subterms:   piped_term_t -> piped_term_t list
val get_subterm:    piped_term_t -> string -> piped_term_t

(* Compilation Operators *)
val bigsum_rewrite: 
   ?rewrite_mode:bs_rewrite_mode_t   -> (* Rewrite Mode *)
   piped_term_t                      -> (* Term to be rewritten *)
   unit

val build_deltas: 
   ?compile_deletes:bool        -> (* Set to false to compile only inserts *)
   ((string * var_t list) list) -> (* Schema *)
   piped_term_t ->                 (* Base term (with unbuilt deltas) *)
   unit

val build_subterms_for_delta: 
   bool ->                       (* Insert or Delete *)
   string ->                     (* Relation name *)
   var_t list ->                 (* Relation variables *)
   piped_term_t ->               (* Base term *)
   unit

val build_subterms_for_deltas:
   piped_term_t -> unit

(* Accessors *)
val fold_pcalc: (* Each operator has a var_t list, the set of input variables at 
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

val string_of_piped_term: piped_term_t -> string