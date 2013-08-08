(**
Tools for managing indices over datastructures that DBToaster runtimes 
maintain.  

All datastructures used in M3 have a map-style interface.  The datastructure
describes a (potentially infinite) set of mappings from tuples to values.
For example we could have the map [M1[][A,B]] assigned values as follows:
{[
[1,2] -> 2
[2,3] -> 6
[1,3] -> 4
]}
If [M1[][A,B]] is evaluated in a scope where both [A] and [B] are bound, then
we will read only a single value.  For example:
[(A ^= 1)*(B ^= 2)*M1[][A,B]]
evaluates to [2].  This is easy to implement (e.g., with a HashMap).  
Similarly, if none of the variables are bound, then we simply iterate over 
all of the elements of the datastructure (also easy).  However, it might be 
the case that only some (but not all) of the variables are bound.  For 
example:
[(A ^= 1)*M1[][A,B]] evaluates to the set [{ [1,2] -> 2, [1,3] -> 4 }]. 

In order to implement this sort of access efficiently in the runtime (in the 
general case), we need to build and maintain special index structures.  For
example, if only [A] is bound, we need to be able to efficiently iterate over 
the keys in the set that have the corresponding value of A.  For this 
example, we would create the secondary index over A:
{[
[1] -> { [1,2], [1,3] }
[2] -> { [2,3] }
]}

Similarly, if we wanted to be able to iterate over the elements of the set 
with [B] bound, we would create the secondary index over B:
{[
[2] -> { [1,2] }
[3] -> { [2,3], [1,3] }
]}
Whenever we insert into (or delete from) M1, we also update the secondary
index (or indices) for M1.  

Ultimately, the datastructure needs to be able to support partial accesses to 
any subset of its keys.  However, we have a fixed workload, and we know which
combination of keys we will need to employ (This is obtained by analyzing
the M3 code to find which variables are in scope wherever the map is accessed
or by identifying all the Slice operations in the K3 code).

We refer to the set of map keys accessed in a partial access as an {b binding
pattern}, or just a pattern.  This module contains tools for managing the
patterns used in a program (be it M3, K3, or other).  

A pattern consists of the array indices of the keys that {b are} bound in
the map access.  For example, for [M1[][A,B]], an access with [A] bound would
have the pattern [[0]].
*)


(******************* Patterns *******************)
(**
   A pattern: the base type of the module.  For both types of patterns, the
   first part of the pattern is a list of key names included solely for 
   debugging purposes.  Nothing relies, nor should rely on these strings 
   containing anything even remotely useful.  
   
   The second half is the actual pattern, a list of array indices of the keys
   that are bound in the map access that the pattern describes.
   
   It should be noted that Input patterns will never actually be used, and 
   should probably be phased out as soon as possible.
*)
type pattern =
  In of (string list * int list)  (** Binding pattern of the input variables
                                      in a map access *)
| Out of (string list * int list) (** Binding pattern of the output variables 
                                      in a map access *)

(**
   A pattern map: the full set of patterns used in a K3 or M3 program.  Every
   element of this list describes the patterns affecting a single map, the 
   string is the map's name.
*)
type pattern_map = (string * pattern list) list

(**/**)
(** Utility method for finding the index of elements in an array *)
let index l x = ListExtras.index_of x l
(**/**)

(**[equal_pat a b]
   
   Test two patterns for equivalence
   @param a    A pattern
   @param b    A pattern
   @return     True if [a] and [b] describe the same pattern
*)
let equal_pat a b = match a,b with
| In(u,v), In(x,y) | Out(u,v), Out(x,y) -> v = y
| _,_ -> false

(**[make_in_pattern dimensions accesses]
   
   Create a pattern for the input portion of a map access
   @param dimensions The full set of variable names in the map's input schema
   @param accesses   The set of bound variables in this access
   @return           An Input pattern describing [accesses] in [dimensions]
*)
let make_in_pattern dimensions accesses =
   In(accesses, List.map (index dimensions) accesses)

(**[make_out_pattern dimensions accesses]
   
   Create a pattern for the output portion of a map access
   @param dimensions The full set of variable names in the map's input schema
   @param accesses   The set of bound variables in this access
   @return           An Output pattern describing [accesses] in [dimensions]
*)
let make_out_pattern dimensions accesses =
   Out(accesses, List.map (index dimensions) accesses)

(**[get_pattern pattern]

   Get the pattern component of a pattern object.
   @param pattern A pattern object
   @return        The pattern component (the bound indices) of [pattern]
*)
let get_pattern = function | In(x,y) | Out(x,y) -> y

(**[get_pattern_vars pattern]

   Get the pattern component of a pattern object.
   @param pattern A pattern object
   @return        The pattern vars (the debug strings) of [pattern]
*)
let get_pattern_vars = function | In(x,y) | Out(x,y) -> x

(**[empty_pattern_map ()]

   A default, empty pattern map object.
*)
let empty_pattern_map() = []

(**/**)
(**[get_filtered_patterns filter_f pm mapn]
   
   Utility accessor for pattern maps.  Return the pattern component of the
   patterns in a pattern map that satisfy a specified filter function.
   
   Used only to implement the get_*_patterns functions
*)
let get_filtered_patterns filter_f pm mapn =
   let map_patterns = 
      if List.mem_assoc mapn pm
      then List.assoc mapn pm else []
   in List.map get_pattern (List.filter filter_f map_patterns)
(**/**)

(**[get_in_patterns pm mapn]
   
   Get the input patterns of a specific map in a patternmap
   @param pm         A patternmap
   @param mapn       The name of a map in [pm]
   @return           The input patterns of [mapn] in [pm] or [[]] if
                     no such map exists.
*)
let get_in_patterns (pm:pattern_map) mapn = get_filtered_patterns
(function | In _ -> true | _ -> false) pm mapn

(**[get_out_patterns pm mapn]
   
   Get the input patterns of a specific map in a patternmap
   @param pm         A patternmap
   @param mapn       The name of a map in [pm]
   @return           The output patterns of [mapn] in [pm] or [[]] if
                     no such map exists.
*)
let get_out_patterns (pm:pattern_map) mapn = get_filtered_patterns
(function | Out _ -> true | _ -> false) pm mapn

(**[add_pattern patternmap mapname_pattern]
   
   Add the indicated pattern to a patternmap.
   @param patternmap          A patternmap
   @param mapname_pattern     A tuple consisting of a map name and a pattern
   @return                    [patternmap] extended with [pattern] associated
                              with [mapname].
*)
let add_pattern patternmap mapname_pattern =
   let (mapn,pat) = mapname_pattern in
   let existing = 
      if List.mem_assoc mapn patternmap 
      then List.assoc mapn patternmap else [] 
   in
   let new_pats = 
      pat::(List.filter (fun x -> not(equal_pat x pat)) existing) 
   in
      (mapn, new_pats)::(List.remove_assoc mapn patternmap)

(**[merge_pattern_maps p1 p2]
   
   Merge two pattern maps together
   @param p1   A pattern map
   @param p2   A pattern map
   @return     The union of [p1] and [p2]
*)
let merge_pattern_maps p1 p2 =
   let aux pm (mapn, pats) =
      if List.mem_assoc mapn pm then
         List.fold_left (fun acc p -> add_pattern acc (mapn, p)) pm pats
      else (mapn, pats)::pm
   in List.fold_left aux p1 p2

(**[singleton_pattern_map mapname_pattern]
   
   Create a patternmap with only a single map with a single pattern.
   @param mapname_pattern   A tuple consisting of a map name and a pattern
   @return                  A patternmap containing [mapname] and [pattern]
*)
let singleton_pattern_map mapname_pattern =
   let (mapn,pat) = mapname_pattern in [(mapn, [pat])]

(**[string_of_pattern p]
   
   Generate a human-readable string describing the pattern.
   @param p A pattern
   @return  The string representation of [p]
*)
let string_of_pattern (p:pattern) =
   let f (x: string list) (y: int list) = 
      List.map (fun (a,b) -> a^":"^b)
         (List.combine (List.map string_of_int y) x)
   in match p with
      | In(x,y) -> "in{"^(String.concat "," (f x y))^"}"
      | Out(x,y) -> "out{"^(String.concat "," (f x y))^"}"

(**[patterns_to_nice_string pm]
   
   Generate a nice human-readable string representation of a pattern map
   @param pm A pattern map
   @return   The string representation of [pm]
*)
let patterns_to_nice_string pm =
   let patlist_to_string pl = List.fold_left (fun acc pat ->
      acc^(if acc = "" then acc else " , ")^(string_of_pattern pat)) "" pl
   in
      List.fold_left (fun acc (mapn, pats) ->
         acc^"\n"^mapn^": "^(patlist_to_string pats)^";") "" pm

(**[patterns_to_string pm]
   
   Generate a human-readable string representation of a pattern map
   @param pm A pattern map
   @return   The string representation of [pm]
*)
let patterns_to_string pm =
   let patlist_to_string pl = List.fold_left (fun acc pat ->
      let pat_str = String.concat "," (
         match pat with | In(x,y) | Out(x,y) ->
            List.map (fun (a,b) -> a^":"^b)
                     (List.combine (List.map string_of_int y) x))
      in
         acc^(if acc = "" then acc else " / ")^pat_str) "" pl
   in
      List.fold_left (fun acc (mapn, pats) ->
         acc^"\n"^mapn^": "^(patlist_to_string pats)) "" pm


(**[create_pattern_map_from_access mapn theta_vars key_vars]
   
   Creates a pattern map containing a single out pattern corresponding
   to a map access.
   @param mapn       Name of the map being accessed pattern map.
   @param theta_vars List of variables that are in scope at the time of 
                     the access
   @param key_vars   List of variables used as key for the map access.
   @return           A pattern map with a single pattern corresponding to 
                     the access.
*)
let create_pattern_map_from_access mapn theta_vars key_vars =
   let bound_vars = ListAsSet.inter key_vars theta_vars in 
   let valid_pattern = List.length bound_vars < List.length key_vars in
   if valid_pattern then 
      singleton_pattern_map (mapn, 
                            (make_out_pattern (List.map fst key_vars) 
                                              (List.map fst bound_vars)))
   else
      empty_pattern_map()

(**[extract_from_calc theta_vars calc]

   Extracts a pattern map for all of the map accesses in a calculus expression
   @param theta_vars  List of variables that are in scope when the calculus 
   expression gets evaluated.
   @param calc        An M3 calculus expression.
   @return            The pattern map for all map accesses in [calc]
*)
let rec extract_from_calc (theta_vars: Type.var_t list) 
                           (calc: Calculus.expr_t) : pattern_map = 
   let sum_pattern_fn _ sum_pats : pattern_map = 
        List.fold_left merge_pattern_maps (empty_pattern_map()) sum_pats 
   in
   let prod_pattern_fn _ prod_pats : pattern_map = 
      List.fold_left merge_pattern_maps (empty_pattern_map()) prod_pats 
   in
   let neg_pattern_fn _ neg_pat : pattern_map = neg_pat
   in
   let leaf_pattern_fn (tvars,_) lf_calc : pattern_map = 
            
      begin match lf_calc with
         | Calculus.Value _ 
         | Calculus.Cmp   _ -> empty_pattern_map()
         | Calculus.AggSum( gb_vars, agg_calc ) -> 
            extract_from_calc tvars agg_calc
         | Calculus.Lift( v, lift_calc ) ->
            extract_from_calc tvars lift_calc
(***** BEGIN EXISTS HACK *****)
         | Calculus.Exists( lift_calc ) ->
            extract_from_calc tvars lift_calc
(***** END EXISTS HACK *****)
         | Calculus.Rel( reln, relv ) -> 
            create_pattern_map_from_access reln tvars relv
         | Calculus.External( mapn, inv, outv, _, init_calc_opt ) -> 
            (* all input variables must be bound at this point in order 
               to be able to evaluate the expression *)
            assert( List.length (ListAsSet.diff inv tvars) = 0 );
            let incr_pat = create_pattern_map_from_access mapn tvars 
                                                          outv in
            let init_pat = begin match init_calc_opt with
               | Some(init_calc) -> extract_from_calc tvars init_calc
               | None -> empty_pattern_map()
            end in
            merge_pattern_maps init_pat incr_pat
      end
   in
   Calculus.fold ~scope:theta_vars
                       sum_pattern_fn prod_pattern_fn 
                       neg_pattern_fn leaf_pattern_fn calc

(**[extract_from_stmt theta_vars stmt]

   Extracts a pattern map for all of the map accesses in a statement
   @param theta_vars  List of variables that are in scope when the statement
    gets executed (aka. trigger arguments.)
   @param stmt   An M3 statement
   @return            The pattern map for all map accesses in [stmt]
*)
let extract_from_stmt theta_vars (stmt : Plan.stmt_t) : pattern_map =      
   let (lmapn, linv, loutv, ret_type, init_calc_opt) = 
      Plan.expand_ds_name stmt.Plan.target_map 
   in
   let incr_calc = stmt.Plan.update_expr in

   (* all lhs input variables must be free, they cannot be bound by trigger 
      args *)
   assert (List.length (ListAsSet.inter theta_vars linv) = 0);
   let theta_w_linv = ListAsSet.union theta_vars   linv  in
   let theta_w_lhs  = ListAsSet.union theta_w_linv loutv in

   let stmt_patterns = create_pattern_map_from_access lmapn theta_vars loutv in
   let init_patterns = begin match init_calc_opt with
         | Some(init_calc) -> extract_from_calc theta_w_lhs init_calc 
         | None            -> empty_pattern_map()
      end
   in
   let incr_patterns = extract_from_calc theta_w_linv incr_calc in
   List.fold_left merge_pattern_maps 
                            stmt_patterns
                            ([init_patterns; incr_patterns])

(**[extract_from_trigger trig]

   Extracts a pattern map for all of the map accesses in a trigger
   @param trig   An M3 trigger
   @return          The pattern map for all map accesses in [trig]
*)
let extract_from_trigger (trig : M3.trigger_t) : pattern_map =
   let trig_args = Schema.event_vars trig.M3.event in
   List.fold_left merge_pattern_maps 
                        (empty_pattern_map()) 
                        (List.map (extract_from_stmt trig_args) 
                                  !(trig.M3.statements))
(**[extract_patterns triggers]

   Extracts a pattern map for all of the map accesses in a set of triggers
   @param triggers   A set of M3 triggers
   @return           The pattern map for all map accesses in [triggers]
*)
let extract_patterns (triggers : M3.trigger_t list) : pattern_map =
   List.fold_left merge_pattern_maps 
                        (empty_pattern_map()) 
                        (List.map extract_from_trigger triggers)