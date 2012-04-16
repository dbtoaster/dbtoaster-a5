(**
   Utilities for performing list-based translation.  The basic type [table_fn_t] 
   describes a functional mapping as a list of 2-tuples, with the first elements 
   representing the domain, and the second representing the range.  
*)

(**
   A list, or table of mappings, represented as 2-tuples.  Each element of the 
   table represents a mapping from the first element of the 2-tuple to the 
   second.
*)
type ('a, 'b) table_fn_t = ('a * 'b) list

(**
   Raised if elements of the domain are duplicated
*)
exception NonFunctionalMappingException

(**
   Apply the functional mapping to an element.
   @param theta   The functional mapping table
   @param default The output value to be used if the [x] is not present in the 
                  domain of [theta]
   @param x       The value to be mapped
   @return        The mapping of [x] in [theta], or [default] if [x] is not 
                  present in the domain of [theta]
   @raise NonFunctionalMappingException If [theta]'s domain has duplicates
*)
let apply (theta: ('a, 'b) table_fn_t)
          (default: 'b) (x: 'a): 'b =
   let g (y, z) = if(x = y) then [z] else []
   in
   let x2 = List.flatten (List.map g theta)
   in
   if (List.length x2) = 0 then default
   else if (List.length x2) = 1 then (List.hd x2)
   else raise NonFunctionalMappingException

(**
   Apply the functional mapping to an element or leave it untouched if the
   element is not present in the functional mapping table.
   @param theta The functional mapping table
   @param x     The value to be mapped
   @return      The mapping of [x] in [theta], or [x] if it is not present in 
                the domain of [theta]
   @raise NonFunctionalMappingException If [theta]'s domain has duplicates
*)
let apply_if_present (theta: ('a,'a) table_fn_t) (x: 'a): 'b =
   apply theta x x

(**
   Stringify the provided functional mapping table.
   @param theta      The functional mapping table
   @param left_to_s  A stringifier for elements of the mapping's domain
   @param right_to_s A stringifier for elements of the mapping's range
   @return           The stringified functional mapping
*)
let string_of_table_fn (theta:('a,'b) table_fn_t)
                       (left_to_s:('a -> string))
                       (right_to_s:('b -> string)): string =
  match (List.fold_left (fun accum (x,y) ->
       Some((match accum with | Some(a) -> a^", " | None -> "")^
            "{ "^(left_to_s x)^
            " => "^(right_to_s y)^
            " }"
       )
     ) None theta) with
   | Some(a) -> "[ "^a^" ]"
   | None -> "[]"

(**
   Stringify the provided string -> string functional mapping table
   @param theta The functional mapping table
   @return      The stringified functional mapping
*)
let string_of_string_fn theta: string =
   "{" ^(ListExtras.string_of_list ~sep:", " (fun (x,y) -> x^"->"^y) theta)^"}"

(**
   Apply the functional mapping to an element, raising an exception if it is
   not present in the functional mapping table.
   @param theta The functional mapping table
   @param x     The value to be mapped
   @return      The mapping of [x] in [theta]
   @raise NonFunctionalMappingException If [theta]'s domain has duplicates or if
                                        [x] is not present in [theta]'s domain
*)
let apply_strict (theta: ('a, 'b) table_fn_t) (x: 'a): 'b =
   let g (y, z) = if(x = y) then [z] else []
   in
   let x2 = List.flatten (List.map g theta)
   in
   if (List.length x2) = 1 then (List.hd x2)
   else raise NonFunctionalMappingException

(**
   Compute the domain of a functional mapping
   @param theta The functional mapping table
   @return      The domain of the functional mapping table
*)
let dom (theta: ('a, 'b) table_fn_t): 'a list = fst (List.split theta)

(**
   Compute the range/image of a functional mapping
   @param theta The functional mapping table
   @return      The range/image of the functional mapping table
*)
let img (theta: ('a, 'b) table_fn_t): 'a list = snd (List.split theta)

(**
   Determine whether an element is in the domain of a functional mapping
   @param theta The functional mapping table
   @param x     An element of the type of the domain of the mapping
   @return      true if [x] is in the domain of [theta]
*)
let in_dom (theta: ('a, 'b) table_fn_t) (a:'a): bool = 
   List.mem_assoc a theta

(**
   Determine whether an element is in the range/image of a functional mapping
   @param theta The functional mapping table
   @param x     An element of the type of the range/image of the mapping
   @return      true if [x] is in the range/image of [theta]
*)
let in_img (theta: ('a, 'b) table_fn_t) (b:'b): bool =
   List.exists (fun (_,cmp_b) -> cmp_b = b) theta

(**
   Determine whether a mapping is functional (has no duplicates in its domain)
   @param theta The functional mapping table
   @return      true if the domain of [theta] contains no duplicates
*)
let functional (theta: ('a, 'b) table_fn_t): bool =
   let d = dom theta in
   ((ListAsSet.no_duplicates d) = d)

(**
   Determine whether a mapping is onto (has no duplicates in its range/image)
   @param theta The functional mapping table
   @return      true if the range/image of [theta] contains no duplicates
*)
let onto (theta: ('a, 'b) table_fn_t): bool =
   let i = img theta in 
   ((ListAsSet.no_duplicates i) = i)

(**
   Attempt to merge two functional mappings
   @param theta1  The first input functional mapping table
   @param theta2  The second input functional mapping table
   @return        A Some() wrapped mapping if the two functional mappings can be
                  merged into a functional and onto mapping, or None otherwise.
*)
let merge (theta1:('a,'b) table_fn_t) (theta2:('a,'b) table_fn_t):
          (('a,'b) table_fn_t) option = 
   if List.exists (fun a -> (List.assoc a theta1) <> (List.assoc a theta2))
                  (ListAsSet.inter (dom theta1) (dom theta2))
   then None else 
   let merged = ListAsSet.union theta1 theta2 in
   if onto merged then Some(merged) else None

(**
   Attempt to murge multiple functional mappings.  Identical to folding 
   Function.merge over a list of mapping tables.
   @param thetas  The list of functional mapping tables.
   @return        A Some() wrapped mapping if the functional mapping tables
                  can be successfullly merged (as in merge), or None if not.
*)
let multimerge (thetas:('a,'b) table_fn_t list): ('a,'b) table_fn_t option =
   List.fold_left (fun a b -> match a with Some(sa) -> merge sa b | None->None)
                  (Some([])) thetas

(**
   Find identities in a functional mapping table. (mappings that map to 
   themselves.
   @param theta The functional mapping table
   @return      A list of all elements that would be mapped to themselves
*)
let identities (theta: ('a, 'a) table_fn_t): ('a list) =
   let f (x,y) = if (x=y) then [x] else [] in
   List.flatten (List.map f theta)

(**
   Determine if the provided functional mapping has a transitive closure (i.e., 
   if applying the mapping a second time could produce a different mapping).  
   @param theta The functional mapping table
   @return      true if the mapping is its own transitive closure
*)
let intransitive (theta: ('a, 'a) table_fn_t): bool =
   ((ListAsSet.diff (ListAsSet.inter (dom theta) (img theta))
                    (identities theta)) = [])
