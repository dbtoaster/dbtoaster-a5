module StringMap = Map.Make(String)
module StringSet = Set.Make(String)

(* Set operations on lists. We do not enforce that the input lists
   have no duplicates. Goal: I want to see the lists for easier debugging,
   otherwise I would instantiate Set (which would be abstract). *)
module ListAsSet =
struct
   (* computes the difference l1 - l2 *)
   let diff l1 l2 =
      let eq x y = x = y in
      let f x = if List.exists (eq x) l2 then [] else [x] in
      List.flatten (List.map f l1)

   let subset l1 l2 = ((diff l1 l2) = [])     (* is l1 a subset of l2 ? *)
   let inter l1 l2 = diff l1 (diff l1 l2)               (* intersection *)
   let union l1 l2 = l1 @ (diff l2 l1)
(* alternative impl
   let union l1 l2 = l1@(List.filter (fun k -> (not (List.mem k l1))) l2)
*)

   (* eliminates duplicates if the element lists of l are duplicate free. *)
   let multiunion l = List.fold_left union [] l

   let multiinter l = List.fold_left inter (List.hd l) (List.tl l)

   (* not the most intelligent of implementations. quadratic time *)
   let no_duplicates l = multiunion (List.map (fun x -> [x]) l)

   let member a l = (subset [a] l)         (* is "a" a member of set l? *)

   (* computes all subsets of r that have exactly k elements.
      does not produce duplicate sets if r does not have duplicates.
      sorting -- e.g. (List.sort Pervasives.compare l) --
      improves readability of the result. *)
   let subsets_of_size k r =
      let rec add_k_elements_to_from k l r =
         (* select_gt: selects those elements of r that are greater than x *)
         let select_gt x r =
            let gt y = y > x in
            List.filter (gt) r in
         let f x =
             (add_k_elements_to_from (k-1) (l@[x]) (select_gt x r)) in
         if k <= 0 then [l]
         else           List.flatten (List.map f r) in
      add_k_elements_to_from k [] r


   (* distributes a list of lists, e.g.

      distribute [[[1;2]; [2;3]]; [[3;4]; [4;5]]; [[5;6]]];;

      results in

      [[[1; 2]; [3; 4]; [5; 6]]; [[2; 3]; [3; 4]; [5; 6]];
       [[1; 2]; [4; 5]; [5; 6]]; [[2; 3]; [4; 5]; [5; 6]]]
   *)
   let rec distribute (l: 'a list list) =
      if l = [] then [[]]
      else
         let f tail =
            let g x = [x] @ tail in
            List.map (g) (List.hd l)
         in
         List.flatten (List.map f (distribute (List.tl l)));;
    
end;;

module ListExtras = 
struct
   let scan (f:('a list -> 'a -> 'a list -> 'b)) (l:('a list)): ('b list) = 
      let rec iterate prev curr_next =
         match curr_next with 
            | []         -> []
            | curr::next -> (f prev curr next) :: (iterate (prev@[curr]) next)
      in iterate [] l

   let reduce_assoc (l:('a * 'b) list): (('a * ('b list)) list) =
      List.fold_right (fun (a,b) ret ->
         if List.mem_assoc a ret
         then (a, b :: (List.assoc a ret)) :: (List.remove_assoc a ret)
         else (a, [b]) :: ret
      ) l []

   let flatten_list_pair (l:('a list * 'b list) list): ('a list * 'b list) =
      let (a, b) = List.split l in
         (List.flatten a, List.flatten b)

   let outer_join_assoc (a:(('c * 'a) list)) (b:(('c * 'b) list)):
                        ('c * ('a option * 'b option)) list =
      let (outer_b, join) = 
         List.fold_left (fun (b, join) (c, a_v) ->
            (  List.remove_assoc c b,
               join @ 
                  [  c, ((Some(a_v)),
                     (if List.mem_assoc c b then Some(List.assoc c b) 
                                            else None))
                  ])
         ) (b, []) a
      in
         join @ (List.map (fun (c, b_v) -> (c, ((None), (Some(b_v))))) outer_b)
end

module MapAsSet =
struct
  let union (merge:(string -> 'a -> 'a -> 'a)) 
            (map_a:'a StringMap.t)
            (map_b:'a StringMap.t): 'a StringMap.t =
    StringMap.fold (fun k v accum -> 
      if StringMap.mem k accum then
        StringMap.add k (merge k (StringMap.find k accum) v) accum
      else 
        StringMap.add k v accum
    ) map_a map_b

  let left_priority k vl vr = vl
  let right_priority k vl vr = vr
  
  let union_right a b: 'a StringMap.t = 
    (union right_priority a b)
  
  let singleton k v = StringMap.add k v StringMap.empty
  
end;;


(*
ListAsSet.subsets_of_size 3 [2;1;4;3] =
   [[1; 2; 3]; [1; 2; 4]; [1; 3; 4]; [2; 3; 4]];;

ListAsSet.subsets_of_size 5 [1;2;3;4] = [];;
*)



module HyperGraph =
struct
(* compute the connected components of the hypergraph where the nodes
   in guard_set may be ignored.
   factorizes a MultiNatJoin of leaves given in list form.

   The top-level result list thus conceptually is a MultiProduct.
*)
let rec connected_components (get_nodes: 'edge_t -> 'node_t list)
                             (hypergraph: 'edge_t list): ('edge_t list list)
   =
   let rec complete_component c g =
      if (g = []) then c
      else if (c = []) then c
      else
         let relevant_set = (List.flatten (List.map get_nodes c)) in
         let neighbor e = ((ListAsSet.inter relevant_set (get_nodes e)) != [])
         in
         let newset = (List.filter neighbor g) in
         c @ (complete_component newset (ListAsSet.diff g newset)) in
   if hypergraph = [] then []
   else
      let c = complete_component [List.hd hypergraph] (List.tl hypergraph) in
      [c] @ (connected_components get_nodes (ListAsSet.diff hypergraph c))
end



module MixedHyperGraph =
struct
   type ('a, 'b) edge_t = AEdge of 'a | BEdge of 'b
   type ('a, 'b) hypergraph_t = (('a, 'b) edge_t) list

   let make (alist: 'a list) (blist: 'b list): ('a, 'b) hypergraph_t =
        (List.map (fun x -> AEdge x) alist)
      @ (List.map (fun x -> BEdge x) blist)

   let connected_components (a_get_nodes: 'a -> 'node_t)
                            (b_get_nodes: 'b -> 'node_t)
                            (hypergraph: ('a, 'b) hypergraph_t):
                            ((('a, 'b) hypergraph_t) list) =
      let get_nodes hyperedge =
         match hyperedge with AEdge(a) -> a_get_nodes a
                            | BEdge(b) -> b_get_nodes b
      in
      HyperGraph.connected_components get_nodes hypergraph

   let extract_atoms (component: ('a, 'b) hypergraph_t):
                                 (('a list) * ('b list)) =
      let extract_atom x = match x with AEdge(a) -> ([a], [])
                                      | BEdge(b) -> ([], [b])
      in
      let (a_ll, b_ll) = (List.split (List.map extract_atom component))
      in
      (List.flatten a_ll, List.flatten b_ll)
end



let string_of_list0 (sep: string) (elem_to_string: 'a -> string)
                    (l: 'a list): string =
   if (l = []) then ""
   else List.fold_left (fun x y -> x^sep^(elem_to_string y))
                       (elem_to_string (List.hd l))
                       (List.tl l)

(* (string_of_list ", " ["a"; "b"; "c"] = "a, b, c". *)
let string_of_list (sep: string) (l: string list): string =
   string_of_list0 sep (fun x->x) l

let list_to_string (elem_to_string: 'a -> string) (l: 'a list) : string =
   "[ "^(string_of_list0 "; " elem_to_string l)^" ]"

let option_default (default:'a) (base:'a option) : 'a =
  match base with Some(a) -> a | None -> default;;

module Function =
struct
   type ('a, 'b) table_fn_t = ('a * 'b) list

   exception NonFunctionalMappingException

   (* for partial functions with a default value to be returned if x is
      not in the domain of theta. *)
   let apply (theta: ('a, 'b) table_fn_t)
             (default: 'b) (x: 'a): 'b =
      let g (y, z) = if(x = y) then [z] else []
      in
      let x2 = List.flatten (List.map g theta)
      in
      if (List.length x2) = 0 then default
      else if (List.length x2) = 1 then (List.hd x2)
      else raise NonFunctionalMappingException
    
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

   let apply_strict (theta: ('a, 'b) table_fn_t) (x: 'a): 'b =
      let g (y, z) = if(x = y) then [z] else []
      in
      let x2 = List.flatten (List.map g theta)
      in
      if (List.length x2) = 1 then (List.hd x2)
      else raise NonFunctionalMappingException


   let dom (theta: ('a, 'b) table_fn_t): 'a list = fst (List.split theta)
   let img (theta: ('a, 'b) table_fn_t): 'a list = snd (List.split theta)

   let functional (theta: ('a, 'b) table_fn_t): bool =
      let d = dom theta in
      ((ListAsSet.no_duplicates d) = d)

   let identities (theta: ('a, 'a) table_fn_t): ('a list) =
      let f (x,y) = if (x=y) then [x] else [] in
      List.flatten (List.map f theta)

   let intransitive (theta: ('a, 'a) table_fn_t): bool =
      ((ListAsSet.diff (ListAsSet.inter (dom theta) (img theta))
                       (identities theta)) = [])

   (* only for functions from strings to strings *)
   let string_of_string_fn theta: string =
      "{" ^(string_of_list ", " (List.map (fun (x,y) -> x^"->"^y) theta))^"}"
end



module Vars =
struct
   (* the input is a list of pairs of equated variables.
      The result is a list of lists of variables and in general contains
      duplicates
   *)
   let filter_selfs eqns = List.filter (fun (a,b) -> a <> b) eqns 
   
   let get_selfs eqns = List.filter (fun (a,b) -> a = b) eqns 
   
   let equivalence_classes (eqs: ('v * 'v) list) =
      List.map ListAsSet.multiunion
         (HyperGraph.connected_components (fun x -> x)
             (List.map (fun (x,y) -> [x;y]) eqs))

   let closure (equalities: ('v * 'v) list) (vars: 'v list): ('v list) =
      ListAsSet.union vars
         (List.flatten (List.filter (fun x -> (ListAsSet.inter x vars) != [])
                                    (equivalence_classes equalities)))

   type 'v mapping_t = ('v * 'v) list

   (* given a list of variables to be unified (i.e., a single variable from
      this list is to be chosen by which all others will be replaced),
       a variable that occurs in list parameter_vars, and
      output a mapping from the variables to the chosen variable. *)
   let unifier0 (vars_to_unify: 'v list)   (* an equivalence class *)
               (parameter_vars: 'v list):  (* the unifier must be the identity
                                              on these variables *)
               ('v mapping_t * (('v * 'v) list)) =
      let bv = ListAsSet.inter vars_to_unify parameter_vars
      in
      let incons =
         (
         if (List.length bv > 1) then
            List.map (fun x -> ((List.hd bv), x)) (List.tl bv)
            (* This means we are not allowed to unify; we must
               keep a constraint around checking that the elements
               of bv are the same. *)
         else []
         )
      in 
      let v = if (bv = []) then (List.hd vars_to_unify) else (List.hd bv)
      in
      (List.flatten (List.map (fun x -> [(x,v)]) vars_to_unify), incons)

   (* returns a most general unifier of the variables occuring in the
      equations, and respecting those unless they are inconsistent
      with a given set of required identities. Returns the unifier and
      a fully descriptive set of inconsistent equations. For example,

      (unifier [("x", "y"); ("y", "z")] ["z"]) =
      ([("x", "z"); ("y", "z")], [])

      (unifier [("x", "y"); ("y", "z")] ["y"; "z"]) =
      ([("x", "y"); ("y", "y"); ("z", "y")], [("y", "z")])

      Note the ("z", "y") tuple. This is inconsistent with the requirement
      that the mapping is the identity on z. However, in those cases
      where the inconsistent equations hold (here, when the variable y and
      z have the same value), the mapping is ok. This is the way to use
      inconsistent equations; they are additional requirements to be
      checked.
   *)
   let unifier (equations: ('v * 'v) list) (identities: 'v list):
               ('v mapping_t * (('v * 'v) list)) =
      let (x, y) = (List.split
                      (List.map (fun comp -> (unifier0 comp identities))
                                (equivalence_classes (filter_selfs equations))))
      in
      ((List.flatten x)@(get_selfs equations), List.flatten y)

   (* mapping is a partial function. returns mapping(x) if mapping is defined
      on x, otherwise x. *)
   let apply_mapping (mapping: 'v mapping_t) (x: 'v): 'v =
      Function.apply mapping x x
end


(*
let b = [("A", "x"); ("B", "y"); ("A", "z"); ("C", "u"); ("C", "x");
         ("B", "v")];;

Vars.equivalence_classes b =
   [["A"; "x"; "A"; "z"; "C"; "x"; "C"; "u"]; ["B"; "y"; "B"; "v"]];;

Vars.closure [("x", "y"); ("u", "v"); ("v", "y")] ["x"] =
   ["x"; "y"; "v"; "u"];;
*)



(* turn a list into a list of pairs of values and their positions in the list.
   Examle:
   add_positions ["a"; "b"; "c"] 1 = [("a", 1); ("b", 2); ("c", 3)];;
*)
let rec add_positions (l: 'a list) i =
   if (l = []) then []
   else ((List.hd l), i) :: (add_positions (List.tl l) (i+1))

(* add_names "foo" ["a"; "b"] = [("foo1", "a"), ("foo2", "b")] *)
let add_names (name_prefix: string) (l: 'a list): ((string * 'a) list) =
   let add_name (x, i) = (name_prefix^(string_of_int i), x)
   in
   List.map add_name (add_positions l 1)


(* creates all k-tuples of elements of the list src.
   Example: k_tuples 3 [1;2] =
            [[1; 1; 1]; [2; 1; 1]; [1; 2; 1]; [2; 2; 1]; [1; 1; 2];
             [2; 1; 2]; [1; 2; 2]; [2; 2; 2]]
*)
let rec k_tuples k (src: 'a list) : 'a list list =
   if (k <= 0) then [[]]
   else List.flatten (List.map (fun t -> List.map (fun x -> x::t) src)
                               (k_tuples (k-1) src))

(* no such thing as read everything in Ocaml *)
let complete_read in_c =
   let buffer = ref "" in
   (
      try
         while true do
            buffer := (!buffer)^(input_line in_c)^"\n"
         done
      with End_of_file -> ()
   );
   !buffer

(* IO Operation abstraction - one level up from streams.  Mostly there to
   support file/close blocks, but has some useful temporary file functionality
   and eases simultaneous use of both channels and raw filenames (and 
   eventually, sockets as well perhaps? *)
module GenericIO =
struct
  type out_t = 
    | O_FileName of string * open_flag list
    | O_FileDescriptor of out_channel
    (* TempFile with a continuation; After write() has finished its callback
       block, The TempFile continuation is invoked with the filename of the
       temporary file.  The file will be deleted on continuation return *)
    | O_TempFile of string * string * (string -> unit)
  ;;
  
  type in_t =
    | I_FileName of string
    | I_FileDescriptor of in_channel
  ;;
  
  (* write fd (fun out -> ... ; (write to out);) *)
  let write (fd:out_t) (block:out_channel -> unit): unit =
    match fd with
    | O_FileName(fn,flags) -> 
      let file = open_out_gen flags 0x777 fn in
        (block file;close_out file)
    | O_FileDescriptor(file) -> block file
    | O_TempFile(prefix, suffix, finished_cb) -> 
      let (filename, file) = (Filename.open_temp_file prefix suffix) in
        (block file;flush file;close_out file;
         finished_cb filename;Unix.unlink filename)
  ;;

  (* read fd (fun in -> ... ; (read from in);) *)
  let read (fd:in_t) (block:in_channel -> unit): unit =
    match fd with
    | I_FileName(fn) -> 
      let file = open_in fn in
        (block file;close_in file)
    | I_FileDescriptor(file) -> block file
  ;;
end

exception GenericCompileError of string;;

let give_up err = raise (GenericCompileError(err));;

(* Utilities for managing command line arguments (analagous to Ruby's GetOpt) *)
module ParseArgs =
struct
  type flag_type_t = 
    | NO_ARG    (* Flag takes no argument, next term processed normally *)
    | OPT_ARG   (* Next term processed normally if flag, or as arg if not *)
    | ARG       (* Next term is arg; Error if flag; replaces old *)
    | ARG_LIST  (* Next term is arg; Error if flag; appended to old *)
  ;;
  
  type flags_t = 
    ( string list *             (* Command line flag (eg -l, --list) *)
      (string * flag_type_t) *  (* Flag identifier, Flag type *)
      string *                  (* Flag argument names *)
      string                    (* Flag documentation *)
    ) list;;
  
  type compiled_flags_t = 
      (string * flag_type_t) StringMap.t * string * string list
  ;;
  
  type arguments_t = (string list) StringMap.t
  ;;
  
  (* Translate a flags_t (see above) into a compiled_flags_t;
     The compiled version uses a string map rather than a list, and stores
     the documentation in a more output-friendly form.
   *)
  let compile (flags:flags_t): compiled_flags_t = 
    let (cflags, helptext_left,helptext_right,help_width) =
      List.fold_left (fun (m,doc_left,doc_right,doc_width) (klist,v,pdoc,doc) -> 
        let left_text = (List.fold_left (fun accum k ->
            if accum = "" then k else accum^"|"^k
          ) "" klist)^(if pdoc = "" then "" else " "^pdoc)
        in
          (
            List.fold_left (fun m2 k ->
              StringMap.add k v m2
            ) m klist,
            doc_left@[left_text],
            doc_right@[doc],
            if (String.length left_text)+5 > doc_width 
              then (String.length left_text)+5
              else doc_width
          ) 
      )(StringMap.empty,[],[],0) flags
    in
      ( cflags, 
        List.fold_left2 (fun accum left right -> 
          accum^left^
          (String.make (help_width-(String.length left)) ' ')^
          right^"\n"
        ) "" helptext_left helptext_right,
        List.fold_left (fun accum left -> 
          accum @ ["["^left^"]"]
        ) [] helptext_left
      );;
  
  (* Parse flag arguments using a compiled_flags_t;
     Returns a value that can be used with the various flag_* functions below.
     Creates several "default" flags:
       - "FILES" => The non-flag/flag-argument parameters (ie, input files)
       - "$0"    => The process name
   *)
  let parse ((flags,_,_):compiled_flags_t): arguments_t =
    let (parse_state,(last_flag,last_type)) = 
      Array.fold_left (fun (parse_state,(last_flag,last_type)) arg ->
        if (String.length arg > 1) && (arg.[0] = '-') then 
          ((*print_line ("arg: "^arg^"; FLAG!");*)
            if (last_type <> NO_ARG) && (last_type <> OPT_ARG) then
              give_up ("Missing argument to flag: "^last_flag)
            else
              if not (StringMap.mem arg flags) then
                give_up ("Unknown flag: "^arg)
              else
                let (flag, flag_type) = StringMap.find arg flags in
                  match flag_type with
                  | NO_ARG -> 
                      (StringMap.add flag [""] parse_state, (flag, flag_type))
                  | OPT_ARG ->
                      (StringMap.add flag [] parse_state, (flag, flag_type))
                  | _ -> (parse_state, (flag, flag_type))
          )
        else
          ((*print_line ("arg: "^arg^"; NOT FLAG:"^last_flag);*)
            let append_to_entry k v m =
              if StringMap.mem k m then
                StringMap.add k ((StringMap.find k m)@[v]) m
              else
                StringMap.add k [v] m
            in
              match last_type with
              | NO_ARG -> 
                  (append_to_entry "FILES" arg parse_state, ("", NO_ARG))
              | OPT_ARG -> 
                  (StringMap.add last_flag [arg] parse_state, ("", NO_ARG))
              | ARG -> 
                  (StringMap.add last_flag [arg] parse_state, ("", NO_ARG))
              | ARG_LIST -> 
                  (append_to_entry last_flag arg parse_state, ("", NO_ARG))
          )
      ) (StringMap.empty, ("$0", ARG)) Sys.argv
    in
      parse_state;;
  
  (* Return a list of values associated with a flag.  Value returned depends
     on the flag's type.
       NO_ARG   => [] -> false | [""] -> true
       OPT_ARG  => [] (flag not present) | [""] (arg not present) | [something]
       ARG      => [] (flag not present) | [something]
       ARG_LIST => the list of arguments to the flag
  *)
  let flag_vals (arguments:arguments_t) (flag:string): string list = 
    if StringMap.mem flag arguments then (StringMap.find flag arguments)
                                    else [];;
  
  (* Return a single value as a string option; Fails on ARG_LIST flags *)
  let flag_val (arguments:arguments_t) (flag:string): string option = 
    match (flag_vals arguments flag) with
    | []   -> None
    | [a]  -> Some(a)
    | _    -> failwith "Internal Error: single-value flag argument with multiple values";;
  
  (* Return a single value; Userfriendly error if there's no value *)
  let flag_val_force (arguments:arguments_t) (flag:string): string =
    match flag_val arguments flag with
    | None    -> give_up ("Missing flag: "^flag)
    | Some(a) -> a;;
  
  (* Return an ARG_LIST as a StringSet *)
  let flag_set (arguments:arguments_t) (flag:string): StringSet.t = 
    List.fold_right (fun f s -> StringSet.add (String.uppercase f) s)
                    (flag_vals arguments flag) StringSet.empty;;
  
  (* For NO_ARG or OPT_ARG: Return true if the flag is present, false if not *)
  let flag_bool (arguments:arguments_t) (flag:string): bool =
    match (flag_vals arguments flag) with
    | [] -> false
    | _  -> true;;
  
  (*
    Generate programmer-friendly curried forms of the flag_* functions
    
    eg: 
      let (flag_vals, flag_val, flag_val_force, flag_set, flag_bool) = 
        ParseArgs.curry arguments;;
  *)
  let curry (arguments:arguments_t) = 
    ( flag_vals      arguments,
      flag_val       arguments,
      flag_val_force arguments,
      flag_set       arguments,
      flag_bool      arguments
    )
  
  (*
    Produce user-readable, nicely formatted, documentation on program usage
    using the help text provided when specifying the flags.
  *)
  let helptext (arguments:arguments_t)
               ((_,helptext,flagtext):compiled_flags_t): string = 
    (
      let max_width = 80 in
      let app_name = (flag_val_force arguments "$0") in
      let (indent_width,skip_line) = 
        if (String.length app_name) * 4 > max_width then
          (4, "\n    ") else (String.length app_name, "")
      in
      let (indented_app_invocation,_) = 
        (List.fold_left (fun (accum,width) field ->
          if width + (String.length field) + 1 > max_width then
            ( accum^"\n"^(String.make (indent_width+1) ' ')^field,
              (String.length field)+1 )
          else
            ( accum^" "^field, width+(String.length field)+1 )
        ) ("", indent_width) (flagtext@["file1 [file2 [...]]"]))
      in
        app_name^skip_line^indented_app_invocation^"\n\n"^helptext
    )
end

(* IndentedPrinting
  Module for producing indented/wrapped multiline syntax/code/trees/etc...
   
  IndentedPrinting proceeds in two phases:
  1: Caller generates a tree of IndentedPrinting.t representing the code tree.
  2: IndentedPrinting.to_lines recursively processes the code tree generating
     a list of strings (one line per entry) that have been properly indented
     and wrapped (the wrapping is still approximate).
  2.a: Alternatively, to_string invokes to_lines and produces a single string.
  
  The point of this exercise is to separate the symbolic representation of 
  the printed data from the exact typesetting requirements.  Each member of the
  IndentedPrinting.t union has its own typesetting rules, defined as follows.
  
  Leaf(block)
    A block of text to be placed on one line.  No wrapping is performed, 
    though the block may be merged in with other text on one line if it fits,
    or indented by its containing expressions.
  
  Lines
    Subexpressions to be placed on multiple lines.  The subexpressions are 
    evaluated independently and placed on separate lines.
  
  TList((lparen,rparen),(lelemparen,relemparen) option,sep,exp list)
    An encoded list; Will be formatted as
    lparen lelemparen exp1 relemparen sep lelemparen exp2 relemparen ... rparen
    If a term in exp is multi-line, a newline will be placed after lelemparen
    and before relemparen.  If sep falls off the end of the line, a newline will
    be placed after sep. 
  
  Node((lparen,rparen),(op,rop),lhs,rhs)
    A binary operator: Formatted as follows:
      {lparen lhs op rop rhs rparen}
    If necessary a linebreak will be inserted between op and rop.
    If rhs evaluates to multiple lines, rparen will appear on its own line.
    If both LHS and RHS span multiple lines, a linebreak will be inserted 
    between lhs and op (ie, op will appear on its own line)
    All lines except the ones containing lparen and rparen (the latter, only if 
    it is on its own line) will be indented by the width of lparen.
    All lines in rhs will also be indented by the width of rop
  
  Parens((lparen,rparen),subexp)
    Parentheses around subexp: Formatted as follows:
      {lparen subexp rparen}
    If subexp evaluates to multiple lines, then rparen will appear on its own 
    line and all lines of subexp except the first will be indented by the width
    of lparen
*)

module IndentedPrinting =
struct
  type op_defn_t = string * string (* op,rop *)
  type parens_t = string * string (* lparen, rparen *)
  
  type t = 
  | Leaf of string
  | Node of parens_t * op_defn_t * t * t
  | Parens of parens_t * t
  | Lines of t list
  | TList of parens_t * parens_t option * string * t list
  
  let indent_lines indent = 
    List.map (fun x->(String.make indent ' ')^x);;
  
  let rec to_debug_line (node:t): string =
    match node with
    | Leaf(s) -> s
    | Node((lparen,rparen),(op,rop),lhs,rhs) ->
      lparen^(to_debug_line lhs)^op^rop^(to_debug_line rhs)^rparen
    | Parens((lparen,rparen),subexp) -> 
      lparen^(to_debug_line subexp)^rparen
    | Lines(exps) ->
      (string_of_list " " (List.map to_debug_line exps))
    | TList(parens,None,sep,exps) ->
      to_debug_line (TList(parens,Some("",""),sep,exps))
    | TList((lparen,rparen),(Some(lelemparen,relemparen)),sep,exps) ->
      lparen^(string_of_list sep 
               (List.map (fun x -> lelemparen^(to_debug_line x)^relemparen)
                         exps))^rparen
  
  let rec to_lines (width:int) (node:t): string list= 
    match node with
    | Leaf(s) -> [s]
    
    | Node((lparen,rparen),(op,rop),lhs,rhs) ->
      (* Generate line(s) for the subexpressions *)
      let lh_list = 
        to_lines (width - (String.length lparen) - 
                          (String.length op)) lhs
      in
      let rh_list = 
        to_lines (width - (String.length lparen) - 
                          (String.length rparen)) rhs 
      in
        (* Check if we have to do any sort of multiline hackery:
           ie: lhs or rhs contains multiple lines, or we need to split
           between op and rop *)
        if (List.length lh_list > 1) || (List.length rh_list > 1) ||
           ( (String.length (List.hd lh_list)) + 
             (String.length (List.hd rh_list)) +
             (String.length lparen) + (String.length op) + 
             (String.length rop) + (String.length rparen) > width ) 
        then
          (
            (* If the lhs is a multiline expression then we need to split
               off the first line and merge it with lparen, indent the others
               and put op on its own line *)
            if (List.length lh_list > 1) then
              [ lparen ^ (List.hd lh_list)] @
              (indent_lines (String.length lparen) (List.tl lh_list)) @
              [ "  "^op ]
            else
            (* If the lhs fits on one line, we can keep it on the current line
               and leave op there *)
              [ lparen ^ (List.hd lh_list) ^ op ]
          ) @
          (
            (* If the rhs is a multiline expression then we do something similar
               to what we do with lhs above *)
            if (List.length rh_list > 1) then
              [ (String.make (String.length lparen) ' ')^
                rop ^ (List.hd rh_list) ] @
              (indent_lines ((String.length rop) + (String.length lparen))
                            (List.tl rh_list)) @ 
              [ rparen ]
            else 
              [ (String.make (String.length lparen) ' ')^
                rop ^ (List.hd rh_list) ^ rparen ]
          )
        else
          (* If the entire expression fits on one line, then good *)
          [ lparen ^ (List.hd lh_list) ^ op ^ rop ^ (List.hd rh_list) ^ rparen ]

    | Parens((lparen,rparen),subexp) ->
      let sub_list = 
        to_lines (width - (String.length lparen) - 
                          (String.length rparen)) subexp
      in
        if (List.length sub_list > 1) then
          [ lparen ^ (List.hd sub_list) ] @
          (indent_lines (String.length lparen) (List.tl sub_list))@
          (indent_lines (String.length lparen) [rparen])
        else 
          [ lparen ^ (List.hd sub_list) ^ rparen ]

    | TList((lparen,rparen),None,sep,exps) ->
      to_lines width (TList((lparen,rparen),Some("",""),sep,exps))
    | TList((lparen,rparen),Some(lelemparen,relemparen),sep,[]) ->
      [lparen^rparen]
    | TList((lparen,rparen),Some(lelemparen,relemparen),sep,exp::[]) ->
      (match (to_lines (width-(String.length lparen)
                             -(String.length lelemparen)) exp)
         with
         | [] -> [lparen^rparen]
         | [a] -> [lparen^lelemparen^a^relemparen^rparen]
         | a::sublist -> [lparen^lelemparen^a]@(indent_lines 2 sublist)@
                         [relemparen^rparen]
      )
    | TList((lparen,rparen),Some(lelemparen,relemparen),sep,exp::rest) ->
      let exp_lines = (to_lines (width-(String.length lparen)
                                      -(String.length lelemparen)) exp) in
      let rest_lines = 
         (to_lines width 
                   (TList(("",rparen),Some(lelemparen,relemparen),sep,rest))) in
      (match (exp_lines,rest_lines) with
         | (_,[]) -> failwith "TList generation error: recursion fail!"
         | ([],[b]) -> [lparen^lelemparen^relemparen^sep^b];
         | ([],b)   -> [lparen^lelemparen^relemparen^sep]@b;
         | ([a],[b]) -> 
            if (String.length a) + (String.length b) > width
            then [lparen^lelemparen^a^relemparen^sep;"  "^b]
            else [lparen^lelemparen^a^relemparen^sep^b]
         | ([a],b) -> 
            (lparen^lelemparen^a^relemparen^sep)::b
         | (a::ra,b) ->
            [lparen^lelemparen^a]@
            (indent_lines 2 (List.rev (List.tl (List.rev ra))))@
            [(List.hd (List.rev ra))^relemparen^sep]@b
      )

    | Lines([])    -> [""]
    | Lines(a::[]) -> to_lines width a
    | Lines(a::l)  -> (to_lines width a)@(to_lines width (Lines(l)))
  ;;
  
  let to_string (width:int) (node:t): string = 
    ((String.concat "\n" (to_lines width node))^"\n")
end

(* Debug
  Tools for a globally-managed debugging/logging system.  
  
  Debug is essentially a glorified StringSet that allows programmers to insert
  statements into their code to be triggered only if a particular string, or 
  mode is in the set.  In effect it's like Log4J, etc...
  
  Mode management:
    set_modes:  Declare the set of modes that are active.
    activate:   Activate a specific mode.
    deactivate: Deactivate a specific mode.
    
  Debugging
    exec:   Given a mode and a block->unit, execute the block if the mode is 
            active.
    print:  Given a mode and a block->string, execute the block and print its
            return value if the mode is active.  If the mode is not active then
            the block will not be executed.
    active: Return whether or not a particular mode is active.
*)
module Debug = 
struct
  
  module DebugInternal =
  struct
    let debug_modes = ref StringSet.empty;
  end
  
  let set_modes new_modes = DebugInternal.debug_modes := new_modes;;
  
  let activate (mode:string): unit = 
    DebugInternal.debug_modes := 
      StringSet.add mode !DebugInternal.debug_modes;;
  let deactivate (mode:string): unit = 
    DebugInternal.debug_modes := 
      StringSet.remove mode !DebugInternal.debug_modes;;
  
  let exec (mode:string) (f:(unit->'a)): unit =
    if StringSet.mem mode !DebugInternal.debug_modes 
      then let _ = f () in () else ();;

  let print (mode:string) (f:(unit->string)): unit =
    exec mode (fun () -> print_endline (f ()));;
  
  let active df = StringSet.mem df !DebugInternal.debug_modes;;
  
  let os () =
    let fdes = (Unix.open_process_in "uname") in
    let ostr = try input_line fdes with End_of_file -> "???" in
    let _ = (Unix.close_process_in fdes) in
      ostr
  
  let showdiff exp_str fnd_str = 
     print_string ("--Expected--\n"^exp_str^
               "\n\n--Result--\n"^fnd_str^"\n\n"); 
     match (os ()) with
       | "Darwin" -> (
            GenericIO.write (GenericIO.O_TempFile("exp", ".diff", (fun exp_f -> 
            GenericIO.write (GenericIO.O_TempFile("fnd", ".diff", (fun fnd_f ->
               let _ = Unix.system("opendiff "^exp_f^" "^fnd_f) in ()
            ))) (fun fd -> output_string fd (fnd_str^"\n"))
            ))) (fun fd -> output_string fd (exp_str^"\n"))
         )
       | _ -> ()
  
  let log_unit_test 
        (title:string) (to_s:'a -> string) (result:'a) (expected:'a) : unit =
    if result = expected then print_endline (title^": Passed")
    else 
      (
        print_endline (title^": Failed");
        showdiff (to_s expected) (to_s result);
        exit 1
      );;
  
  let log_unit_test_list
        (title:string) (to_s:'a -> string) 
        (result:'a list) (expected:'a list) : unit =
    if result = expected then print_endline (title^": Passed")
    else 
      (
        let rec diff_lists rlist elist = 
          let recurse rstr estr new_rlist new_elist =
            let (new_rstr, new_estr) = diff_lists new_rlist new_elist in
              (rstr^"\n"^new_rstr,estr^"\n"^new_estr)
          in   
            if List.length rlist > 0 then
              if List.length elist > 0 then
                if (List.hd rlist) <> (List.hd elist) then
                  recurse (to_s (List.hd rlist)) (to_s (List.hd elist)) 
                          (List.tl rlist) (List.tl elist)
                else
                  diff_lists (List.tl rlist) (List.tl elist)
              else
                recurse (to_s (List.hd rlist)) "--empty line--" 
                        (List.tl rlist) []
            else
              if List.length elist > 0 then
                recurse "--empty line--" (to_s (List.hd elist)) 
                        [] (List.tl elist)
              else
                ("","")
        in
        let (result_diffs, expected_diffs) = diff_lists result expected in
        print_endline (title^": Failed");
        showdiff (expected_diffs) (result_diffs);
        exit 1
      );;
end

module UnitTest =
struct
end
