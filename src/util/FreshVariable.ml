(**
   Utility functions for generating custom, fresh variable names
*)

module StringMap = Map.Make(String)

(**/**)
type variable_class = {
   counter      : int ref;
   source_file  : string;
   prefix       : string;
}

let classes:(variable_class StringMap.t) ref = ref StringMap.empty;;

let global_prefix = ref "__";;
(**/**)

(**
   Set the global prefix for all generated variables (default: '__')
*)
let set_prefix (new_prefix:string) = global_prefix := new_prefix
;;
(**
   Format a variable name given a prefix and a unique integer
*)
let format ?(inline = "") (prefix:string) (idx:int) =
   (!global_prefix)^prefix^inline^"_"^(string_of_int idx)
;;
(**
   Generate a fresh variable of the class with the specified prefix
*)
let generate ?(inline="") (prefix:string) = 
   let { counter = ctr } = StringMap.find prefix !classes in
      ctr := !ctr + 1;
      format ~inline:inline prefix !ctr
;;
(**
   Declare a new variable class
   @param   source_file  The name of the source file the variable class will be
                         used in (for debugging purposes)
   @param   prefix       The prefix that will be associated with the class
   @return               A function for generating variables of the class
*)
let declare_class (source_file:string) (prefix:string) = 
   let decl = {
      counter     = ref 0;
      source_file = source_file;
      prefix      = prefix
   } in
   if StringMap.mem prefix !classes then (
      Debug.Logger.bug "FRESHVARIABLE" ~detail:(fun () ->
         let {source_file = original_file} = StringMap.find prefix !classes in
            "Original declaration in file '"^original_file^
            "'\nDuplicate declaration  in file '"^source_file^"'"
      ) ("Duplicate declaration of symbol generator with prefix "^prefix)
   );
   classes := StringMap.add prefix decl !classes;
   (fun ?(inline="") () -> 
      decl.counter := !(decl.counter) + 1; 
      format ~inline:inline prefix !(decl.counter)
   )
;;
(**
   Dump a list of all known classes
*)
let declared_classes () = 
   StringMap.fold (fun k v accum -> 
      accum^v.prefix^" (in "^v.source_file^")\n"
   ) !classes ""
;;
let rec mk_safe_name ?(idx=None) (unsafe_names:string list) (base:string) =
   let (curr_name,next_idx) = 
      match idx with
         | None -> (base, 1)
         | Some(idx) -> (base^"_"^(string_of_int idx), idx+1)
   in
      if List.mem curr_name unsafe_names then
         mk_safe_name ~idx:(Some(next_idx)) unsafe_names base
      else
         curr_name