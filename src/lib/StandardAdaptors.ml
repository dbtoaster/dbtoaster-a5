(*
 * Terminology:
 * -- adaptor: a function to create structured tuples from raw input data
 * -- generator: a function to create an adaptor, in essence compiling
 *    adaptors since adaptors should be efficient as they see each packet
 *    coming off the network.
 *
 * Standard adaptors.
 * -- defines a set of parameters and transformations for use in all adaptors,
 *     i.e. a library of adaptor functions.
 * -- defines two generators to build adaptors:
 *   ++ csv generator
 *   ++ orderbook generator
 *
 *)

open Type
open Constants
open Schema
open Values
open Sources

exception AbortEventConstruction

(* Standard parameters/transformations for all adaptors *)

(* param_val: delimiter *)
let field_tokens param_val =
   let delim_re = Str.regexp (Str.quote param_val)
   in (fun tuple -> Str.split delim_re tuple)

(* param_val: comma-separated field offsets, i.e. "0,4,8,12" *)
let offset_tokens param_val =
   let offsets = List.map int_of_string (Str.split (Str.regexp ",") 
                                                   param_val) 
   in
   if List.length offsets = 0 then
      failwith "Offset tokenizer: no offsets specified!"
   else 
      let aux tuple (prev_off, acc) off =
         try (off, acc@[String.sub tuple prev_off (off - prev_off)])
         with Invalid_argument(_) ->
            failwith ("Offset tokenizer: invalid offsets "^
               (string_of_int prev_off)^", "^(string_of_int off))
      in (fun tuple -> snd (List.fold_left (aux tuple)
         ((List.hd offsets), []) ((List.tl offsets)@[String.length tuple])))
 
(* Tokenizers: param key, tokenize fn list *)
let tokenizers : (string * (string -> string -> string list)) list =
   [("delimiter", field_tokens); ("offsets", offset_tokens)]

(* param_val: field indexes to project down to, i.e. "0,2,3" *)
let preproject param_val =
   let field_ids =
      List.map int_of_string (Str.split (Str.regexp ",") param_val)
   in (fun fields -> List.map (List.nth fields) field_ids)

(* param_val: "upper" | "lower" *)
let change_case param_val =
   match param_val with
    | "upper" -> (fun fields -> List.map String.uppercase_ascii fields)
    | "lower" -> (fun fields -> List.map String.lowercase_ascii fields)
    | _ -> (fun fields -> fields)

(* param_val: (field, offset, len) list
 * takes substrings of those fields specified in the param val, with the given
 * offsets and lengths. *)
let substring param_val =
   let split_re = Str.regexp "," in
   let spec_re = Str.regexp "[0-9]*,[0-9]*,[0-9]*" in
   let pos = ref 0 in
   let spec = ref [] in
   while Str.string_match spec_re param_val !pos do
      let triple = Str.matched_string param_val in
      let t_fields = List.map int_of_string (Str.split split_re triple) in
      match t_fields with
       | [fid;off;len] ->
         begin
            if List.mem_assoc fid !spec then
               failwith ("Duplicate substring found for field " ^
                         (string_of_int fid));
            spec := (fid,(off,len))::(!spec); 
            pos := !pos + (String.length triple)
         end
       | _ -> failwith "invalid specification construction"
   done;
   let aux (counter,acc) f =
      if List.mem_assoc counter !spec then
         let (off,len) = List.assoc counter !spec in
         (counter+1,acc@[String.sub f off len])
      else (counter+1,acc@[f])
   in (fun fields -> snd (List.fold_left aux (0,[]) fields))

let skiplines param_val = 
  let num = ref (int_of_string param_val) in
  (fun fields -> 
    if !num > 0 then (num := !num - 1; raise AbortEventConstruction)
                else fields
  )

let trimwhitespace param_val = 
  (fun fields -> List.map (Str.global_replace (Str.regexp "^ *\\(.*[^ ]\\) *$")
                                              "\\1") fields)

(* Preprocessing: param key, prep fn *)
let preprocessors : (string * (string -> string list -> string list)) list =
   [("project", preproject); ("case", change_case); ("substring", substring);
    ("skiplines", skiplines); ("trimwhitespace", trimwhitespace)]

(* param_val: (int|float|date|hash|string) list
 * Builds a tuple, i.e. a const_t list from a string list given the expected
 * types of fields.
 * -- if there are more fields than types, we use the last type to construct
 *    remaining fields.
 *) 
(* TODO: better hashing function control *)
(* TODO: handle "event" and "order" schema elements, as seen in C++ adaptors *)
let build_tuple (reln,relv,_) param_val support_sch =
   let types = (List.map Type.string_of_type support_sch) @ 
               (Str.split (Str.regexp ",") param_val) in
   let full_sch = (List.map (fun x -> ("tmp",x)) support_sch) @ relv in
   let assert_type vn vt scht cmpt= 
      if vt <> cmpt then failwith (
         "Mismatch between schema of "^reln^" and schema provided to CSV "^
         "adaptor: '"^param_val^"'.  Adaptor schema is "^scht^
         ", but relation schema says "^(Type.string_of_type vt)^
         " for variable "^vn
      )
   in
   let build_fns = 
     try 
       List.map2 (fun t (vn,vt) -> match t with
        | "int" | "long" -> assert_type vn vt t TInt;
                   (fun x -> try CInt(int_of_string x) with 
                    Failure(_) -> failwith ("Could not convert int: '"^x^"'"))
        | "float" | "double" -> assert_type vn vt t TFloat;
                   (fun x -> try CFloat(float_of_string x) with
                    Failure(_) -> failwith ("Could not convert float: '"^x^"'"))
        | "date" -> 
          assert_type vn vt t TDate;
          (fun x -> Constants.parse_date x)
        | "string" -> 
           assert_type vn vt t TString;
           (fun x -> CString(x))
        | "hash" -> 
           assert_type vn vt t TInt;
           (fun x -> CInt(Hashtbl.hash x))
        | _ -> failwith ("invalid const_t type "^t)) types full_sch 
     with | Invalid_argument(_) ->
               failwith ("Schema mismatch: Adaptor got ["^param_val^"]; while "^
                         "expecting schema: "^
                         (ListExtras.ocaml_of_list Type.string_of_var relv))
   in
   let num_fns = List.length build_fns in
   let extend_fn = List.hd (List.rev build_fns) in
   let rec sub acc l off len =
      if len = 0 then acc
      else if off > 0 then sub acc (List.tl l) (off-1) len 
      else sub (acc@[List.hd l]) (List.tl l) off (len-1)
   in
   if num_fns = 1 then (fun fields -> List.map extend_fn fields)
   else 
   (fun fields ->
      try 
         let nf = List.length fields in
         let common =
            if nf = num_fns then List.map2 (fun bf f -> bf f) build_fns fields
            else if nf = 0 then
               raise AbortEventConstruction (* no fields => no event *)
            else if nf < num_fns then
               List.map2 (fun bf f -> bf f) (sub [] build_fns 0 nf) fields
            else 
               List.map2 (fun bf f -> bf f) build_fns (sub [] fields 0 num_fns)
         in
         let extension = if nf > num_fns then
            List.map extend_fn (sub [] fields num_fns (nf - num_fns)) else []
         in common@extension
      with Failure(msg) -> 
         failwith (msg^" in tuple "^
                   (ListExtras.ocaml_of_list (fun x->x) fields))
   )

let build_rel_tuple (reln,relv,relt) support_sch =
   let synthesized_schema =
      ListExtras.string_of_list ~sep:"," 
         (fun (_,t) -> Type.string_of_type t) relv
   in build_tuple (reln,relv,relt) synthesized_schema support_sch 

(* Constructors: param key, const fn *)
let constructors rel_sch : 
      (string * (string -> Type.type_t list -> string list -> const_t list)) 
            list =
   [("schema", (build_tuple rel_sch))]

(* TODO: precision management transformations *)

(* param_val: field ids to project *)
let postproject param_val =
   let field_ids =
      List.map int_of_string (Str.split (Str.regexp ",") param_val)
   in (fun fields -> List.map (List.nth fields) field_ids)

(* Post processing: param key, postp fn *)
let postprocessors : (string * (string -> const_t list -> const_t list)) list =
   [("postproject", postproject)]

let constant_event param_val =
  match param_val with
    | "insert" -> (fun fields -> [(0, AInsert,  fields)])
    | "delete" -> (fun fields -> [(0, ADelete, fields)])
    | _ -> failwith ("invalid event type "^param_val)

let parametrized_event param_val =
  let rules = List.map (fun rule ->
      match (Str.split (Str.regexp ":") rule) with 
        | [k;"insert"] -> (CFloat(float(Hashtbl.hash k)), AInsert)
        | [k;"delete"] -> (CFloat(float(Hashtbl.hash k)), ADelete)
        | _ -> failwith ("invalid event type field: "^rule)
    ) (Str.split (Str.regexp ",") param_val)
  in
  let hash_constant cnst = 
     CFloat(float(Hashtbl.hash (Constants.string_of_const cnst) ) )
  in
    (fun fields -> 
      if (List.length fields) <= 1 then raise AbortEventConstruction
      else 
      let order = List.hd fields in
      let ins_del = List.hd (List.tl fields) in
      let remaining_fields = List.tl (List.tl fields) in
      try
        [ (
            Constants.int_of_const order,
            List.assoc (hash_constant(ins_del)) rules, 
            remaining_fields
          )
        ]
      with Not_found -> failwith ("No rule for identifying insert/delete")
    )
    
(* TODO: think of more complex event construction primitives?
 * -- determining event type from a field, equality, general comparison, etc *)

let event_constructors : 
      (string * ((string -> const_t list -> 
                    (int * adaptor_event_t * const_t list) list) * 
                 (string -> Type.type_t list))) list =
   [  ("eventtype", (constant_event, (function _ -> []))); 
      ("triggers",  (parametrized_event, (function _ -> [TInt; TInt])));
      ("deletions", ((function "true" -> parametrized_event "1:insert,0:delete"
                              | _     -> constant_event "insert"),
                     (function "true" -> [TInt; TInt]
                              | _     -> [])))
   ]
  
(* Standard generator, applying above transformations *)
let standard_generator rel_sch params =

   let match_keys l  = List.map (fun (k,v) -> (List.assoc k l) v)
      (List.filter (fun (k,v) -> List.mem_assoc k l) params)
   in
   let match_unique_default l fn_class default =
      let m = match_keys l in match m with
       | [f] -> f
       | [] -> default
       | _ -> failwith ("Multiple "^fn_class^"s specified for CSV adaptor")
   in
   let token_fn = 
     match_unique_default tokenizers "tokenizer" (field_tokens ",") in
   let prep_fns = match_keys preprocessors in
   let const_fn_template = 
      match_unique_default (constructors rel_sch) "constructor" 
                           (build_rel_tuple rel_sch) 
   in
   let post_fns = match_keys postprocessors in
   let (event_const_fn_name, event_const_fn_args) = 
      try 
         List.find (fun (k,_) -> List.mem_assoc k event_constructors) params
      with Not_found ->
         ("eventtype", "insert")
   in      

   let (event_const_fn_template, support_sch_fn) = 
      List.assoc event_const_fn_name event_constructors
   in
   let event_const_fn = event_const_fn_template event_const_fn_args in
   let support_sch = support_sch_fn event_const_fn_args in
   let const_fn = const_fn_template support_sch in
   (fun tuple ->
(*      print_endline tuple;*)
      try
        ( 
          let pre_fields = List.fold_left
             (fun acc_fields prf -> prf acc_fields) (token_fn tuple) prep_fns in
          let post_fields = List.fold_left
             (fun acc_fields postf -> postf acc_fields) 
             (const_fn pre_fields) 
             post_fns
          in event_const_fn post_fields
        )
      with AbortEventConstruction -> []
    ) 

(* Generic CSV, w/ user-defined field delimiter *)
let csv_params ?(event = "none") delim schema =
   let schema_val = if schema = "" then "float" else schema in
   ("csv", [("delimiter", delim); ("schema", schema_val)] @ 
      (match event with ""     -> ["eventtype", "insert"]
                      | "none" -> []
                      | _      -> ["eventtype", event]
      )
   )

let insert_csv delim = csv_params ~event:"insert" delim ""
let delete_csv delim = csv_params ~event:"delete" delim ""

let csv_generator = standard_generator
let csv_generator_wrapper default_params rel_sch extra_params =
  let p = (List.filter (fun (k,v) ->
    not(List.mem_assoc k extra_params)) default_params)@extra_params
  in csv_generator rel_sch p

(* Order books *)
(* TODO: generalize usage of fixed order book schema through field bindings *)
let bids_params = [("book", "bids")]
let asks_params = [("book", "asks")]

let orderbook_generator rel_sch params =
   let required = ["book"] in
   let optional = [("brokers", "0"); ("insert-only", "false")] in
   
   (* Get required params *)
   let valid = List.for_all (fun k -> List.mem_assoc k params) required in
   if not(valid) then
      List.iter (fun k -> if not(List.mem_assoc k params) then
         failwith ("Orderbook generator missing required param "^k)) required;
         
   (* Get optional params *)
   let optionals = List.map (fun (k,default) -> 
      if List.mem_assoc k params
      then (k, List.assoc k params) else (k,default)) optional in
   
   (* Configuration *)
   let book_type = List.assoc "book" params in

   (* Only add broker ids if # brokers > 0 *)
   let num_brokers = try
      let r = int_of_string (List.assoc "brokers" optionals) in
      (*print_endline ("Orderbook # brokers: "^(string_of_int r)); *) r
      with Invalid_argument _ -> 0 in
   
   let deterministic_broker = 
      if List.mem_assoc "deterministic" params then
         (String.lowercase_ascii (List.assoc "deterministic" params)) = "yes"
      else false
   in
   
   let insert_only = 
      if List.mem_assoc "insert-only" params then
         bool_of_string (String.lowercase_ascii (List.assoc "insert-only" params))
      else false
   in

   let book_orders = Hashtbl.create 1000 in
   
   (* returns action, order_id, price vol pair if # brokers <= 0
    * otherwise action, order_id, broker_id-price-volume triple *)
   let ff l i = float_of_string (List.nth l i) in
   let fi l i = int_of_string (List.nth l i) in
   let fs l i = List.nth l i in
   let get_order_fields tup =
      let t = Str.split (Str.regexp ",") tup in
      let ts,i,a,v,p =
        (ff t 0, fi t 1, fs t 2, ff t 3, ff t 4)
      in if num_brokers > 0 then
           let broker_id = 
             if deterministic_broker 
             then i mod num_brokers
             else Random.int(num_brokers)
           in (a,i,[CFloat(ts); CInt(i);CInt(broker_id);CFloat(v);CFloat(p)])
         else a,i,[CFloat(ts); CInt(i);CFloat(v);CFloat(p)]
   in
   
   (* helpers for trigger input construction *)
   let v_idx = if num_brokers > 0 then 3 else 2 in
   let p_idx = if num_brokers > 0 then 4 else 3 in
   let getts tuple = List.hd tuple in
   let getoid tuple = List.nth tuple 1 in
   let getbid tuple =
      if num_brokers > 0 then List.nth tuple 2
      else failwith "no broker field found" in
   let getv tuple = List.nth tuple v_idx in
   let getp tuple = List.nth tuple p_idx in

   let insert tuple = (0, AInsert, tuple) in
   let delete tuple = (0, ADelete, tuple) in

   (* Common actions across book types *)
   let adaptor_common action order_id tuple =
      let existing_tuple = if Hashtbl.mem book_orders order_id
         then Some(Hashtbl.find book_orders order_id) else None
      in match (existing_tuple, action) with
       | (None,_) -> []
       
       | (Some(old_tuple), "E") ->
         let subtract x y = 
            Constants.Math.sum x (Constants.Math.neg y) in 
         let new_tuple =
            let nvp =
               [subtract (getv old_tuple) (getv tuple);
                subtract (getp old_tuple) (getp tuple)]
            in [getts old_tuple; getoid old_tuple]@
               (if num_brokers > 0 then [getbid old_tuple] else [])@nvp
         in
         (if (getv new_tuple) = CFloat(0.0)
          then (Hashtbl.remove book_orders order_id;
                if insert_only then [] else [delete old_tuple])
          else (Hashtbl.replace book_orders order_id new_tuple;
                (if insert_only then [] else [delete old_tuple]) @
                [insert new_tuple]))

       | (Some(old_tuple), "F") | (Some(old_tuple), "D") ->
          Hashtbl.remove book_orders order_id;
          (if insert_only then [] else [delete old_tuple])

       | (_,"X") | (_,"C") | (_,"T") -> []
       | (_,_) -> failwith ("invalid orderbook message type "^action)
   in
   let add_to_book order_id tuple =
      Hashtbl.replace book_orders order_id tuple; [insert tuple] 
   in
   match book_type with
    | "bids" -> (fun tuple ->
      let (action, order_id, t) = get_order_fields tuple in
      begin match action with
       | "B" -> add_to_book order_id t
       | "S" -> []
       | _ -> adaptor_common action order_id t
      end)
    
    | "asks" -> (fun tuple ->
      let (action, order_id, t) = get_order_fields tuple in
      begin match action with
       | "B" -> []
       | "S" -> add_to_book order_id t
       | _ -> adaptor_common action order_id t
      end)
    
    | _ -> failwith ("invalid orderbook type: "^book_type)

let generators =
   [("csv", standard_generator);
    ("orderbook", orderbook_generator)]

(* TODO: unit tests for generators *)

let initialize() =
   List.iter (fun (x,y) -> Adaptors.add x y) generators
